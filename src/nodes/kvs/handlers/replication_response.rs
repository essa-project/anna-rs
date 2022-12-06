use crate::{
    lattice::Lattice,
    messages::{
        replication_factor::ReplicationFactor,
        request::ModifyTuple,
        response::{ResponseTuple, ResponseType},
        Response, TcpMessage,
    },
    metadata::MetadataKey,
    nodes::{kvs::KvsNode, send_tcp_message},
    AnnaError, ClientKey, Key, ALL_TIERS,
};
use eyre::{anyhow, bail, Context};
use std::{collections::HashMap, time::Instant};

impl KvsNode {
    /// Handles incoming replication response messages.
    pub async fn replication_response_handler(&mut self, response: Response) -> eyre::Result<()> {
        let work_start = Instant::now();

        if response.error.is_err() {
            bail!("invalid replication response");
        }

        let tuple = match response.tuples.as_slice() {
            [tuple] => tuple,
            other => bail!("expected single response tuple, got `{:?}`", other),
        };

        if tuple.ty != ResponseType::Get {
            bail!("invalid replication response");
        }

        let key = match &tuple.key {
            Key::Metadata(MetadataKey::Replication { key }) => key.clone(),
            other => bail!("expected replication metadata key, got {:?}", other),
        };

        match tuple.error {
            None => {
                let lww_value = tuple
                    .lattice
                    .as_ref()
                    .ok_or_else(|| anyhow!("tuple lattice is None in replication response"))?
                    .as_lww()?;

                let rep_data: ReplicationFactor =
                    rmp_serde::from_slice(lww_value.reveal().value().as_slice())
                        .context("failed to decode replication factor")?;

                for global in &rep_data.global {
                    self.key_replication_map
                        .entry(key.clone())
                        .or_default()
                        .global_replication
                        .insert(global.tier, global.value);
                }

                for local in &rep_data.local {
                    self.key_replication_map
                        .entry(key.clone())
                        .or_default()
                        .local_replication
                        .insert(local.tier, local.value);
                }
            }
            Some(AnnaError::KeyDoesNotExist) => {
                // KEY_DNE means that the receiving thread was responsible for the metadata
                // but didn't have any values stored -- we use the default rep factor
                self.init_replication(key.clone());
            }
            Some(AnnaError::WrongThread) => {
                // this means that the node that received the rep factor request was not
                // responsible for that metadata
                self.issue_replication_factor_request(key.clone()).await?;
                return Ok(());
            }
            error => {
                bail!(
                    "Unexpected error type {:?} in replication factor response.",
                    error
                );
            }
        }

        let key = Key::from(key);
        if self.pending_requests.contains_key(&key) {
            let threads = self
                .hash_ring_util
                .try_get_responsible_threads(
                    self.wt.replication_response_topic(&self.zenoh_prefix),
                    key.clone(),
                    &self.global_hash_rings,
                    &self.local_hash_rings,
                    &self.key_replication_map,
                    &[self.config_data.self_tier],
                    &self.zenoh,
                    &self.zenoh_prefix,
                    &mut self.node_connections,
                )
                .await?;

            if let Some(threads) = threads {
                let responsible = threads.contains(&self.wt);

                for request in self.pending_requests.remove(&key).unwrap_or_default() {
                    let now = Instant::now();

                    if let Some(request_addr) = &request.addr {
                        let mut response = request.new_response();

                        if responsible {
                            let mut tp = ResponseTuple {
                                key: key.clone(),
                                lattice: None,
                                ty: request.operation.response_ty(),
                                error: None,
                                invalidate: false,
                            };

                            match self.key_operation_handler(request.operation) {
                                Ok(lattice) => tp.lattice = lattice,
                                Err(error) => tp.error = Some(error),
                            }
                            response.tuples.push(tp);

                            self.report_data.record_key_access(&key, now);
                        } else {
                            let tp = ResponseTuple {
                                key: key.clone(),
                                lattice: None,
                                ty: request.operation.response_ty(),
                                error: Some(AnnaError::WrongThread),
                                invalidate: false,
                            };
                            response.tuples.push(tp);
                        }
                        if let Some(mut reply_stream) = request.reply_stream {
                            send_tcp_message(&TcpMessage::Response(response), &mut reply_stream)
                                .await
                                .context("failed to send reply via TCP")?;
                        } else {
                            let serialized_response = rmp_serde::to_vec(&response)
                                .context("failed to serialize key response")?;
                            self.zenoh
                                .put(request_addr, serialized_response)
                                .await
                                .map_err(|e| eyre::eyre!(e))?;
                        }
                    } else if responsible {
                        // only put requests should fall into this category
                        if request.operation.response_ty() == ResponseType::Put {
                            let _ = self.key_operation_handler(request.operation);
                            self.report_data.record_key_access(&key, now);
                        } else {
                            log::error!("Received a GET request with no response address.");
                        }
                    }
                }
            } else {
                log::error!("Missing key replication factor in process pending request routine.");
            }

            self.pending_requests.remove(&key);
        }

        if self.pending_gossip.contains_key(&key) {
            let threads = self
                .hash_ring_util
                .try_get_responsible_threads(
                    self.wt.replication_response_topic(&self.zenoh_prefix),
                    key.clone(),
                    &self.global_hash_rings,
                    &self.local_hash_rings,
                    &self.key_replication_map,
                    &[self.config_data.self_tier],
                    &self.zenoh,
                    &self.zenoh_prefix,
                    &mut self.node_connections,
                )
                .await?;

            if let Some(threads) = threads {
                if threads.contains(&self.wt) {
                    for gossip in self.pending_gossip.remove(&key).unwrap_or_default() {
                        self.kvs.put(key.clone(), gossip.lattice_value.clone())?;
                    }
                } else {
                    let mut gossip_map: HashMap<String, Vec<ModifyTuple>> = HashMap::new();

                    // forward the gossip
                    for thread in &threads {
                        let entry = gossip_map
                            .entry(thread.gossip_topic(&self.zenoh_prefix))
                            .or_default();

                        for gossip in self.pending_gossip.remove(&key).unwrap_or_default() {
                            let tp = ModifyTuple {
                                key: key.clone(),
                                value: gossip.lattice_value,
                            };
                            entry.push(tp);
                        }
                    }

                    // redirect gossip
                    self.redirect_gossip(gossip_map).await?;
                }
            } else {
                log::error!("Missing key replication factor in process pending gossip routine.");
            }

            self.pending_gossip.remove(&key);
        }

        let time_elapsed = Instant::now() - work_start;
        self.report_data.record_working_time(time_elapsed, 5);

        Ok(())
    }

    fn init_replication(&mut self, key: ClientKey) {
        let entry = self.key_replication_map.entry(key).or_default();
        for &tier in ALL_TIERS {
            entry.global_replication.insert(
                tier,
                self.config_data.tier_metadata[&tier].default_replication,
            );
            entry
                .local_replication
                .insert(tier, self.config_data.default_local_replication);
        }
    }
}
