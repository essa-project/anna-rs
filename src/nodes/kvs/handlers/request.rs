use crate::{
    messages::{request::KeyOperation, response::ResponseTuple, Request, TcpMessage},
    nodes::{
        kvs::{KvsNode, PendingRequest},
        send_tcp_message,
    },
    AnnaError, Key,
};
use anna_api::{
    lattice::{causal::SingleKeyCausalLattice, MaxLattice},
    LatticeValue,
};
use eyre::Context;
use smol::net::TcpStream;
use std::time::Instant;

impl KvsNode {
    pub(crate) fn key_operation_handler(
        &mut self,
        operation: KeyOperation,
    ) -> Result<Option<LatticeValue>, AnnaError> {
        use anna_api::lattice::Lattice;
        use std::collections::hash_map::Entry;

        match operation {
            KeyOperation::Get(key) => match self.kvs.get(&key) {
                Some(value) => Ok(Some(value.clone())),
                None => Err(AnnaError::KeyDoesNotExist),
            },

            KeyOperation::Put(tuple) => {
                let key = tuple.key.clone();
                self.kvs.put(key.into(), tuple.value.clone())?;
                self.local_changeset.insert(tuple.key);
                Ok(Some(tuple.value))
            }
            KeyOperation::SetAdd(key, value) => {
                match self.kvs.entry(key.clone()) {
                    Entry::Vacant(entry) => {
                        entry.insert(LatticeValue::SingleCausal(
                            SingleKeyCausalLattice::create_with_default_clock(value),
                        ));
                    }
                    Entry::Occupied(mut entry) => {
                        if let LatticeValue::SingleCausal(set) = entry.get_mut() {
                            set.reveal_mut().value.merge(&value);
                            set.reveal_mut().vector_clock.insert(
                                format!("{}/{}", self.node_id, self.thread_id),
                                MaxLattice::new(self.gossip_epoch),
                            )
                        } else {
                            return Err(AnnaError::Lattice);
                        }
                    }
                }

                self.local_changeset.insert(key);
                Ok(None)
            }
            KeyOperation::MapAdd(key, value) => {
                match self.kvs.entry(key.clone()) {
                    Entry::Vacant(entry) => {
                        entry.insert(LatticeValue::SingleCausalMap(
                            SingleKeyCausalLattice::create_with_default_clock(value),
                        ));
                    }
                    Entry::Occupied(mut entry) => {
                        if let LatticeValue::SingleCausalMap(set) = entry.get_mut() {
                            set.reveal_mut().value.merge(&value);
                            set.reveal_mut().vector_clock.insert(
                                format!("{}/{}", self.node_id, self.thread_id),
                                MaxLattice::new(self.gossip_epoch),
                            )
                        } else {
                            return Err(AnnaError::Lattice);
                        }
                    }
                }

                self.local_changeset.insert(key);
                Ok(None)
            }
        }
    }

    /// Handles incoming request messages.
    pub async fn request_handler(
        &mut self,
        request: Request,
        reply_stream: Option<TcpStream>,
    ) -> eyre::Result<()> {
        let work_start = Instant::now();

        let mut response = request.new_response();

        let response_addr = request.response_address;

        let response_id = request.request_id;

        for tuple in request.request.operations {
            // first check if the thread is responsible for the key
            let key = tuple.key().clone();

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
                if !threads.contains(&self.wt) {
                    match key {
                        Key::Metadata(key) => {
                            // this means that this node is not responsible for this metadata key
                            let tp = ResponseTuple {
                                key: key.into(),
                                lattice: None,
                                ty: tuple.response_ty(),
                                error: Some(AnnaError::WrongThread),
                                invalidate: Default::default(),
                            };

                            response.tuples.push(tp);
                        }
                        Key::Client(key) => {
                            // if we don't know what threads are responsible, we issue a rep
                            // factor request and make the request pending
                            self.issue_replication_factor_request(key.clone()).await?;

                            self.pending_requests.entry(key.into()).or_default().push(
                                PendingRequest {
                                    operation: tuple,
                                    addr: response_addr.clone(),
                                    response_id: response_id.clone(),
                                    reply_stream: reply_stream.clone(),
                                },
                            );
                        }
                    }
                } else {
                    // if we know the responsible threads, we process the request
                    let mut tp = ResponseTuple {
                        key: key.clone(),
                        lattice: None,
                        ty: tuple.response_ty(),
                        error: None,
                        invalidate: false,
                    };

                    match self.key_operation_handler(tuple) {
                        Ok(value) => tp.lattice = value,
                        Err(err) => tp.error = Some(err),
                    }

                    if let Key::Client(key) = &key {
                        if let Some(&address_cache_size) = request.address_cache_size.get(key) {
                            if address_cache_size != threads.len() {
                                tp.invalidate = true;
                            }
                        }
                    }

                    response.tuples.push(tp);

                    self.report_data.record_key_access(&key, Instant::now());
                }
            } else {
                self.pending_requests
                    .entry(key.clone())
                    .or_default()
                    .push(PendingRequest {
                        operation: tuple,
                        addr: response_addr.clone(),
                        response_id: response_id.clone(),
                        reply_stream: reply_stream.clone(),
                    });
            }
        }

        let time_elapsed = Instant::now() - work_start;
        self.report_data.record_working_time(time_elapsed, 3);

        if let Some(response_addr) = response_addr {
            if !response.tuples.is_empty() {
                if let Some(mut reply_stream) = reply_stream {
                    send_tcp_message(&TcpMessage::Response(response), &mut reply_stream)
                        .await
                        .context("failed to send reply via TCP")?;
                } else {
                    let serialized_response =
                        rmp_serde::to_vec(&response).context("failed to serialize key response")?;
                    self.zenoh
                        .put(&response_addr, serialized_response)
                        .await
                        .map_err(|e| eyre::eyre!(e))?;
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use zenoh::prelude::{Receiver, SplitBuffer, ZFuture};

    use crate::{
        lattice::{
            causal::{SingleKeyCausalLattice, VectorClock, VectorClockValuePair},
            last_writer_wins::Timestamp,
            LastWriterWinsLattice, Lattice, MaxLattice, OrderedSetLattice, SetLattice,
        },
        messages::{
            request::{KeyOperation, ModifyTuple, RequestData},
            Request, Response,
        },
        nodes::kvs::kvs_test_instance,
        store::LatticeValue,
        topics::ClientThread,
        zenoh_test_instance, ClientKey,
    };

    use std::{
        collections::{BTreeSet, HashSet},
        time::Duration,
    };

    fn get_key_request(
        key: ClientKey,
        node_id: String,
        request_id: String,
        zenoh_prefix: &str,
    ) -> Request {
        Request {
            request: RequestData {
                operations: vec![KeyOperation::Get(key.into())],
            },
            response_address: Some(
                ClientThread::new(node_id, 0)
                    .response_topic(zenoh_prefix)
                    .to_string(),
            ),
            request_id: Some(request_id),
            address_cache_size: Default::default(),
        }
    }

    fn put_key_request(
        key: ClientKey,
        lattice_value: LatticeValue,
        node_id: String,
        request_id: String,
        zenoh_prefix: &str,
    ) -> Request {
        Request {
            request: RequestData {
                operations: vec![KeyOperation::Put(ModifyTuple {
                    key: key.into(),
                    value: lattice_value,
                })],
            },
            response_address: Some(
                ClientThread::new(node_id, 0)
                    .response_topic(zenoh_prefix)
                    .to_string(),
            ),
            request_id: Some(request_id),
            address_cache_size: Default::default(),
        }
    }

    #[test]
    fn user_get_lww_test() {
        let zenoh = zenoh_test_instance();
        let zenoh_prefix = uuid::Uuid::new_v4().to_string();
        let mut subscriber = zenoh
            .subscribe(format!("{}/**", zenoh_prefix))
            .wait()
            .unwrap();

        let mut server = kvs_test_instance(zenoh.clone(), zenoh_prefix.clone());

        let key: ClientKey = "key".into();
        let value = "value".as_bytes().to_owned();
        let lattice = LatticeValue::Lww(LastWriterWinsLattice::from_pair(Timestamp::now(), value));
        server.kvs.put(key.clone().into(), lattice.clone()).unwrap();
        server.key_replication_map.entry(key.clone()).or_default();

        let request_id = "user_get_lww_test_request_id";
        let get_request = get_key_request(
            key.clone(),
            server.node_id.clone(),
            request_id.to_owned(),
            &zenoh_prefix,
        );

        assert_eq!(server.local_changeset.len(), 0);

        smol::block_on(server.request_handler(get_request, None)).unwrap();

        let message = subscriber
            .receiver()
            .recv_timeout(Duration::from_secs(5))
            .unwrap();

        let response: Response =
            rmp_serde::from_slice(&message.value.payload.contiguous()).unwrap();

        assert_eq!(response.response_id.as_deref(), Some(request_id));
        assert_eq!(response.tuples.len(), 1);

        let rtp = &response.tuples[0];

        assert_eq!(rtp.key, key.clone().into());
        assert_eq!(rtp.lattice.as_ref().unwrap(), &lattice);
        assert_eq!(rtp.error, None);

        assert_eq!(server.local_changeset.len(), 0);
        assert_eq!(server.report_data.access_count(), 1);
        assert_eq!(server.report_data.key_access_count(&key.into()), 1);
    }

    #[test]
    fn user_get_set_test() {
        let zenoh = zenoh_test_instance();
        let zenoh_prefix = uuid::Uuid::new_v4().to_string();
        let mut subscriber = zenoh
            .subscribe(format!("{}/**", zenoh_prefix))
            .wait()
            .unwrap();

        let mut server = kvs_test_instance(zenoh.clone(), zenoh_prefix.clone());

        let key: ClientKey = "key".into();
        let mut s = HashSet::new();
        s.insert("value1".as_bytes().to_owned());
        s.insert("value2".as_bytes().to_owned());
        s.insert("value3".as_bytes().to_owned());
        let set_lattice = SetLattice::new(s);
        server
            .kvs
            .put(key.clone().into(), LatticeValue::Set(set_lattice.clone()))
            .unwrap();
        server.key_replication_map.entry(key.clone()).or_default();

        let request_id = "user_get_set_test_request_id";
        let get_request = get_key_request(
            key.clone(),
            server.node_id.clone(),
            request_id.to_owned(),
            &zenoh_prefix,
        );

        assert_eq!(server.local_changeset.len(), 0);

        smol::block_on(server.request_handler(get_request, None)).unwrap();

        let message = subscriber
            .receiver()
            .recv_timeout(Duration::from_secs(5))
            .unwrap();

        let response: Response =
            rmp_serde::from_slice(&message.value.payload.contiguous()).unwrap();

        assert_eq!(response.response_id.as_deref(), Some(request_id));
        assert_eq!(response.tuples.len(), 1);

        let rtp = &response.tuples[0];

        assert_eq!(rtp.key, key.clone().into());
        assert_eq!(
            rtp.lattice.as_ref().unwrap(),
            &LatticeValue::Set(set_lattice)
        );
        assert_eq!(rtp.error, None);

        assert_eq!(server.local_changeset.len(), 0);
        assert_eq!(server.report_data.access_count(), 1);
        assert_eq!(server.report_data.key_access_count(&key.into()), 1);
    }

    #[test]
    fn user_get_ordered_set_test() {
        let zenoh = zenoh_test_instance();
        let zenoh_prefix = uuid::Uuid::new_v4().to_string();
        let mut subscriber = zenoh
            .subscribe(format!("{}/**", zenoh_prefix))
            .wait()
            .unwrap();

        let mut server = kvs_test_instance(zenoh.clone(), zenoh_prefix.clone());

        let key: ClientKey = "key".into();
        let mut s = BTreeSet::new();
        s.insert("value1".as_bytes().to_owned());
        s.insert("value2".as_bytes().to_owned());
        s.insert("value3".as_bytes().to_owned());
        let lattice = OrderedSetLattice::new(s);

        server
            .kvs
            .put(
                key.clone().into(),
                LatticeValue::OrderedSet(lattice.clone()),
            )
            .unwrap();
        server.key_replication_map.entry(key.clone()).or_default();

        let request_id = "user_get_ordered_set_test_request_id";
        let get_request = get_key_request(
            key.clone(),
            server.node_id.clone(),
            request_id.to_owned(),
            &zenoh_prefix,
        );

        assert_eq!(server.local_changeset.len(), 0);

        smol::block_on(server.request_handler(get_request, None)).unwrap();

        let message = subscriber
            .receiver()
            .recv_timeout(Duration::from_secs(10))
            .unwrap();

        let response: Response =
            rmp_serde::from_slice(&message.value.payload.contiguous()).unwrap();

        assert_eq!(response.response_id.as_deref(), Some(request_id));
        assert_eq!(response.tuples.len(), 1);

        let rtp = &response.tuples[0];

        assert_eq!(rtp.key, key.clone().into());
        assert_eq!(
            rtp.lattice.as_ref().unwrap().as_ordered_set().unwrap(),
            &lattice
        );
        assert_eq!(rtp.error, None);

        assert_eq!(server.local_changeset.len(), 0);
        assert_eq!(server.report_data.access_count(), 1);
        assert_eq!(server.report_data.key_access_count(&key.into()), 1);
    }

    #[test]
    fn user_get_causal_test() {
        let zenoh = zenoh_test_instance();
        let zenoh_prefix = uuid::Uuid::new_v4().to_string();
        let mut subscriber = zenoh
            .subscribe(format!("{}/**", zenoh_prefix))
            .wait()
            .unwrap();

        let key: ClientKey = "key".into();
        let p = {
            let mut vector_clock = VectorClock::default();
            vector_clock.insert("1".into(), MaxLattice::new(1));
            vector_clock.insert("2".into(), MaxLattice::new(1));
            let mut value = SetLattice::default();
            value.insert("value1".as_bytes().to_owned());
            value.insert("value2".as_bytes().to_owned());
            value.insert("value3".as_bytes().to_owned());
            VectorClockValuePair::new(vector_clock, value)
        };
        let lattice = SingleKeyCausalLattice::new(p);

        let mut server = kvs_test_instance(zenoh.clone(), zenoh_prefix.clone());

        server
            .kvs
            .put(
                key.clone().into(),
                LatticeValue::SingleCausal(lattice.clone()),
            )
            .unwrap();
        server.key_replication_map.entry(key.clone()).or_default();

        let request_id = "user_get_causal_test_request_id";
        let get_request = get_key_request(
            key.clone(),
            server.node_id.clone(),
            request_id.to_owned(),
            &zenoh_prefix,
        );

        assert_eq!(server.local_changeset.len(), 0);

        smol::block_on(server.request_handler(get_request, None)).unwrap();

        let message = subscriber
            .receiver()
            .recv_timeout(Duration::from_secs(10))
            .unwrap();
        let response: Response =
            rmp_serde::from_slice(&message.value.payload.contiguous()).unwrap();

        assert_eq!(response.response_id.as_deref(), Some(request_id));
        assert_eq!(response.tuples.len(), 1);

        let rtp = &response.tuples[0];

        assert_eq!(rtp.key, key.clone().into());

        let left_value = rtp.lattice.as_ref().unwrap().as_single_causal().unwrap();

        assert_eq!(left_value.reveal(), lattice.reveal());
        assert_eq!(rtp.error, None);

        assert_eq!(server.local_changeset.len(), 0);
        assert_eq!(server.report_data.access_count(), 1);
        assert_eq!(server.report_data.key_access_count(&key.into()), 1);
    }

    #[test]
    fn user_put_and_get_lww_test() {
        let zenoh = zenoh_test_instance();
        let zenoh_prefix = uuid::Uuid::new_v4().to_string();
        let mut subscriber = zenoh
            .subscribe(format!("{}/**", zenoh_prefix))
            .wait()
            .unwrap();

        let key: ClientKey = "key".into();
        let value = "value".as_bytes().to_owned();

        let mut server = kvs_test_instance(zenoh.clone(), zenoh_prefix.clone());
        server.key_replication_map.entry(key.clone()).or_default();

        let request_id = "user_put_and_get_lww_test_put_request_id";
        let lattice_value = LastWriterWinsLattice::from_pair(Timestamp::now(), value);
        let put_request = put_key_request(
            key.clone(),
            lattice_value.clone().into(),
            server.node_id.clone(),
            request_id.to_owned(),
            &zenoh_prefix,
        );

        assert_eq!(server.local_changeset.len(), 0);

        smol::block_on(server.request_handler(put_request, None)).unwrap();

        let message = subscriber
            .receiver()
            .recv_timeout(Duration::from_secs(10))
            .unwrap();
        let response: Response =
            rmp_serde::from_slice(&message.value.payload.contiguous()).unwrap();

        assert_eq!(response.response_id.as_deref(), Some(request_id));
        assert_eq!(response.tuples.len(), 1);

        let rtp = &response.tuples[0];

        assert_eq!(rtp.key, key.clone().into());
        assert_eq!(rtp.error, None);

        assert_eq!(server.local_changeset.len(), 1);
        assert_eq!(server.report_data.access_count(), 1);
        assert_eq!(server.report_data.key_access_count(&key.clone().into()), 1);

        let request_id = "user_put_and_get_lww_test_get_request_id";
        let get_request = get_key_request(
            key.clone(),
            server.node_id.clone(),
            request_id.to_owned(),
            &zenoh_prefix,
        );

        smol::block_on(server.request_handler(get_request, None)).unwrap();

        let message = subscriber
            .receiver()
            .recv_timeout(Duration::from_secs(10))
            .unwrap();
        let response: Response =
            rmp_serde::from_slice(&message.value.payload.contiguous()).unwrap();

        assert_eq!(response.response_id.as_deref(), Some(request_id));
        assert_eq!(response.tuples.len(), 1);

        let rtp = &response.tuples[0];

        assert_eq!(rtp.key, key.clone().into());
        assert_eq!(
            rtp.lattice.as_ref().unwrap().as_lww().unwrap(),
            &lattice_value
        );
        assert_eq!(rtp.error, None);

        assert_eq!(server.local_changeset.len(), 1);
        assert_eq!(server.report_data.access_count(), 2);
        assert_eq!(server.report_data.key_access_count(&key.into()), 2);
    }

    #[test]
    fn user_put_and_get_set_test() {
        let zenoh = zenoh_test_instance();
        let zenoh_prefix = uuid::Uuid::new_v4().to_string();
        let mut subscriber = zenoh
            .subscribe(format!("{}/**", zenoh_prefix))
            .wait()
            .unwrap();

        let key: ClientKey = "key".into();
        let mut s = HashSet::new();
        s.insert("value1".as_bytes().to_owned());
        s.insert("value2".as_bytes().to_owned());
        s.insert("value3".as_bytes().to_owned());
        let lattice = SetLattice::new(s.clone());

        let mut server = kvs_test_instance(zenoh.clone(), zenoh_prefix.clone());
        server.key_replication_map.entry(key.clone()).or_default();

        let request_id = "user_put_and_get_set_test_put_request_id";
        let put_request = put_key_request(
            key.clone(),
            LatticeValue::Set(lattice),
            server.node_id.clone(),
            request_id.to_owned(),
            &zenoh_prefix,
        );

        assert_eq!(server.local_changeset.len(), 0);

        smol::block_on(server.request_handler(put_request, None)).unwrap();

        let message = subscriber
            .receiver()
            .recv_timeout(Duration::from_secs(10))
            .unwrap();
        let response: Response =
            rmp_serde::from_slice(&message.value.payload.contiguous()).unwrap();

        assert_eq!(response.response_id.as_deref(), Some(request_id));
        assert_eq!(response.tuples.len(), 1);

        let rtp = &response.tuples[0];

        assert_eq!(rtp.key, key.clone().into());
        assert_eq!(rtp.error, None);

        assert_eq!(server.local_changeset.len(), 1);
        assert_eq!(server.report_data.access_count(), 1);
        assert_eq!(server.report_data.key_access_count(&key.clone().into()), 1);

        let request_id = "user_put_and_get_set_test_get_request_id";
        let get_request = get_key_request(
            key.clone(),
            server.node_id.clone(),
            request_id.to_owned(),
            &zenoh_prefix,
        );

        smol::block_on(server.request_handler(get_request, None)).unwrap();

        let message = subscriber
            .receiver()
            .recv_timeout(Duration::from_secs(10))
            .unwrap();
        let response: Response =
            rmp_serde::from_slice(&message.value.payload.contiguous()).unwrap();

        assert_eq!(response.response_id.as_deref(), Some(request_id));
        assert_eq!(response.tuples.len(), 1);

        let rtp = &response.tuples[0];

        assert_eq!(rtp.key, key.clone().into());
        assert_eq!(rtp.lattice.as_ref().unwrap().as_set().unwrap().reveal(), &s);
        assert_eq!(rtp.error, None);

        assert_eq!(server.local_changeset.len(), 1);
        assert_eq!(server.report_data.access_count(), 2);
        assert_eq!(server.report_data.key_access_count(&key.into()), 2);
    }

    #[test]
    fn user_put_and_get_ordered_set_test() {
        let zenoh = zenoh_test_instance();
        let zenoh_prefix = uuid::Uuid::new_v4().to_string();
        let mut subscriber = zenoh
            .subscribe(format!("{}/**", zenoh_prefix))
            .wait()
            .unwrap();

        let key: ClientKey = "key".into();
        let mut s = BTreeSet::new();
        s.insert("value2".as_bytes().to_owned());
        s.insert("value1".as_bytes().to_owned());
        s.insert("value3".as_bytes().to_owned());
        let lattice = OrderedSetLattice::new(s.clone());

        let mut server = kvs_test_instance(zenoh.clone(), zenoh_prefix.clone());
        server.key_replication_map.entry(key.clone()).or_default();

        let request_id = "user_put_and_get_ordered_set_test_put_request_id";
        let put_request = put_key_request(
            key.clone(),
            LatticeValue::OrderedSet(lattice),
            server.node_id.clone(),
            request_id.to_owned(),
            &zenoh_prefix,
        );

        assert_eq!(server.local_changeset.len(), 0);

        smol::block_on(server.request_handler(put_request, None)).unwrap();

        let message = subscriber
            .receiver()
            .recv_timeout(Duration::from_secs(10))
            .unwrap();
        let response: Response =
            rmp_serde::from_slice(&message.value.payload.contiguous()).unwrap();

        assert_eq!(response.response_id.as_deref(), Some(request_id));
        assert_eq!(response.tuples.len(), 1);

        let rtp = &response.tuples[0];

        assert_eq!(rtp.key, key.clone().into());
        assert_eq!(rtp.error, None);

        assert_eq!(server.local_changeset.len(), 1);
        assert_eq!(server.report_data.access_count(), 1);
        assert_eq!(server.report_data.key_access_count(&key.clone().into()), 1);

        let request_id = "user_put_and_get_ordered_set_test_get_request_id";
        let get_request = get_key_request(
            key.clone(),
            server.node_id.clone(),
            request_id.to_owned(),
            &zenoh_prefix,
        );

        smol::block_on(server.request_handler(get_request, None)).unwrap();

        let message = subscriber
            .receiver()
            .recv_timeout(Duration::from_secs(10))
            .unwrap();
        let response: Response =
            rmp_serde::from_slice(&message.value.payload.contiguous()).unwrap();

        assert_eq!(response.response_id.as_deref(), Some(request_id));
        assert_eq!(response.tuples.len(), 1);

        let rtp = &response.tuples[0];

        assert_eq!(rtp.key, key.clone().into());
        assert_eq!(
            rtp.lattice
                .as_ref()
                .unwrap()
                .as_ordered_set()
                .unwrap()
                .reveal(),
            &s
        );
        assert_eq!(rtp.error, None);

        assert_eq!(server.local_changeset.len(), 1);
        assert_eq!(server.report_data.access_count(), 2);
        assert_eq!(server.report_data.key_access_count(&key.into()), 2);
    }

    #[test]
    fn user_put_and_get_causal_test() {
        let zenoh = zenoh_test_instance();
        let zenoh_prefix = uuid::Uuid::new_v4().to_string();
        let mut subscriber = zenoh
            .subscribe(format!("{}/**", zenoh_prefix))
            .wait()
            .unwrap();

        let key: ClientKey = "key".into();
        let p = {
            let mut vector_clock = VectorClock::default();
            vector_clock.insert("1".into(), MaxLattice::new(1));
            vector_clock.insert("2".into(), MaxLattice::new(1));
            let mut value = SetLattice::default();
            value.insert("value1".as_bytes().to_owned());
            value.insert("value2".as_bytes().to_owned());
            value.insert("value3".as_bytes().to_owned());
            VectorClockValuePair::new(vector_clock, value)
        };
        let lattice = SingleKeyCausalLattice::new(p);

        let mut server = kvs_test_instance(zenoh.clone(), zenoh_prefix.clone());
        server.key_replication_map.entry(key.clone()).or_default();

        let request_id = "user_put_and_get_causal_test_put_request_id";
        let put_request = put_key_request(
            key.clone(),
            LatticeValue::SingleCausal(lattice.clone()),
            server.node_id.clone(),
            request_id.to_owned(),
            &zenoh_prefix,
        );

        assert_eq!(server.local_changeset.len(), 0);

        smol::block_on(server.request_handler(put_request, None)).unwrap();

        let message = subscriber
            .receiver()
            .recv_timeout(Duration::from_secs(10))
            .unwrap();
        let response: Response =
            rmp_serde::from_slice(&message.value.payload.contiguous()).unwrap();

        assert_eq!(response.response_id.as_deref(), Some(request_id));
        assert_eq!(response.tuples.len(), 1);

        let rtp = &response.tuples[0];

        assert_eq!(rtp.key, key.clone().into());
        assert_eq!(rtp.error, None);

        assert_eq!(server.local_changeset.len(), 1);
        assert_eq!(server.report_data.access_count(), 1);
        assert_eq!(server.report_data.key_access_count(&key.clone().into()), 1);

        let request_id = "user_put_and_get_causal_test_get_request_id";
        let get_request = get_key_request(
            key.clone(),
            server.node_id.clone(),
            request_id.to_owned(),
            &zenoh_prefix,
        );

        smol::block_on(server.request_handler(get_request, None)).unwrap();

        let message = subscriber
            .receiver()
            .recv_timeout(Duration::from_secs(10))
            .unwrap();
        let response: Response =
            rmp_serde::from_slice(&message.value.payload.contiguous()).unwrap();

        assert_eq!(response.response_id.as_deref(), Some(request_id));
        assert_eq!(response.tuples.len(), 1);

        let rtp = &response.tuples[0];

        assert_eq!(rtp.key, key.clone().into());

        let left_value = rtp.lattice.as_ref().unwrap().as_single_causal().unwrap();

        assert_eq!(left_value.reveal(), lattice.reveal());
        assert_eq!(rtp.error, None);

        assert_eq!(server.local_changeset.len(), 1);
        assert_eq!(server.report_data.access_count(), 2);
        assert_eq!(server.report_data.key_access_count(&key.into()), 2);
    }

    // TODO: Test key address cache invalidation
    // TODO: Test replication factor request and making the request pending
    // TODO: Test metadata operations -- does this matter?
}
