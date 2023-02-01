use crate::{
    messages::{response::ResponseTuple, Request, TcpMessage},
    nodes::{
        kvs::{KvsNode, MixKeyOperation, PendingRequest},
        send_tcp_message,
    },
    AnnaError, Key,
};

use eyre::Context;
use smol::net::TcpStream;
use std::time::Instant;

impl KvsNode {
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

        let timestamp = request.timestamp;

        let iter = request
            .client_operations
            .into_iter()
            .map(MixKeyOperation::Client)
            .chain(
                request
                    .inner_operations
                    .into_iter()
                    .map(MixKeyOperation::Inner),
            );

        for tuple in iter {
            // first check if the thread is responsible for the key
            let key = tuple.key().clone();

            let threads = self.try_get_responsible_threads(key.clone(), None).await?;

            if let Some(threads) = threads {
                if !threads.contains(&self.wt) {
                    match key {
                        Key::Metadata(key) => {
                            // this means that this node is not responsible for this metadata key
                            let tp = ResponseTuple {
                                key: key.into(),
                                lattice: None,
                                raw_lattice: None,
                                metadata: None,
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
                                    timestamp,
                                },
                            );
                        }
                    }
                } else {
                    // if we know the responsible threads, we process the request
                    let mut tp = ResponseTuple {
                        key: key.clone(),
                        lattice: None,
                        raw_lattice: None,
                        metadata: None,
                        ty: tuple.response_ty(),
                        error: None,
                        invalidate: false,
                    };

                    match tuple {
                        MixKeyOperation::Inner(operation) => {
                            match self.inner_key_operation_handler(operation, timestamp) {
                                Ok((raw_lattice, metadata)) => {
                                    tp.raw_lattice = raw_lattice;
                                    tp.metadata = metadata;
                                }

                                Err(err) => tp.error = Some(err),
                            }
                        }
                        MixKeyOperation::Client(operation) => {
                            match self.key_operation_handler(operation, timestamp) {
                                Ok(value) => {
                                    tp.lattice = value;
                                }
                                Err(err) => tp.error = Some(err),
                            }
                        }
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
                        timestamp,
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
                    let serialized_response = rmp_serde::to_vec_named(&response)
                        .context("failed to serialize key response")?;
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
    use chrono::Utc;
    use zenoh::prelude::{Receiver, SplitBuffer, ZFuture};

    use crate::{
        lattice::{
            causal::{SingleKeyCausalLattice, VectorClock, VectorClockValuePair},
            last_writer_wins::Timestamp,
            LastWriterWinsLattice, Lattice, MaxLattice, OrderedSetLattice, SetLattice,
        },
        messages::{request::ClientKeyOperation, response::ClientResponseValue, Request, Response},
        nodes::kvs::kvs_test_instance,
        store::LatticeValue,
        topics::ClientThread,
        zenoh_test_instance, ClientKey,
    };

    use std::{
        collections::{BTreeSet, HashMap, HashSet},
        time::Duration,
    };

    fn get_key_request(
        key: ClientKey,
        node_id: String,
        request_id: String,
        zenoh_prefix: &str,
    ) -> Request {
        Request {
            inner_operations: vec![],
            client_operations: vec![ClientKeyOperation::Get(key.into())],
            response_address: Some(
                ClientThread::new(node_id, 0)
                    .response_topic(zenoh_prefix)
                    .to_string(),
            ),
            request_id: Some(request_id),
            address_cache_size: Default::default(),
            timestamp: chrono::Utc::now(),
        }
    }

    fn add_map_request(
        key: ClientKey,
        map: HashMap<String, Vec<u8>>,
        node_id: String,
        request_id: String,
        zenoh_prefix: &str,
    ) -> Request {
        Request {
            inner_operations: vec![],
            client_operations: vec![ClientKeyOperation::MapAdd(key, map)],
            response_address: Some(
                ClientThread::new(node_id, 0)
                    .response_topic(zenoh_prefix)
                    .to_string(),
            ),
            request_id: Some(request_id),
            address_cache_size: Default::default(),
            timestamp: chrono::Utc::now(),
        }
    }

    fn add_set_request(
        key: ClientKey,
        set: HashSet<Vec<u8>>,
        node_id: String,
        request_id: String,
        zenoh_prefix: &str,
    ) -> Request {
        Request {
            inner_operations: vec![],
            client_operations: vec![ClientKeyOperation::SetAdd(key, set)],
            response_address: Some(
                ClientThread::new(node_id, 0)
                    .response_topic(zenoh_prefix)
                    .to_string(),
            ),
            request_id: Some(request_id),
            address_cache_size: Default::default(),
            timestamp: chrono::Utc::now(),
        }
    }

    fn put_lww_request(
        key: ClientKey,
        lww_value: Vec<u8>,
        node_id: String,
        request_id: String,
        zenoh_prefix: &str,
    ) -> Request {
        Request {
            inner_operations: vec![],
            client_operations: vec![ClientKeyOperation::Put(key, lww_value)],
            response_address: Some(
                ClientThread::new(node_id, 0)
                    .response_topic(zenoh_prefix)
                    .to_string(),
            ),
            request_id: Some(request_id),
            address_cache_size: Default::default(),
            timestamp: chrono::Utc::now(),
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
        assert_eq!(rtp.lattice.as_ref().unwrap(), &lattice.into());
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
            &ClientResponseValue::Set(set_lattice.reveal().clone())
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
            rtp.lattice.as_ref().unwrap(),
            &ClientResponseValue::OrderedSet(lattice.reveal().clone())
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

        let left_value = rtp.lattice.as_ref().unwrap();

        assert_eq!(left_value, &LatticeValue::SingleCausal(lattice).into());
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
        let now = Utc::now();
        let mut put_request = put_lww_request(
            key.clone(),
            value.clone(),
            server.node_id.clone(),
            request_id.to_owned(),
            &zenoh_prefix,
        );
        put_request.timestamp = now;

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
            rtp.lattice.as_ref().unwrap(),
            &ClientResponseValue::Bytes(value)
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

        let mut server = kvs_test_instance(zenoh.clone(), zenoh_prefix.clone());
        server.key_replication_map.entry(key.clone()).or_default();

        let request_id = "user_put_and_get_set_test_put_request_id";
        let put_request = add_set_request(
            key.clone(),
            s.clone(),
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
        assert_eq!(rtp.lattice.as_ref().unwrap(), &ClientResponseValue::Set(s));
        assert_eq!(rtp.error, None);

        assert_eq!(server.local_changeset.len(), 1);
        assert_eq!(server.report_data.access_count(), 2);
        assert_eq!(server.report_data.key_access_count(&key.into()), 2);
    }

    #[test]
    fn user_put_and_get_hashmap_test() {
        let zenoh = zenoh_test_instance();
        let zenoh_prefix = uuid::Uuid::new_v4().to_string();
        let mut subscriber = zenoh
            .subscribe(format!("{}/**", zenoh_prefix))
            .wait()
            .unwrap();

        let key: ClientKey = "key".into();
        let mut map = HashMap::new();
        map.insert("key1".to_string(), "value2".as_bytes().to_owned());
        map.insert("key2".to_string(), "value1".as_bytes().to_owned());
        map.insert("key3".to_string(), "value3".as_bytes().to_owned());

        let mut server = kvs_test_instance(zenoh.clone(), zenoh_prefix.clone());
        server.key_replication_map.entry(key.clone()).or_default();

        let request_id = "user_put_and_get_ordered_set_test_put_request_id";
        let put_request = add_map_request(
            key.clone(),
            map.clone(),
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
            rtp.lattice.as_ref().unwrap(),
            &ClientResponseValue::Map(map)
        );
        assert_eq!(rtp.error, None);

        assert_eq!(server.local_changeset.len(), 1);
        assert_eq!(server.report_data.access_count(), 2);
        assert_eq!(server.report_data.key_access_count(&key.into()), 2);
    }

    // TODO: Test key address cache invalidation
    // TODO: Test replication factor request and making the request pending
    // TODO: Test metadata operations -- does this matter?
}
