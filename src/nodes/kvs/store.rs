use crate::{
    lattice::{
        causal::SingleKeyCausalLattice,
        last_writer_wins::{Timestamp, TimestampValuePair},
        CounterLattice, LastWriterWinsLattice, Lattice, MapLattice, MaxLattice, SetLattice,
    },
    messages::{request::KeyOperation, response::ClientResponseValue},
    store::LatticeValue,
    AnnaError, Key,
};

use super::KvsNode;

impl KvsNode {
    pub(crate) fn key_operation_handler(
        &mut self,
        operation: KeyOperation,
        timestamp: chrono::DateTime<chrono::Utc>,
    ) -> Result<(Option<ClientResponseValue>, Option<Vec<u8>>), AnnaError> {
        use std::collections::hash_map::Entry;

        match operation {
            KeyOperation::Get(key) => match self.kvs.get(&key.into()) {
                Some(value) => Ok((Some(value.clone().into()), None)),
                None => Err(AnnaError::KeyDoesNotExist),
            },
            KeyOperation::GetMetadata(key) => match self.kvs.get(&key.clone().into()) {
                Some(LatticeValue::Lww(lww)) => Ok((None, Some(lww.reveal().value().clone()))),
                Some(_) => {
                    log::warn!("Get a metadata type is not Lww by key {key:?}");
                    Err(AnnaError::Lattice)
                }
                None => Err(AnnaError::KeyDoesNotExist),
            },
            KeyOperation::Put(key, value) => {
                let key = Key::Client(key);
                self.kvs.put(
                    key.clone(),
                    LatticeValue::Lww(LastWriterWinsLattice::new(TimestampValuePair::new(
                        Timestamp(timestamp),
                        value,
                    ))),
                )?;
                self.local_changeset.insert(key);
                Ok((None, None))
            }
            KeyOperation::PutMetadata(key, value) => {
                let key = Key::Metadata(key);
                self.kvs.put(
                    key.clone(),
                    LatticeValue::Lww(LastWriterWinsLattice::new(TimestampValuePair::new(
                        Timestamp(timestamp),
                        value,
                    ))),
                )?;
                self.local_changeset.insert(key);
                Ok((None, None))
            }
            KeyOperation::SetAdd(key, value) => {
                let key = Key::Client(key);
                let value = SetLattice::new(value);

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
                Ok((None, None))
            }
            KeyOperation::MapAdd(key, value) => {
                let key = Key::Client(key);
                let value = MapLattice::new(
                    value
                        .into_iter()
                        .map(|(k, v)| {
                            (
                                k,
                                LastWriterWinsLattice::new(TimestampValuePair::new(
                                    Timestamp(timestamp),
                                    v,
                                )),
                            )
                        })
                        .collect(),
                );

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
                Ok((None, None))
            }
            KeyOperation::Inc(key, value) => {
                let key = Key::Client(key);

                let value = match self.kvs.entry(key.clone()) {
                    Entry::Vacant(entry) => {
                        let mut counter = CounterLattice::new();
                        counter.inc(
                            format!("{}/{}", self.node_id, self.thread_id),
                            self.gossip_epoch,
                            value,
                        );
                        entry.insert(LatticeValue::Counter(counter));
                        value
                    }
                    Entry::Occupied(mut entry) => {
                        if let LatticeValue::Counter(counter) = entry.get_mut() {
                            counter.inc(
                                format!("{}/{}", self.node_id, self.thread_id),
                                self.gossip_epoch,
                                value,
                            );
                            counter.total()
                        } else {
                            return Err(AnnaError::Lattice);
                        }
                    }
                };

                self.local_changeset.insert(key);
                Ok((Some(ClientResponseValue::Int(value)), None))
            }
        }
    }
}
