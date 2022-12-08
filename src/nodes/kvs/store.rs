use anna_api::{
    lattice::{causal::SingleKeyCausalLattice, MaxLattice},
    AnnaError, LatticeValue,
};

use crate::{messages::request::KeyOperation, Key};

use super::KvsNode;

impl KvsNode {
    pub(crate) fn key_operation_handler(
        &mut self,
        operation: KeyOperation,
    ) -> Result<Option<LatticeValue>, AnnaError> {
        use anna_api::lattice::Lattice;
        use std::collections::hash_map::Entry;

        match operation {
            KeyOperation::Get(key) => match self.kvs.get(&key.into()) {
                Some(value) => Ok(Some(value.clone())),
                None => Err(AnnaError::KeyDoesNotExist),
            },
            KeyOperation::GetMetadata(key) => match self.kvs.get(&key.into()) {
                Some(value) => Ok(Some(value.clone())),
                None => Err(AnnaError::KeyDoesNotExist),
            },
            KeyOperation::Put(key, value) => {
                let key = Key::Client(key);
                self.kvs.put(key.clone(), value.clone())?;
                self.local_changeset.insert(key);
                Ok(None)
            }
            KeyOperation::PutMetadata(key, value) => {
                let key = Key::Metadata(key);
                self.kvs.put(key.clone(), LatticeValue::Lww(value))?;
                self.local_changeset.insert(key);
                Ok(None)
            }
            KeyOperation::SetAdd(key, value) => {
                let key = Key::Client(key);

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
                let key = Key::Client(key);

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
}
