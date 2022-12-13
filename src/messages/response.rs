//! Provides the main [`Response`] struct and related types.

use std::collections::{BTreeSet, HashMap, HashSet};

use crate::lattice::causal::MultiKeyCausalLattice;
use crate::lattice::SetLattice;
use crate::{store::LatticeValue, AnnaError, Key};

/// A response to a [`Request`][super::Request].
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Response {
    /// The request_id specified in the corresponding KeyRequest. Used to
    /// associate asynchornous requests and responses.
    pub response_id: Option<String>,
    /// Any errors associated with the whole request. Individual tuple errors are
    /// captured in the corresponding KeyTuple. This will only be set if the whole
    /// request times out.
    pub error: Result<(), AnnaError>,
    /// The individual response pairs associated with this request. There is a
    /// 1-to-1 mapping between these and the KeyTuples in the corresponding
    /// KeyRequest.
    pub tuples: Vec<ResponseTuple>,
}

/// Specifies the type of operation that we executed.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub enum ResponseType {
    /// Response to a request to retrieve data from the KVS.
    Get,
    /// Response to a request to put data into the KVS.
    Put,
    /// Response to a request to add set into the KVS.
    SetAdd,
    /// Response to a request to add hashmap into the KVS.
    MapAdd,
}

/// A protobuf to represent an individual key, both for requests and responses.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ResponseTuple {
    /// The key name for this response.
    pub key: Key,
    /// The lattice value for this key, if this key is a [`ClientKey`][anna_api::ClientKey]
    pub lattice: Option<ClientResponseValue>,
    /// The metadata for this key, if this key is a [`MetadataKey`][crate::metadata::MetadataKey].
    pub(crate) metadata: Option<Vec<u8>>,
    /// The type of response being sent back to the client (see RequestType).
    pub ty: ResponseType,
    /// The error type specified by the server (see AnnaError).
    pub error: Option<AnnaError>,
    /// A boolean set by the server if the client's address_cache_size does not
    /// match the metadata stored by the server.
    pub invalidate: bool,
}

/// Respond to the request that key is a [`ClientKey`][anna_api::ClientKey]
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum ClientResponseValue {
    /// respond a int
    Int(i64),
    /// respond a bytes
    Bytes(Vec<u8>),
    /// respond a hashmap
    Map(HashMap<String, Vec<u8>>),
    /// respond a ordered set
    OrderedSet(BTreeSet<Vec<u8>>),
    /// respond a set
    Set(HashSet<Vec<u8>>),
    /// Multi-key causal lattice, currently this mode is only for testing purpose
    MultiCausal(MultiKeyCausalLattice<SetLattice<Vec<u8>>>),
}

impl From<LatticeValue> for ClientResponseValue {
    fn from(lattice: LatticeValue) -> Self {
        use anna_api::lattice::Lattice;
        match lattice {
            LatticeValue::Lww(value) => ClientResponseValue::Bytes(value.reveal().value().clone()),
            LatticeValue::Set(value) => ClientResponseValue::Set(value.reveal().clone()),
            LatticeValue::OrderedSet(value) => {
                ClientResponseValue::OrderedSet(value.reveal().clone())
            }
            LatticeValue::SingleCausal(set) => {
                ClientResponseValue::Set(set.reveal().value.reveal().clone())
            }
            LatticeValue::SingleCausalMap(map) => {
                let map = map.reveal().value.reveal();
                let mut rmap = HashMap::with_capacity(map.len());
                for (k, v) in map {
                    rmap.insert(k.clone(), v.reveal().value().clone());
                }
                ClientResponseValue::Map(rmap)
            }
            LatticeValue::MultiCausal(mul_key_lattice) => {
                ClientResponseValue::MultiCausal(mul_key_lattice.clone())
            }
        }
    }
}
