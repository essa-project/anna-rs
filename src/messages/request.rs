//! Provides the main [`Request`] struct and related types.

use anna_api::lattice::SetLattice;

use super::response::{Response, ResponseType};
use crate::{store::LatticeValue, ClientKey, Key};
use std::collections::HashMap;

/// An individual GET or PUT request; each request can batch multiple keys.
///
/// The target node responds with a [`Response`][super::Response].
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Request {
    /// A client-specific ID used to match asynchronous requests with responses.
    pub request_id: Option<String>,
    /// The zenoh topic at which the client is waiting for the server's response.
    pub response_address: Option<String>,
    /// The number of server addresses the client is aware of for a particular
    /// key; used for DHT membership change optimization.
    pub address_cache_size: HashMap<ClientKey, usize>,
    /// The type and data of this request.
    pub request: RequestData,
}

impl Request {
    /// Constructs a new [`Response`] for the request.
    ///
    /// Sets [`response_id`][Response::response_id] and `ty`[Response::ty] fields accordingly.
    /// The [`error`][Response::error] field is initialized with [`Ok(())`][Result::ok] and
    /// the [`tuples`][Response::tuples] field with an empty list.
    pub fn new_response(&self) -> Response {
        Response {
            response_id: self.request_id.clone(),
            tuples: Default::default(),
            error: Ok(()),
        }
    }
}

/// Specifies the request type and associated data.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum RequestData {
    /// Operate the stored values.
    Operation {
        /// The list of operations that we want to apply.
        operations: Vec<KeyOperation>,
    },
    /// Performs the given updates that from gossip in the key value store.
    Gossip {
        /// A list of updates batched in gossip request.
        tuples: Vec<ModifyTuple>,
    },
}

impl RequestData {
    /// Splits the request into a list of operations.
    ///
    /// For Operation requests, this returns the `operations` in [RequestData::Operation].
    /// For Gossip requests, it returns a list of [`KeyOperation::Put`] variants.
    pub fn into_tuples(self) -> Vec<KeyOperation> {
        match self {
            RequestData::Operation { operations } => operations,
            RequestData::Gossip { tuples } => {
                log::warn!("received a gossip request, but expect not.");
                tuples.into_iter().map(KeyOperation::Put).collect()
            }
        }
    }
}

/// Describes an assign operation on a specific key.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ModifyTuple {
    /// The key that should be updated.
    pub key: Key,
    /// The new value that should be merged into the current one.
    pub value: LatticeValue,
}

/// Abstraction for a single key operation.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum KeyOperation {
    /// Get the value of a key.
    Get(Key),
    /// Assign a new value to a key.
    Put(ModifyTuple),
    /// Merge a single-key causal lattice to a key.
    SetAdd(Key, SetLattice<Vec<u8>>),
}

impl KeyOperation {
    /// Returns the key that this operation reads/writes.
    pub fn key(&self) -> &Key {
        match self {
            KeyOperation::Get(key) => key,
            KeyOperation::Put(t) => &t.key,
            KeyOperation::SetAdd(t, _) => t,
        }
    }

    /// Returns the suitable [`ResponseType`] for the operation.
    pub fn response_ty(&self) -> ResponseType {
        match self {
            KeyOperation::Get(_) => ResponseType::Get,
            KeyOperation::Put(_) => ResponseType::Put,
            KeyOperation::SetAdd(..) => ResponseType::SetAdd,
        }
    }
}
