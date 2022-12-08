//! Provides the main [`Request`] struct and related types.

use anna_api::lattice::{LastWriterWinsLattice, MapLattice, SetLattice};

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
pub struct RequestData {
    /// Operate the stored values.
    /// The list of operations that we want to apply.
    pub operations: Vec<KeyOperation>,
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
    /// Add the value of one or more fields on a a single-key causal hashmap lattice.
    MapAdd(Key, MapLattice<String, LastWriterWinsLattice<Vec<u8>>>),
}

impl KeyOperation {
    /// Returns the key that this operation reads/writes.
    pub fn key(&self) -> &Key {
        match self {
            KeyOperation::Get(key) => key,
            KeyOperation::Put(t) => &t.key,
            KeyOperation::SetAdd(key, _) => key,
            KeyOperation::MapAdd(key, _) => key,
        }
    }

    /// Returns the suitable [`ResponseType`] for the operation.
    pub fn response_ty(&self) -> ResponseType {
        match self {
            KeyOperation::Get(_) => ResponseType::Get,
            KeyOperation::Put(_) => ResponseType::Put,
            KeyOperation::SetAdd(..) => ResponseType::SetAdd,
            KeyOperation::MapAdd(..) => ResponseType::MapAdd,
        }
    }
}
