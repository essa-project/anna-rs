//! Provides the main [`Request`] struct and related types.

use super::response::{Response, ResponseType};
use crate::ClientKey;
use std::collections::{HashMap, HashSet};

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
    pub client_operations: Vec<KeyOperation>,
    /// The request creation time.
    pub timestamp: chrono::DateTime<chrono::Utc>,
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

/// Abstraction for a single key operation.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum KeyOperation {
    /// Get the value of a client key.
    Get(ClientKey),
    /// Assign a new value to a client key.
    Put(ClientKey, Vec<u8>),
    /// Merge a single-key causal lattice to a key.
    SetAdd(ClientKey, HashSet<Vec<u8>>),
    /// Add the value of one or more fields on a a single-key causal hashmap lattice.
    MapAdd(ClientKey, HashMap<String, Vec<u8>>),
    /// Increase the value to a client key.
    Inc(ClientKey, i64),
}

impl KeyOperation {
    /// Returns the key that this operation reads/writes.
    pub fn key(&self) -> ClientKey {
        match self {
            KeyOperation::Get(key) => key.clone(),
            KeyOperation::Put(key, _) => key.clone(),
            KeyOperation::SetAdd(key, _) => key.clone(),
            KeyOperation::MapAdd(key, _) => key.clone(),
            KeyOperation::Inc(key, _) => key.clone(),
        }
    }

    /// Returns the suitable [`ResponseType`] for the operation.
    pub fn response_ty(&self) -> ResponseType {
        match self {
            KeyOperation::Get(_) => ResponseType::Get,
            KeyOperation::Put(..) => ResponseType::Put,
            KeyOperation::SetAdd(..) => ResponseType::SetAdd,
            KeyOperation::MapAdd(..) => ResponseType::MapAdd,
            KeyOperation::Inc(..) => ResponseType::Inc,
        }
    }
}
