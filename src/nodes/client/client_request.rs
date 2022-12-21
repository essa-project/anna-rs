use anna_api::{
    messages::{request::KeyOperation, Request},
    topics::KvsThread,
    ClientKey,
};
use std::{collections::HashMap, time::Instant};

#[derive(Debug, Clone)]
pub struct PendingRequest {
    pub tp: Instant,
    pub node: KvsThread,
    pub request: ClientRequest,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClientRequest {
    pub operation: KeyOperation,
    /// The topic at which the client is waiting for the server's response.
    pub response_address: String,
    /// A client-specific ID used to match asynchronous requests with responses.
    pub request_id: String,
    /// The number of server addresses the client is aware of for a particular
    /// key; used for DHT membership change optimization.
    pub address_cache_size: HashMap<ClientKey, usize>,

    pub timestamp: Instant,
}

impl From<ClientRequest> for Request {
    fn from(r: ClientRequest) -> Self {
        Request {
            request: vec![r.operation],
            response_address: Some(r.response_address),
            request_id: Some(r.request_id),
            address_cache_size: r.address_cache_size,
            timestamp: chrono::Utc::now(),
        }
    }
}
