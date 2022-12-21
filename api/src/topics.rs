//! Defines the zenoh topic paths that should be used for messages.
//!
//! Allows to address specific threads of specific nodes.

use std::convert::TryInto;

// The topic on which clients send key address requests to routing nodes.
const KEY_ADDRESS_TOPIC: &str = "key_address";

// The topic on which clients receive responses from the KVS.
const USER_RESPONSE_TOPIC: &str = "user_response";

// The topic on which clients receive responses from the routing tier.
const USER_KEY_ADDRESS_TOPIC: &str = "user_key_address";

// The topic on which KVS servers listen for requests for data.
const KEY_REQUEST_TOPIC: &str = "key_request";

const TCP_PORT_TOPIC: &str = "tcp_port";

// The topic on which routing servers listen for cluster membership requests.
const SEED_TOPIC: &str = "seed";

/// Provides the topic paths for addressing a specific thread of a specific _KVS_ node.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct KvsThread {
    /// The ID of the addressed KVS node.
    pub node_id: String,
    /// The ID of the addressed thread on the KVS node.
    pub thread_id: u32,
}

impl KvsThread {
    /// Address the given thread on the given node.
    pub fn new(node_id: String, thread_id: u32) -> Self {
        Self { node_id, thread_id }
    }

    /// The topic on which [`Request`][crate::messages::Request] messages are sent.
    pub fn request_topic(&self, prefix: &str) -> String {
        format!(
            "{}/{}/{}/{}",
            prefix, self.node_id, KEY_REQUEST_TOPIC, self.thread_id
        )
        .try_into()
        .unwrap()
    }
}

/// Provides the topic paths for addressing a specific thread of a specific _client_ node.
#[derive(Debug, Clone)]
pub struct ClientThread {
    /// The node ID of the client node.
    pub node_id: String,
    /// The ID of the addressed thread.
    pub thread_id: u32,
}

impl ClientThread {
    /// Address the given thread of the given client node.
    pub fn new(node_id: String, thread_id: u32) -> Self {
        Self { node_id, thread_id }
    }

    /// The topic on which [`Response`][crate::messages::Response] messages should be sent in
    /// reply to requests.
    ///
    /// Clients send [`Request`][crate::messages::Request] messages to KVS nodes and pass
    /// this topic as reply topic.
    pub fn response_topic(&self, prefix: &str) -> String {
        format!(
            "{}/{}/{}/{}",
            prefix, self.node_id, USER_RESPONSE_TOPIC, self.thread_id
        )
        .try_into()
        .unwrap()
    }

    /// The topic on which [`AddressResponse`][crate::messages::AddressResponse] messages should
    /// be sent in reply to address requests.
    ///
    /// Clients send [`AddressRequest`][crate::messages::AddressRequest] messages to routing
    /// nodes and pass this topic as reply topic.
    pub fn address_response_topic(&self, prefix: &str) -> String {
        format!(
            "{}/{}/{}/{}",
            prefix, self.node_id, USER_KEY_ADDRESS_TOPIC, self.thread_id
        )
        .try_into()
        .unwrap()
    }
}

/// Provides the topic paths for addressing a specific thread of a specific _routing_ node.
///
/// Each KVS has a configured routing node that it should address.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RoutingThread {
    /// The ID if the routing node.
    pub node_id: String,
    /// The addressed thread ID of the routing node.
    pub thread_id: u32,
}

impl RoutingThread {
    /// KVS node threads send a `"join"` request on this topic on startup.
    ///
    /// The routing node should reply with a
    /// [`ClusterMembership`][crate::messages::cluster_membership::ClusterMembership]
    /// message. Unlike most other messages in this crate, the `"join"` is sent as
    /// zenoh [`get`][zenoh::Workspace::get] requests with an immediate reply.
    pub fn seed_topic(prefix: &str) -> String {
        format!("{}/{}", prefix, SEED_TOPIC).try_into().unwrap()
    }

    /// Addresses the given thread on the given routing node.
    pub fn new(node_id: String, thread_id: u32) -> Self {
        Self { node_id, thread_id }
    }

    /// Nodes can request the public IP address of the routing node under this topic.
    pub fn tcp_addr_topic(&self, prefix: &str) -> String {
        format!(
            "{}/{}/{}/{}",
            prefix, self.node_id, TCP_PORT_TOPIC, self.thread_id
        )
        .try_into()
        .unwrap()
    }

    /// Client nodes request the KVS node responsible for a given key on this topic.
    ///
    /// The sent messages are of type [`AddressRequest`][crate::messages::AddressRequest].
    pub fn address_request_topic(&self, prefix: &str) -> String {
        format!(
            "{}/{}/{}/{}",
            prefix, self.node_id, KEY_ADDRESS_TOPIC, self.thread_id
        )
        .try_into()
        .unwrap()
    }

    /// The routing node responds to "ping" messages sent on this topic.
    pub fn ping_topic(&self, prefix: &str) -> String {
        format!("{}/{}/{}/{}", prefix, self.node_id, "ping", self.thread_id)
            .try_into()
            .unwrap()
    }
}
