//! Messages related to gossip.

use crate::store::LatticeValue;

use crate::Key;

/// Describes an assign operation on a specific key.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct GossipDataTuple {
    /// The key that should be updated.
    pub key: Key,
    /// The new value that should be merged into the current one.
    pub value: LatticeValue,
}

/// Specifies the associated data for gossip update.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct GossipRequest {
    /// A list of updates batched in gossip request.
    pub tuples: Vec<GossipDataTuple>,
}
