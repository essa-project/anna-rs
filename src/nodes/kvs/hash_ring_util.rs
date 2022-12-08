use anna_api::ClientKey;

use crate::{messages::Tier, topics::KvsThread, Key};

use super::KvsNode;

impl KvsNode {
    pub(crate) async fn issue_replication_factor_request(
        &mut self,
        key: ClientKey,
    ) -> eyre::Result<()> {
        self.hash_ring_util
            .issue_replication_factor_request(
                self.wt.replication_response_topic(&self.zenoh_prefix),
                key.clone(),
                &self.global_hash_rings[&Tier::Memory],
                &self.local_hash_rings[&Tier::Memory],
                &self.zenoh,
                &self.zenoh_prefix,
                &mut self.node_connections,
            )
            .await
    }

    pub(crate) async fn try_get_responsible_threads(
        &mut self,
        key: Key,
        tiers: Option<&[Tier]>,
    ) -> eyre::Result<Option<Vec<KvsThread>>> {
        self.hash_ring_util
            .try_get_responsible_threads(
                self.wt.replication_response_topic(&self.zenoh_prefix),
                key,
                &self.global_hash_rings,
                &self.local_hash_rings,
                &self.key_replication_map,
                tiers.unwrap_or(&[self.config_data.self_tier]),
                &self.zenoh,
                &self.zenoh_prefix,
                &mut self.node_connections,
            )
            .await
    }
}
