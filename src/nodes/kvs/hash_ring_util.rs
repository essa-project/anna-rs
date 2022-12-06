use anna_api::ClientKey;

use crate::messages::Tier;

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
}
