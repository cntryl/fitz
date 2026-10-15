//! Test probe for the same commit choice resolution used by KV actors.
use super::state::KvFamilyRuntime;

impl KvFamilyRuntime<'_> {
    pub(super) fn resolve_commit_persistence(
        &self,
        persistence: crate::domains::CommitPersistence,
    ) -> crate::domains::WritePolicy {
        persistence.storage_policy(
            self.core.sync_write_policy.is_cloud() || self.core.buffered_write_policy.is_cloud(),
        )
    }
}
