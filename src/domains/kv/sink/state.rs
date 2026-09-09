//! KV domain sink state for session-scoped transaction dispatch.
//
// Committed KV writes flow straight to Midge and persist according to the
// `WriteOptions` selected when the transaction commits. Active `tx_id`
// handles, resource locks, and admin snapshot entries are separate live
// in-memory state owned by the current broker process. `cleanup_session`
// intentionally discards that state on disconnect, and broker restart clears
// it wholesale instead of attempting transaction recovery.

use super::commands::KvDomainCommand;
use super::locks::{KvResourceLockKey, KvResourceLockOwner};
use crate::runtime::{CleanedUpSessions, Router};
use std::collections::HashMap;
use std::sync::Arc;

pub(super) struct KvFamilyState {
    pub(super) store: crate::domains::kv::store::KvStore,
    pub(super) actors: HashMap<u64, crate::domains::kv::KvActor>,
    pub(super) resource_locks: HashMap<KvResourceLockKey, KvResourceLockOwner>,
    pub(super) watch_registries: HashMap<u64, crate::domains::kv::watch_registry::KvWatchRegistry>,
    pub(super) cleaned_up_sessions: CleanedUpSessions,
    pub(super) router: Arc<Router>,
    pub(super) projection: Arc<crate::domains::kv::admin_projection::KvAdminProjection>,
    pub(super) metrics: Option<crate::domains::kv::metrics::KvMetrics>,
    pub(super) sync_write_policy: crate::domains::WritePolicy,
    pub(super) buffered_write_policy: crate::domains::WritePolicy,
    pub(super) idle_transaction_ttl: std::time::Duration,
}

pub(super) struct KvFamilyRuntime<'a> {
    pub(super) core: &'a mut KvFamilyState,
}

#[derive(Clone)]
pub(super) struct KvDomainConfig {
    pub(super) store: crate::domains::kv::store::KvStore,
    pub(super) router: Arc<Router>,
    pub(super) projection: Arc<crate::domains::kv::admin_projection::KvAdminProjection>,
    pub(super) metrics: Option<crate::domains::kv::metrics::KvMetrics>,
    pub(super) sync_write_policy: crate::domains::WritePolicy,
    pub(super) buffered_write_policy: crate::domains::WritePolicy,
    pub(super) idle_transaction_ttl: std::time::Duration,
}

pub(super) enum KvAdminTransactionUpdate {
    None,
    Upsert(crate::control::admin::KvTransaction),
    Remove { session_id: u64, tx_id: u64 },
}

pub(super) struct KvOperationOutcome {
    pub(super) response: crate::domains::kv::KvResponse,
    pub(super) admin_update: KvAdminTransactionUpdate,
    pub(super) commit_notification: Option<KvCommitNotification>,
}

pub(super) struct KvCommitNotification {
    pub(super) resource_key: KvResourceLockKey,
    pub(super) mutation_count: u64,
}

impl KvOperationOutcome {
    #[must_use]
    pub(super) fn new(
        response: crate::domains::kv::KvResponse,
        admin_update: KvAdminTransactionUpdate,
        commit_notification: Option<KvCommitNotification>,
    ) -> Self {
        Self {
            response,
            admin_update,
            commit_notification,
        }
    }
}

pub(crate) struct KvDomain {
    pub(super) family_runtime: crate::runtime::FamilyActorPoolRuntime<KvDomainCommand>,
    pub(super) route_families: Vec<crate::runtime::routing::RouteFamily>,
    pub(super) active: Arc<std::sync::atomic::AtomicBool>,
    pub(super) config: KvDomainConfig,
}
