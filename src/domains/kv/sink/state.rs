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

/// Domain-owned cross-family transaction index for gauges and admin inventory.
/// Gauge writes share the same lock as mutations so concurrent families cannot
/// publish an older count after a newer one.
#[derive(Default)]
pub(super) struct KvActiveTransactions {
    by_transaction: parking_lot::Mutex<HashMap<(u64, u64), KvResourceLockKey>>,
}

impl KvActiveTransactions {
    pub(super) fn upsert(
        &self,
        session_id: u64,
        transaction: &crate::control::admin::KvTransaction,
        metrics: Option<&crate::domains::kv::metrics::KvMetrics>,
    ) {
        let mut active = self.by_transaction.lock();
        active.insert(
            (session_id, transaction.tx_id),
            KvResourceLockKey::new(
                transaction.route_family,
                &transaction.realm,
                &transaction.area,
                &transaction.resource,
            ),
        );
        if let Some(metrics) = metrics {
            metrics.set_active_transactions(active.len());
        }
    }

    pub(super) fn remove(
        &self,
        session_id: u64,
        tx_id: u64,
        metrics: Option<&crate::domains::kv::metrics::KvMetrics>,
    ) {
        let mut active = self.by_transaction.lock();
        active.remove(&(session_id, tx_id));
        if let Some(metrics) = metrics {
            metrics.set_active_transactions(active.len());
        }
    }

    pub(super) fn remove_session(
        &self,
        session_id: u64,
        metrics: Option<&crate::domains::kv::metrics::KvMetrics>,
    ) {
        let mut active = self.by_transaction.lock();
        active.retain(|(active_session, _), _| *active_session != session_id);
        if let Some(metrics) = metrics {
            metrics.set_active_transactions(active.len());
        }
    }

    pub(super) fn count(&self) -> usize {
        self.by_transaction.lock().len()
    }

    pub(super) fn count_for_resource(&self, resource_key: &KvResourceLockKey) -> usize {
        self.by_transaction
            .lock()
            .values()
            .filter(|active_key| *active_key == resource_key)
            .count()
    }

    pub(super) fn refresh_gauge(&self, metrics: &crate::domains::kv::metrics::KvMetrics) {
        let active = self.by_transaction.lock();
        metrics.set_active_transactions(active.len());
    }
}

pub(super) struct KvFamilyState {
    pub(super) store: crate::domains::kv::store::KvStore,
    pub(super) actors: HashMap<u64, crate::domains::kv::KvActor>,
    pub(super) resource_locks: HashMap<KvResourceLockKey, KvResourceLockOwner>,
    pub(super) watch_registries: HashMap<u64, crate::domains::kv::watch_registry::KvWatchRegistry>,
    pub(super) cleaned_up_sessions: CleanedUpSessions,
    pub(super) router: Arc<Router>,
    pub(super) projection: Arc<crate::domains::kv::admin_projection::KvAdminProjection>,
    pub(super) active_transactions: Arc<KvActiveTransactions>,
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
    pub(super) active_transactions: Arc<KvActiveTransactions>,
    pub(super) metrics: Option<crate::domains::kv::metrics::KvMetrics>,
    pub(super) sync_write_policy: crate::domains::WritePolicy,
    pub(super) buffered_write_policy: crate::domains::WritePolicy,
    pub(super) idle_transaction_ttl: std::time::Duration,
}

pub(super) enum KvAdminTransactionUpdate {
    None,
    Upsert {
        session_id: u64,
        transaction: crate::control::admin::KvTransaction,
    },
    Remove {
        session_id: u64,
        tx_id: u64,
    },
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
