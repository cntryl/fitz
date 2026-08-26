//! Live resource-lock identities and ownership coordination.

use super::state::KvDomainRuntime;
use crate::domains::kv::{KvActor, KvMessage};
use parking_lot::Mutex;
use std::sync::Arc;

/// Identifies the in-memory write lock owner for a single resource scope.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct KvResourceLockKey {
    pub(super) family_id: u64,
    pub(super) realm: String,
    pub(super) area: String,
    pub(super) resource: String,
}

impl KvResourceLockKey {
    #[must_use]
    pub(crate) fn new(family_id: u64, realm: &str, area: &str, resource: &str) -> Self {
        Self {
            family_id,
            realm: realm.to_string(),
            area: area.to_string(),
            resource: resource.to_string(),
        }
    }

    #[must_use]
    pub(super) fn from_scope(scope: &crate::domains::kv::KvResourceScope) -> Self {
        Self::new(
            scope.route_family.as_u64(),
            &scope.realm,
            &scope.area,
            &scope.resource,
        )
    }
}

#[derive(Clone, Copy)]
pub(super) struct KvResourceLockOwner {
    /// Session that owns the active write transaction lock.
    pub(super) session_id: u64,
    /// Active transaction id that currently holds the lock.
    pub(super) tx_id: u64,
    /// Last request activity used for idle lock expiry.
    pub(super) last_activity: std::time::Instant,
}

struct KvTransactionLock {
    tx_id: u64,
    resource_key: KvResourceLockKey,
}

impl KvDomainRuntime<'_> {
    pub(super) fn expire_idle_transactions_for_session(&self, session_id: u64) {
        let actor = self.core.actors.lock().get(&session_id).cloned();
        if let Some(actor) = actor {
            self.remove_expired_transactions(session_id, &actor);
        }
    }

    fn remove_expired_transactions(&self, session_id: u64, actor: &Arc<Mutex<KvActor>>) {
        for tx_id in actor
            .lock()
            .expire_idle_transactions(self.core.idle_transaction_ttl)
        {
            self.core
                .resource_locks
                .lock()
                .retain(|_, owner| owner.session_id != session_id || owner.tx_id != tx_id);
            self.core.projection.remove_transaction(session_id, tx_id);
        }
    }

    pub(super) fn expire_resource_lock_if_idle(&self, resource_key: &KvResourceLockKey) {
        let owner = self.core.resource_locks.lock().get(resource_key).copied();
        let Some(owner) =
            owner.filter(|owner| owner.last_activity.elapsed() >= self.core.idle_transaction_ttl)
        else {
            return;
        };
        let actor = self.core.actors.lock().get(&owner.session_id).cloned();
        if let Some(actor) = actor {
            actor.lock().rollback_transaction(owner.tx_id);
        }
        self.core.resource_locks.lock().remove(resource_key);
        self.core
            .projection
            .remove_transaction(owner.session_id, owner.tx_id);
    }

    fn transaction_lock(message: &crate::domains::kv::KvMessage) -> Option<KvTransactionLock> {
        let (tx_id, scope) = match message {
            KvMessage::Begin { .. } => return None,
            KvMessage::Commit { tx_id, scope }
            | KvMessage::Rollback { tx_id, scope }
            | KvMessage::Get { tx_id, scope, .. }
            | KvMessage::Put { tx_id, scope, .. }
            | KvMessage::Insert { tx_id, scope, .. }
            | KvMessage::Delete { tx_id, scope, .. }
            | KvMessage::DeleteRange { tx_id, scope, .. }
            | KvMessage::Scan { tx_id, scope, .. } => (*tx_id, scope),
        };
        Some(KvTransactionLock {
            tx_id,
            resource_key: KvResourceLockKey::from_scope(scope),
        })
    }

    pub(super) fn touch_resource_lock(
        &self,
        session_id: u64,
        message: &crate::domains::kv::KvMessage,
    ) {
        let Some(transaction_lock) = Self::transaction_lock(message) else {
            return;
        };
        let mut locks = self.core.resource_locks.lock();
        if let Some(owner) = locks.get_mut(&transaction_lock.resource_key) {
            if owner.session_id == session_id && owner.tx_id == transaction_lock.tx_id {
                owner.last_activity = std::time::Instant::now();
            }
        }
    }

    pub(super) fn conflicting_session_for_resource(
        &self,
        session_id: u64,
        resource_key: &KvResourceLockKey,
    ) -> Option<u64> {
        self.core
            .resource_locks
            .lock()
            .get(resource_key)
            .filter(|owner| owner.session_id != session_id)
            .map(|owner| owner.session_id)
    }

    /// Returns true if this session currently holds the write lock for the resource.
    ///
    /// The lock table tracks write transactions only.
    pub(super) fn session_holds_resource_lock(
        &self,
        session_id: u64,
        resource_key: &KvResourceLockKey,
    ) -> bool {
        self.core
            .resource_locks
            .lock()
            .get(resource_key)
            .is_some_and(|owner| owner.session_id == session_id)
    }

    pub(super) fn resource_key_for_tx(
        &self,
        session_id: u64,
        tx_id: u64,
    ) -> Option<KvResourceLockKey> {
        let actor = self.core.actors.lock().get(&session_id).cloned();
        actor
            .and_then(|actor| actor.lock().resource_scope_for_tx(tx_id))
            .map(|scope| KvResourceLockKey::from_scope(&scope))
    }
}
