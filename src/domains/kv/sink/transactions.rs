//! BEGIN, COMMIT, ROLLBACK, and transaction outcome coordination.

use super::locks::{KvResourceLockKey, KvResourceLockOwner};
use super::state::{
    KvAdminTransactionUpdate, KvCommitNotification, KvDomainRuntime, KvOperationOutcome,
};
use crate::domains::kv::{KvError, KvResponse};
use crate::runtime::{DeliveryError, Envelope};

impl KvDomainRuntime<'_> {
    pub(super) fn handle_actor_operation_frame(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        request_started: std::time::Instant,
        operation_started: std::time::Instant,
        kv_message: crate::domains::kv::KvMessage,
    ) -> Result<(), DeliveryError> {
        use crate::domains::kv::{KvMessage, TxMode};
        if Self::kv_message_family(&kv_message) != meta.route_family {
            let response = Self::error_response("route family mismatch");
            self.route_kv_response(envelope, meta, &response, request_started)?;
            return Ok(());
        }
        if self.is_cleaned_up_session(meta.session_id) {
            let response = Self::error_response("session already closed");
            self.route_kv_response(envelope, meta, &response, request_started)?;
            return Ok(());
        }

        let kv_message = self.apply_write_options(kv_message);
        let session_id = meta.session_id;
        let read_tx_id = match &kv_message {
            KvMessage::Get { tx_id, .. } | KvMessage::Scan { tx_id, .. } => Some(*tx_id),
            _ => None,
        };
        let is_commit = matches!(&kv_message, KvMessage::Commit { .. });

        if matches!(
            &kv_message,
            KvMessage::Begin {
                mode: TxMode::ReadWrite,
                ..
            }
        ) {
            if let KvMessage::Begin { scope, .. } = &kv_message {
                self.expire_resource_lock_if_idle(&KvResourceLockKey::new(
                    scope.route_family.as_u64(),
                    &scope.realm,
                    &scope.area,
                    &scope.resource,
                ));
            }
        } else {
            self.expire_idle_transactions_for_session(session_id);
        }

        tracing::trace!(
            domain = "kv",
            session_id = session_id,
            msg_type = meta.message_type,
            "KV deliver: getting or creating actor for session"
        );

        self.touch_resource_lock(session_id, &kv_message);
        let KvOperationOutcome {
            response,
            admin_update,
            commit_notification,
        } = self.dispatch_actor_operation(session_id, meta, kv_message);
        if matches!(
            &response,
            KvResponse::Error {
                error: KvError::InvalidTxId,
                ..
            }
        ) {
            self.counter_inc("fitz_kv_invalid_transaction_rejects_total");
        }
        match (&response, read_tx_id, is_commit) {
            (KvResponse::GetResult { .. } | KvResponse::ScanResult { .. }, Some(tx_id), _) => {
                if let Some(resource_key) = self.resource_key_for_tx(session_id, tx_id) {
                    self.record_read_latency(&resource_key, operation_started);
                }
            }
            (KvResponse::CommitOk, _, true) => {
                if let Some(notification) = commit_notification.as_ref() {
                    self.record_write_latency(&notification.resource_key, operation_started);
                }
            }
            _ => {}
        }
        self.apply_admin_transaction_update(admin_update);
        if let Some(notification) = commit_notification {
            self.route_kv_notification(&notification.resource_key, notification.mutation_count);
        }

        tracing::debug!(
            domain = "kv",
            session = meta.session_id,
            response = ?std::mem::discriminant(&response),
            "KV actor returned response"
        );

        let route_result = self.route_kv_response(envelope, meta, &response, request_started);
        if route_result.is_err() {
            if let KvResponse::BeginOk { tx_id } = response {
                self.rollback_undeliverable_begin(meta.session_id, meta.route_family, tx_id);
            }
        }
        route_result
    }

    fn rollback_undeliverable_begin(
        &self,
        session_id: u64,
        route_family: crate::runtime::routing::RouteFamily,
        tx_id: u64,
    ) {
        let Some(key) = self.resource_key_for_tx(session_id, tx_id) else {
            return;
        };
        let scope = crate::domains::kv::KvResourceScope::new(
            route_family,
            key.realm,
            key.area,
            key.resource,
        );
        let outcome = self.handle_rollback_frame(
            session_id,
            route_family,
            tx_id,
            crate::domains::kv::KvMessage::Rollback { tx_id, scope },
        );
        self.apply_admin_transaction_update(outcome.admin_update);
    }

    pub(super) fn handle_begin_read_write(
        &self,
        session_id: u64,
        lock_key: &KvResourceLockKey,
        kv_message: crate::domains::kv::KvMessage,
    ) -> KvOperationOutcome {
        let held_by_same_session = self.session_holds_resource_lock(session_id, lock_key);
        if self
            .conflicting_session_for_resource(session_id, lock_key)
            .is_some()
        {
            return KvOperationOutcome::new(
                KvResponse::Error {
                    error: KvError::Conflict("resource locked by another session".to_string()),
                },
                KvAdminTransactionUpdate::None,
                None,
            );
        }
        if held_by_same_session {
            return KvOperationOutcome::new(
                KvResponse::Error {
                    error: KvError::Conflict(
                        "resource already has a read-write transaction for this session"
                            .to_string(),
                    ),
                },
                KvAdminTransactionUpdate::None,
                None,
            );
        }

        let log_context = "BEGIN (ReadWrite, acquiring lock)";
        let actor = self.actor_for_session(session_id, "begin");
        let mut actor = actor.lock();
        tracing::trace!(
            domain = "kv",
            session_id = session_id,
            "Calling actor.handle() for {log_context}"
        );
        let response = actor.handle(kv_message);
        if let KvResponse::BeginOk { tx_id } = response {
            self.core.resource_locks.lock().insert(
                lock_key.clone(),
                KvResourceLockOwner {
                    session_id,
                    tx_id,
                    last_activity: std::time::Instant::now(),
                },
            );
            tracing::trace!(
                domain = "kv",
                session_id = session_id,
                tx_id = tx_id,
                "BEGIN succeeded with actor-owned transaction scope"
            );
            let transaction = crate::control::admin::KvTransaction::snapshot(
                lock_key.family_id,
                tx_id,
                session_id,
                &lock_key.realm,
                &lock_key.area,
                &lock_key.resource,
                &chrono::Utc::now().to_rfc3339(),
            );
            KvOperationOutcome::new(
                response,
                KvAdminTransactionUpdate::Upsert(transaction),
                None,
            )
        } else {
            KvOperationOutcome::new(response, KvAdminTransactionUpdate::None, None)
        }
    }

    pub(super) fn handle_commit_frame(
        &self,
        session_id: u64,
        route_family: crate::runtime::routing::RouteFamily,
        tx_id: u64,
        kv_message: crate::domains::kv::KvMessage,
    ) -> KvOperationOutcome {
        let actor = self.actor_for_session(session_id, "commit");
        let mut actor = actor.lock();
        tracing::trace!(
            domain = "kv",
            session_id = session_id,
            tx_id = tx_id,
            "Calling actor.handle() for COMMIT"
        );
        let mutation_count = actor.mutation_count_for_tx(tx_id).unwrap_or(0);
        let lock_key = actor
            .resource_scope_for_tx(tx_id)
            .map(|scope| KvResourceLockKey::from_scope(&scope));
        if lock_key
            .as_ref()
            .is_some_and(|key| key.family_id != route_family.as_u64())
        {
            return KvOperationOutcome::new(
                KvResponse::Error {
                    error: KvError::InvalidRequest("route family mismatch".to_string()),
                },
                KvAdminTransactionUpdate::None,
                None,
            );
        }
        let had_transaction = lock_key.is_some();
        let response = actor.handle(kv_message);
        let admin_update = if had_transaction && actor.resource_scope_for_tx(tx_id).is_none() {
            if let Some(lock_key) = &lock_key {
                self.core.resource_locks.lock().remove(lock_key);
            }
            KvAdminTransactionUpdate::Remove { session_id, tx_id }
        } else {
            KvAdminTransactionUpdate::None
        };
        if let KvResponse::CommitOk = response {
            if let Some(lock_key) = lock_key {
                let notify = (mutation_count > 0).then_some(KvCommitNotification {
                    resource_key: lock_key,
                    mutation_count,
                });
                KvOperationOutcome::new(response, admin_update, notify)
            } else {
                KvOperationOutcome::new(response, admin_update, None)
            }
        } else {
            self.counter_inc("fitz_kv_commits_failed_total");
            KvOperationOutcome::new(response, admin_update, None)
        }
    }

    pub(super) fn handle_rollback_frame(
        &self,
        session_id: u64,
        route_family: crate::runtime::routing::RouteFamily,
        tx_id: u64,
        kv_message: crate::domains::kv::KvMessage,
    ) -> KvOperationOutcome {
        let actor = self.actor_for_session(session_id, "rollback");
        let mut actor = actor.lock();
        tracing::trace!(
            domain = "kv",
            session_id = session_id,
            tx_id = tx_id,
            "Calling actor.handle() for ROLLBACK"
        );
        let resource_scope = actor.resource_scope_for_tx(tx_id);
        if resource_scope
            .as_ref()
            .is_some_and(|scope| scope.route_family != route_family)
        {
            return KvOperationOutcome::new(
                KvResponse::Error {
                    error: KvError::InvalidRequest("route family mismatch".to_string()),
                },
                KvAdminTransactionUpdate::None,
                None,
            );
        }
        let response = actor.handle(kv_message);
        let admin_update =
            if resource_scope.is_some() && actor.resource_scope_for_tx(tx_id).is_none() {
                if let Some(scope) = &resource_scope {
                    self.core
                        .resource_locks
                        .lock()
                        .remove(&KvResourceLockKey::from_scope(scope));
                }
                KvAdminTransactionUpdate::Remove { session_id, tx_id }
            } else {
                KvAdminTransactionUpdate::None
            };
        if let KvResponse::RollbackOk = response {
            self.counter_inc("fitz_kv_rollbacks_total");
            KvOperationOutcome::new(response, admin_update, None)
        } else {
            KvOperationOutcome::new(response, admin_update, None)
        }
    }
}
