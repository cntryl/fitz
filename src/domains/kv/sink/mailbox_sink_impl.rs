use super::model::{
    Arc, DeliveryError, Envelope, KvAdminTransactionUpdate, KvClientFrame, KvClientRequest,
    KvDomainActor, KvDomainCommand, KvDomainRuntime, KvDomainSink, KvResourceLockKey,
    KvResourceLockOwner, MailboxSink, Mutex, Ordering,
};
#[cfg(test)]
use crate::dispatch::protocol::frame_context::FrameContext;
use crate::runtime::{Actor, Context};

impl MailboxSink for KvDomainSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.actor.try_send(KvDomainCommand::Deliver(envelope))
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.actor
            .try_send_high_priority(KvDomainCommand::Deliver(envelope))
    }
}

impl Actor for KvDomainActor {
    type Message = KvDomainCommand;

    fn receive(&mut self, msg: Self::Message, _ctx: &mut Context<Self>) {
        match msg {
            KvDomainCommand::Deliver(envelope) => {
                if let Err(error) = self.state.runtime().deliver_envelope(&envelope) {
                    tracing::warn!(domain = "kv", error = %error, "KV actor delivery failed");
                }
            }
            KvDomainCommand::CleanupSession(session_id, reply) => {
                self.state.runtime().cleanup_session(session_id);
                let _ = reply.send(());
            }
            KvDomainCommand::ReadActiveTransactionCount(reply) => {
                let _ = reply.send(self.state.runtime().active_transaction_count());
            }
            #[cfg(test)]
            KvDomainCommand::SyncAdminSnapshot(reply) => {
                self.state.runtime().sync_admin_snapshot();
                let _ = reply.send(());
            }
            #[cfg(test)]
            KvDomainCommand::ReadLatencySnapshots(resource_key, reply) => {
                let _ = reply.send(self.state.runtime().latency_snapshots(&resource_key));
            }
            #[cfg(test)]
            KvDomainCommand::ApplyWriteOptions(message, reply) => {
                let _ = reply.send(self.state.runtime().apply_write_options(message));
            }
            #[cfg(test)]
            KvDomainCommand::PanicForTests => {
                panic!("test KV domain actor panic");
            }
        }
    }
}

impl KvDomainRuntime<'_> {
    pub(super) fn deliver_envelope(&self, envelope: &Envelope) -> Result<(), DeliveryError> {
        if self.handle_cleanup_envelope(envelope) {
            return Ok(());
        }
        self.ensure_active()?;
        Self::log_delivery(envelope);

        let request = Self::extract_request(envelope)?;
        let meta = request.meta;
        let request_started = self.record_request_start();
        if !Self::valid_request_envelope(envelope, meta) {
            let response = Self::error_response("route family mismatch");
            let response_meta = Self::response_meta_for_source(envelope, meta);
            self.route_kv_response(envelope, response_meta, &response, request_started)?;
            return Ok(());
        }

        let operation_started = Self::record_operation_start();
        let Some(parsed_frame) =
            self.parse_request_frame(envelope, meta, request.frame, request_started)
        else {
            return Ok(());
        };

        match parsed_frame {
            KvClientFrame::Sub(sub_msg) => {
                self.handle_subscription_frame(envelope, meta, request_started, sub_msg)
            }
            KvClientFrame::Op(kv_message) => self.handle_actor_operation_frame(
                envelope,
                meta,
                request_started,
                operation_started,
                kv_message,
            ),
        }
    }

    fn handle_cleanup_envelope(&self, envelope: &Envelope) -> bool {
        if let Some(cleanup) = envelope.payload::<crate::runtime::SessionCleanup>() {
            self.cleanup_session(cleanup.session_id);
            return true;
        }

        false
    }

    fn ensure_active(&self) -> Result<(), DeliveryError> {
        if !self.active.load(Ordering::Relaxed) {
            return Err(DeliveryError::ActorStopped);
        }

        Ok(())
    }

    fn log_delivery(envelope: &Envelope) {
        tracing::debug!(
            domain = "kv",
            destination = %envelope.destination(),
            source = ?envelope.source(),
            "KV domain sink: received envelope"
        );
    }

    fn extract_request(envelope: &Envelope) -> Result<KvClientRequest, DeliveryError> {
        Self::request_from_envelope(envelope).ok_or_else(|| {
            tracing::warn!(
                domain = "kv",
                destination = ?envelope.destination(),
                "Envelope payload was not KvClientRequest"
            );
            DeliveryError::ActorStopped
        })
    }

    fn record_operation_start() -> std::time::Instant {
        std::time::Instant::now()
    }

    fn record_request_start(&self) -> Option<std::time::Instant> {
        self.core
            .metrics
            .as_ref()
            .map(super::super::metrics::KvMetrics::record_request_start)
    }

    fn parse_request_frame(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        frame: Result<KvClientFrame, String>,
        request_started: Option<std::time::Instant>,
    ) -> Option<KvClientFrame> {
        let parsed_frame = match frame {
            Ok(msg) => msg,
            Err(e) => {
                tracing::warn!(
                    domain = "kv",
                    session = meta.session_id,
                    msg_type = meta.message_type,
                    error = %e,
                    "Failed to parse KV message"
                );
                let response = Self::error_response(&e);
                let response_meta = Self::response_meta_for_source(envelope, meta);
                let _ = self.route_kv_response(envelope, response_meta, &response, request_started);
                return None;
            }
        };

        tracing::debug!(
            domain = "kv",
            session = meta.session_id,
            channel = ?meta.channel,
            msg_type = meta.message_type,
            "Parsed KV message successfully"
        );

        Some(parsed_frame)
    }

    fn handle_subscription_frame(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        request_started: Option<std::time::Instant>,
        sub_msg: crate::domains::kv::KvSubscriptionMessage,
    ) -> Result<(), DeliveryError> {
        let response = match sub_msg {
            crate::domains::kv::KvSubscriptionMessage::Subscribe {
                family_id,
                pattern,
                session_id,
                subscriber,
            } => self
                .handle_kv_subscribe(envelope, meta, family_id, &pattern, session_id, subscriber),
            crate::domains::kv::KvSubscriptionMessage::Unsubscribe {
                family_id,
                pattern,
                session_id,
                subscriber,
            } => self.handle_kv_unsubscribe(
                envelope,
                meta,
                family_id,
                &pattern,
                session_id,
                &subscriber,
            ),
        };

        self.refresh_metrics_gauges();
        self.route_kv_response(envelope, meta, &response, request_started)
    }

    fn handle_kv_subscribe(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        family_id: crate::runtime::routing::RouteFamily,
        pattern: &crate::runtime::routing::Route,
        session_id: u64,
        subscriber: crate::runtime::routing::RouteAddress,
    ) -> crate::domains::kv::KvResponse {
        if Self::valid_subscription_request(envelope, meta, family_id, session_id, &subscriber) {
            let compiled = match Self::compile_kv_subscription_pattern(pattern) {
                Ok(compiled) => compiled,
                Err(response) => return response,
            };
            let subscription_id = {
                let mut watch_actors = self.core.watch_actors.lock();
                let actor = watch_actors
                    .entry(family_id.as_u64())
                    .or_insert_with(|| crate::domains::kv::watch::KvWatchActor::new(family_id));
                let subscription_id = actor.subscribe(session_id, compiled, subscriber);
                if subscription_id.is_err() && actor.is_empty() {
                    watch_actors.remove(&family_id.as_u64());
                }
                subscription_id
            };
            subscription_id.map_or_else(
                |error| crate::domains::kv::KvResponse::Error { error },
                |subscription_id| crate::domains::kv::KvResponse::SubscribeOk { subscription_id },
            )
        } else {
            Self::error_response("route family mismatch")
        }
    }

    fn handle_kv_unsubscribe(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        family_id: crate::runtime::routing::RouteFamily,
        pattern: &crate::runtime::routing::Route,
        session_id: u64,
        subscriber: &crate::runtime::routing::RouteAddress,
    ) -> crate::domains::kv::KvResponse {
        if Self::valid_subscription_request(envelope, meta, family_id, session_id, subscriber) {
            if let Err(response) = Self::compile_kv_subscription_pattern(pattern) {
                return response;
            }
            let mut watch_actors = self.core.watch_actors.lock();
            let remove_family = if let Some(actor) = watch_actors.get_mut(&family_id.as_u64()) {
                actor.unsubscribe(session_id, pattern.as_str());
                actor.is_empty()
            } else {
                false
            };
            if remove_family {
                watch_actors.remove(&family_id.as_u64());
            }
            crate::domains::kv::KvResponse::UnsubscribeOk
        } else {
            Self::error_response("route family mismatch")
        }
    }

    fn compile_kv_subscription_pattern(
        pattern: &crate::runtime::routing::Route,
    ) -> Result<crate::runtime::matcher::Pattern, crate::domains::kv::KvResponse> {
        crate::runtime::DomainKind::Kv
            .descriptor()
            .compile_registration_pattern(pattern.as_str())
            .map_err(|error| crate::domains::kv::KvResponse::Error {
                error: crate::domains::kv::KvError::InvalidSubscriptionPattern(error),
            })
    }

    fn handle_actor_operation_frame(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        request_started: Option<std::time::Instant>,
        operation_started: std::time::Instant,
        kv_message: crate::domains::kv::KvMessage,
    ) -> Result<(), DeliveryError> {
        use crate::domains::kv::{KvMessage, TxMode};
        if Self::kv_message_family(&kv_message) != meta.route_family {
            let response = Self::error_response("route family mismatch");
            self.route_kv_response(envelope, meta, &response, request_started)?;
            return Ok(());
        }

        let kv_message = self.apply_write_options(kv_message);
        let session_id = meta.session_id;

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

        let (response, admin_update, commit_notification) = match &kv_message {
            KvMessage::Begin { scope, mode, .. } if *mode == TxMode::ReadWrite => self
                .handle_begin_read_write(
                    session_id,
                    scope.route_family.as_u64(),
                    &scope.realm,
                    &scope.area,
                    &scope.resource,
                    &kv_message,
                ),
            KvMessage::Commit { tx_id, .. } => {
                self.handle_commit_frame(session_id, meta.route_family, *tx_id, &kv_message)
            }
            KvMessage::Rollback { tx_id, .. } => {
                self.handle_rollback_frame(session_id, meta.route_family, *tx_id, &kv_message)
            }
            _ => self.handle_regular_operation_frame(session_id, meta.message_type, &kv_message),
        };
        self.touch_resource_lock(session_id, &kv_message);
        if matches!(
            &response,
            crate::domains::kv::KvResponse::Error {
                error: crate::domains::kv::KvError::InvalidTxId,
                ..
            }
        ) {
            crate::observability::counter_inc("fitz_kv_invalid_transaction_rejects_total");
        }
        match (&kv_message, &response) {
            (KvMessage::Get { tx_id, .. }, crate::domains::kv::KvResponse::GetResult { .. })
            | (KvMessage::Scan { tx_id, .. }, crate::domains::kv::KvResponse::ScanResult { .. }) => {
                if let Some(resource_key) = self.resource_key_for_tx(session_id, *tx_id) {
                    self.record_read_latency(&resource_key, operation_started);
                }
            }
            (KvMessage::Commit { .. }, crate::domains::kv::KvResponse::CommitOk) => {
                if let Some((resource_key, _)) = commit_notification.as_ref() {
                    self.record_write_latency(resource_key, operation_started);
                }
            }
            _ => {}
        }
        self.apply_admin_transaction_update(admin_update);
        if let Some((resource_key, mutation_count)) = commit_notification {
            self.route_kv_notification(&resource_key, mutation_count);
        }

        tracing::debug!(
            domain = "kv",
            session = meta.session_id,
            response = ?std::mem::discriminant(&response),
            "KV actor returned response"
        );

        self.route_kv_response(envelope, meta, &response, request_started)
    }

    fn actor_for_session(
        &self,
        session_id: u64,
        context: &str,
    ) -> Arc<Mutex<crate::domains::kv::KvActor>> {
        self.core
            .actors
            .lock()
            .entry(session_id)
            .or_insert_with(|| {
                tracing::trace!(
                    domain = "kv",
                    session_id = session_id,
                    "Creating new KvActor instance ({context})"
                );
                Arc::new(Mutex::new(crate::domains::kv::KvActor::new(
                    self.core.store.clone(),
                )))
            })
            .clone()
    }

    fn expire_idle_transactions_for_session(&self, session_id: u64) {
        let actor = self.core.actors.lock().get(&session_id).cloned();
        if let Some(actor) = actor {
            self.remove_expired_transactions(session_id, &actor);
        }
    }

    fn remove_expired_transactions(
        &self,
        session_id: u64,
        actor: &Arc<Mutex<crate::domains::kv::KvActor>>,
    ) {
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

    fn expire_resource_lock_if_idle(&self, resource_key: &KvResourceLockKey) {
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

    fn transaction_resource(
        message: &crate::domains::kv::KvMessage,
    ) -> Option<(u64, KvResourceLockKey)> {
        use crate::domains::kv::KvMessage;
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
        Some((
            tx_id,
            KvResourceLockKey::new(
                scope.route_family.as_u64(),
                &scope.realm,
                &scope.area,
                &scope.resource,
            ),
        ))
    }

    fn touch_resource_lock(&self, session_id: u64, message: &crate::domains::kv::KvMessage) {
        let Some((tx_id, resource_key)) = Self::transaction_resource(message) else {
            return;
        };
        let mut locks = self.core.resource_locks.lock();
        if let Some(owner) = locks.get_mut(&resource_key) {
            if owner.session_id == session_id && owner.tx_id == tx_id {
                owner.last_activity = std::time::Instant::now();
            }
        }
    }

    fn handle_begin_read_write(
        &self,
        session_id: u64,
        family_id: u64,
        realm: &str,
        area: &str,
        resource: &str,
        kv_message: &crate::domains::kv::KvMessage,
    ) -> (
        crate::domains::kv::KvResponse,
        KvAdminTransactionUpdate,
        Option<(KvResourceLockKey, u64)>,
    ) {
        use crate::domains::kv::{KvError, KvResponse};

        let lock_key = KvResourceLockKey::new(family_id, realm, area, resource);
        let held_by_same_session = self.session_holds_resource_write_lock(session_id, &lock_key);
        if self
            .conflicting_session_for_resource(session_id, &lock_key)
            .is_some()
        {
            return (
                KvResponse::Error {
                    error: KvError::Conflict("resource locked by another session".to_string()),
                },
                KvAdminTransactionUpdate::None,
                None,
            );
        }
        if held_by_same_session {
            return (
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
        let response = actor.handle(kv_message.clone());
        if let KvResponse::BeginOk { tx_id } = response {
            self.core.resource_locks.lock().insert(
                lock_key,
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
                family_id,
                tx_id,
                session_id,
                realm,
                area,
                resource,
                &chrono::Utc::now().to_rfc3339(),
            );
            (
                response,
                KvAdminTransactionUpdate::Upsert(transaction),
                None,
            )
        } else {
            (response, KvAdminTransactionUpdate::None, None)
        }
    }

    fn handle_commit_frame(
        &self,
        session_id: u64,
        route_family: crate::runtime::routing::RouteFamily,
        tx_id: u64,
        kv_message: &crate::domains::kv::KvMessage,
    ) -> (
        crate::domains::kv::KvResponse,
        KvAdminTransactionUpdate,
        Option<(KvResourceLockKey, u64)>,
    ) {
        use crate::domains::kv::KvResponse;

        let actor = self.actor_for_session(session_id, "commit");
        let mut actor = actor.lock();
        tracing::trace!(
            domain = "kv",
            session_id = session_id,
            tx_id = tx_id,
            "Calling actor.handle() for COMMIT"
        );
        let mutation_count = actor.mutation_count_for_tx(tx_id).unwrap_or(0);
        let lock_key =
            actor
                .resource_scope_for_tx(tx_id)
                .map(|(family_id, realm, area, resource)| {
                    KvResourceLockKey::new(family_id, &realm, &area, &resource)
                });
        if lock_key
            .as_ref()
            .is_some_and(|key| key.family_id != route_family.as_u64())
        {
            return (
                crate::domains::kv::KvResponse::Error {
                    error: crate::domains::kv::KvError::InvalidRequest(
                        "route family mismatch".to_string(),
                    ),
                },
                KvAdminTransactionUpdate::None,
                None,
            );
        }
        let had_transaction = lock_key.is_some();
        let response = actor.handle(kv_message.clone());
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
                let notify = (mutation_count > 0).then_some((lock_key, mutation_count));
                (response, admin_update, notify)
            } else {
                (response, admin_update, None)
            }
        } else {
            crate::observability::counter_inc("fitz_kv_commits_failed_total");
            (response, admin_update, None)
        }
    }

    fn handle_rollback_frame(
        &self,
        session_id: u64,
        route_family: crate::runtime::routing::RouteFamily,
        tx_id: u64,
        kv_message: &crate::domains::kv::KvMessage,
    ) -> (
        crate::domains::kv::KvResponse,
        KvAdminTransactionUpdate,
        Option<(KvResourceLockKey, u64)>,
    ) {
        use crate::domains::kv::KvResponse;

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
            .is_some_and(|(family_id, _, _, _)| *family_id != route_family.as_u64())
        {
            return (
                crate::domains::kv::KvResponse::Error {
                    error: crate::domains::kv::KvError::InvalidRequest(
                        "route family mismatch".to_string(),
                    ),
                },
                KvAdminTransactionUpdate::None,
                None,
            );
        }
        let response = actor.handle(kv_message.clone());
        let admin_update =
            if resource_scope.is_some() && actor.resource_scope_for_tx(tx_id).is_none() {
                if let Some((family_id, realm, area, resource)) = &resource_scope {
                    self.core
                        .resource_locks
                        .lock()
                        .remove(&KvResourceLockKey::new(*family_id, realm, area, resource));
                }
                KvAdminTransactionUpdate::Remove { session_id, tx_id }
            } else {
                KvAdminTransactionUpdate::None
            };
        if let KvResponse::RollbackOk = response {
            crate::observability::counter_inc("fitz_kv_rollbacks_total");
            (response, admin_update, None)
        } else {
            (response, admin_update, None)
        }
    }

    fn handle_regular_operation_frame(
        &self,
        session_id: u64,
        message_type: u16,
        kv_message: &crate::domains::kv::KvMessage,
    ) -> (
        crate::domains::kv::KvResponse,
        KvAdminTransactionUpdate,
        Option<(KvResourceLockKey, u64)>,
    ) {
        let actor = self.actor_for_session(session_id, "other operation");
        let mut actor = actor.lock();
        tracing::trace!(
            domain = "kv",
            session_id = session_id,
            msg_type = message_type,
            "Calling actor.handle() for operation"
        );
        (
            actor.handle(kv_message.clone()),
            KvAdminTransactionUpdate::None,
            None,
        )
    }

    fn request_from_envelope(envelope: &Envelope) -> Option<KvClientRequest> {
        if let Some(request) = envelope.payload::<KvClientRequest>() {
            return Some(request.clone());
        }

        #[cfg(test)]
        {
            let frame_ctx = envelope.payload::<FrameContext>()?.clone();
            let subscriber = envelope.source().cloned().unwrap_or_else(|| {
                Self::session_inbox_address(frame_ctx.route_family, frame_ctx.session_id)
            });
            let meta = crate::runtime::ClientFrameMeta::new(
                frame_ctx.session_id,
                test_client_channel_from_protocol(frame_ctx.channel_id),
                frame_ctx.msg_type.as_u16(),
                frame_ctx.route_family,
            );
            let parsed = crate::dispatch::protocol::kv::parse_frame(
                &frame_ctx,
                &frame_ctx.payload,
                frame_ctx.route_family,
                frame_ctx.session_id,
                subscriber,
            )
            .map(|frame| match frame {
                crate::dispatch::protocol::kv::ParsedKvFrame::Op(message) => {
                    KvClientFrame::Op(message)
                }
                crate::dispatch::protocol::kv::ParsedKvFrame::Sub(message) => {
                    KvClientFrame::Sub(message)
                }
            });
            Some(KvClientRequest::new(meta, parsed))
        }

        #[cfg(not(test))]
        {
            None
        }
    }

    fn valid_request_envelope(envelope: &Envelope, meta: crate::runtime::ClientFrameMeta) -> bool {
        meta.route_family == *envelope.destination().family()
            && envelope
                .source()
                .is_none_or(|source| *source.family() == meta.route_family)
    }

    fn valid_subscription_request(
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        family_id: crate::runtime::routing::RouteFamily,
        session_id: u64,
        subscriber: &crate::runtime::routing::RouteAddress,
    ) -> bool {
        family_id == meta.route_family
            && *subscriber.family() == family_id
            && session_id == meta.session_id
            && envelope.source().is_none_or(|source| source == subscriber)
    }

    fn kv_message_family(
        message: &crate::domains::kv::KvMessage,
    ) -> crate::runtime::routing::RouteFamily {
        use crate::domains::kv::KvMessage;
        match message {
            KvMessage::Begin { scope, .. }
            | KvMessage::Commit { scope, .. }
            | KvMessage::Rollback { scope, .. }
            | KvMessage::Get { scope, .. }
            | KvMessage::Put { scope, .. }
            | KvMessage::Insert { scope, .. }
            | KvMessage::Delete { scope, .. }
            | KvMessage::DeleteRange { scope, .. }
            | KvMessage::Scan { scope, .. } => scope.route_family,
        }
    }

    fn error_response(reason: &str) -> crate::domains::kv::KvResponse {
        crate::domains::kv::KvResponse::Error {
            error: crate::domains::kv::KvError::InvalidRequest(reason.to_string()),
        }
    }

    fn response_meta_for_source(
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
    ) -> crate::runtime::ClientFrameMeta {
        envelope.source().map_or(meta, |source| {
            let mut response_meta = meta;
            response_meta.route_family = *source.family();
            response_meta
        })
    }
}

#[cfg(test)]
fn test_client_channel_from_protocol(
    channel: crate::dispatch::protocol::frame::ChannelId,
) -> crate::runtime::ClientChannel {
    match channel {
        crate::dispatch::protocol::frame::ChannelId::Control => {
            crate::runtime::ClientChannel::Control
        }
        crate::dispatch::protocol::frame::ChannelId::Pub => crate::runtime::ClientChannel::Pub,
        crate::dispatch::protocol::frame::ChannelId::Sub => crate::runtime::ClientChannel::Sub,
        crate::dispatch::protocol::frame::ChannelId::Rpc => crate::runtime::ClientChannel::Rpc,
        crate::dispatch::protocol::frame::ChannelId::Lease => crate::runtime::ClientChannel::Lease,
        crate::dispatch::protocol::frame::ChannelId::Internal => {
            crate::runtime::ClientChannel::Internal
        }
    }
}
