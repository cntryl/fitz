use super::model::{
    DeliveryError, Envelope, KvClientFrame, KvClientRequest, KvDomainActor, KvDomainCommand,
    KvDomainRuntime, KvDomainSink, KvResourceLockKey, MailboxSink, Ordering,
};
#[cfg(test)]
use crate::protocol::frame_context::FrameContext;
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
            KvDomainCommand::ApplySyncWriteOptions(message, reply) => {
                let _ = reply.send(self.state.runtime().apply_sync_write_options(message));
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
        let operation_started = Self::record_operation_start();
        let request_started = self.record_request_start();
        let parsed_frame = self.parse_request_frame(meta, request.frame, request_started)?;

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
        meta: crate::runtime::ClientFrameMeta,
        frame: Result<KvClientFrame, String>,
        request_started: Option<std::time::Instant>,
    ) -> Result<KvClientFrame, DeliveryError> {
        let parsed_frame = match frame {
            Ok(msg) => msg,
            Err(e) => {
                if let (Some(metrics), Some(started_at)) =
                    (self.core.metrics.as_ref(), request_started)
                {
                    metrics.record_failure(started_at);
                }
                tracing::warn!(
                    domain = "kv",
                    session = meta.session_id,
                    msg_type = meta.message_type,
                    error = %e,
                    "Failed to parse KV message"
                );
                return Err(DeliveryError::ActorStopped);
            }
        };

        tracing::debug!(
            domain = "kv",
            session = meta.session_id,
            channel = ?meta.channel,
            msg_type = meta.message_type,
            "Parsed KV message successfully"
        );

        Ok(parsed_frame)
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
            } => {
                let subscription_id = {
                    let mut watch_actors = self.core.watch_actors.lock();
                    let actor = watch_actors
                        .entry(family_id.as_u64())
                        .or_insert_with(|| crate::domains::kv::watch::KvWatchActor::new(family_id));
                    actor.subscribe(session_id, pattern.as_str(), subscriber)
                };
                crate::domains::kv::KvResponse::SubscribeOk { subscription_id }
            }
            crate::domains::kv::KvSubscriptionMessage::Unsubscribe {
                family_id,
                pattern,
                session_id,
                ..
            } => {
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
            }
        };

        self.refresh_metrics_gauges();
        self.route_kv_response(envelope, meta, &response, request_started)
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
        let kv_message = self.apply_sync_write_options(kv_message);
        let session_id = meta.session_id;

        tracing::trace!(
            domain = "kv",
            session_id = session_id,
            msg_type = meta.message_type,
            "KV deliver: getting or creating actor for session"
        );

        let (response, should_sync_admin_snapshot, commit_notification) = match &kv_message {
            KvMessage::Begin {
                route_family,
                realm,
                area,
                resource,
                mode,
                ..
            } if *mode == TxMode::ReadWrite => self.handle_begin_read_write(
                session_id,
                route_family.as_u64(),
                realm,
                area,
                resource,
                &kv_message,
            ),
            KvMessage::Commit { tx_id } => {
                self.handle_commit_frame(session_id, *tx_id, &kv_message)
            }
            KvMessage::Rollback { tx_id } => {
                self.handle_rollback_frame(session_id, *tx_id, &kv_message)
            }
            _ => self.handle_regular_operation_frame(session_id, meta.message_type, &kv_message),
        };
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
        if should_sync_admin_snapshot {
            self.sync_admin_snapshot();
        }
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
    ) -> parking_lot::lock_api::MappedMutexGuard<
        '_,
        parking_lot::RawMutex,
        crate::domains::kv::KvActor,
    > {
        parking_lot::MutexGuard::map(self.core.actors.lock(), |actors| {
            actors.entry(session_id).or_insert_with(|| {
                tracing::trace!(
                    domain = "kv",
                    session_id = session_id,
                    "Creating new KvActor instance ({context})"
                );
                crate::domains::kv::KvActor::new(self.core.store.clone())
            })
        })
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
        bool,
        Option<(KvResourceLockKey, u64)>,
    ) {
        use crate::domains::kv::{KvError, KvResponse};

        let lock_key = KvResourceLockKey::new(family_id, realm, area, resource);
        let held_by_same_session = self.active_transactions_for_resource(&lock_key) > 0;
        if self
            .conflicting_session_for_resource(session_id, &lock_key)
            .is_some()
        {
            return (
                KvResponse::Error {
                    error: KvError::Conflict("resource locked by another session".to_string()),
                },
                false,
                None,
            );
        }

        let log_context = if held_by_same_session {
            "BEGIN (ReadWrite)"
        } else {
            "BEGIN (ReadWrite, acquiring lock)"
        };
        let mut actor = self.actor_for_session(session_id, "begin");
        tracing::trace!(
            domain = "kv",
            session_id = session_id,
            "Calling actor.handle() for {log_context}"
        );
        let response = actor.handle(kv_message.clone());
        if let KvResponse::BeginOk { tx_id } = response {
            tracing::trace!(
                domain = "kv",
                session_id = session_id,
                tx_id = tx_id,
                "BEGIN succeeded with actor-owned transaction scope"
            );
            (response, true, None)
        } else {
            (response, false, None)
        }
    }

    fn handle_commit_frame(
        &self,
        session_id: u64,
        tx_id: u64,
        kv_message: &crate::domains::kv::KvMessage,
    ) -> (
        crate::domains::kv::KvResponse,
        bool,
        Option<(KvResourceLockKey, u64)>,
    ) {
        use crate::domains::kv::KvResponse;

        let mut actor = self.actor_for_session(session_id, "commit");
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
        let response = actor.handle(kv_message.clone());
        if let KvResponse::CommitOk = response {
            if let Some(lock_key) = lock_key {
                let notify = (mutation_count > 0).then_some((lock_key, mutation_count));
                (response, true, notify)
            } else {
                (response, true, None)
            }
        } else {
            crate::observability::counter_inc("fitz_kv_commits_failed_total");
            (response, false, None)
        }
    }

    fn handle_rollback_frame(
        &self,
        session_id: u64,
        tx_id: u64,
        kv_message: &crate::domains::kv::KvMessage,
    ) -> (
        crate::domains::kv::KvResponse,
        bool,
        Option<(KvResourceLockKey, u64)>,
    ) {
        use crate::domains::kv::KvResponse;

        let mut actor = self.actor_for_session(session_id, "rollback");
        tracing::trace!(
            domain = "kv",
            session_id = session_id,
            tx_id = tx_id,
            "Calling actor.handle() for ROLLBACK"
        );
        let response = actor.handle(kv_message.clone());
        if let KvResponse::RollbackOk = response {
            crate::observability::counter_inc("fitz_kv_rollbacks_total");
            (response, true, None)
        } else {
            (response, false, None)
        }
    }

    fn handle_regular_operation_frame(
        &self,
        session_id: u64,
        message_type: u16,
        kv_message: &crate::domains::kv::KvMessage,
    ) -> (
        crate::domains::kv::KvResponse,
        bool,
        Option<(KvResourceLockKey, u64)>,
    ) {
        let mut actor = self.actor_for_session(session_id, "other operation");
        tracing::trace!(
            domain = "kv",
            session_id = session_id,
            msg_type = message_type,
            "Calling actor.handle() for operation"
        );
        (actor.handle(kv_message.clone()), false, None)
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
            let parsed = crate::protocol::kv::parse_frame(
                &frame_ctx,
                &frame_ctx.payload,
                frame_ctx.route_family,
                frame_ctx.session_id,
                subscriber,
            )
            .map(|frame| match frame {
                crate::protocol::kv::ParsedKvFrame::Op(message) => KvClientFrame::Op(message),
                crate::protocol::kv::ParsedKvFrame::Sub(message) => KvClientFrame::Sub(message),
            });
            Some(KvClientRequest::new(meta, parsed))
        }

        #[cfg(not(test))]
        {
            None
        }
    }
}

#[cfg(test)]
fn test_client_channel_from_protocol(
    channel: crate::protocol::frame::ChannelId,
) -> crate::runtime::ClientChannel {
    match channel {
        crate::protocol::frame::ChannelId::Control => crate::runtime::ClientChannel::Control,
        crate::protocol::frame::ChannelId::Pub => crate::runtime::ClientChannel::Pub,
        crate::protocol::frame::ChannelId::Sub => crate::runtime::ClientChannel::Sub,
        crate::protocol::frame::ChannelId::Rpc => crate::runtime::ClientChannel::Rpc,
        crate::protocol::frame::ChannelId::Lease => crate::runtime::ClientChannel::Lease,
        crate::protocol::frame::ChannelId::Internal => crate::runtime::ClientChannel::Internal,
    }
}
