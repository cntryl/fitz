use super::super::model::{DeliveryError, Envelope, KvDomainRuntime, KvResourceLockKey};
#[cfg(test)]
use super::test_channels::test_protocol_channel_from_client;
#[cfg(test)]
use crate::protocol::frame_context::FrameContext;

impl KvDomainRuntime<'_> {
    fn kv_route_for_lock(resource_key: &KvResourceLockKey) -> crate::runtime::routing::Route {
        crate::runtime::routing::Route::new(format!(
            "kv://{}/{}/{}",
            resource_key.realm, resource_key.area, resource_key.resource
        ))
    }

    fn route_kv_notify_to_subscription(
        &self,
        session_id: u64,
        subscription_id: u64,
        subscriber: &crate::runtime::routing::RouteAddress,
        route: &crate::runtime::routing::Route,
        mutation_count: u64,
    ) {
        #[cfg(test)]
        {
            let payload = crate::protocol::kv::encode_notify(
                subscription_id,
                route,
                crate::domains::kv::KvNotification { mutation_count },
            );
            let notify_ctx = FrameContext::new(
                session_id,
                crate::protocol::frame::ChannelId::Sub,
                crate::protocol::tlv::MessageType::new(crate::protocol::kv::msg_type::NOTIFY),
                bytes::Bytes::from(payload),
                *subscriber.family(),
            );
            let notify_envelope = Envelope::new(subscriber.clone(), notify_ctx);
            if self.core.router.route(notify_envelope).is_err() {
                crate::observability::counter_inc("fitz_kv_notify_drops_total");
            }
        }

        #[cfg(not(test))]
        {
            let notification = crate::domains::kv::KvClientNotification::new(
                session_id,
                *subscriber.family(),
                subscription_id,
                route.clone(),
                crate::domains::kv::KvNotification { mutation_count },
            );
            let notify_envelope = Envelope::new(subscriber.clone(), notification);
            if self.core.router.route(notify_envelope).is_err() {
                crate::observability::counter_inc("fitz_kv_notify_drops_total");
            }
        }
    }

    pub(in crate::domains::kv::sink) fn route_kv_notification(
        &self,
        resource_key: &KvResourceLockKey,
        mutation_count: u64,
    ) {
        let route = Self::kv_route_for_lock(resource_key);
        let watch_targets = {
            let watch_actors = self.core.watch_actors.lock();
            watch_actors
                .get(&resource_key.family_id)
                .map_or_else(Vec::new, |actor| actor.matching_targets(&route))
        };
        for target in watch_targets {
            self.route_kv_notify_to_subscription(
                target.session_id,
                target.subscription_id,
                &target.subscriber,
                &route,
                mutation_count,
            );
        }
    }

    pub(in crate::domains::kv::sink) fn route_kv_response(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        response: &crate::domains::kv::KvResponse,
        request_started: Option<std::time::Instant>,
    ) -> Result<(), DeliveryError> {
        #[cfg(test)]
        let response_ctx = {
            let response_bytes = crate::protocol::kv::encode_response(response);
            tracing::trace!(
                domain = "kv",
                session = meta.session_id,
                response_len = response_bytes.len(),
                "KV response encoded"
            );

            FrameContext::new(
                meta.session_id,
                test_protocol_channel_from_client(meta.channel),
                crate::protocol::tlv::MessageType::new(meta.message_type),
                bytes::Bytes::from(response_bytes),
                meta.route_family,
            )
        };

        #[cfg(not(test))]
        let response_ctx = crate::domains::kv::KvClientResponse::new(meta, response.clone());

        let Some(response_envelope) = envelope.try_reply_to(response_ctx) else {
            if let (Some(metrics), Some(started_at)) = (self.core.metrics.as_ref(), request_started)
            {
                if matches!(response, crate::domains::kv::KvResponse::Error { .. }) {
                    metrics.record_failure(started_at);
                } else {
                    metrics.record_success(started_at);
                }
            }
            tracing::warn!(
                domain = "kv",
                session = meta.session_id,
                "Cannot route response: envelope has no source address"
            );
            return Ok(());
        };

        match self.core.router.route(response_envelope) {
            Ok(()) => {
                if let (Some(metrics), Some(started_at)) =
                    (self.core.metrics.as_ref(), request_started)
                {
                    if matches!(response, crate::domains::kv::KvResponse::Error { .. }) {
                        metrics.record_failure(started_at);
                    } else {
                        metrics.record_success(started_at);
                    }
                }
                tracing::debug!(
                    domain = "kv",
                    session = meta.session_id,
                    "KV message handled and response routed"
                );
                Ok(())
            }
            Err(error) => {
                if let (Some(metrics), Some(started_at)) =
                    (self.core.metrics.as_ref(), request_started)
                {
                    metrics.record_failure(started_at);
                }
                tracing::warn!(
                    domain = "kv",
                    session = meta.session_id,
                    error = ?error,
                    "Failed to route response"
                );
                Err(DeliveryError::ActorStopped)
            }
        }
    }

    /// Remove all live KV transaction state owned by a disconnected session.
    ///
    /// This is the authoritative boundary for session-scoped cleanup: open
    /// transactions are dropped, resource locks are released, and the admin read
    /// model is refreshed so no durable recovery is implied.
    pub(in crate::domains::kv::sink) fn cleanup_session(&self, session_id: u64) {
        self.core.actors.lock().remove(&session_id);

        {
            let mut watch_actors = self.core.watch_actors.lock();
            for actor in watch_actors.values_mut() {
                actor.remove_session(session_id);
            }
            watch_actors.retain(|_, actor| !actor.is_empty());
        }

        tracing::debug!(
            domain = "kv",
            session = session_id,
            "All KV transactions and resource locks released for session (disconnect cleanup)"
        );
        self.sync_admin_snapshot();
    }

    pub(in crate::domains::kv::sink) fn active_transaction_count(&self) -> usize {
        self.core
            .actors
            .lock()
            .values()
            .map(crate::domains::kv::KvActor::transaction_count)
            .sum()
    }

    pub(in crate::domains::kv::sink) fn apply_sync_write_options(
        &self,
        message: crate::domains::kv::KvMessage,
    ) -> crate::domains::kv::KvMessage {
        match message {
            crate::domains::kv::KvMessage::Begin {
                route_family,
                realm,
                area,
                resource,
                mode,
                write_options,
            } if write_options.is_sync() => crate::domains::kv::KvMessage::Begin {
                route_family,
                realm,
                area,
                resource,
                mode,
                write_options: self.core.sync_write_options,
            },
            message => message,
        }
    }
}
