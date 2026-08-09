use super::{
    subscription_limit_error, Arc, DeliveryError, Envelope, Instant, NoticeDomainCore,
    NoticeMetrics, NoticeSubscription, Ordering, RoutedSubscriptionSet,
};
#[cfg(test)]
use super::{test_client_channel_from_protocol, test_protocol_channel_from_client, FrameContext};

impl NoticeDomainCore {
    pub(super) fn deliver_envelope(&self, envelope: &Envelope) -> Result<(), DeliveryError> {
        if self.handle_cleanup_envelope(envelope) {
            return Ok(());
        }
        self.ensure_active()?;

        if self.handle_domain_publish_envelope(envelope) {
            return Ok(());
        }

        Self::log_delivery(envelope);

        let Some(request) = Self::extract_request(envelope)? else {
            return Ok(());
        };
        let meta = request.meta;
        let request_started = self.record_request_start();

        if !Self::valid_request_envelope(envelope, meta) {
            self.reject_with(envelope, meta, "route family mismatch", request_started);
            return Ok(());
        }

        Self::log_parse_start(meta);

        let Some(notice_msg) =
            self.parse_notice_message(envelope, meta, request.message, request_started)
        else {
            return Ok(());
        };

        if !Self::valid_notice_message(envelope, meta, &notice_msg) {
            self.reject_with(envelope, meta, "route family mismatch", request_started);
            return Ok(());
        }

        let (response_opt, should_sync_admin_snapshot) = self.dispatch_notice_message(notice_msg);
        if should_sync_admin_snapshot {
            self.mark_admin_snapshot_dirty();
        }

        if let Some(response) = response_opt {
            self.route_notice_response(envelope, meta, &response, request_started);
        } else if let (Some(metrics), Some(started_at)) = (self.metrics.as_ref(), request_started) {
            metrics.record_success(started_at);
        }

        Ok(())
    }

    fn handle_cleanup_envelope(&self, envelope: &Envelope) -> bool {
        if let Some(cleanup) = envelope.payload::<crate::runtime::SessionCleanup>() {
            self.unsubscribe_all_for_session(cleanup.session_id);
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

    fn handle_domain_publish_envelope(&self, envelope: &Envelope) -> bool {
        if let Some(event) = envelope.payload::<crate::runtime::DomainPublishEvent>() {
            if *envelope.destination().family() != event.family_id {
                self.counter_add("fitz_notice_publish_family_mismatch_total", 1);
                return true;
            }
            self.handle_domain_publish(event);
            return true;
        }

        false
    }

    fn log_delivery(envelope: &Envelope) {
        tracing::debug!(
            domain = "notice",
            destination = %envelope.destination(),
            source = ?envelope.source(),
            "Notice domain sink: received envelope"
        );
    }

    fn extract_request(
        envelope: &Envelope,
    ) -> Result<Option<crate::domains::notice::NoticeClientRequest>, DeliveryError> {
        if let Some(request) = Self::request_from_envelope(envelope) {
            Ok(Some(request))
        } else {
            tracing::warn!(
                domain = "notice",
                "Envelope payload was not NoticeClientRequest"
            );
            Err(DeliveryError::ActorStopped)
        }
    }

    fn record_request_start(&self) -> Option<Instant> {
        self.metrics
            .as_ref()
            .map(NoticeMetrics::record_request_start)
    }

    fn reject_with(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        reason: &str,
        request_started: Option<Instant>,
    ) {
        let response = Self::error_response(reason);
        let response_meta = Self::response_meta_for_source(envelope, meta);
        self.route_notice_response(envelope, response_meta, &response, request_started);
    }

    fn log_parse_start(meta: crate::runtime::ClientFrameMeta) {
        tracing::debug!(
            domain = "notice",
            session = meta.session_id,
            msg_type = meta.message_type,
            "Notice: parsing request"
        );
    }

    fn parse_notice_message(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        message: Result<crate::domains::notice::protocol::NotificationMessage, String>,
        request_started: Option<Instant>,
    ) -> Option<crate::domains::notice::protocol::NotificationMessage> {
        match message {
            Ok(message) => Some(message),
            Err(error) => {
                tracing::warn!(domain = "notice", error = %error, "Failed to parse notice message");
                self.reject_with(envelope, meta, &error, request_started);
                None
            }
        }
    }

    fn dispatch_notice_message(
        &self,
        notice_msg: crate::domains::notice::protocol::NotificationMessage,
    ) -> (Option<crate::domains::notice::NoticeResponse>, bool) {
        use crate::domains::notice::protocol::NotificationMessage;
        use crate::domains::notice::NoticeResponse;

        match notice_msg {
            NotificationMessage::Publish(pub_msg) => {
                self.publish_route_payload(pub_msg.family_id, &pub_msg.route, &pub_msg.payload);
                (None, false)
            }
            NotificationMessage::Subscribe(sub_msg) => self.handle_subscribe_message(&sub_msg),
            NotificationMessage::Unsubscribe(unsub_msg) => {
                let family_id = unsub_msg.family_id;
                let mut families = self.families.lock();
                let removed = if let Some(state) = families.get_mut(&family_id) {
                    let removed = state.remove_subscription_for_session(
                        unsub_msg.family_id,
                        unsub_msg.session_id.0,
                        unsub_msg.subscription_id,
                    );
                    if state.is_empty() {
                        families.remove(&family_id);
                    }
                    removed
                } else {
                    false
                };
                if removed {
                    self.counter_add("fitz_notice_unsubscribes_total", 1);
                }
                (Some(NoticeResponse::Ok), removed)
            }
            NotificationMessage::UnsubscribeAll(unsub_all) => {
                let session_id = unsub_all.session_id.0;
                let removed = self.unsubscribe_all_for_session(session_id);
                tracing::debug!(
                    domain = "notice",
                    session = session_id,
                    "All subscriptions removed for session"
                );
                (Some(NoticeResponse::Ok), removed > 0)
            }
            NotificationMessage::Deliver(_) => (Some(NoticeResponse::Ok), false),
        }
    }

    fn handle_subscribe_message(
        &self,
        sub_msg: &crate::domains::notice::protocol::SubscribeMessage,
    ) -> (Option<crate::domains::notice::NoticeResponse>, bool) {
        if let Some(response) = self.try_reuse_existing(sub_msg) {
            return (Some(response), false);
        }
        let compiled = match Self::compile_pattern(sub_msg) {
            Ok(compiled) => compiled,
            Err(response) => return (Some(response), false),
        };
        let (response, state_changed) = self.allocate_and_insert(sub_msg, compiled);
        (Some(response), state_changed)
    }

    fn compile_pattern(
        sub_msg: &crate::domains::notice::protocol::SubscribeMessage,
    ) -> Result<crate::runtime::matcher::Pattern, crate::domains::notice::NoticeResponse> {
        crate::runtime::DomainKind::Notice
            .descriptor()
            .compile_registration_pattern(sub_msg.pattern.as_str())
            .map_err(|error| {
                tracing::warn!(
                    domain = "notice",
                    session = sub_msg.session_id.0,
                    "Rejected invalid subscription pattern"
                );
                crate::domains::notice::NoticeResponse::Error(error)
            })
    }

    fn try_reuse_existing(
        &self,
        sub_msg: &crate::domains::notice::protocol::SubscribeMessage,
    ) -> Option<crate::domains::notice::NoticeResponse> {
        let families = self.families.lock();
        let id = families.get(&sub_msg.family_id).and_then(|state| {
            state.find_existing_id(sub_msg.session_id.0, sub_msg.pattern.as_str())
        })?;
        tracing::debug!(
            domain = "notice",
            session = sub_msg.session_id.0,
            subscription_id = id,
            pattern = sub_msg.pattern.as_str(),
            "Notice subscription already exists (idempotent)"
        );
        Some(crate::domains::notice::NoticeResponse::SubscribeOk {
            subscription_id: id,
        })
    }

    fn allocate_and_insert(
        &self,
        sub_msg: &crate::domains::notice::protocol::SubscribeMessage,
        compiled: crate::runtime::matcher::Pattern,
    ) -> (crate::domains::notice::NoticeResponse, bool) {
        use crate::domains::notice::NoticeResponse;

        let mut families = self.families.lock();
        let session_subscription_count = families
            .values()
            .map(|state| state.subscription_count_for_session(sub_msg.session_id.0))
            .sum::<usize>();
        let state = families
            .entry(sub_msg.family_id)
            .or_insert_with(RoutedSubscriptionSet::new);

        let (response, state_changed) = if let Some(error) =
            subscription_limit_error(state, session_subscription_count, sub_msg, &compiled)
        {
            (error, false)
        } else {
            let Ok(new_id) =
                self.next_sub_id
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                        current.checked_add(1)
                    })
            else {
                let state_empty = state.is_empty();
                if state_empty {
                    families.remove(&sub_msg.family_id);
                }
                return (
                    NoticeResponse::Error("subscription ID space exhausted".to_string()),
                    false,
                );
            };
            state.insert(
                sub_msg.family_id,
                NoticeSubscription {
                    pattern: compiled,
                    pattern_route: Arc::from(sub_msg.pattern.as_str()),
                    session_id: sub_msg.session_id.0,
                    subscription_id: new_id,
                    subscriber: sub_msg.subscriber.clone(),
                },
            );

            tracing::debug!(
                domain = "notice",
                session = sub_msg.session_id.0,
                subscription_id = new_id,
                pattern = sub_msg.pattern.as_str(),
                "Notice subscription added"
            );
            (
                NoticeResponse::SubscribeOk {
                    subscription_id: new_id,
                },
                true,
            )
        };

        (response, state_changed)
    }

    fn valid_request_envelope(envelope: &Envelope, meta: crate::runtime::ClientFrameMeta) -> bool {
        meta.route_family == *envelope.destination().family()
            && envelope
                .source()
                .is_none_or(|source| *source.family() == meta.route_family)
    }

    fn valid_notice_message(
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        message: &crate::domains::notice::protocol::NotificationMessage,
    ) -> bool {
        use crate::domains::notice::protocol::NotificationMessage;

        match message {
            NotificationMessage::Publish(publish) => publish.family_id == meta.route_family,
            NotificationMessage::Subscribe(subscribe) => {
                subscribe.family_id == meta.route_family
                    && subscribe.session_id.0 == meta.session_id
                    && *subscribe.subscriber.family() == subscribe.family_id
                    && envelope
                        .source()
                        .is_none_or(|source| source == &subscribe.subscriber)
            }
            NotificationMessage::Unsubscribe(unsubscribe) => {
                unsubscribe.family_id == meta.route_family
                    && unsubscribe.session_id.0 == meta.session_id
            }
            NotificationMessage::UnsubscribeAll(unsubscribe_all) => {
                unsubscribe_all.session_id.0 == meta.session_id
                    && *unsubscribe_all.subscriber.family() == meta.route_family
                    && envelope
                        .source()
                        .is_none_or(|source| source == &unsubscribe_all.subscriber)
            }
            NotificationMessage::Deliver(_) => false,
        }
    }

    fn error_response(reason: &str) -> crate::domains::notice::NoticeResponse {
        crate::domains::notice::NoticeResponse::Error(reason.to_string())
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

    fn route_notice_response(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        response: &crate::domains::notice::NoticeResponse,
        request_started: Option<Instant>,
    ) {
        #[cfg(test)]
        let response_ctx = {
            let mut payload_encoder =
                crate::dispatch::protocol::payload_codec::PayloadEncoder::with_capacity(256);
            let response_bytes = crate::dispatch::protocol::notice_codec::encode_response_into(
                response,
                &mut payload_encoder,
            );
            FrameContext::new(
                meta.session_id,
                test_protocol_channel_from_client(meta.channel),
                crate::dispatch::protocol::tlv::MessageType::new(meta.message_type),
                bytes::Bytes::from(response_bytes),
                meta.route_family,
            )
        };

        #[cfg(not(test))]
        let response_ctx =
            crate::domains::notice::NoticeClientResponse::new(meta, response.clone());

        if let Some(response_envelope) = envelope.try_reply_to(response_ctx) {
            if let Err(error) = self.router.route(response_envelope) {
                if let Some(metrics) = self.metrics.as_ref() {
                    metrics.record_response_drop();
                } else {
                    crate::observability::counter_inc(
                        crate::domains::notice::metrics::METRIC_RESPONSE_DROPS_TOTAL,
                    );
                }
                tracing::warn!(
                    domain = "notice",
                    session_id = meta.session_id,
                    route_family = meta.route_family.as_u64(),
                    error = %error,
                    "Dropped best-effort Notice response"
                );
            }
        }

        if let (Some(metrics), Some(started_at)) = (self.metrics.as_ref(), request_started) {
            if response.is_failure() {
                metrics.record_failure(started_at);
            } else {
                metrics.record_success(started_at);
            }
        }
    }

    fn request_from_envelope(
        envelope: &Envelope,
    ) -> Option<crate::domains::notice::NoticeClientRequest> {
        if let Some(request) = envelope.payload::<crate::domains::notice::NoticeClientRequest>() {
            return Some(request.clone());
        }

        #[cfg(test)]
        {
            let frame_ctx = envelope.payload::<FrameContext>()?.clone();
            let subscriber = envelope.source().cloned().unwrap_or_else(|| {
                crate::runtime::routing::RouteAddress::new(
                    *envelope.destination().family(),
                    crate::runtime::routing::Route::new(format!(
                        "inbox://session/{}",
                        frame_ctx.session_id
                    )),
                )
            });
            let meta = crate::runtime::ClientFrameMeta::new(
                frame_ctx.session_id,
                test_client_channel_from_protocol(frame_ctx.channel_id),
                frame_ctx.msg_type.as_u16(),
                frame_ctx.route_family,
            );
            let parsed = crate::dispatch::protocol::notice_codec::parse_request(
                &frame_ctx,
                &frame_ctx.payload,
                *envelope.destination().family(),
                crate::session::SessionId(frame_ctx.session_id),
                subscriber,
            );
            Some(crate::domains::notice::NoticeClientRequest::new(
                meta, parsed,
            ))
        }

        #[cfg(not(test))]
        {
            None
        }
    }
}
