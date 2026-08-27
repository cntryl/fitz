use super::{
    debug, extract_auth_route_for_domain, info, obs, trace, Bytes, ChannelId, CloseReason,
    DomainDispatchPayload, Ingress, IngressDecision, RuntimeIngress, SessionEvent, SessionFrame,
};
use crate::session::SessionInfo;

impl Default for RuntimeIngress {
    fn default() -> Self {
        Self::new(true) // Default: auth required
    }
}

#[async_trait::async_trait]
impl Ingress for RuntimeIngress {
    async fn on_open(&self, session: SessionInfo) -> Result<u64, String> {
        Ok(self.session_registry().open_session(session))
    }

    async fn on_frame(
        &self,
        session_id: u64,
        channel_id: ChannelId,
        msg_type: crate::protocol::tlv::MessageType,
        message_payload: Bytes,
    ) -> IngressDecision {
        let _ingress_latency =
            crate::observability::ScopedHistogramUs::new(obs::METRIC_INGRESS_FRAME_TOTAL_LATENCY);
        // Record frame received counter
        if let Ok(collector) = std::panic::catch_unwind(crate::observability::metrics) {
            collector.counter_inc(obs::METRIC_FRAMES_RECEIVED);
        }

        debug!(
            session_id = session_id,
            channel = ?channel_id,
            msg_type = msg_type.as_u16(),
            payload_len = message_payload.len(),
            "Ingress on_frame: enter"
        );

        let should_notify_handler = self.event_handler.is_some();

        let (route_family, notify_frame) = {
            match self
                .session_authenticator()
                .authenticate_frame(
                    session_id,
                    channel_id,
                    msg_type,
                    &message_payload,
                    should_notify_handler,
                )
                .await
            {
                Ok(authenticated) => authenticated,
                Err(decision) => return decision,
            }
        };

        if let Some(frame) = &notify_frame {
            debug!(
                session_id = session_id,
                "Ingress: auth completed, notifying frame handler"
            );
            if let Some(handler) = &self.event_handler {
                handler(SessionEvent::Frame(frame.clone()));
            }
            // We've performed auth as a side-effect (anonymous or JWT on any frame)
            // and should continue processing the current message.
        }

        let should_notify_handler_late = should_notify_handler && notify_frame.is_none();
        if should_notify_handler_late {
            if let Err(decision) = self
                .domain_frame_dispatcher()
                .dispatch_if_domain(
                    session_id,
                    channel_id,
                    route_family,
                    msg_type,
                    DomainDispatchPayload::Shared(&message_payload),
                )
                .await
            {
                return decision;
            }

            if let Some(handler) = &self.event_handler {
                handler(SessionEvent::Frame(SessionFrame {
                    session_id,
                    channel_id,
                    payload: message_payload,
                }));
            }
        } else if let Err(decision) = self
            .domain_frame_dispatcher()
            .dispatch_if_domain(
                session_id,
                channel_id,
                route_family,
                msg_type,
                DomainDispatchPayload::Owned(message_payload),
            )
            .await
        {
            return decision;
        }

        trace!(
            session_id = session_id,
            msg_type = msg_type.as_u16(),
            "Ingress: returning Accept"
        );
        IngressDecision::Accept
    }

    fn get_session_info(&self, session_id: u64) -> Option<SessionInfo> {
        self.get_session(session_id)
    }

    fn get_route_family(&self, session_id: u64) -> Option<crate::runtime::routing::RouteFamily> {
        self.session_registry().route_family(session_id)
    }

    fn record_frame_received(&self, session_id: u64) {
        self.session_registry().record_frame_received(session_id);
    }

    fn record_frame_sent(&self, session_id: u64) {
        self.session_registry().record_frame_sent(session_id);
    }

    async fn on_close(&self, session_id: u64, reason: CloseReason) {
        if self.closing_sessions.insert(session_id, ()).is_some() {
            return;
        }
        if self.session_registry().session(session_id).is_none() {
            self.closing_sessions.remove(&session_id);
            return;
        }

        // Record session closed counter
        if let Ok(collector) = std::panic::catch_unwind(crate::observability::metrics) {
            collector.counter_inc(obs::METRIC_SESSIONS_CLOSED);
        }

        info!(session_id = session_id, reason = %reason, "Ingress: session closing");

        let route_family = self.session_registry().route_family(session_id);
        self.session_cleanup_coordinator()
            .cleanup_on_close(session_id, route_family)
            .await;

        self.session_registry().finalize_close(session_id);

        // Notify handler if present
        if let Some(handler) = &self.event_handler {
            handler(SessionEvent::Close(session_id, reason));
        }
        self.closing_sessions.remove(&session_id);
    }
}

impl RuntimeIngress {
    /// Try to derive a precise Route from the frame payload for authorization
    #[cfg_attr(not(test), allow(dead_code))]
    #[allow(clippy::unused_self)]
    pub(super) fn derive_route_for_frame(
        &self,
        _session_info: &SessionInfo,
        msg_type: crate::protocol::tlv::MessageType,
        payload: &Bytes,
    ) -> Result<Option<crate::runtime::routing::Route>, String> {
        let Some(spec) =
            RuntimeIngress::domain_dispatch_for_msg_type(msg_type).map_err(str::to_string)?
        else {
            return Ok(None);
        };

        extract_auth_route_for_domain(spec.domain, msg_type.as_u16(), payload.as_ref())
            .map(|route| route.map(|route| crate::runtime::routing::Route::new(route.as_ref())))
    }
}
