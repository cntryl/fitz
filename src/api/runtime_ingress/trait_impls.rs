use super::{
    debug, info, obs, trace, Bytes, ChannelId, CloseReason, DispatchDomain, DomainDispatchPayload,
    Ingress, IngressDecision, RuntimeIngress, SessionEvent, SessionFrame,
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
        self.session_cleanup_coordinator().retry_pending().await;
        Ok(self.session_registry().open_session(session))
    }

    async fn on_frame(
        &self,
        session_id: u64,
        channel_id: ChannelId,
        msg_type: crate::protocol::tlv::MessageType,
        message_payload: Bytes,
    ) -> IngressDecision {
        self.session_cleanup_coordinator().retry_pending().await;

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
        self.session_cleanup_coordinator().retry_pending().await;

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
    }
}

impl RuntimeIngress {
    /// Try to derive a precise Route from the frame payload for authorization
    #[cfg_attr(not(test), allow(dead_code))]
    #[allow(clippy::too_many_lines, clippy::unused_self)]
    pub(super) fn derive_route_for_frame(
        &self,
        session_info: &SessionInfo,
        msg_type: crate::protocol::tlv::MessageType,
        payload: &Bytes,
    ) -> Result<Option<crate::runtime::routing::Route>, String> {
        use crate::protocol::frame_context::FrameContext;
        use crate::runtime::routing::Route;

        let ctx = FrameContext::new(
            session_info.session_id,
            crate::protocol::frame::ChannelId::Pub,
            msg_type,
            payload.clone(),
            session_info.route_family,
        );

        let mt = msg_type.as_u16();
        match mt {
            100..=110 => {
                if matches!(mt, 109 | 110) {
                    return crate::protocol::kv_codec::extract_auth_route(mt, payload.as_ref())
                        .and_then(|route| {
                            route
                                .map(|route| {
                                    Self::canonicalize_domain_route_str(DispatchDomain::Kv, route)
                                        .map(|canonical| Route::new(canonical.as_ref()))
                                })
                                .transpose()
                        });
                }

                // KV domain: Per CLIENT_SPEC, all operations now include route on wire
                // RouteFamily comes from the session, not from the route
                // Parse message to extract route for authorization
                match crate::protocol::kv::parse_request(
                    mt,
                    session_info.route_family,
                    payload.as_ref(),
                ) {
                    Ok(kmsg) => match kmsg {
                        crate::domains::kv::KvMessage::Begin {
                            realm,
                            area,
                            resource,
                            ..
                        } => Ok(Some(Route::new(format!("kv://{realm}/{area}/{resource}")))),
                        crate::domains::kv::KvMessage::Get { .. }
                        | crate::domains::kv::KvMessage::Put { .. }
                        | crate::domains::kv::KvMessage::Insert { .. }
                        | crate::domains::kv::KvMessage::Delete { .. }
                        | crate::domains::kv::KvMessage::DeleteRange { .. }
                        | crate::domains::kv::KvMessage::Scan { .. } => {
                            // Operations now include full route; authorization was checked at BEGIN time
                            Ok(None)
                        }
                        crate::domains::kv::KvMessage::Commit { .. }
                        | crate::domains::kv::KvMessage::Rollback { .. } => {
                            // Transaction control operations don't need re-authorization
                            Ok(None)
                        }
                    },
                    Err(e) => Err(e),
                }
            }
            500..=504 => match crate::protocol::notice_codec::parse_request(
                &ctx,
                payload.as_ref(),
                session_info.route_family,
                crate::session::SessionId(session_info.session_id),
                crate::runtime::routing::RouteAddress::new(
                    session_info.route_family,
                    Route::new(""),
                ),
            ) {
                Ok(crate::domains::notice::protocol::NotificationMessage::Publish(p)) => Ok(Some(
                    Self::canonicalize_domain_route(DispatchDomain::Notice, &p.route)?,
                )),
                Ok(crate::domains::notice::protocol::NotificationMessage::Subscribe(s)) => {
                    Ok(Some(Self::canonicalize_domain_route(
                        DispatchDomain::Notice,
                        &s.pattern,
                    )?))
                }
                Ok(_) => Ok(None),
                Err(e) => Err(e),
            },
            300..=399 => match crate::protocol::rpc_codec::parse_request(
                &ctx,
                payload.as_ref(),
                session_info.route_family,
            ) {
                Ok(crate::domains::rpc::protocol::RpcMessage::Request(r)) => {
                    Ok(Some(r.route.clone()))
                }
                Ok(
                    crate::domains::rpc::protocol::RpcMessage::RegisterWorker {
                        worker_addr, ..
                    }
                    | crate::domains::rpc::protocol::RpcMessage::UnregisterWorker { worker_addr },
                ) => Ok(Some(worker_addr.route().clone())),
                Ok(_) => Ok(None),
                Err(e) => Err(e),
            },
            200..=299 => crate::protocol::queue_codec::extract_auth_route(
                msg_type.as_u16(),
                payload.as_ref(),
            )
            .and_then(|route| {
                route
                    .map(|value| {
                        Self::canonicalize_domain_route_str(DispatchDomain::Queue, value).map(
                            |canonical| crate::runtime::routing::Route::new(canonical.as_ref()),
                        )
                    })
                    .transpose()
            }),
            400..=499 => crate::protocol::lease_codec::extract_auth_route(
                msg_type.as_u16(),
                payload.as_ref(),
            )
            .and_then(|route| {
                route
                    .map(|value| {
                        Self::canonicalize_domain_route_str(DispatchDomain::Lease, value).map(
                            |canonical| crate::runtime::routing::Route::new(canonical.as_ref()),
                        )
                    })
                    .transpose()
            }),
            600..=699 => {
                match crate::protocol::stream_codec::extract_auth_route(
                    ctx.msg_type.0,
                    payload.as_ref(),
                ) {
                    Ok(Some(route_str)) => Ok(Some(Self::canonicalize_domain_route(
                        DispatchDomain::Stream,
                        &Route::new(route_str),
                    )?)),
                    Ok(None) => Ok(None),
                    Err(e) => Err(e),
                }
            }
            700..=799 => {
                match crate::protocol::schedule_codec::parse_request(
                    &ctx,
                    payload.as_ref(),
                    session_info.route_family,
                    crate::session::SessionId(session_info.session_id),
                    crate::runtime::routing::RouteAddress::new(
                        session_info.route_family,
                        Route::new(""),
                    ),
                ) {
                    Ok(crate::domains::schedule::ScheduleMessage::Create {
                        route,
                        cron: _,
                        payload: _,
                    }) => Ok(Some(Self::canonicalize_domain_route(
                        DispatchDomain::Schedule,
                        &Route::new(route),
                    )?)),
                    Ok(crate::domains::schedule::ScheduleMessage::Subscribe { route, .. }) => {
                        Ok(Some(Self::canonicalize_domain_route(
                            DispatchDomain::Schedule,
                            &route,
                        )?))
                    }
                    Ok(crate::domains::schedule::ScheduleMessage::Unsubscribe {
                        route, ..
                    }) => Ok(Some(Self::canonicalize_domain_route(
                        DispatchDomain::Schedule,
                        &route,
                    )?)),
                    Ok(_) => Ok(None),
                    Err(e) => Err(e),
                }
            }
            _ => Ok(None),
        }
    }
}
