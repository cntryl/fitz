use super::*;

impl Default for RuntimeIngress {
    fn default() -> Self {
        Self::new(true) // Default: auth required
    }
}

#[async_trait::async_trait]
impl Ingress for RuntimeIngress {
    async fn on_open(&self, session: SessionInfo) -> Result<u64, String> {
        self.retry_pending_session_cleanups().await;

        let session_id = session.session_id;

        // Record session opened counter
        if let Ok(collector) = std::panic::catch_unwind(crate::observability::metrics) {
            collector.counter_inc(obs::METRIC_SESSIONS_CREATED);
        }

        info!(
            session_id = session_id,
            transport = %session.transport_kind,
            peer_addr = ?session.peer_addr,
            authenticated = session.authenticated,
            "Ingress: session opened"
        );

        self.sessions.insert(session_id, session.clone());
        self.session_inbox_routes.insert(
            session_id,
            crate::runtime::routing::Route::new(format!("inbox://session/{session_id}")),
        );
        if let Some(admin_read_model) = &self.admin_read_model {
            admin_read_model.record_session_open(&session);
        }

        // Create a per-session SessionActor with permissions
        // When auth is not required, grant all permissions to unauthenticated sessions
        let permissions = if self.auth_required {
            session.permissions_snapshot.clone()
        } else {
            SessionPermissions::all()
        };

        self.session_actors.insert(
            session_id,
            crate::session::actor::SessionActor::new(
                crate::session::session::SessionId(session_id),
                permissions,
            ),
        );

        if let Some(handler) = &self.event_handler {
            handler(SessionEvent::Open(session_id, session));
        }

        Ok(session_id)
    }

    async fn on_frame(
        &self,
        session_id: u64,
        channel_id: ChannelId,
        msg_type: crate::protocol::tlv::MessageType,
        message_payload: Bytes,
    ) -> IngressDecision {
        self.retry_pending_session_cleanups().await;

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
        let mut message_payload = Some(message_payload);

        // Auth gating: if session is not authenticated, only allow CONNECT control messages
        // and verify JWTs before taking the map write guard.
        let mut notify_frame: Option<SessionFrame> = None;
        let needs_authentication = match self.sessions.get(&session_id) {
            Some(entry) => !entry.authenticated,
            None => {
                warn!(
                    session_id = session_id,
                    "Ingress: frame for unknown session"
                );
                return IngressDecision::Close(format!("unknown session: {}", session_id));
            }
        };
        let verified_auth = if needs_authentication && self.auth_required {
            if channel_id != ChannelId::Control
                || msg_type != crate::protocol::tlv::MessageType::CONNECT
            {
                warn!(session_id = session_id, channel = ?channel_id, msg_type = msg_type.as_u16(), "Ingress: unauthenticated, CONNECT required");
                return IngressDecision::Close("unauthenticated: connect required".to_string());
            }

            let compact = std::str::from_utf8(message_payload.as_ref().unwrap())
                .unwrap_or("")
                .to_string();
            debug!(
                session_id = session_id,
                jwt_len = compact.len(),
                "Ingress: verifying CONNECT JWT"
            );

            let auth_config = self
                .auth_config
                .clone()
                .unwrap_or_else(|| crate::auth::AuthConfig::from_env(true));

            match crate::auth::verified_jwt_with_claims_config(
                &compact,
                &auth_config,
                &self.auth_claims_config,
            )
            .await
            {
                Ok(verified) => {
                    let route_family =
                        match self.resolve_authenticated_route_family(&verified.raw_claims) {
                            Ok(route_family) => route_family,
                            Err(e) => {
                                error!(
                                    session_id = session_id,
                                    error = %e,
                                    "Ingress: CONNECT failed (route family resolution)"
                                );
                                return IngressDecision::Close(format!("connect failed: {}", e));
                            }
                        };
                    Some((verified.permissions, verified.claims, route_family))
                }
                Err(e) => {
                    error!(
                        session_id = session_id,
                        error = %e,
                        "Ingress: CONNECT failed (verification)"
                    );
                    return IngressDecision::Close(format!("connect failed: {}", e));
                }
            }
        } else {
            None
        };

        let route_family = {
            let Some(mut entry) = self.sessions.get_mut(&session_id) else {
                warn!(
                    session_id = session_id,
                    "Ingress: frame for unknown session"
                );
                return IngressDecision::Close(format!("unknown session: {}", session_id));
            };
            if !entry.authenticated {
                if self.auth_required {
                    let Some((snapshot, claims, route_family)) = verified_auth else {
                        return IngressDecision::Close(
                            "connect failed: session authentication state changed".to_string(),
                        );
                    };
                    self.apply_authenticated_session(
                        session_id,
                        &mut entry,
                        claims,
                        snapshot,
                        route_family,
                    );
                    if should_notify_handler {
                        notify_frame = Some(SessionFrame {
                            session_id,
                            channel_id,
                            payload: message_payload.as_ref().unwrap().clone(),
                        });
                    }
                } else {
                    // If auth is not required, grant full anonymous access
                    let snapshot = crate::auth::default_anonymous_permissions();
                    entry.permissions_snapshot = snapshot.clone();
                    entry.authenticated = true;
                    entry.route_family = crate::runtime::routing::RouteFamily::new(1);

                    self.session_actors.insert(
                        session_id,
                        crate::session::actor::SessionActor::new(
                            crate::session::session::SessionId(session_id),
                            snapshot,
                        ),
                    );

                    if should_notify_handler {
                        notify_frame = Some(SessionFrame {
                            session_id,
                            channel_id,
                            payload: message_payload.as_ref().unwrap().clone(),
                        });
                    }
                } // Close else block for auth_required check
            }
            entry.route_family
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

        // Dispatch to router if configured (domain dispatch)
        if let Some(router) = &self.router {
            match Self::domain_dispatch_for_msg_type(msg_type) {
                Err(reason) => {
                    warn!(
                        session_id = session_id,
                        msg_type = msg_type.as_u16(),
                        reason = reason,
                        "Ingress: client sent server-to-client-only message type"
                    );
                    return IngressDecision::Close(reason.to_string());
                }
                Ok(Some(spec)) => {
                    let dispatch = DomainDispatchRequest {
                        router,
                        session_id,
                        channel_id,
                        route_family,
                        domain: spec.domain,
                        policy: spec.policy,
                        msg_type,
                        preserve_payload_for_handler: should_notify_handler
                            && notify_frame.is_none(),
                    };
                    if let Err(decision) =
                        self.authorize_and_dispatch_domain_frame(dispatch, &mut message_payload)
                    {
                        return decision;
                    }
                }
                Ok(None) => {}
            }
        }

        // Notify handler if present (if we haven't already notified via `notify_frame`)
        if should_notify_handler && notify_frame.is_none() {
            if let Some(handler) = &self.event_handler {
                handler(SessionEvent::Frame(SessionFrame {
                    session_id,
                    channel_id,
                    payload: message_payload.take().unwrap(),
                }));
            }
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
        self.sessions
            .get(&session_id)
            .map(|session| session.route_family)
    }

    fn record_frame_received(&self, session_id: u64) {
        if let Some(session) = self.sessions.get(&session_id) {
            session.record_frame_received();
        }
    }

    fn record_frame_sent(&self, session_id: u64) {
        if let Some(session) = self.sessions.get(&session_id) {
            session.record_frame_sent();
        }
    }

    async fn on_close(&self, session_id: u64, reason: CloseReason) {
        self.retry_pending_session_cleanups().await;

        // Record session closed counter
        if let Ok(collector) = std::panic::catch_unwind(crate::observability::metrics) {
            collector.counter_inc(obs::METRIC_SESSIONS_CLOSED);
        }

        info!(session_id = session_id, reason = %reason, "Ingress: session closing");

        let route_family = self.sessions.get(&session_id).map(|s| s.route_family);

        // Dispatch cleanup to all subscribable domains before removing session state.
        // This ensures lock/subscription cleanup has completed before tests or callers
        // observe a decreased session count.
        if let (Some(router), Some(route_family)) = (&self.router, route_family) {
            let router = router.clone();
            match tokio::task::spawn_blocking(move || {
                dispatch_session_cleanup(router.as_ref(), route_family, session_id)
            })
            .await
            {
                Ok(failed_domains) => {
                    if failed_domains.is_empty() {
                        tracing::debug!(
                            session_id = session_id,
                            route_family = route_family.id(),
                            "Ingress: dispatched cleanup to KV, Notice, RPC, Stream, Schedule, Lease, and Queue domains"
                        );
                    } else {
                        self.record_cleanup_failure(
                            session_id,
                            route_family,
                            &failed_domains,
                            true,
                        );
                    }
                }
                Err(e) => {
                    self.pending_session_cleanups
                        .insert(session_id, PendingSessionCleanup { route_family });
                    tracing::warn!(
                        session_id = session_id,
                        route_family = route_family.id(),
                        error = %e,
                        "Ingress: cleanup worker task failed"
                    );
                }
            }
        }

        // Remove session state after domain cleanup completes.
        self.finalize_session_close(session_id);

        // Notify handler if present
        if let Some(handler) = &self.event_handler {
            handler(SessionEvent::Close(session_id, reason));
        }
    }
}

impl RuntimeIngress {
    /// Try to derive a precise Route from the frame payload for authorization
    #[cfg_attr(not(test), allow(dead_code))]
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
                        } => Ok(Some(Route::new(format!(
                            "kv://{}/{}/{}",
                            realm, area, resource
                        )))),
                        crate::domains::kv::KvMessage::Get {
                            route_family: _,
                            resource: _,
                            ..
                        }
                        | crate::domains::kv::KvMessage::Put {
                            route_family: _,
                            resource: _,
                            ..
                        }
                        | crate::domains::kv::KvMessage::Insert {
                            route_family: _,
                            resource: _,
                            ..
                        }
                        | crate::domains::kv::KvMessage::Delete {
                            route_family: _,
                            resource: _,
                            ..
                        }
                        | crate::domains::kv::KvMessage::DeleteRange {
                            route_family: _,
                            resource: _,
                            ..
                        }
                        | crate::domains::kv::KvMessage::Scan {
                            route_family: _,
                            resource: _,
                            ..
                        } => {
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
                    Self::canonicalize_domain_route(DispatchDomain::Notice, p.route.clone())?,
                )),
                Ok(crate::domains::notice::protocol::NotificationMessage::Subscribe(s)) => {
                    Ok(Some(Self::canonicalize_domain_route(
                        DispatchDomain::Notice,
                        s.pattern.clone(),
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
                Ok(crate::domains::rpc::protocol::RpcMessage::RegisterWorker { worker_addr }) => {
                    Ok(Some(worker_addr.route().clone()))
                }
                Ok(crate::domains::rpc::protocol::RpcMessage::UnregisterWorker { worker_addr }) => {
                    Ok(Some(worker_addr.route().clone()))
                }
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
                        Route::new(route_str),
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
                        Route::new(route),
                    )?)),
                    Ok(crate::domains::schedule::ScheduleMessage::Subscribe { route, .. }) => {
                        Ok(Some(Self::canonicalize_domain_route(
                            DispatchDomain::Schedule,
                            route.clone(),
                        )?))
                    }
                    Ok(crate::domains::schedule::ScheduleMessage::Unsubscribe {
                        route, ..
                    }) => Ok(Some(Self::canonicalize_domain_route(
                        DispatchDomain::Schedule,
                        route.clone(),
                    )?)),
                    Ok(_) => Ok(None),
                    Err(e) => Err(e),
                }
            }
            _ => Ok(None),
        }
    }
}
