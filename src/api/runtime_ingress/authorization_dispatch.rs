use super::*;

impl RuntimeIngress {
    pub(super) fn unauthorized_error_code(domain: DispatchDomain) -> u16 {
        match domain {
            DispatchDomain::Kv => crate::protocol::error_codes::kv::ERR_UNAUTHORIZED,
            DispatchDomain::Queue => crate::protocol::error_codes::queue::ERR_UNAUTHORIZED,
            DispatchDomain::Rpc => crate::protocol::error_codes::rpc::ERR_UNAUTHORIZED,
            DispatchDomain::Lease => crate::protocol::error_codes::lease::ERR_UNAUTHORIZED,
            DispatchDomain::Notice => crate::protocol::error_codes::notice::ERR_UNAUTHORIZED,
            DispatchDomain::Stream => crate::protocol::error_codes::stream::ERR_UNAUTHORIZED,
            DispatchDomain::Schedule => crate::protocol::error_codes::schedule::ERR_UNAUTHORIZED,
        }
    }

    pub(super) fn encode_domain_error_body(code: u16, message: &str) -> Bytes {
        let body = crate::protocol::error_codes::encode_error_body(code, message);
        Bytes::from(body)
    }

    pub(super) fn route_error_response_delivery_failure(
        &self,
        session_id: u64,
        domain: DispatchDomain,
        error: crate::runtime::router::RouteError,
    ) -> IngressDecision {
        match error {
            crate::runtime::router::RouteError::DeliveryFailed(
                _,
                crate::runtime::router::DeliveryError::MailboxFull { .. }
                | crate::runtime::router::DeliveryError::HighLaneFull { .. },
            ) => {
                warn!(
                    session_id = session_id,
                    domain = domain.as_str(),
                    "Ingress: unauthorized response backpressure"
                );
                IngressDecision::Backpressure
            }
            error => {
                error!(
                    session_id = session_id,
                    domain = domain.as_str(),
                    error = %error,
                    "Ingress: unauthorized response delivery failed"
                );
                IngressDecision::Close(format!("unauthorized response delivery failed: {}", error))
            }
        }
    }

    pub(super) fn send_unauthorized_domain_response(
        &self,
        dispatch: &DomainDispatchRequest<'_>,
    ) -> Result<(), IngressDecision> {
        let payload = Self::encode_domain_error_body(
            Self::unauthorized_error_code(dispatch.domain),
            "unauthorized: permission denied",
        );
        let response_ctx = crate::protocol::frame_context::FrameContext::new(
            dispatch.session_id,
            dispatch.channel_id,
            dispatch.msg_type,
            payload,
            dispatch.route_family,
        );
        let source = crate::runtime::routing::RouteAddress::new(
            dispatch.route_family,
            dispatch.domain.inbound_route().clone(),
        );
        let destination = crate::runtime::routing::RouteAddress::new(
            dispatch.route_family,
            self.cached_session_inbox_route(dispatch.session_id),
        );
        let envelope =
            crate::runtime::envelope::Envelope::from_route(source, destination, response_ctx);

        dispatch.router.route(envelope).map_err(|error| {
            self.route_error_response_delivery_failure(dispatch.session_id, dispatch.domain, error)
        })
    }

    pub(super) fn derive_auth_route_for_frame<'a>(
        domain: DispatchDomain,
        msg_type: crate::protocol::tlv::MessageType,
        payload: &'a [u8],
    ) -> Result<Option<Cow<'a, str>>, String> {
        extract_auth_route_for_domain(domain, msg_type.as_u16(), payload)
    }

    pub(super) fn authorize_domain_targets(
        &self,
        session_id: u64,
        msg_type: crate::protocol::tlv::MessageType,
        domain: DispatchDomain,
        access: crate::auth::Access,
        targets: &AuthorizationTargets<'_>,
    ) -> Result<(), AuthorizationFailure> {
        let Some(actor_ref) = self.get_session_actor(session_id) else {
            warn!(
                session_id = session_id,
                "Ingress: missing session actor for authorization"
            );
            return Err(AuthorizationFailure::MissingSessionActor);
        };

        let (auth_target, auth_target_count) = targets.span_target();

        let _span = tracing::debug_span!(
            obs::SPAN_PERMISSION_CHECK,
            session_id = session_id,
            route = auth_target,
            route_count = auth_target_count,
            access = ?access,
        );
        let _guard = _span.enter();
        let start = Instant::now();

        let (authorized, denied_route, denied_route_count) =
            targets.authorize(&actor_ref, access, domain.wildcard_route());

        if let Ok(collector) = std::panic::catch_unwind(crate::observability::metrics) {
            let elapsed_us = start.elapsed().as_micros() as u64;
            collector.histogram_observe_us(obs::METRIC_PERMISSION_CHECK_LATENCY, elapsed_us);
        }

        if !authorized {
            warn!(
                session_id = session_id,
                msg_type = msg_type.as_u16(),
                route = denied_route,
                route_count = denied_route_count,
                access = ?access,
                "Ingress: authorization DENIED"
            );

            if let Ok(collector) = std::panic::catch_unwind(crate::observability::metrics) {
                collector.counter_inc(obs::METRIC_AUTH_FAILURES);
            }

            return Err(AuthorizationFailure::PermissionDenied);
        }

        Ok(())
    }

    pub(super) fn dispatch_domain_frame(
        &self,
        dispatch: DomainDispatchRequest<'_>,
        message_payload: &mut Option<Bytes>,
    ) -> Result<(), IngressDecision> {
        let route = dispatch.domain.inbound_route().clone();
        let addr = crate::runtime::routing::RouteAddress::new(dispatch.route_family, route);
        let dispatch_payload = if dispatch.preserve_payload_for_handler {
            message_payload.as_ref().unwrap().clone()
        } else {
            message_payload.take().unwrap()
        };
        let ctx = crate::protocol::frame_context::FrameContext::new(
            dispatch.session_id,
            dispatch.channel_id,
            dispatch.msg_type,
            dispatch_payload,
            dispatch.route_family,
        );
        let source = crate::runtime::routing::RouteAddress::new(
            dispatch.route_family,
            self.cached_session_inbox_route(dispatch.session_id),
        );
        let envelope = match dispatch.domain {
            DispatchDomain::Kv => {
                let meta = crate::runtime::ClientFrameMeta::new(
                    dispatch.session_id,
                    crate::api::frame_adapter::client_channel_from_protocol(dispatch.channel_id),
                    dispatch.msg_type.as_u16(),
                    dispatch.route_family,
                );
                let parsed = crate::protocol::kv::parse_frame(
                    &ctx,
                    &ctx.payload,
                    dispatch.route_family,
                    dispatch.session_id,
                    source.clone(),
                )
                .map(|frame| match frame {
                    crate::protocol::kv::ParsedKvFrame::Op(message) => {
                        crate::domains::kv::KvClientFrame::Op(message)
                    }
                    crate::protocol::kv::ParsedKvFrame::Sub(message) => {
                        crate::domains::kv::KvClientFrame::Sub(message)
                    }
                });
                let request = crate::domains::kv::KvClientRequest::new(meta, parsed);
                crate::runtime::envelope::Envelope::from_route(source, addr, request)
            }
            DispatchDomain::Lease => {
                let meta = crate::runtime::ClientFrameMeta::new(
                    dispatch.session_id,
                    crate::api::frame_adapter::client_channel_from_protocol(dispatch.channel_id),
                    dispatch.msg_type.as_u16(),
                    dispatch.route_family,
                );
                let parsed = crate::protocol::lease_codec::parse_frame(
                    &ctx,
                    &ctx.payload,
                    dispatch.route_family,
                    dispatch.session_id,
                    source.clone(),
                )
                .map(|frame| match frame {
                    crate::protocol::lease_codec::ParsedLeaseFrame::Op(message) => {
                        crate::domains::lease::LeaseClientFrame::Op(message)
                    }
                    crate::protocol::lease_codec::ParsedLeaseFrame::Sub(message) => {
                        crate::domains::lease::LeaseClientFrame::Sub(message)
                    }
                });
                let request = crate::domains::lease::LeaseClientRequest::new(meta, parsed);
                crate::runtime::envelope::Envelope::from_route(source, addr, request)
            }
            DispatchDomain::Notice => {
                let meta = crate::runtime::ClientFrameMeta::new(
                    dispatch.session_id,
                    crate::api::frame_adapter::client_channel_from_protocol(dispatch.channel_id),
                    dispatch.msg_type.as_u16(),
                    dispatch.route_family,
                );
                let parsed = crate::protocol::notice_codec::parse_request(
                    &ctx,
                    &ctx.payload,
                    dispatch.route_family,
                    crate::session::SessionId(dispatch.session_id),
                    source.clone(),
                );
                let request = crate::domains::notice::NoticeClientRequest::new(meta, parsed);
                crate::runtime::envelope::Envelope::from_route(source, addr, request)
            }
            DispatchDomain::Schedule => {
                let meta = crate::runtime::ClientFrameMeta::new(
                    dispatch.session_id,
                    crate::api::frame_adapter::client_channel_from_protocol(dispatch.channel_id),
                    dispatch.msg_type.as_u16(),
                    dispatch.route_family,
                );
                let parsed = crate::protocol::schedule_codec::parse_request(
                    &ctx,
                    &ctx.payload,
                    dispatch.route_family,
                    crate::session::SessionId(dispatch.session_id),
                    source.clone(),
                );
                let request = crate::domains::schedule::ScheduleClientRequest::new(meta, parsed);
                crate::runtime::envelope::Envelope::from_route(source, addr, request)
            }
            DispatchDomain::Stream => {
                let meta = crate::runtime::ClientFrameMeta::new(
                    dispatch.session_id,
                    crate::api::frame_adapter::client_channel_from_protocol(dispatch.channel_id),
                    dispatch.msg_type.as_u16(),
                    dispatch.route_family,
                );
                let parsed = crate::protocol::stream_codec::parse_request(
                    &ctx,
                    &ctx.payload,
                    dispatch.route_family,
                    crate::session::SessionId(dispatch.session_id),
                    source.clone(),
                );
                let request = crate::domains::stream::StreamClientRequest::new(meta, parsed);
                crate::runtime::envelope::Envelope::from_route(source, addr, request)
            }
            DispatchDomain::Queue => {
                let meta = crate::runtime::ClientFrameMeta::new(
                    dispatch.session_id,
                    crate::api::frame_adapter::client_channel_from_protocol(dispatch.channel_id),
                    dispatch.msg_type.as_u16(),
                    dispatch.route_family,
                );
                let parsed = crate::protocol::queue_codec::parse_frame(
                    &ctx,
                    &ctx.payload,
                    dispatch.route_family,
                    dispatch.session_id,
                    source.clone(),
                )
                .map(|frame| match frame {
                    crate::protocol::queue_codec::ParsedQueueFrame::Op(message) => {
                        crate::domains::queue::QueueClientFrame::Op(message)
                    }
                    crate::protocol::queue_codec::ParsedQueueFrame::Sub(message) => {
                        crate::domains::queue::QueueClientFrame::Sub(message)
                    }
                });
                let request = crate::domains::queue::QueueClientRequest::new(meta, parsed);
                crate::runtime::envelope::Envelope::from_route(source, addr, request)
            }
            DispatchDomain::Rpc => {
                let meta = crate::runtime::ClientFrameMeta::new(
                    dispatch.session_id,
                    crate::api::frame_adapter::client_channel_from_protocol(dispatch.channel_id),
                    dispatch.msg_type.as_u16(),
                    dispatch.route_family,
                );
                let parsed = crate::protocol::rpc_codec::parse_request(
                    &ctx,
                    &ctx.payload,
                    dispatch.route_family,
                );
                let request = crate::domains::rpc::RpcClientRequest::new(meta, parsed);
                crate::runtime::envelope::Envelope::from_route(source, addr, request)
            }
        };
        debug!(
            session_id = dispatch.session_id,
            domain = dispatch.domain.as_str(),
            msg_type = dispatch.msg_type.as_u16(),
            route = %envelope.destination(),
            source = ?envelope.source(),
            "Ingress: routing envelope to domain"
        );

        let dispatch_start = Instant::now();
        let dispatch_result = dispatch
            .router
            .route_to_domain(dispatch.domain.as_str(), envelope);
        if let Ok(collector) = std::panic::catch_unwind(crate::observability::metrics) {
            collector.histogram_observe_us(
                obs::METRIC_INGRESS_DOMAIN_DISPATCH_LATENCY,
                dispatch_start.elapsed().as_micros() as u64,
            );
        }

        match dispatch_result {
            Ok(()) => Ok(()),
            Err(crate::runtime::router::RouteError::DeliveryFailed(
                _,
                crate::runtime::router::DeliveryError::MailboxFull { .. }
                | crate::runtime::router::DeliveryError::HighLaneFull { .. },
            )) => {
                warn!(
                    session_id = dispatch.session_id,
                    domain = dispatch.domain.as_str(),
                    "Ingress: domain dispatch backpressure"
                );
                Err(IngressDecision::Backpressure)
            }
            Err(e) => {
                error!(
                    session_id = dispatch.session_id,
                    domain = dispatch.domain.as_str(),
                    error = %e,
                    "Ingress: router.route failed for domain dispatch"
                );
                Err(IngressDecision::Close(format!(
                    "route delivery failed: {}",
                    e
                )))
            }
        }
    }

    pub(super) fn authorize_and_dispatch_domain_frame(
        &self,
        dispatch: DomainDispatchRequest<'_>,
        message_payload: &mut Option<Bytes>,
    ) -> Result<(), IngressDecision> {
        debug!(
            session_id = dispatch.session_id,
            msg_type = dispatch.msg_type.as_u16(),
            domain = dispatch.domain.as_str(),
            "Ingress: resolved domain for msg_type"
        );

        let payload_ref = message_payload.as_deref().unwrap();
        let auth_route_start = Instant::now();
        let targets = match Self::resolve_authorization_targets(
            dispatch.domain,
            dispatch.msg_type,
            payload_ref,
            dispatch.policy,
        ) {
            Ok((targets, access)) => (targets, access),
            Err(error) => {
                warn!(
                    session_id = dispatch.session_id,
                    error = %error,
                    domain = dispatch.domain.as_str(),
                    "Ingress: failed to derive route for authorization"
                );
                return Err(IngressDecision::Close(format!(
                    "authorization parse failed: {}",
                    error
                )));
            }
        };
        let (targets, access) = targets;

        if let Ok(collector) = std::panic::catch_unwind(crate::observability::metrics) {
            collector.histogram_observe_us(
                obs::METRIC_INGRESS_AUTH_ROUTE_LATENCY,
                auth_route_start.elapsed().as_micros() as u64,
            );
        }

        self.authorize_domain_targets(
            dispatch.session_id,
            dispatch.msg_type,
            dispatch.domain,
            access,
            &targets,
        )
        .map_err(|failure| match failure {
            AuthorizationFailure::MissingSessionActor => {
                IngressDecision::Close("unauthorized: session actor missing".to_string())
            }
            AuthorizationFailure::PermissionDenied => self
                .send_unauthorized_domain_response(&dispatch)
                .map(|()| IngressDecision::Accept)
                .unwrap_or_else(|decision| decision),
        })?;
        self.dispatch_domain_frame(dispatch, message_payload)
    }
}
