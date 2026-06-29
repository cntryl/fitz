use super::*;

pub(super) struct DomainFrameDispatcher<'a> {
    ingress: &'a RuntimeIngress,
}

impl RuntimeIngress {
    pub(super) fn domain_frame_dispatcher(&self) -> DomainFrameDispatcher<'_> {
        DomainFrameDispatcher { ingress: self }
    }
}

impl DomainFrameDispatcher<'_> {
    pub(super) fn dispatch_if_domain(
        &self,
        session_id: u64,
        channel_id: ChannelId,
        route_family: crate::runtime::routing::RouteFamily,
        msg_type: crate::protocol::tlv::MessageType,
        should_preserve_payload: bool,
        message_payload: &mut Option<Bytes>,
    ) -> Result<(), IngressDecision> {
        let Some(router) = &self.ingress.router else {
            return Ok(());
        };

        match Self::domain_dispatch_for_msg_type(msg_type) {
            Err(reason) => {
                warn!(
                    session_id = session_id,
                    msg_type = msg_type.as_u16(),
                    reason = reason,
                    "Ingress: client sent server-to-client-only message type"
                );
                Err(IngressDecision::Close(reason.to_string()))
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
                    preserve_payload_for_handler: should_preserve_payload,
                };
                self.authorize_and_dispatch_domain_frame(dispatch, message_payload)
            }
            Ok(None) => Ok(()),
        }
    }

    pub(super) fn domain_dispatch_for_msg_type(
        msg_type: crate::protocol::tlv::MessageType,
    ) -> Result<Option<DomainAuthorizationSpec>, &'static str> {
        crate::api::runtime_ingress::domain_registry::IngressDomainRegistry::dispatch_spec_for_msg_type(
            msg_type,
        )
    }

    fn cached_session_inbox_route(&self, session_id: u64) -> crate::runtime::routing::Route {
        self.ingress
            .session_registry()
            .cached_inbox_route(session_id)
    }

    fn unauthorized_error_code(domain: DispatchDomain) -> u16 {
        crate::api::runtime_ingress::domain_registry::IngressDomainRegistry::descriptor_for_domain(
            domain,
        )
        .unauthorized_error_code
    }

    fn encode_domain_error_body(code: u16, message: &str) -> Bytes {
        let body = crate::protocol::error_codes::encode_error_body(code, message);
        Bytes::from(body)
    }

    fn route_error_response_delivery_failure(
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
                IngressDecision::Close(format!("unauthorized response delivery failed: {error}"))
            }
        }
    }

    fn send_unauthorized_domain_response(
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

    fn derive_auth_route_for_frame<'a>(
        domain: DispatchDomain,
        msg_type: crate::protocol::tlv::MessageType,
        payload: &'a [u8],
    ) -> Result<Option<Cow<'a, str>>, String> {
        extract_auth_route_for_domain(domain, msg_type.as_u16(), payload)
    }

    fn authorize_domain_targets(
        &self,
        session_id: u64,
        msg_type: crate::protocol::tlv::MessageType,
        domain: DispatchDomain,
        access: crate::auth::Access,
        targets: &AuthorizationTargets<'_>,
    ) -> Result<(), AuthorizationFailure> {
        let Some(actor_ref) = self.ingress.session_registry().session_actor(session_id) else {
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

    fn dispatch_domain_frame(
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
        let source = crate::runtime::routing::RouteAddress::new(
            dispatch.route_family,
            self.cached_session_inbox_route(dispatch.session_id),
        );
        let descriptor =
            crate::api::runtime_ingress::domain_registry::IngressDomainRegistry::descriptor_for_domain(
                dispatch.domain,
            );
        let envelope = descriptor.build_request_envelope(
            crate::api::runtime_ingress::domain_registry::DomainEnvelopeBuildRequest {
                session_id: dispatch.session_id,
                channel_id: dispatch.channel_id,
                route_family: dispatch.route_family,
                msg_type: dispatch.msg_type,
                payload: dispatch_payload,
                source,
                destination: addr,
            },
        );
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
            Err(error) => {
                error!(
                    session_id = dispatch.session_id,
                    domain = dispatch.domain.as_str(),
                    error = %error,
                    "Ingress: router.route failed for domain dispatch"
                );
                Err(IngressDecision::Close(format!(
                    "route delivery failed: {error}"
                )))
            }
        }
    }

    fn authorize_and_dispatch_domain_frame(
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
                    "authorization parse failed: {error}"
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
                .map_or_else(|decision| decision, |()| IngressDecision::Accept),
        })?;
        self.dispatch_domain_frame(dispatch, message_payload)
    }

    pub(super) fn resolve_authorization_targets<'a>(
        domain: DispatchDomain,
        msg_type: crate::protocol::tlv::MessageType,
        payload: &'a [u8],
        policy: AuthorizationPolicy,
    ) -> Result<(AuthorizationTargets<'a>, crate::auth::Access), String> {
        match policy {
            AuthorizationPolicy::SessionOwned => Ok((
                AuthorizationTargets::SessionOwned,
                crate::auth::Access::Read,
            )),
            AuthorizationPolicy::WildcardScoped(access) => Ok((
                AuthorizationTargets::Single(Cow::Borrowed(domain.wildcard_route())),
                access,
            )),
            AuthorizationPolicy::KvBeginModeScoped => {
                let access = Self::kv_begin_access(payload)?;
                let route = Self::derive_auth_route_for_frame(domain, msg_type, payload)?
                    .ok_or_else(|| "KV BEGIN authorization route missing".to_string())?;
                Ok((AuthorizationTargets::Single(route), access))
            }
            AuthorizationPolicy::MultiRouteScoped(access) => {
                if domain != DispatchDomain::Schedule || msg_type.as_u16() != 706 {
                    return Err(
                        "multi-route authorization is only supported for schedule batch create"
                            .to_string(),
                    );
                }

                let routes = crate::protocol::schedule_codec::extract_batch_auth_routes(payload)?
                    .into_iter()
                    .map(|route| canonicalize_dispatch_route_str(domain, route))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok((AuthorizationTargets::Multiple(routes), access))
            }
            AuthorizationPolicy::RouteScoped(access) => {
                let target = Self::derive_auth_route_for_frame(domain, msg_type, payload)?
                    .map(AuthorizationTargets::Single)
                    .ok_or_else(|| {
                        format!(
                            "{} route-scoped authorization route missing",
                            domain.as_str()
                        )
                    })?;
                Ok((target, access))
            }
        }
    }

    pub(super) fn kv_begin_access(payload: &[u8]) -> Result<crate::auth::Access, String> {
        if payload.len() < 6 {
            return Err("BEGIN payload too short".to_string());
        }

        let route_len =
            u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]) as usize;
        let mode_offset = 4 + route_len;

        if mode_offset > payload.len() {
            return Err("BEGIN route overflow".to_string());
        }

        if mode_offset >= payload.len() {
            return Err("BEGIN mode byte missing".to_string());
        }

        let access = match payload[mode_offset] {
            0 => crate::auth::Access::Read,
            1 => crate::auth::Access::Write,
            _ => return Err("Invalid transaction mode".to_string()),
        };

        let durability_offset = mode_offset + 1;
        if durability_offset >= payload.len() {
            return Err("BEGIN durability byte missing".to_string());
        }

        match payload[durability_offset] {
            0 | 1 => Ok(access),
            value => Err(format!("Invalid durability mode: {value}")),
        }
    }
}
