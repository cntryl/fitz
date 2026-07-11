use super::{
    canonicalize_dispatch_route_str, extract_auth_route_for_domain, AuthorizationFailure,
    AuthorizationPolicy, AuthorizationTargets, ChannelId, Cow, DispatchDomain,
    DomainAuthorizationSpec, DomainDispatchPayload, DomainDispatchRequest, IngressDecision,
    RuntimeIngress,
};
use crate::observability as obs;
use bytes::Bytes;
use std::time::{Duration, Instant};
use tracing::{debug, error, warn};

const DOMAIN_DISPATCH_BACKPRESSURE_POLICY: DomainDispatchBackpressurePolicy =
    DomainDispatchBackpressurePolicy {
        wait_budget: Duration::from_millis(2),
        retry_delay: Duration::from_micros(50),
    };

#[derive(Clone, Copy)]
struct DomainDispatchBackpressurePolicy {
    wait_budget: Duration,
    retry_delay: Duration,
}

impl DomainDispatchBackpressurePolicy {
    fn within_budget(self, started_at: Instant) -> bool {
        started_at.elapsed() < self.wait_budget
    }

    async fn wait_before_retry(self) {
        tokio::time::sleep(self.retry_delay).await;
    }
}

pub(super) struct DomainFrameDispatcher<'a> {
    ingress: &'a RuntimeIngress,
}

impl RuntimeIngress {
    pub(super) fn domain_frame_dispatcher(&self) -> DomainFrameDispatcher<'_> {
        DomainFrameDispatcher { ingress: self }
    }
}

impl DomainFrameDispatcher<'_> {
    fn elapsed_micros_u64(start: Instant) -> u64 {
        u64::try_from(start.elapsed().as_micros().min(u128::from(u64::MAX))).unwrap_or(u64::MAX)
    }

    pub(super) async fn dispatch_if_domain(
        &self,
        session_id: u64,
        channel_id: ChannelId,
        route_family: crate::runtime::routing::RouteFamily,
        msg_type: crate::protocol::tlv::MessageType,
        payload: DomainDispatchPayload<'_>,
    ) -> Result<(), IngressDecision> {
        let Some(router) = &self.ingress.router else {
            return Ok(());
        };

        // CONNECT is consumed by the session authenticator and is not a
        // domain message. Every other client message must be present in the
        // exact protocol manifest.
        if msg_type == crate::protocol::tlv::MessageType::CONNECT {
            return Ok(());
        }

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
                    payload,
                };
                self.authorize_and_dispatch_domain_frame(dispatch).await
            }
            Ok(None) => Err(IngressDecision::Close(format!(
                "unsupported message type: {}",
                msg_type.as_u16()
            ))),
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

    fn encode_rpc_terminal_error_payload(
        correlation_id: &uuid::Uuid,
        code: u16,
        message: &str,
    ) -> Bytes {
        let mut response_encoder = crate::protocol::payload_codec::PayloadEncoder::with_capacity(
            crate::protocol::rpc_codec::terminal_error_response_message_capacity(message),
        );
        let mut error_encoder = crate::protocol::payload_codec::PayloadEncoder::with_capacity(
            crate::protocol::rpc_codec::error_body_capacity(message),
        );
        Bytes::from(
            crate::protocol::rpc_codec::encode_terminal_error_response_message_into(
                correlation_id,
                code,
                message,
                &mut response_encoder,
                &mut error_encoder,
            ),
        )
    }

    fn send_rpc_submit_error_response(
        &self,
        dispatch: &DomainDispatchRequest<'_>,
        request_payload: &[u8],
        code: u16,
        message: &'static str,
    ) -> Result<(), IngressDecision> {
        let correlation_id = crate::protocol::rpc_codec::extract_request_correlation_id(
            request_payload,
        )
        .map_err(|error| {
            IngressDecision::Close(format!(
                "rpc submit error correlation extraction failed: {error}"
            ))
        })?;
        let payload = Self::encode_rpc_terminal_error_payload(&correlation_id, code, message);
        let response_ctx = crate::protocol::frame_context::FrameContext::new(
            dispatch.session_id,
            dispatch.channel_id,
            crate::protocol::tlv::MessageType::new(303),
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
            Self::route_error_response_delivery_failure(dispatch.session_id, dispatch.domain, error)
        })
    }

    fn send_unauthorized_domain_response(
        &self,
        dispatch: &DomainDispatchRequest<'_>,
        request_payload: &[u8],
    ) -> Result<(), IngressDecision> {
        if dispatch.domain == DispatchDomain::Rpc && dispatch.msg_type.as_u16() == 302 {
            return self.send_rpc_submit_error_response(
                dispatch,
                request_payload,
                crate::protocol::error_codes::rpc::ERR_UNAUTHORIZED,
                "unauthorized: permission denied",
            );
        }

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
            Self::route_error_response_delivery_failure(dispatch.session_id, dispatch.domain, error)
        })
    }

    fn derive_auth_route_for_frame(
        domain: DispatchDomain,
        msg_type: crate::protocol::tlv::MessageType,
        payload: &[u8],
    ) -> Result<Option<Cow<'_, str>>, String> {
        extract_auth_route_for_domain(domain, msg_type.as_u16(), payload).and_then(|route| {
            route
                .map(|route| {
                    let manifest_entry = crate::protocol::manifest::client_entry(msg_type)
                        .map_err(str::to_string)?;
                    if let Some(required_scheme) = manifest_entry.route_scheme {
                        let required_prefix = format!("{required_scheme}://");
                        if !route.starts_with(&required_prefix) {
                            return Err(format!(
                                "message {} requires {required_scheme} route scheme",
                                msg_type.as_u16()
                            ));
                        }
                    }
                    Ok(route)
                })
                .transpose()
        })
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

        let span = tracing::debug_span!(
            obs::SPAN_PERMISSION_CHECK,
            session_id = session_id,
            route = auth_target,
            route_count = auth_target_count,
            access = ?access,
        );
        let _span_guard = span.enter();
        let start = Instant::now();

        let (authorized, denied_route, denied_route_count) =
            targets.authorize(&actor_ref, access, domain.wildcard_route());

        if let Ok(collector) = std::panic::catch_unwind(crate::observability::metrics) {
            let elapsed_us = Self::elapsed_micros_u64(start);
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

    fn domain_dispatch_backpressured(error: &crate::runtime::router::RouteError) -> bool {
        matches!(
            error,
            crate::runtime::router::RouteError::DeliveryFailed(
                _,
                crate::runtime::router::DeliveryError::MailboxFull { .. }
                    | crate::runtime::router::DeliveryError::HighLaneFull { .. },
            )
        )
    }

    fn record_backpressure_retry() {
        obs::counter_inc(obs::METRIC_INGRESS_DOMAIN_BACKPRESSURE_RETRIES);
    }

    fn record_backpressure_accepted(started_at: Instant, retries: u64) {
        if retries == 0 {
            return;
        }

        obs::counter_inc(obs::METRIC_INGRESS_DOMAIN_BACKPRESSURE_ACCEPTED);
        obs::histogram_observe_us(
            obs::METRIC_INGRESS_DOMAIN_BACKPRESSURE_WAIT_LATENCY,
            Self::elapsed_micros_u64(started_at),
        );
    }

    fn record_backpressure_exhausted(started_at: Instant) {
        obs::counter_inc(obs::METRIC_INGRESS_DOMAIN_BACKPRESSURE_EXHAUSTED);
        obs::histogram_observe_us(
            obs::METRIC_INGRESS_DOMAIN_BACKPRESSURE_WAIT_LATENCY,
            Self::elapsed_micros_u64(started_at),
        );
    }

    async fn dispatch_domain_frame(
        &self,
        dispatch: DomainDispatchRequest<'_>,
    ) -> Result<(), IngressDecision> {
        let DomainDispatchRequest {
            router,
            session_id,
            channel_id,
            route_family,
            domain,
            policy: _,
            msg_type,
            payload,
        } = dispatch;
        let route = domain.inbound_route().clone();
        let addr = crate::runtime::routing::RouteAddress::new(route_family, route);
        let dispatch_payload = payload.into_dispatch_bytes();
        let source = crate::runtime::routing::RouteAddress::new(
            route_family,
            self.cached_session_inbox_route(session_id),
        );
        let descriptor =
            crate::api::runtime_ingress::domain_registry::IngressDomainRegistry::descriptor_for_domain(
                domain,
            );

        let backpressure_started_at = Instant::now();
        let mut retries = 0_u64;
        let policy = DOMAIN_DISPATCH_BACKPRESSURE_POLICY;

        loop {
            let envelope = descriptor.build_request_envelope(
                crate::api::runtime_ingress::domain_registry::DomainEnvelopeBuildRequest {
                    session_id,
                    channel_id,
                    route_family,
                    msg_type,
                    payload: dispatch_payload.clone(),
                    source: source.clone(),
                    destination: addr.clone(),
                },
            );

            debug!(
                session_id = session_id,
                domain = domain.as_str(),
                msg_type = msg_type.as_u16(),
                route = %addr,
                source = ?source,
                "Ingress: routing envelope to domain"
            );

            let dispatch_start = Instant::now();
            let dispatch_result = router.route_to_domain(domain.as_str(), envelope);
            if let Ok(collector) = std::panic::catch_unwind(crate::observability::metrics) {
                collector.histogram_observe_us(
                    obs::METRIC_INGRESS_DOMAIN_DISPATCH_LATENCY,
                    Self::elapsed_micros_u64(dispatch_start),
                );
            }

            match dispatch_result {
                Ok(()) => {
                    Self::record_backpressure_accepted(backpressure_started_at, retries);
                    return Ok(());
                }
                Err(error)
                    if Self::domain_dispatch_backpressured(&error)
                        && policy.within_budget(backpressure_started_at) =>
                {
                    retries = retries.saturating_add(1);
                    Self::record_backpressure_retry();
                    policy.wait_before_retry().await;
                }
                Err(error) if Self::domain_dispatch_backpressured(&error) => {
                    Self::record_backpressure_exhausted(backpressure_started_at);
                    warn!(
                        session_id = session_id,
                        domain = domain.as_str(),
                        retries = retries,
                        waited_us = Self::elapsed_micros_u64(backpressure_started_at),
                        "Ingress: domain dispatch backpressure"
                    );
                    return Err(IngressDecision::Backpressure);
                }
                Err(error) => {
                    error!(
                        session_id = session_id,
                        domain = domain.as_str(),
                        error = %error,
                        "Ingress: router.route failed for domain dispatch"
                    );
                    return Err(IngressDecision::Close(format!(
                        "route delivery failed: {error}"
                    )));
                }
            }
        }
    }

    async fn authorize_and_dispatch_domain_frame(
        &self,
        dispatch: DomainDispatchRequest<'_>,
    ) -> Result<(), IngressDecision> {
        debug!(
            session_id = dispatch.session_id,
            msg_type = dispatch.msg_type.as_u16(),
            domain = dispatch.domain.as_str(),
            "Ingress: resolved domain for msg_type"
        );

        let auth_route_start = Instant::now();
        let targets = match Self::resolve_authorization_targets(
            dispatch.domain,
            dispatch.msg_type,
            dispatch.payload.as_bytes(),
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
                if dispatch.domain == DispatchDomain::Rpc && dispatch.msg_type.as_u16() == 302 {
                    return self.send_rpc_submit_error_response(
                        &dispatch,
                        dispatch.payload.as_bytes(),
                        crate::protocol::error_codes::rpc::ERR_BACKEND_ERROR,
                        "RPC request parse failed",
                    );
                }
                return Err(IngressDecision::Close(format!(
                    "authorization parse failed: {error}"
                )));
            }
        };
        let (targets, access) = targets;

        if let Ok(collector) = std::panic::catch_unwind(crate::observability::metrics) {
            collector.histogram_observe_us(
                obs::METRIC_INGRESS_AUTH_ROUTE_LATENCY,
                Self::elapsed_micros_u64(auth_route_start),
            );
        }

        if let Err(failure) = self.authorize_domain_targets(
            dispatch.session_id,
            dispatch.msg_type,
            dispatch.domain,
            access,
            &targets,
        ) {
            return Err(match failure {
                AuthorizationFailure::MissingSessionActor => {
                    IngressDecision::Close("unauthorized: session actor missing".to_string())
                }
                AuthorizationFailure::PermissionDenied => self
                    .send_unauthorized_domain_response(&dispatch, dispatch.payload.as_bytes())
                    .map_or_else(|decision| decision, |()| IngressDecision::Accept),
            });
        }

        self.dispatch_domain_frame(dispatch).await
    }

    pub(super) fn resolve_authorization_targets(
        domain: DispatchDomain,
        msg_type: crate::protocol::tlv::MessageType,
        payload: &[u8],
        policy: AuthorizationPolicy,
    ) -> Result<(AuthorizationTargets<'_>, crate::auth::Access), String> {
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
