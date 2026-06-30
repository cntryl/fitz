use super::{AuthorizationPolicy, Bytes, ChannelId, DispatchDomain, DomainAuthorizationSpec};

type AuthRouteExtractor = for<'a> fn(u16, &'a [u8]) -> Result<Option<&'a str>, String>;
type MessagePolicyResolver = fn(u16) -> Result<Option<AuthorizationPolicy>, &'static str>;
type RequestEnvelopeBuilder = fn(DomainEnvelopeBuildRequest) -> crate::runtime::Envelope;

pub(crate) struct IngressDomainDescriptor {
    pub(super) manifest: &'static crate::runtime::DomainDescriptor,
    pub(super) unauthorized_error_code: u16,
    message_policy: MessagePolicyResolver,
    extract_auth_route: AuthRouteExtractor,
    build_request_envelope: RequestEnvelopeBuilder,
}

pub(crate) struct DomainEnvelopeBuildRequest {
    pub(crate) session_id: u64,
    pub(crate) channel_id: ChannelId,
    pub(crate) route_family: crate::runtime::routing::RouteFamily,
    pub(crate) msg_type: crate::protocol::tlv::MessageType,
    pub(crate) payload: Bytes,
    pub(crate) source: crate::runtime::routing::RouteAddress,
    pub(crate) destination: crate::runtime::routing::RouteAddress,
}

pub(crate) struct IngressDomainRegistry;

impl IngressDomainDescriptor {
    pub(super) fn kind(&self) -> DispatchDomain {
        self.manifest.kind
    }

    pub(super) fn extract_auth_route<'a>(
        &self,
        msg_type: u16,
        payload: &'a [u8],
    ) -> Result<Option<&'a str>, String> {
        (self.extract_auth_route)(msg_type, payload)
    }

    pub(crate) fn build_request_envelope(
        &self,
        request: DomainEnvelopeBuildRequest,
    ) -> crate::runtime::Envelope {
        (self.build_request_envelope)(request)
    }
}

impl IngressDomainRegistry {
    pub(super) fn all() -> &'static [IngressDomainDescriptor; 7] {
        &INGRESS_DOMAIN_DESCRIPTORS
    }

    pub(super) fn descriptor_for_domain(
        domain: DispatchDomain,
    ) -> &'static IngressDomainDescriptor {
        match domain {
            DispatchDomain::Kv => &INGRESS_DOMAIN_DESCRIPTORS[0],
            DispatchDomain::Queue => &INGRESS_DOMAIN_DESCRIPTORS[1],
            DispatchDomain::Notice => &INGRESS_DOMAIN_DESCRIPTORS[2],
            DispatchDomain::Stream => &INGRESS_DOMAIN_DESCRIPTORS[3],
            DispatchDomain::Rpc => &INGRESS_DOMAIN_DESCRIPTORS[4],
            DispatchDomain::Lease => &INGRESS_DOMAIN_DESCRIPTORS[5],
            DispatchDomain::Schedule => &INGRESS_DOMAIN_DESCRIPTORS[6],
        }
    }

    pub(super) fn dispatch_spec_for_msg_type(
        msg_type: crate::protocol::tlv::MessageType,
    ) -> Result<Option<DomainAuthorizationSpec>, &'static str> {
        let msg_type = msg_type.as_u16();

        for descriptor in Self::all() {
            if let Some(policy) = (descriptor.message_policy)(msg_type)? {
                return Ok(Some(DomainAuthorizationSpec {
                    domain: descriptor.kind(),
                    policy,
                }));
            }
        }

        Ok(None)
    }

    pub(crate) fn descriptor_for_msg_type(
        msg_type: crate::protocol::tlv::MessageType,
    ) -> Result<Option<&'static IngressDomainDescriptor>, &'static str> {
        let msg_type = msg_type.as_u16();

        for descriptor in Self::all() {
            if (descriptor.message_policy)(msg_type)?.is_some() {
                return Ok(Some(descriptor));
            }
        }

        Ok(None)
    }
}

static INGRESS_DOMAIN_DESCRIPTORS: [IngressDomainDescriptor; 7] = [
    IngressDomainDescriptor {
        manifest: crate::runtime::DomainKind::Kv.descriptor(),
        unauthorized_error_code: crate::protocol::error_codes::kv::ERR_UNAUTHORIZED,
        message_policy: kv_message_policy,
        extract_auth_route: crate::protocol::kv_codec::extract_auth_route,
        build_request_envelope: build_kv_request_envelope,
    },
    IngressDomainDescriptor {
        manifest: crate::runtime::DomainKind::Queue.descriptor(),
        unauthorized_error_code: crate::protocol::error_codes::queue::ERR_UNAUTHORIZED,
        message_policy: queue_message_policy,
        extract_auth_route: crate::protocol::queue_codec::extract_auth_route,
        build_request_envelope: build_queue_request_envelope,
    },
    IngressDomainDescriptor {
        manifest: crate::runtime::DomainKind::Notice.descriptor(),
        unauthorized_error_code: crate::protocol::error_codes::notice::ERR_UNAUTHORIZED,
        message_policy: notice_message_policy,
        extract_auth_route: crate::protocol::notice_codec::extract_auth_route,
        build_request_envelope: build_notice_request_envelope,
    },
    IngressDomainDescriptor {
        manifest: crate::runtime::DomainKind::Stream.descriptor(),
        unauthorized_error_code: crate::protocol::error_codes::stream::ERR_UNAUTHORIZED,
        message_policy: stream_message_policy,
        extract_auth_route: crate::protocol::stream_codec::extract_auth_route,
        build_request_envelope: build_stream_request_envelope,
    },
    IngressDomainDescriptor {
        manifest: crate::runtime::DomainKind::Rpc.descriptor(),
        unauthorized_error_code: crate::protocol::error_codes::rpc::ERR_UNAUTHORIZED,
        message_policy: rpc_message_policy,
        extract_auth_route: crate::protocol::rpc_codec::extract_auth_route,
        build_request_envelope: build_rpc_request_envelope,
    },
    IngressDomainDescriptor {
        manifest: crate::runtime::DomainKind::Lease.descriptor(),
        unauthorized_error_code: crate::protocol::error_codes::lease::ERR_UNAUTHORIZED,
        message_policy: lease_message_policy,
        extract_auth_route: crate::protocol::lease_codec::extract_auth_route,
        build_request_envelope: build_lease_request_envelope,
    },
    IngressDomainDescriptor {
        manifest: crate::runtime::DomainKind::Schedule.descriptor(),
        unauthorized_error_code: crate::protocol::error_codes::schedule::ERR_UNAUTHORIZED,
        message_policy: schedule_message_policy,
        extract_auth_route: crate::protocol::schedule_codec::extract_auth_route,
        build_request_envelope: build_schedule_request_envelope,
    },
];

fn kv_message_policy(msg_type: u16) -> Result<Option<AuthorizationPolicy>, &'static str> {
    use crate::auth::Access;

    match msg_type {
        100 => Ok(Some(AuthorizationPolicy::KvBeginModeScoped)),
        101..=108 => Ok(Some(AuthorizationPolicy::SessionOwned)),
        109 | 110 => Ok(Some(AuthorizationPolicy::RouteScoped(Access::Read))),
        111 => Err("invalid message type: 111 is server-to-client only"),
        _ => Ok(None),
    }
}

fn queue_message_policy(msg_type: u16) -> Result<Option<AuthorizationPolicy>, &'static str> {
    use crate::auth::Access;

    match msg_type {
        200..=204 => Ok(Some(AuthorizationPolicy::RouteScoped(Access::Write))),
        207 | 208 => Ok(Some(AuthorizationPolicy::RouteScoped(Access::Read))),
        209 => Err("invalid message type: 209 is server-to-client only"),
        205 | 206 | 210..=299 => Err("invalid message type: unsupported queue operation"),
        _ => Ok(None),
    }
}

fn rpc_message_policy(msg_type: u16) -> Result<Option<AuthorizationPolicy>, &'static str> {
    use crate::auth::Access;

    match msg_type {
        300 | 301 => Ok(Some(AuthorizationPolicy::RouteScoped(Access::All))),
        302 => Ok(Some(AuthorizationPolicy::RouteScoped(Access::Write))),
        303 | 304 => Ok(Some(AuthorizationPolicy::SessionOwned)),
        305 if msg_type == 305 => Err("invalid message type: 305 is server-to-client only"),
        305..=399 => Ok(Some(AuthorizationPolicy::RouteScoped(Access::Read))),
        _ => Ok(None),
    }
}

fn lease_message_policy(msg_type: u16) -> Result<Option<AuthorizationPolicy>, &'static str> {
    use crate::auth::Access;

    match msg_type {
        400..=402 => Ok(Some(AuthorizationPolicy::RouteScoped(Access::Write))),
        403 | 407 | 408 => Ok(Some(AuthorizationPolicy::RouteScoped(Access::Read))),
        409 => Err("invalid message type: 409 is server-to-client only"),
        404..=406 | 410..=499 => Err("invalid message type: unsupported lease operation"),
        _ => Ok(None),
    }
}

fn notice_message_policy(msg_type: u16) -> Result<Option<AuthorizationPolicy>, &'static str> {
    use crate::auth::Access;

    match msg_type {
        500 => Ok(Some(AuthorizationPolicy::RouteScoped(Access::Write))),
        501 => Ok(Some(AuthorizationPolicy::RouteScoped(Access::Read))),
        502 | 503 => Ok(Some(AuthorizationPolicy::SessionOwned)),
        504 => Err("invalid message type: 504 is server-to-client only"),
        505..=599 => Err("invalid message type: 505-599 are unsupported notice operations"),
        _ => Ok(None),
    }
}

fn stream_message_policy(msg_type: u16) -> Result<Option<AuthorizationPolicy>, &'static str> {
    use crate::auth::Access;

    match msg_type {
        600 => Ok(Some(AuthorizationPolicy::RouteScoped(Access::Write))),
        601..=603 => Ok(Some(AuthorizationPolicy::SessionOwned)),
        604..=608 => Ok(Some(AuthorizationPolicy::RouteScoped(Access::Read))),
        609 => Err("invalid message type: 609 is server-to-client only"),
        _ => Ok(None),
    }
}

fn schedule_message_policy(msg_type: u16) -> Result<Option<AuthorizationPolicy>, &'static str> {
    use crate::auth::Access;

    match msg_type {
        700 | 701 => Ok(Some(AuthorizationPolicy::RouteScoped(Access::Write))),
        706 => Ok(Some(AuthorizationPolicy::MultiRouteScoped(Access::Write))),
        702 => Ok(Some(AuthorizationPolicy::WildcardScoped(Access::Read))),
        703 | 704 => Ok(Some(AuthorizationPolicy::RouteScoped(Access::Read))),
        705 => Err("invalid message type: 705 is server-to-client only"),
        _ => Ok(None),
    }
}

fn frame_context(request: &DomainEnvelopeBuildRequest) -> crate::protocol::FrameContext {
    crate::protocol::frame_context::FrameContext::new(
        request.session_id,
        request.channel_id,
        request.msg_type,
        request.payload.clone(),
        request.route_family,
    )
}

fn client_frame_meta(request: &DomainEnvelopeBuildRequest) -> crate::runtime::ClientFrameMeta {
    crate::runtime::ClientFrameMeta::new(
        request.session_id,
        crate::api::frame_adapter::client_channel_from_protocol(request.channel_id),
        request.msg_type.as_u16(),
        request.route_family,
    )
}

fn build_kv_request_envelope(request: DomainEnvelopeBuildRequest) -> crate::runtime::Envelope {
    let ctx = frame_context(&request);
    let meta = client_frame_meta(&request);
    let parsed = crate::protocol::kv::parse_frame(
        &ctx,
        &ctx.payload,
        request.route_family,
        request.session_id,
        request.source.clone(),
    )
    .map(|frame| match frame {
        crate::protocol::kv::ParsedKvFrame::Op(message) => {
            crate::domains::kv::KvClientFrame::Op(message)
        }
        crate::protocol::kv::ParsedKvFrame::Sub(message) => {
            crate::domains::kv::KvClientFrame::Sub(message)
        }
    });
    let client_request = crate::domains::kv::KvClientRequest::new(meta, parsed);
    crate::runtime::Envelope::from_route(request.source, request.destination, client_request)
}

fn build_queue_request_envelope(request: DomainEnvelopeBuildRequest) -> crate::runtime::Envelope {
    let ctx = frame_context(&request);
    let meta = client_frame_meta(&request);
    let parsed = crate::protocol::queue_codec::parse_frame(
        &ctx,
        &ctx.payload,
        request.route_family,
        request.session_id,
        request.source.clone(),
    )
    .map(|frame| match frame {
        crate::protocol::queue_codec::ParsedQueueFrame::Op(message) => {
            crate::domains::queue::QueueClientFrame::Op(message)
        }
        crate::protocol::queue_codec::ParsedQueueFrame::Sub(message) => {
            crate::domains::queue::QueueClientFrame::Sub(message)
        }
    });
    let client_request = crate::domains::queue::QueueClientRequest::new(meta, parsed);
    crate::runtime::Envelope::from_route(request.source, request.destination, client_request)
}

fn build_notice_request_envelope(request: DomainEnvelopeBuildRequest) -> crate::runtime::Envelope {
    let ctx = frame_context(&request);
    let meta = client_frame_meta(&request);
    let parsed = crate::protocol::notice_codec::parse_request(
        &ctx,
        &ctx.payload,
        request.route_family,
        crate::session::SessionId(request.session_id),
        request.source.clone(),
    );
    let client_request = crate::domains::notice::NoticeClientRequest::new(meta, parsed);
    crate::runtime::Envelope::from_route(request.source, request.destination, client_request)
}

fn build_stream_request_envelope(request: DomainEnvelopeBuildRequest) -> crate::runtime::Envelope {
    let ctx = frame_context(&request);
    let meta = client_frame_meta(&request);
    let parsed = crate::protocol::stream_codec::parse_request(
        &ctx,
        &ctx.payload,
        request.route_family,
        crate::session::SessionId(request.session_id),
        request.source.clone(),
    );
    let client_request = crate::domains::stream::StreamClientRequest::new(meta, parsed);
    crate::runtime::Envelope::from_route(request.source, request.destination, client_request)
}

fn build_rpc_request_envelope(request: DomainEnvelopeBuildRequest) -> crate::runtime::Envelope {
    let ctx = frame_context(&request);
    let meta = client_frame_meta(&request);
    let parsed =
        crate::protocol::rpc_codec::parse_request(&ctx, &ctx.payload, request.route_family);
    let client_request = crate::domains::rpc::RpcClientRequest::new(meta, parsed);
    crate::runtime::Envelope::from_route(request.source, request.destination, client_request)
}

fn build_lease_request_envelope(request: DomainEnvelopeBuildRequest) -> crate::runtime::Envelope {
    let ctx = frame_context(&request);
    let meta = client_frame_meta(&request);
    let parsed = crate::protocol::lease_codec::parse_frame(
        &ctx,
        &ctx.payload,
        request.route_family,
        request.session_id,
        request.source.clone(),
    )
    .map(|frame| match frame {
        crate::protocol::lease_codec::ParsedLeaseFrame::Op(message) => {
            crate::domains::lease::LeaseClientFrame::Op(message)
        }
        crate::protocol::lease_codec::ParsedLeaseFrame::Sub(message) => {
            crate::domains::lease::LeaseClientFrame::Sub(message)
        }
    });
    let client_request = crate::domains::lease::LeaseClientRequest::new(meta, parsed);
    crate::runtime::Envelope::from_route(request.source, request.destination, client_request)
}

fn build_schedule_request_envelope(
    request: DomainEnvelopeBuildRequest,
) -> crate::runtime::Envelope {
    let ctx = frame_context(&request);
    let meta = client_frame_meta(&request);
    let parsed = crate::protocol::schedule_codec::parse_request(
        &ctx,
        &ctx.payload,
        request.route_family,
        crate::session::SessionId(request.session_id),
        request.source.clone(),
    );
    let client_request = crate::domains::schedule::ScheduleClientRequest::new(meta, parsed);
    crate::runtime::Envelope::from_route(request.source, request.destination, client_request)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::DomainKind;
    use std::collections::HashSet;

    #[test]
    fn should_define_exactly_one_ingress_descriptor_for_every_domain_kind() {
        // Arrange
        let descriptors = IngressDomainRegistry::all();

        // Act
        let descriptor_kinds = descriptors
            .iter()
            .map(IngressDomainDescriptor::kind)
            .collect::<HashSet<_>>();
        let all_kinds = DomainKind::ALL.into_iter().collect::<HashSet<_>>();

        // Assert
        assert_eq!(descriptors.len(), DomainKind::ALL.len());
        assert_eq!(descriptor_kinds, all_kinds);
    }

    #[test]
    fn should_preserve_message_type_dispatch_policy() {
        // Arrange
        let cases = [
            (
                100,
                Some((DomainKind::Kv, AuthorizationPolicy::KvBeginModeScoped)),
            ),
            (
                200,
                Some((
                    DomainKind::Queue,
                    AuthorizationPolicy::RouteScoped(crate::auth::Access::Write),
                )),
            ),
            (
                302,
                Some((
                    DomainKind::Rpc,
                    AuthorizationPolicy::RouteScoped(crate::auth::Access::Write),
                )),
            ),
            (
                403,
                Some((
                    DomainKind::Lease,
                    AuthorizationPolicy::RouteScoped(crate::auth::Access::Read),
                )),
            ),
            (
                501,
                Some((
                    DomainKind::Notice,
                    AuthorizationPolicy::RouteScoped(crate::auth::Access::Read),
                )),
            ),
            (
                604,
                Some((
                    DomainKind::Stream,
                    AuthorizationPolicy::RouteScoped(crate::auth::Access::Read),
                )),
            ),
            (
                706,
                Some((
                    DomainKind::Schedule,
                    AuthorizationPolicy::MultiRouteScoped(crate::auth::Access::Write),
                )),
            ),
            (112, None),
        ];

        // Act
        let actual = cases
            .iter()
            .map(|(msg_type, _)| {
                IngressDomainRegistry::dispatch_spec_for_msg_type(
                    crate::protocol::tlv::MessageType::new(*msg_type),
                )
                .expect("dispatch policy should resolve")
                .map(|spec| (spec.domain, spec.policy))
            })
            .collect::<Vec<_>>();
        let expected = cases
            .iter()
            .map(|(_, expected)| *expected)
            .collect::<Vec<_>>();

        // Assert
        assert_eq!(actual, expected);
    }

    #[test]
    fn should_preserve_invalid_message_rejections() {
        // Arrange
        let cases = [
            (111, "invalid message type: 111 is server-to-client only"),
            (209, "invalid message type: 209 is server-to-client only"),
            (205, "invalid message type: unsupported queue operation"),
            (409, "invalid message type: 409 is server-to-client only"),
            (404, "invalid message type: unsupported lease operation"),
            (504, "invalid message type: 504 is server-to-client only"),
            (
                505,
                "invalid message type: 505-599 are unsupported notice operations",
            ),
            (609, "invalid message type: 609 is server-to-client only"),
            (705, "invalid message type: 705 is server-to-client only"),
        ];

        // Act
        let actual = cases
            .iter()
            .map(|(msg_type, _)| {
                IngressDomainRegistry::dispatch_spec_for_msg_type(
                    crate::protocol::tlv::MessageType::new(*msg_type),
                )
                .expect_err("message type should be rejected")
            })
            .collect::<Vec<_>>();
        let expected = cases
            .iter()
            .map(|(_, expected)| *expected)
            .collect::<Vec<_>>();

        // Assert
        assert_eq!(actual, expected);
    }
}
