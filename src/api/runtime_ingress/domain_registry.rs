use super::{AuthorizationPolicy, Bytes, ChannelId, DispatchDomain, DomainAuthorizationSpec};

type AuthRouteExtractor = for<'a> fn(u16, &'a [u8]) -> Result<Option<&'a str>, String>;
type RequestEnvelopeBuilder = fn(DomainEnvelopeBuildRequest) -> crate::runtime::Envelope;

pub(crate) struct IngressDomainDescriptor {
    pub(super) manifest: &'static crate::runtime::DomainDescriptor,
    pub(super) unauthorized_error_code: u16,
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
    #[cfg(test)]
    pub(super) fn kind(&self) -> DispatchDomain {
        self.manifest.kind
    }

    pub(crate) fn domain_name(&self) -> &'static str {
        self.manifest.kind.as_str()
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
    #[cfg(test)]
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
        let Some(dispatch) = crate::dispatch::client_dispatch(msg_type)? else {
            return Ok(None);
        };
        let policy = manifest_authorization_policy(dispatch.authorization)
            .ok_or("manifest message has no authorization policy")?;
        Ok(Some(DomainAuthorizationSpec {
            domain: dispatch.domain,
            policy,
        }))
    }

    pub(crate) fn descriptor_for_msg_type(
        msg_type: crate::protocol::tlv::MessageType,
    ) -> Result<Option<&'static IngressDomainDescriptor>, &'static str> {
        let entry = crate::protocol::manifest::client_entry(msg_type)?;
        let Some(domain) = Self::domain_from_manifest_name(entry.domain) else {
            return Ok(None);
        };
        Ok(Some(Self::descriptor_for_domain(domain)))
    }

    fn domain_from_manifest_name(name: &str) -> Option<DispatchDomain> {
        match name {
            "kv" => Some(DispatchDomain::Kv),
            "queue" => Some(DispatchDomain::Queue),
            "notice" => Some(DispatchDomain::Notice),
            "stream" => Some(DispatchDomain::Stream),
            "rpc" => Some(DispatchDomain::Rpc),
            "lease" => Some(DispatchDomain::Lease),
            "schedule" => Some(DispatchDomain::Schedule),
            _ => None,
        }
    }
}

static INGRESS_DOMAIN_DESCRIPTORS: [IngressDomainDescriptor; 7] = [
    IngressDomainDescriptor {
        manifest: crate::runtime::DomainKind::Kv.descriptor(),
        unauthorized_error_code: crate::protocol::error_codes::kv::ERR_UNAUTHORIZED,
        extract_auth_route: crate::protocol::kv_codec::extract_auth_route,
        build_request_envelope: build_kv_request_envelope,
    },
    IngressDomainDescriptor {
        manifest: crate::runtime::DomainKind::Queue.descriptor(),
        unauthorized_error_code: crate::protocol::error_codes::queue::ERR_UNAUTHORIZED,
        extract_auth_route: crate::protocol::queue_codec::extract_auth_route,
        build_request_envelope: build_queue_request_envelope,
    },
    IngressDomainDescriptor {
        manifest: crate::runtime::DomainKind::Notice.descriptor(),
        unauthorized_error_code: crate::protocol::error_codes::notice::ERR_UNAUTHORIZED,
        extract_auth_route: crate::protocol::notice_codec::extract_auth_route,
        build_request_envelope: build_notice_request_envelope,
    },
    IngressDomainDescriptor {
        manifest: crate::runtime::DomainKind::Stream.descriptor(),
        unauthorized_error_code: crate::protocol::error_codes::stream::ERR_UNAUTHORIZED,
        extract_auth_route: crate::protocol::stream_codec::extract_auth_route,
        build_request_envelope: build_stream_request_envelope,
    },
    IngressDomainDescriptor {
        manifest: crate::runtime::DomainKind::Rpc.descriptor(),
        unauthorized_error_code: crate::protocol::error_codes::rpc::ERR_UNAUTHORIZED,
        extract_auth_route: crate::protocol::rpc_codec::extract_auth_route,
        build_request_envelope: build_rpc_request_envelope,
    },
    IngressDomainDescriptor {
        manifest: crate::runtime::DomainKind::Lease.descriptor(),
        unauthorized_error_code: crate::protocol::error_codes::lease::ERR_UNAUTHORIZED,
        extract_auth_route: crate::protocol::lease_codec::extract_auth_route,
        build_request_envelope: build_lease_request_envelope,
    },
    IngressDomainDescriptor {
        manifest: crate::runtime::DomainKind::Schedule.descriptor(),
        unauthorized_error_code: crate::protocol::error_codes::schedule::ERR_UNAUTHORIZED,
        extract_auth_route: crate::protocol::schedule_codec::extract_auth_route,
        build_request_envelope: build_schedule_request_envelope,
    },
];

fn manifest_authorization_policy(
    authorization: crate::protocol::manifest::ManifestAuthorization,
) -> Option<AuthorizationPolicy> {
    use crate::auth::Access;
    use crate::protocol::manifest::ManifestAuthorization;

    match authorization {
        ManifestAuthorization::None => None,
        ManifestAuthorization::RouteRead => Some(AuthorizationPolicy::RouteScoped(Access::Read)),
        ManifestAuthorization::RouteWrite => Some(AuthorizationPolicy::RouteScoped(Access::Write)),
        ManifestAuthorization::RouteAll => Some(AuthorizationPolicy::RouteScoped(Access::All)),
        ManifestAuthorization::SessionOwned => Some(AuthorizationPolicy::SessionOwned),
        ManifestAuthorization::KvBeginMode => Some(AuthorizationPolicy::KvBeginModeScoped),
        ManifestAuthorization::WildcardRead => {
            Some(AuthorizationPolicy::WildcardScoped(Access::Read))
        }
        ManifestAuthorization::MultiRouteWrite => {
            Some(AuthorizationPolicy::MultiRouteScoped(Access::Write))
        }
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
    let DomainEnvelopeBuildRequest {
        session_id,
        channel_id,
        route_family,
        msg_type,
        payload,
        source,
        destination,
    } = request;
    let ctx = crate::protocol::frame_context::FrameContext::new(
        session_id,
        channel_id,
        msg_type,
        payload,
        route_family,
    );
    let meta = crate::runtime::ClientFrameMeta::new(
        session_id,
        crate::api::frame_adapter::client_channel_from_protocol(channel_id),
        msg_type.as_u16(),
        route_family,
    );
    let parsed = crate::protocol::rpc_codec::parse_request(&ctx, &ctx.payload, route_family);
    let client_request =
        crate::domains::rpc::RpcClientRequest::new_with_payload(meta, parsed, ctx.payload.clone());
    crate::runtime::Envelope::from_route(source, destination, client_request)
}

fn build_lease_request_envelope(request: DomainEnvelopeBuildRequest) -> crate::runtime::Envelope {
    let meta = client_frame_meta(&request);
    let msg_type = request.msg_type.as_u16();

    if matches!(
        msg_type,
        crate::protocol::lease_codec::msg_type::ACQUIRE
            | crate::protocol::lease_codec::msg_type::RENEW
            | crate::protocol::lease_codec::msg_type::RELEASE
            | crate::protocol::lease_codec::msg_type::QUERY
    ) {
        let parsed = crate::protocol::lease_codec::parse_prepared_request(
            msg_type,
            request.route_family,
            request.session_id,
            &request.payload,
        );
        let client_request =
            crate::domains::lease::protocol::PreparedLeaseClientRequest::new(meta, parsed);
        return crate::runtime::Envelope::from_route(
            request.source,
            request.destination,
            client_request,
        );
    }

    let ctx = frame_context(&request);
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

    fn lease_build_request(msg_type: u16, payload: Bytes) -> DomainEnvelopeBuildRequest {
        let family = crate::runtime::routing::RouteFamily::new(41);
        DomainEnvelopeBuildRequest {
            session_id: 7,
            channel_id: ChannelId::Lease,
            route_family: family,
            msg_type: crate::protocol::tlv::MessageType::new(msg_type),
            payload,
            source: crate::runtime::routing::RouteAddress::new(
                family,
                crate::runtime::routing::Route::new("inbox://session/7"),
            ),
            destination: crate::runtime::routing::RouteAddress::new(
                family,
                crate::runtime::routing::Route::new("lease://acme/locks/resource"),
            ),
        }
    }

    fn lease_acquire_payload(route: &str, owner_id: &str) -> Bytes {
        let mut encoder = crate::protocol::payload_codec::PayloadEncoder::new();
        encoder.put_string(route);
        encoder.put_string(owner_id);
        encoder.put_u64(30);
        encoder.put_u32(0);
        Bytes::from(encoder.finish())
    }

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
            111, 112, 199, 209, 201, 205, 299, 304, 305, 306, 399, 409, 404, 504, 505, 609, 610,
            699, 705, 707, 799,
        ];

        // Act
        let actual = cases
            .iter()
            .map(|msg_type| {
                IngressDomainRegistry::dispatch_spec_for_msg_type(
                    crate::protocol::tlv::MessageType::new(*msg_type),
                )
                .expect_err("message type should be rejected")
            })
            .collect::<Vec<_>>();
        let expected = cases
            .iter()
            .map(|msg_type| {
                if crate::protocol::manifest::entry(crate::protocol::tlv::MessageType::new(
                    *msg_type,
                ))
                .is_some()
                {
                    "message type is server-to-client only"
                } else if *msg_type == 304 {
                    "invalid message type: unsupported rpc operation"
                } else {
                    "unsupported message type"
                }
            })
            .collect::<Vec<_>>();

        // Assert
        assert_eq!(actual, expected);
    }

    #[test]
    fn should_build_prepared_lease_request_for_operation_frame() {
        // Arrange
        let descriptor = IngressDomainRegistry::descriptor_for_domain(DispatchDomain::Lease);
        let payload = lease_acquire_payload("lease://acme/locks/resource", "owner");

        // Act
        let envelope = descriptor.build_request_envelope(lease_build_request(400, payload));
        let request = envelope
            .payload::<crate::domains::lease::protocol::PreparedLeaseClientRequest>()
            .expect("prepared lease request");

        // Assert
        assert_eq!(request.meta.session_id, 7);
        assert_eq!(request.meta.message_type, 400);
        let operation = request.frame.as_ref().expect("prepared lease operation");
        match operation {
            crate::domains::lease::protocol::PreparedLeaseOperation::Acquire {
                key,
                owner_id,
                ttl_secs,
                wait_seconds,
            } => {
                assert_eq!(key.realm, "acme");
                assert_eq!(key.area, "locks");
                assert_eq!(key.resource, "resource");
                assert_eq!(owner_id, "session:7:owner");
                assert_eq!(*ttl_secs, 30);
                assert_eq!(*wait_seconds, 0);
            }
            other => panic!("expected prepared acquire, got {other:?}"),
        }
    }

    #[test]
    fn should_keep_lease_subscription_on_public_request_path() {
        // Arrange
        let descriptor = IngressDomainRegistry::descriptor_for_domain(DispatchDomain::Lease);
        let mut encoder = crate::protocol::payload_codec::PayloadEncoder::new();
        encoder.put_string("lease://acme/locks/resource");

        // Act
        let envelope = descriptor
            .build_request_envelope(lease_build_request(407, Bytes::from(encoder.finish())));

        // Assert
        assert!(envelope
            .payload::<crate::domains::lease::protocol::PreparedLeaseClientRequest>()
            .is_none());
        assert!(envelope
            .payload::<crate::domains::lease::LeaseClientRequest>()
            .is_some());
    }
}
