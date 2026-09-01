use super::{AuthorizationPolicy, DispatchDomain, DomainAuthorizationSpec};
#[cfg(test)]
use super::{Bytes, ChannelId};

type AuthRouteExtractor = for<'a> fn(u16, &'a [u8]) -> Result<Option<&'a str>, String>;
type RequestEnvelopeBuilder = fn(
    crate::runtime::DomainKind,
    crate::dispatch::DomainEnvelopeBuildRequest,
) -> crate::runtime::Envelope;

pub(crate) use crate::dispatch::DomainEnvelopeBuildRequest;

pub(crate) struct IngressDomainDescriptor {
    pub(super) manifest: &'static crate::runtime::DomainDescriptor,
    pub(super) unauthorized_error_code: u16,
    /// Code returned when the domain did not answer before its deadline.
    ///
    /// Deliberately NOT a backpressure/"queue full" code. Those mean the
    /// request was rejected without being accepted, so a client may safely
    /// retry. A deadline expiry means the opposite: the command was already
    /// enqueued and may still execute, so the outcome is unknown. Only queue
    /// ACK is deduplicated, so an automatic retry of a SEND would enqueue the
    /// message twice.
    ///
    /// Every value here must sit outside `REQ-PROTO-012`'s retryable set
    /// (1004, 4005, 5001, 6001, 6002, 6003, 6004, 7010). Those codes tell a
    /// compliant SDK the request was never accepted, which is the opposite of
    /// what a deadline expiry means. Notably RPC uses its backend code rather
    /// than `ERR_RPC_TIMEOUT`, which is documented retryable.
    pub(super) indeterminate_error_code: u16,
    /// Code returned when the domain mailbox stayed full and the command was
    /// never enqueued.
    ///
    /// The opposite of `indeterminate_error_code`: nothing was accepted, so the
    /// client may safely re-send. Every value here must be inside
    /// `REQ-PROTO-012`'s retryable set, or a compliant client gives up on a
    /// request it could have retried - the response message says "retry with
    /// backoff", and a fatal code contradicts it.
    pub(super) backpressure_error_code: u16,
    extract_auth_route: AuthRouteExtractor,
    build_request_envelope: RequestEnvelopeBuilder,
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
        (self.build_request_envelope)(self.manifest.kind, request)
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

    /// Resolve a manifest domain name back to its dispatch domain.
    ///
    /// Derived from the shared descriptor table rather than a separate literal
    /// match, so the name accepted here is always the one `as_str` produces and
    /// the two cannot drift apart.
    fn domain_from_manifest_name(name: &str) -> Option<DispatchDomain> {
        DispatchDomain::ALL
            .into_iter()
            .find(|domain| domain.as_str() == name)
    }
}

static INGRESS_DOMAIN_DESCRIPTORS: [IngressDomainDescriptor; 7] = [
    IngressDomainDescriptor {
        manifest: crate::runtime::DomainKind::Kv.descriptor(),
        unauthorized_error_code: crate::protocol::error_codes::kv::ERR_UNAUTHORIZED,
        backpressure_error_code: crate::protocol::error_codes::kv::ERR_BUSY,
        indeterminate_error_code: crate::protocol::error_codes::kv::ERR_BACKEND_ERROR,
        extract_auth_route: crate::protocol::kv_codec::extract_auth_route,
        build_request_envelope: crate::dispatch::build_request_envelope,
    },
    IngressDomainDescriptor {
        manifest: crate::runtime::DomainKind::Queue.descriptor(),
        unauthorized_error_code: crate::protocol::error_codes::queue::ERR_UNAUTHORIZED,
        backpressure_error_code: crate::protocol::error_codes::queue::ERR_QUEUE_FULL,
        indeterminate_error_code: crate::protocol::error_codes::queue::ERR_BACKEND_ERROR,
        extract_auth_route: crate::protocol::queue_codec::extract_auth_route,
        build_request_envelope: crate::dispatch::build_request_envelope,
    },
    IngressDomainDescriptor {
        manifest: crate::runtime::DomainKind::Notice.descriptor(),
        unauthorized_error_code: crate::protocol::error_codes::notice::ERR_UNAUTHORIZED,
        backpressure_error_code: crate::protocol::error_codes::notice::ERR_BUSY,
        indeterminate_error_code: crate::protocol::error_codes::notice::ERR_BACKEND_ERROR,
        extract_auth_route: crate::protocol::notice_codec::extract_auth_route,
        build_request_envelope: crate::dispatch::build_request_envelope,
    },
    IngressDomainDescriptor {
        manifest: crate::runtime::DomainKind::Stream.descriptor(),
        unauthorized_error_code: crate::protocol::error_codes::stream::ERR_UNAUTHORIZED,
        backpressure_error_code: crate::protocol::error_codes::stream::ERR_BUSY,
        indeterminate_error_code: crate::protocol::error_codes::stream::ERR_BACKEND_ERROR,
        extract_auth_route: crate::protocol::stream_codec::extract_auth_route,
        build_request_envelope: crate::dispatch::build_request_envelope,
    },
    IngressDomainDescriptor {
        manifest: crate::runtime::DomainKind::Rpc.descriptor(),
        unauthorized_error_code: crate::protocol::error_codes::rpc::ERR_UNAUTHORIZED,
        backpressure_error_code: crate::protocol::error_codes::rpc::ERR_RPC_BACKPRESSURE,
        indeterminate_error_code: crate::protocol::error_codes::rpc::ERR_BACKEND_ERROR,
        extract_auth_route: crate::protocol::rpc_codec::extract_auth_route,
        build_request_envelope: crate::dispatch::build_request_envelope,
    },
    IngressDomainDescriptor {
        manifest: crate::runtime::DomainKind::Lease.descriptor(),
        unauthorized_error_code: crate::protocol::error_codes::lease::ERR_UNAUTHORIZED,
        backpressure_error_code: crate::protocol::error_codes::lease::ERR_QUEUE_FULL,
        indeterminate_error_code: crate::protocol::error_codes::lease::ERR_TIMEOUT,
        extract_auth_route: crate::protocol::lease_codec::extract_auth_route,
        build_request_envelope: crate::dispatch::build_request_envelope,
    },
    IngressDomainDescriptor {
        manifest: crate::runtime::DomainKind::Schedule.descriptor(),
        unauthorized_error_code: crate::protocol::error_codes::schedule::ERR_UNAUTHORIZED,
        backpressure_error_code: crate::protocol::error_codes::schedule::ERR_BACKEND_ERROR,
        indeterminate_error_code: crate::protocol::error_codes::schedule::ERR_TIMEOUT,
        extract_auth_route: crate::protocol::schedule_codec::extract_auth_route,
        build_request_envelope: crate::dispatch::build_request_envelope,
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
    fn should_resolve_each_domain_to_its_own_ingress_descriptor() {
        // `descriptor_for_domain` maps each variant to a hardcoded
        // `INGRESS_DOMAIN_DESCRIPTORS` index. Set-equality alone cannot catch a
        // reordered array, which would silently pair every domain with another
        // domain's auth-route extractor and unauthorized error code.

        // Arrange
        let domains = DomainKind::ALL;

        // Act
        let resolved =
            domains.map(|domain| IngressDomainRegistry::descriptor_for_domain(domain).kind());

        // Assert
        assert_eq!(resolved, domains);
    }

    #[test]
    fn should_resolve_every_manifest_domain_name() {
        // Arrange
        let names = DomainKind::ALL.map(DomainKind::as_str);

        // Act
        let resolved = names.map(IngressDomainRegistry::domain_from_manifest_name);

        // Assert
        assert_eq!(resolved, DomainKind::ALL.map(Some));
        assert_eq!(
            IngressDomainRegistry::domain_from_manifest_name("nope"),
            None
        );
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
            (
                707,
                Some((
                    DomainKind::Schedule,
                    AuthorizationPolicy::WildcardScoped(crate::auth::Access::Read),
                )),
            ),
            (
                707,
                Some((
                    DomainKind::Schedule,
                    AuthorizationPolicy::WildcardScoped(crate::auth::Access::Read),
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
            699, 705, 799,
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
    fn should_reject_prepared_lease_acquire_owner_id_over_wire_safe_limit() {
        // Arrange
        let descriptor = IngressDomainRegistry::descriptor_for_domain(DispatchDomain::Lease);
        let owner_id =
            "x".repeat(crate::domains::lease::protocol::LEASE_MAX_OWNER_ID_BYTES.saturating_add(1));
        let payload = lease_acquire_payload("lease://acme/locks/resource", &owner_id);

        // Act
        let envelope = descriptor.build_request_envelope(lease_build_request(400, payload));
        let request = envelope
            .payload::<crate::domains::lease::protocol::PreparedLeaseClientRequest>()
            .expect("prepared lease request");

        // Assert
        assert!(
            request
                .frame
                .as_ref()
                .is_err_and(|message| message.contains("owner_id exceeds maximum length")),
            "oversized logical owner ID must fail before session scoping"
        );
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
