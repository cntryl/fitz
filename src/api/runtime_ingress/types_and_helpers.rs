// LAYER: API
// Ingress trait and reference implementation for the async to sync boundary
//
// # Purpose
//
// This module defines the async `Ingress` trait (the single async/sync boundary)
// and provides a reference implementation `RuntimeIngress` for session lifecycle
// management and event dispatching.
//
// # Design
//
// - **Trait definition** and **reference impl** live together to make the boundary
//   explicit and easy to review.
// - **API** (`api/tcp.rs`, `api/ws/mod.rs`) consumes this trait.
// - **Other session helpers** remain in their respective modules.

use super::{warn, Bytes, ChannelId, CloseReason, Cow, DispatchDomain, SessionInfo};

pub(super) fn dispatch_session_cleanup(
    router: &crate::runtime::Router,
    route_family: crate::runtime::routing::RouteFamily,
    session_id: u64,
) -> Vec<DispatchDomain> {
    dispatch_session_cleanup_for_domains(
        router,
        route_family,
        session_id,
        &crate::runtime::DomainKind::SESSION_CLEANUP_ORDER,
    )
}

pub(super) fn dispatch_session_cleanup_for_domains(
    router: &crate::runtime::Router,
    route_family: crate::runtime::routing::RouteFamily,
    session_id: u64,
    domains: &[DispatchDomain],
) -> Vec<DispatchDomain> {
    let cleanup = crate::runtime::SessionCleanup { session_id };
    let mut failed_domains = Vec::new();

    for &domain in domains {
        let cleanup_addr =
            crate::runtime::routing::RouteAddress::new(route_family, domain.cleanup_route());
        let cleanup_envelope = crate::runtime::Envelope::new(cleanup_addr, cleanup.clone());

        // Control-plane work: a busy Queue actor can hold up to 16,384
        // client messages ahead of anything on the normal lane, and the
        // ingress coordinator gives up on a cleanup ticket after 2.3s. Cleanup
        // must therefore ride the bounded control lane so it is never hidden
        // behind normal-lane pressure (see architecture.md's Actor Mailbox
        // section).
        if let Err(error) = router.route_high_priority(cleanup_envelope) {
            warn!(
                session_id = session_id,
                route_family = route_family.id(),
                domain = domain.as_str(),
                error = %error,
                "Ingress: session cleanup delivery failed"
            );
            failed_domains.push(domain);
        }
    }

    failed_domains
}

pub(super) fn canonicalize_dispatch_route_str(
    domain: DispatchDomain,
    route: &str,
) -> Result<Cow<'_, str>, String> {
    RuntimeIngress::canonicalize_domain_route_str(domain, route)
}

pub(super) fn extract_auth_route_for_domain(
    domain: DispatchDomain,
    msg_type: u16,
    payload: &[u8],
) -> Result<Option<Cow<'_, str>>, String> {
    let descriptor =
        crate::api::runtime_ingress::domain_registry::IngressDomainPolicy::descriptor_for_domain(
            domain,
        );
    descriptor
        .extract_auth_route(msg_type, payload)
        .and_then(|route| {
            route
                .map(|route| {
                    let route = canonicalize_dispatch_route_str(domain, route)?;
                    if is_pattern_authorization_target(domain, msg_type, route.as_ref()) {
                        // Scheme, depth, and the exact-only rule all come from
                        // the domain descriptor, so ingress authorization and
                        // the domain sink cannot accept different pattern
                        // languages (routing-design.md §4).
                        domain
                            .descriptor()
                            .compile_registration_pattern(route.as_ref())?;
                        return Ok(route);
                    }
                    Ok(route)
                })
                .transpose()
        })
}

pub(super) fn is_subscription_registration_message(domain: DispatchDomain, msg_type: u16) -> bool {
    is_subscription_registration(domain, msg_type)
}

pub(super) fn is_pattern_authorization_target(
    domain: DispatchDomain,
    msg_type: u16,
    target: &str,
) -> bool {
    is_subscription_registration_message(domain, msg_type)
        || (matches!(
            (domain, msg_type),
            (DispatchDomain::Queue, 202) | (DispatchDomain::Stream, 604)
        ) && target.contains('*'))
}

/// Message types whose route is a pattern rather than an exact route: a
/// retained registration (`SUBSCRIBE`/`UNSUBSCRIBE`/`WATCH`/`REGISTER`) or a
/// one-shot patterned read (Lease `LIST`). Both need the same compile +
/// containment treatment so authorization cannot accept a selector the sink
/// would interpret differently (routing-design.md §4).
///
/// This table is message-type routing, not domain policy: the grammar each
/// pattern must satisfy comes from the domain descriptor.
fn is_subscription_registration(domain: DispatchDomain, msg_type: u16) -> bool {
    matches!(
        (domain, msg_type),
        (DispatchDomain::Kv, 109 | 110)
            | (DispatchDomain::Queue, 207 | 208)
            | (DispatchDomain::Stream, 607 | 608)
            | (DispatchDomain::Lease, 407 | 408 | 410)
            | (DispatchDomain::Schedule, 703 | 704)
            | (DispatchDomain::Notice, 501)
            | (DispatchDomain::Rpc, 300 | 301)
    )
}

pub(super) enum AuthorizationTargets<'a> {
    SessionOwned,
    Single(Cow<'a, str>),
    Registration(Cow<'a, str>),
    Multiple(Vec<Cow<'a, str>>),
}

pub(super) enum DomainDispatchPayload<'a> {
    Owned(Bytes),
    Shared(&'a Bytes),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum AuthorizationPolicy {
    RouteScoped(crate::auth::Access),
    WildcardScoped(crate::auth::Access),
    SessionOwned,
    KvBeginModeScoped,
    MultiRouteScoped(crate::auth::Access),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct DomainAuthorizationSpec {
    pub(super) domain: DispatchDomain,
    pub(super) policy: AuthorizationPolicy,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum AuthorizationFailure {
    MissingSessionActor,
    PermissionDenied,
}

pub(super) struct DomainDispatchRequest<'a> {
    pub(super) router: &'a crate::runtime::Router,
    pub(super) session_id: u64,
    pub(super) channel_id: ChannelId,
    pub(super) route_family: crate::runtime::routing::RouteFamily,
    pub(super) domain: DispatchDomain,
    pub(super) policy: AuthorizationPolicy,
    pub(super) msg_type: crate::protocol::tlv::MessageType,
    pub(super) payload: DomainDispatchPayload<'a>,
}

#[derive(Clone, Debug)]
pub(super) struct PendingSessionCleanup {
    pub(super) route_family: crate::runtime::routing::RouteFamily,
    pub(super) pending_domains: Vec<DispatchDomain>,
    pub(super) created_at: std::time::Instant,
    pub(super) attempts: u32,
}

impl AuthorizationTargets<'_> {
    pub(super) fn span_target(&self) -> (&str, usize) {
        match self {
            Self::SessionOwned => ("<session-owned>", 1),
            Self::Single(route) | Self::Registration(route) => (route.as_ref(), 1),
            Self::Multiple(routes) => (
                routes
                    .first()
                    .map_or("<session-owned>", |route| route.as_ref()),
                routes.len(),
            ),
        }
    }

    pub(super) fn authorize(
        &self,
        actor_ref: &crate::session::actor::SessionActor,
        access: crate::auth::Access,
        domain: DispatchDomain,
    ) -> (bool, &str, usize) {
        let wildcard_route = domain.wildcard_route();
        match self {
            Self::SessionOwned => (actor_ref.authorize_session_owned(), "<session-owned>", 1),
            Self::Single(route) => {
                let route = route.as_ref();
                (actor_ref.authorize_route(route, access), route, 1)
            }
            Self::Registration(route) => {
                let route = route.as_ref();
                let pattern = crate::runtime::matcher::Pattern::new(route);
                // A domain restricted to one concrete depth (Lease's
                // CanMatch(3), e.g.) must compare grant and requested
                // selector over that same fixed depth, not the unrestricted
                // `*`/`**` grammar — otherwise a bare `**` alias reads as a
                // strict superset of every literal-or-`*` grant at that
                // depth and a covering grant is wrongly denied
                // (routing-design.md §4).
                let authorized = match domain.descriptor().registration_depth {
                    crate::runtime::matcher::PatternDepth::CanMatch(depth) => {
                        actor_ref.authorize_registration_pattern_at_depth(&pattern, access, depth)
                    }
                    crate::runtime::matcher::PatternDepth::Flexible => {
                        actor_ref.authorize_registration_pattern(&pattern, access)
                    }
                };
                (authorized, route, 1)
            }
            Self::Multiple(routes) => {
                let authorized = routes
                    .iter()
                    .all(|route| actor_ref.authorize_route(route.as_ref(), access));
                (
                    authorized,
                    routes
                        .first()
                        .map_or(wildcard_route, |route| route.as_ref()),
                    routes.len(),
                )
            }
        }
    }
}

impl DomainDispatchPayload<'_> {
    pub(super) fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Owned(bytes) => bytes.as_ref(),
            Self::Shared(bytes) => bytes.as_ref(),
        }
    }

    pub(super) fn into_dispatch_bytes(self) -> Bytes {
        match self {
            Self::Owned(bytes) => bytes,
            Self::Shared(bytes) => bytes.clone(),
        }
    }
}

/// Outcome from the runtime for a single protocol message
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IngressDecision {
    Accept,
    Close(String),
    Backpressure,
}

/// Trait implemented by the session layer to consume transport frames
#[async_trait::async_trait]
pub trait Ingress: Send + Sync {
    /// Called when transport opens a new session
    async fn on_open(&self, session: SessionInfo) -> Result<u64, String>;

    /// Called for every demultiplexed channel message
    async fn on_frame(
        &self,
        session_id: u64,
        channel_id: ChannelId,
        msg_type: crate::protocol::tlv::MessageType,
        message_payload: Bytes,
    ) -> IngressDecision;

    /// Get current session info for transports that need to observe auth-driven updates.
    fn get_session_info(&self, _session_id: u64) -> Option<SessionInfo> {
        None
    }

    /// Get the current route family for a session without cloning full session metadata.
    fn get_route_family(&self, session_id: u64) -> Option<crate::runtime::routing::RouteFamily> {
        self.get_session_info(session_id)
            .map(|session| session.route_family)
    }

    /// Record that the transport accepted a frame from the wire for this session.
    fn record_frame_received(&self, _session_id: u64) {}

    /// Record that the transport wrote a frame to the wire for this session.
    fn record_frame_sent(&self, _session_id: u64) {}

    /// Called when the transport closes the connection
    async fn on_close(&self, session_id: u64, reason: CloseReason);
}

/// Session frame message for dispatching to domain handlers
#[derive(Debug, Clone)]
pub struct SessionFrame {
    pub session_id: u64,
    pub channel_id: ChannelId,
    pub payload: Bytes,
}

/// Session lifecycle event
#[derive(Debug, Clone)]
pub enum SessionEvent {
    Open(u64, SessionInfo),
    Frame(SessionFrame),
    Close(u64, CloseReason),
}

/// Ingress implementation with session tracking
///
/// This reference implementation tracks active sessions and can route
/// frame events to event handlers. It's designed to be embedded in
/// a runtime dispatcher or session manager.
pub struct RuntimeIngress {
    pub(super) registry: super::session_registry::SessionRegistry,
    pub(super) authenticator: super::session_authenticator::SessionAuthenticator,
    pub(super) cleanup: super::session_cleanup_coordinator::SessionCleanupCoordinator,
    pub(super) dispatcher: super::domain_frame_dispatcher::DomainFrameDispatcher,
}
