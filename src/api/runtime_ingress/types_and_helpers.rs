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

use super::{
    warn, AtomicBool, Bytes, ChannelId, CloseReason, Cow, DashMap, DispatchDomain, SessionInfo,
};
use std::sync::Arc;

pub(super) fn dispatch_session_cleanup(
    router: &crate::runtime::Router,
    route_family: crate::runtime::routing::RouteFamily,
    session_id: u64,
) -> Vec<DispatchDomain> {
    dispatch_session_cleanup_for_domains(
        router,
        route_family,
        session_id,
        crate::runtime::DomainRegistry::cleanup_order(),
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

        if let Err(error) = router.route(cleanup_envelope) {
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
        crate::api::runtime_ingress::domain_registry::IngressDomainRegistry::descriptor_for_domain(
            domain,
        );
    descriptor
        .extract_auth_route(msg_type, payload)
        .and_then(|route| {
            route
                .map(|route| canonicalize_dispatch_route_str(domain, route))
                .transpose()
        })
}

pub(super) enum AuthorizationTargets<'a> {
    SessionOwned,
    Single(Cow<'a, str>),
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
            Self::Single(route) => (route.as_ref(), 1),
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
        wildcard_route: &'static str,
    ) -> (bool, &str, usize) {
        match self {
            Self::SessionOwned => (actor_ref.authorize_session_owned(), "<session-owned>", 1),
            Self::Single(route) => {
                let route = route.as_ref();
                (actor_ref.authorize_route(route, access), route, 1)
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
    pub(super) sessions: Arc<DashMap<u64, SessionInfo>>,
    /// Per-session `SessionActor` instances for authorization checks
    pub(super) session_actors: Arc<DashMap<u64, crate::session::actor::SessionActor>>,
    /// Cached per-session inbox routes used as the source address for domain dispatch.
    pub(super) session_inbox_routes: Arc<DashMap<u64, crate::runtime::routing::Route>>,
    /// Best-effort retry tickets for session cleanups that failed initial delivery.
    pub(super) pending_session_cleanups: Arc<DashMap<u64, PendingSessionCleanup>>,
    /// Wakes the dedicated cleanup worker immediately when a ticket is added.
    pub(super) cleanup_wake: Arc<tokio::sync::Notify>,
    /// Ensures one cleanup worker services the pending ticket set.
    pub(super) cleanup_worker_started: Arc<AtomicBool>,
    /// Allows graceful shutdown to stop the worker after tickets drain.
    pub(super) cleanup_shutdown: Arc<AtomicBool>,
    /// Idempotence barrier for the session finalizer.
    pub(super) closed_sessions: Arc<DashMap<u64, ()>>,
    /// Optional router for dispatching frames to domain sinks
    pub(super) router: Option<Arc<crate::runtime::Router>>,
    /// Optional callback for session events (for routing to handlers)
    pub(super) event_handler: Option<Arc<dyn Fn(SessionEvent) + Send + Sync>>,
    /// Route families provisioned at boot and accepted from verified JWT claims.
    pub(super) route_families: Arc<std::collections::HashSet<u32>>,
    /// Whether authentication is required (if false, JWT is ignored and full access granted)
    pub(super) auth_required: bool,
    /// Passive admin snapshot mirror for session lifecycle
    pub(super) admin_read_model: Option<Arc<crate::control::admin::read_model::AdminReadModel>>,

    /// Explicit auth configuration used for CONNECT verification when present.
    pub(super) auth_config: Option<crate::auth::AuthConfig>,
    /// Claim normalization behavior for CONNECT JWTs.
    pub(super) auth_claims_config: crate::auth::AuthClaimsConfig,
    /// Broker-local route-family resolver for verified identity claims.
    pub(super) route_family_resolver: crate::auth::RouteFamilyResolverConfig,
}
