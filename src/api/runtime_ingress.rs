// LAYER: API
//! Ingress trait and reference implementation for the async to sync boundary
//!
//! # Purpose
//!
//! This module defines the async `Ingress` trait (the single async/sync boundary)
//! and provides a reference implementation `RuntimeIngress` for session lifecycle
//! management and event dispatching.
//!
//! # Design
//!
//! - **Trait definition** and **reference impl** live together to make the boundary
//!   explicit and easy to review.
//! - **API** (`api/tcp.rs`, `api/ws/mod.rs`) consumes this trait.
//! - **Other session helpers** remain in their respective modules.

use crate::observability as obs;
use crate::protocol::frame::ChannelId;
use crate::runtime::DomainKind as DispatchDomain;
use crate::session::{CloseReason, SessionInfo, SessionPermissions};
use bytes::Bytes;
use dashmap::DashMap;
use std::borrow::Cow;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, error, info, trace, warn};

fn dispatch_session_cleanup(
    router: &crate::runtime::Router,
    route_family: crate::runtime::routing::RouteFamily,
    session_id: u64,
) -> Vec<DispatchDomain> {
    let cleanup = crate::runtime::SessionCleanup { session_id };
    let mut failed_domains = Vec::new();

    for domain in DispatchDomain::SESSION_CLEANUP_ORDER {
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

fn canonicalize_dispatch_route_str<'a>(
    domain: DispatchDomain,
    route: &'a str,
) -> Result<Cow<'a, str>, String> {
    RuntimeIngress::canonicalize_domain_route_str(domain, route)
}

fn extract_auth_route_for_domain<'a>(
    domain: DispatchDomain,
    msg_type: u16,
    payload: &'a [u8],
) -> Result<Option<Cow<'a, str>>, String> {
    match domain {
        DispatchDomain::Kv => crate::protocol::kv_codec::extract_auth_route(msg_type, payload)
            .and_then(|route| {
                route
                    .map(|route| canonicalize_dispatch_route_str(domain, route))
                    .transpose()
            }),
        DispatchDomain::Queue => {
            crate::protocol::queue_codec::extract_auth_route(msg_type, payload).and_then(|route| {
                route
                    .map(|route| canonicalize_dispatch_route_str(domain, route))
                    .transpose()
            })
        }
        DispatchDomain::Rpc => crate::protocol::rpc_codec::extract_auth_route(msg_type, payload)
            .and_then(|route| {
                route
                    .map(|route| canonicalize_dispatch_route_str(domain, route))
                    .transpose()
            }),
        DispatchDomain::Lease => {
            crate::protocol::lease_codec::extract_auth_route(msg_type, payload).and_then(|route| {
                route
                    .map(|route| canonicalize_dispatch_route_str(domain, route))
                    .transpose()
            })
        }
        DispatchDomain::Notice => {
            crate::protocol::notice_codec::extract_auth_route(msg_type, payload).and_then(|route| {
                route
                    .map(|route| canonicalize_dispatch_route_str(domain, route))
                    .transpose()
            })
        }
        DispatchDomain::Stream => {
            crate::protocol::stream_codec::extract_auth_route(msg_type, payload).and_then(|route| {
                route
                    .map(|route| canonicalize_dispatch_route_str(domain, route))
                    .transpose()
            })
        }
        DispatchDomain::Schedule => crate::protocol::schedule_codec::extract_auth_route(
            msg_type, payload,
        )
        .and_then(|route| {
            route
                .map(|route| canonicalize_dispatch_route_str(domain, route))
                .transpose()
        }),
    }
}

enum AuthorizationTargets<'a> {
    SessionOwned,
    Single(Cow<'a, str>),
    Multiple(Vec<Cow<'a, str>>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AuthorizationPolicy {
    RouteScoped(crate::auth::Access),
    WildcardScoped(crate::auth::Access),
    SessionOwned,
    KvBeginModeScoped,
    MultiRouteScoped(crate::auth::Access),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct DomainAuthorizationSpec {
    domain: DispatchDomain,
    policy: AuthorizationPolicy,
}

#[derive(Debug, PartialEq, Eq)]
enum AuthorizationFailure {
    MissingSessionActor,
    PermissionDenied,
}

struct DomainDispatchRequest<'a> {
    router: &'a crate::runtime::Router,
    session_id: u64,
    channel_id: ChannelId,
    route_family: crate::runtime::routing::RouteFamily,
    domain: DispatchDomain,
    policy: AuthorizationPolicy,
    msg_type: crate::protocol::tlv::MessageType,
    preserve_payload_for_handler: bool,
}

#[derive(Clone, Copy, Debug)]
struct PendingSessionCleanup {
    route_family: crate::runtime::routing::RouteFamily,
}

impl<'a> AuthorizationTargets<'a> {
    fn span_target(&self) -> (&str, usize) {
        match self {
            Self::SessionOwned => ("<session-owned>", 1),
            Self::Single(route) => (route.as_ref(), 1),
            Self::Multiple(routes) => (
                routes
                    .first()
                    .map(|route| route.as_ref())
                    .unwrap_or("<session-owned>"),
                routes.len(),
            ),
        }
    }

    fn authorize(
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
                        .map(|route| route.as_ref())
                        .unwrap_or(wildcard_route),
                    routes.len(),
                )
            }
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
    sessions: Arc<DashMap<u64, SessionInfo>>,
    /// Per-session SessionActor instances for authorization checks
    session_actors: Arc<DashMap<u64, crate::session::actor::SessionActor>>,
    /// Cached per-session inbox routes used as the source address for domain dispatch.
    session_inbox_routes: Arc<DashMap<u64, crate::runtime::routing::Route>>,
    /// Best-effort retry tickets for session cleanups that failed initial delivery.
    pending_session_cleanups: Arc<DashMap<u64, PendingSessionCleanup>>,
    /// Prevent overlapping pending-cleanup sweeps from issuing duplicate retries.
    cleanup_retry_in_progress: Arc<AtomicBool>,
    /// Optional router for dispatching frames to domain sinks
    router: Option<Arc<crate::runtime::Router>>,
    /// Optional callback for session events (for routing to handlers)
    event_handler: Option<Arc<dyn Fn(SessionEvent) + Send + Sync>>,
    /// Route families provisioned at boot and accepted from verified JWT claims.
    route_families: Arc<std::collections::HashSet<u32>>,
    /// Whether authentication is required (if false, JWT is ignored and full access granted)
    auth_required: bool,
    /// Passive admin snapshot mirror for session lifecycle
    admin_read_model: Option<Arc<crate::api::admin::read_model::AdminReadModel>>,

    /// Explicit auth configuration used for CONNECT verification when present.
    auth_config: Option<crate::auth::AuthConfig>,
    /// Claim normalization behavior for CONNECT JWTs.
    auth_claims_config: crate::auth::AuthClaimsConfig,
    /// Broker-local route-family resolver for verified identity claims.
    route_family_resolver: crate::auth::RouteFamilyResolverConfig,
}

impl RuntimeIngress {
    /// Create a new ingress implementation
    pub fn new(auth_required: bool) -> Self {
        Self {
            sessions: Arc::new(DashMap::new()),
            session_actors: Arc::new(DashMap::new()),
            session_inbox_routes: Arc::new(DashMap::new()),
            pending_session_cleanups: Arc::new(DashMap::new()),
            cleanup_retry_in_progress: Arc::new(AtomicBool::new(false)),
            router: None,
            event_handler: None,
            route_families: Arc::new(std::iter::once(1).collect()),
            auth_required,
            admin_read_model: None,
            auth_config: None,
            auth_claims_config: crate::auth::AuthClaimsConfig::default(),
            route_family_resolver: crate::auth::RouteFamilyResolverConfig::default(),
        }
    }

    /// Set the event handler for session events
    ///
    /// The handler is called for each session lifecycle event (open, frame, close).
    pub fn with_event_handler<F>(mut self, handler: F) -> Self
    where
        F: Fn(SessionEvent) + Send + Sync + 'static,
    {
        self.event_handler = Some(Arc::new(handler));
        self
    }

    /// Attach a router reference for dispatching frames directly from ingress
    pub fn with_router(mut self, router: Arc<crate::runtime::Router>) -> Self {
        self.router = Some(router);
        self
    }

    pub fn with_admin_read_model(
        mut self,
        admin_read_model: Arc<crate::api::admin::read_model::AdminReadModel>,
    ) -> Self {
        self.admin_read_model = Some(admin_read_model);
        self
    }

    pub fn with_auth_config(mut self, auth_config: crate::auth::AuthConfig) -> Self {
        self.auth_config = Some(auth_config);
        self
    }

    pub fn with_auth_claims_config(
        mut self,
        auth_claims_config: crate::auth::AuthClaimsConfig,
    ) -> Self {
        self.auth_claims_config = auth_claims_config;
        self
    }

    pub fn with_route_family_resolver(
        mut self,
        route_family_resolver: crate::auth::RouteFamilyResolverConfig,
    ) -> Self {
        self.route_family_resolver = route_family_resolver;
        self
    }

    #[cfg(test)]
    fn with_route_family_map(mut self, mappings: &[(&str, u32)]) -> Self {
        self.route_family_resolver = crate::auth::RouteFamilyResolverConfig::from_mappings(
            crate::auth::DEFAULT_ROUTE_FAMILY_CLAIM,
            mappings
                .iter()
                .map(|(identity, family)| (*identity, *family)),
        );
        self
    }

    pub fn with_route_families(mut self, route_families: &[u32]) -> Self {
        self.route_families = Arc::new(route_families.iter().copied().collect());
        self
    }

    /// Get a clone of the session actor for authorization checks
    pub fn get_session_actor(
        &self,
        session_id: u64,
    ) -> Option<crate::session::actor::SessionActor> {
        self.session_actors
            .get(&session_id)
            .map(|entry| entry.value().clone())
    }

    /// Get a session by ID
    pub fn get_session(&self, session_id: u64) -> Option<SessionInfo> {
        self.sessions
            .get(&session_id)
            .map(|entry| entry.value().clone())
    }

    /// Get all active sessions
    pub fn active_sessions(&self) -> Vec<SessionInfo> {
        self.sessions
            .iter()
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Get session count
    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }

    fn finalize_session_close(&self, session_id: u64) {
        self.sessions.remove(&session_id);
        self.session_actors.remove(&session_id);
        self.session_inbox_routes.remove(&session_id);
        if let Some(admin_read_model) = &self.admin_read_model {
            admin_read_model.record_session_close(session_id);
        }
    }

    fn record_cleanup_failure(
        &self,
        session_id: u64,
        route_family: crate::runtime::routing::RouteFamily,
        failed_domains: &[DispatchDomain],
        store_retry_ticket: bool,
    ) {
        if let Ok(collector) = std::panic::catch_unwind(crate::observability::metrics) {
            collector.counter_add(
                obs::METRIC_SESSION_CLEANUP_FAILURES,
                failed_domains.len() as u64,
            );
        }

        if store_retry_ticket {
            self.pending_session_cleanups
                .insert(session_id, PendingSessionCleanup { route_family });
        }

        tracing::warn!(
            session_id = session_id,
            route_family = route_family.id(),
            failed_domains = ?failed_domains,
            retry_pending = store_retry_ticket,
            "Ingress: session cleanup incomplete"
        );
    }

    async fn retry_pending_session_cleanups(&self) {
        if self.pending_session_cleanups.is_empty() {
            return;
        }

        let Some(router) = &self.router else {
            return;
        };

        if self
            .cleanup_retry_in_progress
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }

        let pending = self
            .pending_session_cleanups
            .iter()
            .map(|entry| (*entry.key(), *entry.value()))
            .collect::<Vec<_>>();
        let pending_len = pending.len();
        let router = router.clone();

        let retry_result = tokio::task::spawn_blocking(move || {
            pending
                .into_iter()
                .map(|(session_id, cleanup)| {
                    (
                        session_id,
                        cleanup.route_family,
                        dispatch_session_cleanup(router.as_ref(), cleanup.route_family, session_id),
                    )
                })
                .collect::<Vec<_>>()
        })
        .await;

        self.cleanup_retry_in_progress
            .store(false, Ordering::SeqCst);

        match retry_result {
            Ok(retry_outcomes) => {
                for (session_id, route_family, failed_domains) in retry_outcomes {
                    if failed_domains.is_empty() {
                        self.pending_session_cleanups.remove(&session_id);
                        tracing::debug!(
                            session_id = session_id,
                            route_family = route_family.id(),
                            "Ingress: pending session cleanup retry succeeded"
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
            }
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    pending_sessions = pending_len,
                    "Ingress: pending cleanup retry worker task failed"
                );
            }
        }
    }

    pub async fn close_all_sessions(&self, reason: CloseReason) {
        let session_ids = self
            .sessions
            .iter()
            .map(|session| *session.key())
            .collect::<Vec<_>>();
        for session_id in session_ids {
            self.on_close(session_id, reason.clone()).await;
        }
    }

    fn resolve_authenticated_route_family(
        &self,
        raw_claims: &crate::auth::RawClaims,
    ) -> Result<crate::runtime::routing::RouteFamily, String> {
        let route_family = self.route_family_resolver.resolve(raw_claims)?;
        if !self.route_families.contains(&route_family) {
            return Err(format!(
                "resolved route family {} is not provisioned",
                route_family
            ));
        }
        Ok(crate::runtime::routing::RouteFamily::new(
            route_family.into(),
        ))
    }

    fn apply_authenticated_session(
        &self,
        session_id: u64,
        entry: &mut SessionInfo,
        claims: crate::auth::Claims,
        snapshot: SessionPermissions,
        route_family: crate::runtime::routing::RouteFamily,
    ) {
        entry.permissions_snapshot = snapshot.clone();
        entry.authenticated = true;
        entry.claims = Some(Arc::new(claims.clone()));
        entry.route_family = route_family;

        let mut actor = crate::session::actor::SessionActor::new(
            crate::session::session::SessionId(session_id),
            snapshot.clone(),
        );
        actor.authenticate(claims, snapshot);
        self.session_actors.insert(session_id, actor);

        if let Some(admin_read_model) = &self.admin_read_model {
            admin_read_model.record_session_update(entry);
        }
    }

    #[cfg_attr(not(test), allow(dead_code))]
    fn canonicalize_domain_route(
        domain: DispatchDomain,
        route: crate::runtime::routing::Route,
    ) -> Result<crate::runtime::routing::Route, String> {
        Self::canonicalize_domain_route_str(domain, route.as_str())
            .map(|route| crate::runtime::routing::Route::new(route.as_ref()))
    }

    fn canonicalize_domain_route_str<'a>(
        domain: DispatchDomain,
        route: &'a str,
    ) -> Result<Cow<'a, str>, String> {
        match domain {
            DispatchDomain::Kv => Self::canonicalize_triplet_route_str(domain, route, true),
            DispatchDomain::Queue | DispatchDomain::Lease | DispatchDomain::Stream => {
                Self::canonicalize_triplet_route_str(domain, route, false)
            }
            DispatchDomain::Rpc | DispatchDomain::Notice | DispatchDomain::Schedule => {
                Ok(Self::scheme_prefixed_route_str(domain.as_str(), route))
            }
        }
    }

    fn scheme_prefixed_route_str<'a>(domain: &str, route: &'a str) -> Cow<'a, str> {
        if route.contains("://") {
            Cow::Borrowed(route)
        } else {
            let trimmed = route.trim_start_matches('/');
            let mut canonical = String::with_capacity(domain.len() + 3 + trimmed.len());
            canonical.push_str(domain);
            canonical.push_str("://");
            canonical.push_str(trimmed);
            Cow::Owned(canonical)
        }
    }

    fn canonicalize_triplet_route_str<'a>(
        domain: DispatchDomain,
        route: &'a str,
        exact: bool,
    ) -> Result<Cow<'a, str>, String> {
        let parts = if exact {
            crate::runtime::routing::route_exact_triplet(route)
        } else {
            crate::runtime::routing::route_triplet(route)
        }
        .ok_or_else(|| {
            format!(
                "{} route must be realm/area/resource{}",
                domain.as_str(),
                if exact { "" } else { " or deeper" }
            )
        })?;

        if parts.realm.is_empty() || parts.area.is_empty() || parts.resource.is_empty() {
            return Err(format!(
                "{} route must include non-empty realm/area/resource",
                domain.as_str()
            ));
        }

        let domain_name = domain.as_str();
        let mut canonical = String::with_capacity(
            domain_name.len() + 3 + parts.realm.len() + parts.area.len() + parts.resource.len() + 2,
        );
        canonical.push_str(domain_name);
        canonical.push_str("://");
        canonical.push_str(parts.realm);
        canonical.push('/');
        canonical.push_str(parts.area);
        canonical.push('/');
        canonical.push_str(parts.resource);

        if route == canonical {
            Ok(Cow::Borrowed(route))
        } else {
            Ok(Cow::Owned(canonical))
        }
    }

    fn cached_session_inbox_route(&self, session_id: u64) -> crate::runtime::routing::Route {
        self.session_inbox_routes
            .get(&session_id)
            .map(|entry| entry.value().clone())
            .unwrap_or_else(|| {
                crate::runtime::routing::Route::new(format!("inbox://session/{session_id}"))
            })
    }

    fn domain_dispatch_for_msg_type(
        msg_type: crate::protocol::tlv::MessageType,
    ) -> Result<Option<DomainAuthorizationSpec>, &'static str> {
        use crate::auth::Access;

        let mt = msg_type.as_u16();

        match mt {
            100 => Ok(Some(DomainAuthorizationSpec {
                domain: DispatchDomain::Kv,
                policy: AuthorizationPolicy::KvBeginModeScoped,
            })),
            101..=108 => Ok(Some(DomainAuthorizationSpec {
                domain: DispatchDomain::Kv,
                policy: AuthorizationPolicy::SessionOwned,
            })),
            109 | 110 => Ok(Some(DomainAuthorizationSpec {
                domain: DispatchDomain::Kv,
                policy: AuthorizationPolicy::RouteScoped(Access::Read),
            })),
            111 => Err("invalid message type: 111 is server-to-client only"),
            200..=204 => Ok(Some(DomainAuthorizationSpec {
                domain: DispatchDomain::Queue,
                policy: AuthorizationPolicy::RouteScoped(Access::Write),
            })),
            207 | 208 => Ok(Some(DomainAuthorizationSpec {
                domain: DispatchDomain::Queue,
                policy: AuthorizationPolicy::RouteScoped(Access::Read),
            })),
            209 => Err("invalid message type: 209 is server-to-client only"),
            205 | 206 | 210..=299 => Err("invalid message type: unsupported queue operation"),
            300 | 301 => Ok(Some(DomainAuthorizationSpec {
                domain: DispatchDomain::Rpc,
                policy: AuthorizationPolicy::RouteScoped(Access::All),
            })),
            302 => Ok(Some(DomainAuthorizationSpec {
                domain: DispatchDomain::Rpc,
                policy: AuthorizationPolicy::RouteScoped(Access::Write),
            })),
            303 | 304 => Ok(Some(DomainAuthorizationSpec {
                domain: DispatchDomain::Rpc,
                policy: AuthorizationPolicy::SessionOwned,
            })),
            305..=399 => Ok(Some(DomainAuthorizationSpec {
                domain: DispatchDomain::Rpc,
                policy: AuthorizationPolicy::RouteScoped(Access::Read),
            })),
            400..=402 => Ok(Some(DomainAuthorizationSpec {
                domain: DispatchDomain::Lease,
                policy: AuthorizationPolicy::RouteScoped(Access::Write),
            })),
            403 => Ok(Some(DomainAuthorizationSpec {
                domain: DispatchDomain::Lease,
                policy: AuthorizationPolicy::RouteScoped(Access::Read),
            })),
            407 | 408 => Ok(Some(DomainAuthorizationSpec {
                domain: DispatchDomain::Lease,
                policy: AuthorizationPolicy::RouteScoped(Access::Read),
            })),
            409 => Err("invalid message type: 409 is server-to-client only"),
            404..=406 | 410..=499 => Err("invalid message type: unsupported lease operation"),
            500 => Ok(Some(DomainAuthorizationSpec {
                domain: DispatchDomain::Notice,
                policy: AuthorizationPolicy::RouteScoped(Access::Write),
            })),
            501 => Ok(Some(DomainAuthorizationSpec {
                domain: DispatchDomain::Notice,
                policy: AuthorizationPolicy::RouteScoped(Access::Read),
            })),
            502 | 503 => Ok(Some(DomainAuthorizationSpec {
                domain: DispatchDomain::Notice,
                policy: AuthorizationPolicy::SessionOwned,
            })),
            504 => Err("invalid message type: 504 is server-to-client only"),
            505..=599 => Err("invalid message type: 505-599 are unsupported notice operations"),
            600 => Ok(Some(DomainAuthorizationSpec {
                domain: DispatchDomain::Stream,
                policy: AuthorizationPolicy::RouteScoped(Access::Write),
            })),
            601..=603 => Ok(Some(DomainAuthorizationSpec {
                domain: DispatchDomain::Stream,
                policy: AuthorizationPolicy::SessionOwned,
            })),
            604..=608 => Ok(Some(DomainAuthorizationSpec {
                domain: DispatchDomain::Stream,
                policy: AuthorizationPolicy::RouteScoped(Access::Read),
            })),
            609 => Err("invalid message type: 609 is server-to-client only"),
            700 | 701 => Ok(Some(DomainAuthorizationSpec {
                domain: DispatchDomain::Schedule,
                policy: AuthorizationPolicy::RouteScoped(Access::Write),
            })),
            706 => Ok(Some(DomainAuthorizationSpec {
                domain: DispatchDomain::Schedule,
                policy: AuthorizationPolicy::MultiRouteScoped(Access::Write),
            })),
            702 => Ok(Some(DomainAuthorizationSpec {
                domain: DispatchDomain::Schedule,
                policy: AuthorizationPolicy::WildcardScoped(Access::Read),
            })),
            703 | 704 => Ok(Some(DomainAuthorizationSpec {
                domain: DispatchDomain::Schedule,
                policy: AuthorizationPolicy::RouteScoped(Access::Read),
            })),
            705 => Err("invalid message type: 705 is server-to-client only"),
            _ => Ok(None),
        }
    }

    fn resolve_authorization_targets<'a>(
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

    fn kv_begin_access(payload: &[u8]) -> Result<crate::auth::Access, String> {
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
            value => Err(format!("Invalid durability mode: {}", value)),
        }
    }

    fn unauthorized_error_code(domain: DispatchDomain) -> u16 {
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
                IngressDecision::Close(format!("unauthorized response delivery failed: {}", error))
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
        let envelope = crate::runtime::envelope::Envelope::from_route(source, addr, ctx);
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
    fn derive_route_for_frame(
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::admin::read_model::AdminReadModel;
    use crate::auth::Access;
    use crate::boot::domains::{
        KvDomainSink, LeaseDomainSink, NoticeDomainSink, QueueDomainSink, RpcDomainSink,
        ScheduleDomainSink, StreamDomainSink,
    };
    use crate::protocol::frame::ChannelId;
    use crate::protocol::payload_codec::PayloadEncoder;
    use crate::protocol::tlv::MessageType;
    use crate::protocol::FrameContext;
    use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
    use crate::runtime::{DeliveryError, Envelope, Mailbox, MailboxSink};
    use crate::session::{SessionInfo, SessionMetadata, SessionPermissions, TransportKind};
    use bytes::Bytes;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex, Once};

    const TEST_AUTH_ISSUER: &str = "https://idp.example";
    const TEST_AUTH_AUDIENCE: &str = "fitz-broker";
    const TEST_AUTH_SECRET: &str = "test-secret-key";
    const TEST_AUTH_JWKS_URL: &str = "https://idp.example/.well-known/jwks.json";

    static TEST_AUTH_JWKS_CACHE: Once = Once::new();

    #[derive(Default)]
    struct CleanupTrackingSink {
        cleanup_sessions: Mutex<Vec<u64>>,
    }

    impl CleanupTrackingSink {
        fn recorded_sessions(&self) -> Vec<u64> {
            self.cleanup_sessions.lock().unwrap().clone()
        }
    }

    impl MailboxSink for CleanupTrackingSink {
        fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
            let cleanup = envelope
                .payload::<crate::runtime::SessionCleanup>()
                .expect("cleanup payload");
            self.cleanup_sessions
                .lock()
                .unwrap()
                .push(cleanup.session_id);
            Ok(())
        }

        fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
            self.deliver(envelope)
        }
    }

    fn make_session_info(id: u64, kind: TransportKind) -> SessionInfo {
        SessionInfo {
            session_id: id,
            transport_kind: kind,
            peer_addr: None,
            metadata: Arc::new(SessionMetadata::new()),
            permissions_snapshot: SessionPermissions::empty(),
            claims: None,
            authenticated: false,
            route_family: crate::runtime::routing::RouteFamily::new(0), // Test mode = family 0
        }
    }

    fn permissions_from_strings(raw_permissions: &[&str]) -> SessionPermissions {
        let permissions = raw_permissions
            .iter()
            .map(|permission| crate::auth::Permission::parse(permission).unwrap())
            .collect();
        SessionPermissions::from_permissions(permissions)
    }

    fn make_authenticated_session_info(
        id: u64,
        kind: TransportKind,
        route_family: RouteFamily,
        raw_permissions: &[&str],
    ) -> SessionInfo {
        let mut session = make_session_info(id, kind);
        session.authenticated = true;
        session.route_family = route_family;
        session.permissions_snapshot = permissions_from_strings(raw_permissions);
        session
    }

    fn install_expired_session_actor(
        ingress: &RuntimeIngress,
        session_id: u64,
        raw_permissions: &[&str],
    ) {
        let permissions = raw_permissions
            .iter()
            .map(|permission| crate::auth::Permission::parse(permission).unwrap())
            .collect::<Vec<_>>();
        let snapshot = SessionPermissions::from_permissions(permissions.clone());
        let claims = crate::auth::Claims {
            sub: format!("test-session-{session_id}"),
            identity_claim: Some("test".to_string()),
            identity_value: Some("test".to_string()),
            permissions,
            exp: 0,
        };
        let mut actor = crate::session::actor::SessionActor::new(
            crate::session::session::SessionId(session_id),
            snapshot.clone(),
        );
        actor.authenticate(claims, snapshot);
        ingress.session_actors.insert(session_id, actor);
    }

    fn auth_spec(msg_type: u16) -> DomainAuthorizationSpec {
        RuntimeIngress::domain_dispatch_for_msg_type(MessageType::new(msg_type))
            .unwrap()
            .unwrap()
    }

    fn receive_frame(mailbox: &Mailbox, label: &str) -> FrameContext {
        mailbox
            .receiver()
            .try_recv()
            .unwrap_or_else(|_| panic!("expected {label}"))
            .into_payload::<FrameContext>()
            .unwrap_or_else(|| panic!("expected {label} frame context"))
    }

    fn decode_domain_error_code(payload: &[u8]) -> u16 {
        let (code, _) =
            crate::protocol::error_codes::decode_error_body(payload).expect("decode domain error");
        code
    }

    fn runtime_auth_config() -> crate::auth::AuthConfig {
        crate::auth::AuthConfig::jwks(
            vec![TEST_AUTH_AUDIENCE.to_string()],
            vec![crate::auth::JwksIssuerConfig {
                issuer: TEST_AUTH_ISSUER.to_string(),
                jwks_url: TEST_AUTH_JWKS_URL.to_string(),
            }],
        )
    }

    fn runtime_ingress_with_jwks_auth() -> RuntimeIngress {
        RuntimeIngress::new(true).with_auth_config(runtime_auth_config())
    }

    fn seed_runtime_jwks_cache() {
        TEST_AUTH_JWKS_CACHE.call_once(|| {
            use base64::Engine;

            let key = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(TEST_AUTH_SECRET);
            let jwks = serde_json::json!({
                "keys": [
                    {
                        "kty": "oct",
                        "kid": "",
                        "k": key,
                    }
                ]
            })
            .to_string();

            crate::auth::cache_jwks_from_json(TEST_AUTH_JWKS_URL, &jwks).unwrap();
        });
    }

    fn signed_jwks_jwt(mut payload: serde_json::Value) -> String {
        use jsonwebtoken::{Algorithm, EncodingKey, Header};

        seed_runtime_jwks_cache();
        if let Some(map) = payload.as_object_mut() {
            match map.get("iss") {
                Some(serde_json::Value::String(value)) if !value.is_empty() => {}
                _ => {
                    map.insert(
                        "iss".to_string(),
                        serde_json::Value::String(TEST_AUTH_ISSUER.to_string()),
                    );
                }
            }

            map.entry("aud".to_string())
                .or_insert_with(|| serde_json::Value::String(TEST_AUTH_AUDIENCE.to_string()));
        }

        jsonwebtoken::encode(
            &Header::new(Algorithm::HS256),
            &payload,
            &EncodingKey::from_secret(TEST_AUTH_SECRET.as_bytes()),
        )
        .unwrap()
    }

    fn encode_notice_subscribe(pattern: &str) -> Bytes {
        let mut encoder = PayloadEncoder::new();
        encoder.put_string(pattern);
        Bytes::from(encoder.finish())
    }

    fn encode_notice_publish(route: &str, payload: &[u8]) -> Bytes {
        let mut encoder = PayloadEncoder::new();
        encoder.put_string(route);
        encoder.put_bytes(payload);
        Bytes::from(encoder.finish())
    }

    fn encode_lease_acquire(route: &str, owner_id: &str, ttl_secs: u64) -> Bytes {
        let mut encoder = PayloadEncoder::new();
        encoder.put_string(route);
        encoder.put_string(owner_id);
        encoder.put_u64(ttl_secs);
        Bytes::from(encoder.finish())
    }

    fn encode_lease_subscribe(pattern: &str) -> Bytes {
        let mut encoder = PayloadEncoder::new();
        encoder.put_string(pattern);
        Bytes::from(encoder.finish())
    }

    fn encode_schedule_create(route: &str, cron: &str, payload: &[u8]) -> Bytes {
        let mut encoder = PayloadEncoder::new();
        encoder.put_string(route);
        encoder.put_string(cron);
        encoder.put_bytes(payload);
        Bytes::from(encoder.finish())
    }

    fn encode_schedule_subscribe(route: &str) -> Bytes {
        let mut encoder = PayloadEncoder::new();
        encoder.put_string(route);
        Bytes::from(encoder.finish())
    }

    fn encode_queue_send(route: &str, body: &[u8]) -> Bytes {
        let mut payload = Vec::new();
        bytes::BufMut::put_u32(&mut payload, route.len() as u32);
        bytes::BufMut::put_slice(&mut payload, route.as_bytes());
        bytes::BufMut::put_u32(&mut payload, body.len() as u32);
        bytes::BufMut::put_slice(&mut payload, body);
        Bytes::from(payload)
    }

    fn encode_queue_reserve(route: &str, inflight_seconds: u64, batch_size: u32) -> Bytes {
        let mut payload = Vec::new();
        bytes::BufMut::put_u32(&mut payload, route.len() as u32);
        bytes::BufMut::put_slice(&mut payload, route.as_bytes());
        bytes::BufMut::put_u64(&mut payload, inflight_seconds);
        bytes::BufMut::put_u8(&mut payload, 1);
        bytes::BufMut::put_u32(&mut payload, batch_size);
        Bytes::from(payload)
    }

    fn queue_receive_response_message_count(frame: &FrameContext) -> u32 {
        assert_eq!(frame.payload[0], 0, "expected success status");
        u32::from_be_bytes(
            frame.payload[1..5]
                .try_into()
                .expect("receive payload should include count"),
        )
    }

    fn parse_rpc_response_frame(
        frame: &FrameContext,
    ) -> crate::domains::rpc::protocol::RpcResponse {
        match crate::protocol::rpc_codec::parse_request(frame, &frame.payload, frame.route_family)
            .expect("parse rpc response frame")
        {
            crate::domains::rpc::protocol::RpcMessage::Response(response) => response,
            other => panic!("expected rpc response, found {other:?}"),
        }
    }

    fn drain_mailbox(mailbox: &Mailbox) {
        while mailbox.receiver().try_recv().is_ok() {}
    }

    fn register_fallback_cleanup_domains(
        router: &Arc<crate::runtime::Router>,
        real_domain: DispatchDomain,
    ) {
        for domain in DispatchDomain::SESSION_CLEANUP_ORDER {
            if domain == real_domain {
                continue;
            }

            router
                .register_domain_pattern(domain.as_str(), Arc::new(CleanupTrackingSink::default()));
        }
    }

    fn make_cleanup_ingress(
        router: Arc<crate::runtime::Router>,
        admin_read_model: Arc<AdminReadModel>,
    ) -> RuntimeIngress {
        runtime_ingress_with_jwks_auth()
            .with_router(router)
            .with_admin_read_model(admin_read_model)
    }

    #[tokio::test]
    async fn should_open_session() {
        let ingress = runtime_ingress_with_jwks_auth().with_route_family_map(&[("acme-prod", 1)]);
        let session = make_session_info(1, TransportKind::WebSocket);

        let result = ingress.on_open(session).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1);
        assert_eq!(ingress.session_count(), 1);
    }

    #[test]
    fn should_process_frame() {
        // Arrange
        let ingress = runtime_ingress_with_jwks_auth().with_route_family_map(&[("acme-prod", 1)]);
        let session = make_session_info(2, TransportKind::WebSocket);

        // Act
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            ingress.on_open(session).await.unwrap();

            // First, perform a connect to authenticate the session
            let payload = serde_json::json!({
                "iss": "",
                "aud": "fitz-broker",
                "sub": "user:2",
                "exp": 9999999999u64,
                "tid": "acme-prod",
                "permissions": ["notice://prod/orders/**#read"]
            });
            let jwt = signed_jwks_jwt(payload);

            let decision = ingress
                .on_frame(
                    2,
                    ChannelId::Control,
                    crate::protocol::tlv::MessageType::CONNECT,
                    Bytes::from(jwt),
                )
                .await;

            // Assert
            assert_eq!(decision, IngressDecision::Accept);
        });
    }

    #[tokio::test]
    async fn should_reject_unknown_session() {
        let ingress = runtime_ingress_with_jwks_auth().with_route_family_map(&[("acme-prod", 1)]);

        let decision = ingress
            .on_frame(
                999,
                ChannelId::Control,
                crate::protocol::tlv::MessageType::new(42),
                Bytes::from("test"),
            )
            .await;

        assert!(matches!(decision, IngressDecision::Close(_)));
    }

    #[test]
    fn should_call_event_handler() {
        // Arrange
        let event_count = Arc::new(AtomicUsize::new(0));
        let count_clone = event_count.clone();
        let ingress = runtime_ingress_with_jwks_auth()
            .with_route_family_map(&[("acme-prod", 1)])
            .with_event_handler(move |_event| {
                count_clone.fetch_add(1, Ordering::SeqCst);
            });
        let session = make_session_info(3, TransportKind::WebSocket);

        // Act
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            ingress.on_open(session).await.unwrap();
            // Authenticate session with a connect
            let payload = serde_json::json!({
                "iss": "",
                "aud": "fitz-broker",
                "sub": "user:3",
                "exp": 9999999999u64,
                "tid": "acme-prod",
                "permissions": ["notice://prod/orders/**#read"]
            });
            let jwt = signed_jwks_jwt(payload);

            ingress
                .on_frame(
                    3,
                    ChannelId::Control,
                    crate::protocol::tlv::MessageType::CONNECT,
                    Bytes::from(jwt),
                )
                .await;
            ingress.on_close(3, CloseReason::ClientClose).await;
        });

        // Assert
        assert_eq!(event_count.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn should_dispatch_session_cleanup_to_all_registered_domains() {
        // Arrange
        let router = crate::runtime::Router::new();
        let route_family = crate::runtime::routing::RouteFamily::new(7);
        let session_id = 42;
        let sinks = DispatchDomain::SESSION_CLEANUP_ORDER
            .iter()
            .copied()
            .map(|domain| {
                let sink = Arc::new(CleanupTrackingSink::default());
                router.register_domain_pattern(domain.as_str(), sink.clone());
                (domain, sink)
            })
            .collect::<Vec<_>>();

        // Act
        let failed_domains = dispatch_session_cleanup(&router, route_family, session_id);

        // Assert
        assert!(failed_domains.is_empty());
        for (_, sink) in sinks {
            assert_eq!(sink.recorded_sessions(), vec![session_id]);
        }
    }

    #[test]
    fn should_report_missing_cleanup_domain_registration() {
        // Arrange
        let router = crate::runtime::Router::new();
        let route_family = crate::runtime::routing::RouteFamily::new(9);
        let session_id = 55;
        let sinks = DispatchDomain::SESSION_CLEANUP_ORDER
            .iter()
            .copied()
            .filter(|domain| *domain != DispatchDomain::Queue)
            .map(|domain| {
                let sink = Arc::new(CleanupTrackingSink::default());
                router.register_domain_pattern(domain.as_str(), sink.clone());
                sink
            })
            .collect::<Vec<_>>();

        // Act
        let failed_domains = dispatch_session_cleanup(&router, route_family, session_id);

        // Assert
        assert_eq!(failed_domains, vec![DispatchDomain::Queue]);
        for sink in sinks {
            assert_eq!(sink.recorded_sessions(), vec![session_id]);
        }
    }

    #[test]
    fn should_keep_session_cleanup_order_aligned_with_domain_manifest() {
        // Arrange
        let mut cleanup_domains = DispatchDomain::SESSION_CLEANUP_ORDER
            .iter()
            .map(|domain| domain.as_str())
            .collect::<Vec<_>>();
        let mut registered_domains = DispatchDomain::ALL
            .iter()
            .map(|domain| domain.as_str())
            .collect::<Vec<_>>();

        // Act
        cleanup_domains.sort_unstable();
        registered_domains.sort_unstable();

        // Assert
        assert_eq!(cleanup_domains, registered_domains);
    }

    #[tokio::test]
    async fn should_cleanup_registered_domains_and_remove_session_state_on_close() {
        // Arrange
        let router = Arc::new(crate::runtime::Router::new());
        let admin_read_model = AdminReadModel::new();
        let ingress = make_cleanup_ingress(router.clone(), admin_read_model.clone());
        let session_id = 77;
        let mut session = make_session_info(session_id, TransportKind::WebSocket);
        session.route_family = RouteFamily::new(77);

        let sinks = DispatchDomain::SESSION_CLEANUP_ORDER
            .iter()
            .copied()
            .map(|domain| {
                let sink = Arc::new(CleanupTrackingSink::default());
                router.register_domain_pattern(domain.as_str(), sink.clone());
                sink
            })
            .collect::<Vec<_>>();

        ingress.on_open(session).await.unwrap();
        assert_eq!(admin_read_model.sessions().len(), 1);

        // Act
        ingress.on_close(session_id, CloseReason::ClientClose).await;

        // Assert
        assert_eq!(ingress.session_count(), 0);
        assert!(ingress.get_session(session_id).is_none());
        assert!(ingress.get_session_actor(session_id).is_none());
        assert!(admin_read_model.sessions().is_empty());
        for sink in sinks {
            assert_eq!(sink.recorded_sessions(), vec![session_id]);
        }
    }

    #[tokio::test]
    async fn should_record_cleanup_failures_when_on_close_cannot_reach_all_domains() {
        // Arrange
        let collector = crate::observability::metrics();

        let router = Arc::new(crate::runtime::Router::new());
        let admin_read_model = AdminReadModel::new();
        let ingress = make_cleanup_ingress(router.clone(), admin_read_model.clone());
        let session_id = 88;
        let mut session = make_session_info(session_id, TransportKind::Tcp);
        session.route_family = RouteFamily::new(88);

        for domain in DispatchDomain::SESSION_CLEANUP_ORDER {
            if domain == DispatchDomain::Queue {
                continue;
            }

            let sink = Arc::new(CleanupTrackingSink::default());
            router.register_domain_pattern(domain.as_str(), sink);
        }

        ingress.on_open(session).await.unwrap();
        let failures_before = collector.counter_get(obs::METRIC_SESSION_CLEANUP_FAILURES);

        // Act
        ingress.on_close(session_id, CloseReason::ClientClose).await;

        // Assert
        assert!(
            collector.counter_get(obs::METRIC_SESSION_CLEANUP_FAILURES) > failures_before,
            "expected cleanup failure metric to increase"
        );
        assert_eq!(ingress.session_count(), 0);
        assert!(ingress.get_session(session_id).is_none());
        assert!(ingress.get_session_actor(session_id).is_none());
        assert!(admin_read_model.sessions().is_empty());
        assert!(ingress.pending_session_cleanups.contains_key(&session_id));
    }

    #[tokio::test]
    async fn should_retry_pending_session_cleanup_on_next_session_open() {
        // Arrange
        let router = Arc::new(crate::runtime::Router::new());
        let admin_read_model = AdminReadModel::new();
        let ingress = make_cleanup_ingress(router.clone(), admin_read_model);
        let session_id = 89;
        let mut session = make_session_info(session_id, TransportKind::Tcp);
        session.route_family = RouteFamily::new(89);

        for domain in DispatchDomain::SESSION_CLEANUP_ORDER {
            if domain == DispatchDomain::Queue {
                continue;
            }

            let sink = Arc::new(CleanupTrackingSink::default());
            router.register_domain_pattern(domain.as_str(), sink);
        }

        ingress.on_open(session).await.unwrap();
        ingress.on_close(session_id, CloseReason::ClientClose).await;
        assert!(ingress.pending_session_cleanups.contains_key(&session_id));

        let queue_sink = Arc::new(CleanupTrackingSink::default());
        router.register_domain_pattern(DispatchDomain::Queue.as_str(), queue_sink.clone());

        let mut next_session = make_session_info(90, TransportKind::WebSocket);
        next_session.route_family = RouteFamily::new(90);

        // Act
        ingress.on_open(next_session).await.unwrap();

        // Assert
        assert!(!ingress.pending_session_cleanups.contains_key(&session_id));
        assert_eq!(queue_sink.recorded_sessions(), vec![session_id]);
    }

    #[tokio::test]
    async fn should_cleanup_real_notice_domain_subscription_on_close() {
        // Arrange
        let family = RouteFamily::new(91);
        let session_id = 91;
        let notice_route = "notice://acme/app/events";
        let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/91"));
        let publisher_address = RouteAddress::new(family, Route::new("inbox://session/11"));
        let notice_address = RouteAddress::new(family, Route::new(notice_route));

        let router = Arc::new(crate::runtime::Router::new());
        let admin_read_model = AdminReadModel::new();
        let notice_sink = Arc::new(NoticeDomainSink::new(
            router.clone(),
            admin_read_model.clone(),
        ));
        let subscriber_mailbox = Arc::new(Mailbox::new(8));

        router.register(subscriber_address.clone(), subscriber_mailbox.clone());
        router.register_domain_pattern("notice", notice_sink.clone());
        register_fallback_cleanup_domains(&router, DispatchDomain::Notice);

        let ingress = make_cleanup_ingress(router, admin_read_model.clone());
        let mut session = make_session_info(session_id, TransportKind::WebSocket);
        session.route_family = family;
        ingress.on_open(session).await.unwrap();

        notice_sink
            .deliver(Envelope::from_route(
                subscriber_address,
                notice_address.clone(),
                FrameContext::new(
                    session_id,
                    ChannelId::Sub,
                    MessageType::new(501),
                    encode_notice_subscribe(notice_route),
                    family,
                ),
            ))
            .expect("subscribe notice route");

        let _subscribe_response = subscriber_mailbox
            .receiver()
            .try_recv()
            .expect("notice subscribe response");
        notice_sink.refresh_admin_snapshot_if_dirty();
        assert_eq!(notice_sink.subscription_count(), 1);
        assert_eq!(admin_read_model.notice_subscriptions(None, None).len(), 1);

        // Act
        ingress.on_close(session_id, CloseReason::ClientClose).await;
        notice_sink.refresh_admin_snapshot_if_dirty();
        notice_sink
            .deliver(Envelope::from_route(
                publisher_address,
                notice_address,
                FrameContext::new(
                    11,
                    ChannelId::Sub,
                    MessageType::new(500),
                    encode_notice_publish(notice_route, b"hello"),
                    family,
                ),
            ))
            .expect("publish after cleanup");

        // Assert
        assert_eq!(notice_sink.subscription_count(), 0);
        assert!(admin_read_model.notice_subscriptions(None, None).is_empty());
        assert!(subscriber_mailbox.receiver().try_recv().is_err());
    }

    #[tokio::test]
    async fn should_cleanup_real_queue_inflight_on_close() {
        // Arrange
        let family = RouteFamily::new(1);
        let sender_session_id = 7;
        let worker_session_id = 92;
        let next_worker_session_id = 12;
        let queue_route = "queue://acme/jobs/emails";
        let queue_address = RouteAddress::new(family, Route::new("queue://inbound"));
        let sender_address = RouteAddress::new(family, Route::new("inbox://session/7"));
        let worker_address = RouteAddress::new(family, Route::new("inbox://session/92"));
        let next_worker_address = RouteAddress::new(family, Route::new("inbox://session/12"));

        let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
        let router = Arc::new(crate::runtime::Router::new());
        let admin_read_model = AdminReadModel::new();
        let queue_sink = Arc::new(QueueDomainSink::new(
            store,
            router.clone(),
            admin_read_model.clone(),
            cntryl_midge::WriteOptions::buffered(),
            crate::utils::idempotency::default_dedup_store(),
        ));

        let sender_mailbox = Arc::new(Mailbox::new(8));
        let worker_mailbox = Arc::new(Mailbox::new(8));
        let next_worker_mailbox = Arc::new(Mailbox::new(8));

        router.register(sender_address.clone(), sender_mailbox.clone());
        router.register(worker_address.clone(), worker_mailbox.clone());
        router.register(next_worker_address.clone(), next_worker_mailbox.clone());
        router.register_domain_pattern("queue", queue_sink.clone());
        register_fallback_cleanup_domains(&router, DispatchDomain::Queue);

        let ingress = make_cleanup_ingress(router, admin_read_model.clone());
        let mut worker_session = make_session_info(worker_session_id, TransportKind::WebSocket);
        worker_session.route_family = family;
        ingress.on_open(worker_session).await.unwrap();

        queue_sink
            .deliver(Envelope::from_route(
                sender_address,
                queue_address.clone(),
                FrameContext::new(
                    sender_session_id,
                    ChannelId::Pub,
                    MessageType::new(200),
                    encode_queue_send(queue_route, b"email"),
                    family,
                ),
            ))
            .expect("enqueue queue message");
        let _send_ack = sender_mailbox
            .receiver()
            .try_recv()
            .expect("enqueue response");

        queue_sink
            .deliver(Envelope::from_route(
                worker_address,
                queue_address.clone(),
                FrameContext::new(
                    worker_session_id,
                    ChannelId::Pub,
                    MessageType::new(202),
                    encode_queue_reserve(queue_route, 30, 1),
                    family,
                ),
            ))
            .expect("reserve queue message");
        let _reserve_ack = worker_mailbox
            .receiver()
            .try_recv()
            .expect("reserve response");

        queue_sink.refresh_admin_snapshot_if_dirty();
        assert_eq!(admin_read_model.queue_inflight(None).len(), 1);

        // Act
        ingress
            .on_close(worker_session_id, CloseReason::ClientClose)
            .await;
        queue_sink.refresh_admin_snapshot_if_dirty();
        queue_sink
            .deliver(Envelope::from_route(
                next_worker_address,
                queue_address,
                FrameContext::new(
                    next_worker_session_id,
                    ChannelId::Pub,
                    MessageType::new(202),
                    encode_queue_reserve(queue_route, 30, 1),
                    family,
                ),
            ))
            .expect("reserve queue message after cleanup");
        queue_sink.refresh_admin_snapshot_if_dirty();

        // Assert
        let reserve_after_cleanup = next_worker_mailbox
            .receiver()
            .try_recv()
            .expect("reserve response after cleanup")
            .into_payload::<FrameContext>()
            .expect("reserve response frame after cleanup");
        assert_eq!(
            queue_receive_response_message_count(&reserve_after_cleanup),
            1
        );

        let queues = admin_read_model.queues(None);
        assert_eq!(queues.len(), 1);
        assert_eq!(queues[0].messages_ready, 0);
        assert_eq!(queues[0].messages_inflight, 1);
        assert_eq!(admin_read_model.queue_inflight(None).len(), 1);
        assert_eq!(
            admin_read_model.queue_inflight(None)[0].session_id,
            next_worker_session_id.to_string()
        );
    }

    #[tokio::test]
    async fn should_cleanup_real_rpc_pending_request_on_close() {
        // Arrange
        let family = RouteFamily::new(93);
        let caller_session_id = 1;
        let worker_session_id = 42;
        let rpc_route = "rpc://acme/system/resource/operation";
        let rpc_address = RouteAddress::new(family, Route::new(rpc_route));
        let caller_address = RouteAddress::new(family, Route::new("inbox://session/1"));
        let worker_address = RouteAddress::new(family, Route::new("inbox://session/42"));

        let router = Arc::new(crate::runtime::Router::new());
        let admin_read_model = AdminReadModel::new();
        let rpc_sink = Arc::new(RpcDomainSink::new(router.clone(), admin_read_model.clone()));
        let caller_mailbox = Arc::new(Mailbox::new(8));
        let worker_mailbox = Arc::new(Mailbox::new(8));

        router.register(caller_address.clone(), caller_mailbox.clone());
        router.register(worker_address.clone(), worker_mailbox.clone());
        router.register_domain_pattern("rpc", rpc_sink.clone());
        register_fallback_cleanup_domains(&router, DispatchDomain::Rpc);

        let ingress = make_cleanup_ingress(router, admin_read_model);
        let mut worker_session = make_session_info(worker_session_id, TransportKind::WebSocket);
        worker_session.route_family = family;
        ingress.on_open(worker_session).await.unwrap();

        let register_frame = crate::benchkit::build_rpc_subscribe(rpc_route);
        let (register_msg_type, register_payload) =
            crate::benchkit::extract_single_tlv_field(&register_frame);
        rpc_sink
            .deliver(Envelope::from_route(
                worker_address,
                rpc_address.clone(),
                FrameContext::new(
                    worker_session_id,
                    ChannelId::Rpc,
                    MessageType::new(register_msg_type),
                    register_payload,
                    family,
                ),
            ))
            .expect("register rpc worker");
        let _register_ack = worker_mailbox
            .receiver()
            .try_recv()
            .expect("rpc worker register response");

        let request_frame = crate::benchkit::build_rpc_request(rpc_route, b"ping");
        let (request_msg_type, request_payload) =
            crate::benchkit::extract_single_tlv_field(&request_frame);
        let request_ctx = FrameContext::new(
            caller_session_id,
            ChannelId::Rpc,
            MessageType::new(request_msg_type),
            request_payload.clone(),
            family,
        );
        let request =
            match crate::protocol::rpc_codec::parse_request(&request_ctx, &request_payload, family)
                .expect("parse rpc request")
            {
                crate::domains::rpc::protocol::RpcMessage::Request(request) => request,
                other => panic!("expected rpc request, found {other:?}"),
            };

        rpc_sink
            .deliver(Envelope::from_route(
                caller_address,
                rpc_address,
                request_ctx,
            ))
            .expect("deliver rpc request");

        assert_eq!(rpc_sink.worker_count(), 1);
        assert_eq!(rpc_sink.pending_request_count(), 1);
        let _worker_request = worker_mailbox
            .receiver()
            .try_recv()
            .expect("worker request delivery");

        // Act
        ingress
            .on_close(worker_session_id, CloseReason::ClientClose)
            .await;

        // Assert
        assert_eq!(rpc_sink.worker_count(), 0);
        assert_eq!(rpc_sink.pending_request_count(), 0);

        let request_ack = caller_mailbox
            .receiver()
            .try_recv()
            .expect("rpc request ack")
            .into_payload::<FrameContext>()
            .expect("rpc request ack frame");
        assert_eq!(request_ack.msg_type.as_u16(), 302);
        assert_eq!(request_ack.payload[0], 0);

        let disconnect_error = caller_mailbox
            .receiver()
            .try_recv()
            .expect("rpc disconnect error")
            .into_payload::<FrameContext>()
            .expect("rpc disconnect error frame");
        assert_eq!(disconnect_error.msg_type.as_u16(), 303);

        let error_response = parse_rpc_response_frame(&disconnect_error);
        assert_eq!(error_response.correlation_id, request.correlation_id);
        assert_eq!(error_response.seq, 0);
        assert!(error_response.stream_end);

        let (error_code, _) =
            crate::protocol::rpc_codec::decode_error_body(error_response.body.as_ref())
                .expect("decode rpc disconnect error body");
        assert_eq!(
            error_code,
            crate::protocol::error_codes::rpc::ERR_WORKER_NOT_FOUND
        );
    }

    #[tokio::test]
    async fn should_cleanup_real_lease_state_on_close() {
        // Arrange
        let family = RouteFamily::new(94);
        let session_id = 94;
        let lease_route = "lease://acme/locks/resource";
        let lease_address = RouteAddress::new(family, Route::new(lease_route));
        let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/94"));

        let router = Arc::new(crate::runtime::Router::new());
        let admin_read_model = AdminReadModel::new();
        let lease_sink = Arc::new(LeaseDomainSink::new(
            router.clone(),
            admin_read_model.clone(),
        ));
        let subscriber_mailbox = Arc::new(Mailbox::new(8));

        router.register(subscriber_address.clone(), subscriber_mailbox.clone());
        router.register_domain_pattern("lease", lease_sink.clone());
        register_fallback_cleanup_domains(&router, DispatchDomain::Lease);

        let ingress = make_cleanup_ingress(router, admin_read_model.clone());
        let mut session = make_session_info(session_id, TransportKind::WebSocket);
        session.route_family = family;
        ingress.on_open(session).await.unwrap();

        lease_sink
            .deliver(Envelope::from_route(
                subscriber_address.clone(),
                lease_address.clone(),
                FrameContext::new(
                    session_id,
                    ChannelId::Sub,
                    MessageType::new(400),
                    encode_lease_acquire(lease_route, "", 30),
                    family,
                ),
            ))
            .expect("acquire lease");
        lease_sink
            .deliver(Envelope::from_route(
                subscriber_address,
                lease_address,
                FrameContext::new(
                    session_id,
                    ChannelId::Sub,
                    MessageType::new(407),
                    encode_lease_subscribe(lease_route),
                    family,
                ),
            ))
            .expect("subscribe lease route");

        assert_eq!(lease_sink.lease_count(), 1);
        assert_eq!(lease_sink.subscription_count(), 1);
        assert_eq!(admin_read_model.leases(None).len(), 1);
        drain_mailbox(&subscriber_mailbox);

        // Act
        ingress.on_close(session_id, CloseReason::ClientClose).await;
        lease_sink
            .deliver(Envelope::new(
                RouteAddress::new(family, Route::new("lease://events")),
                crate::runtime::DomainPublishEvent::new(
                    family,
                    Route::new(lease_route),
                    Bytes::from_static(b"expired"),
                ),
            ))
            .expect("publish lease event after cleanup");

        // Assert
        assert_eq!(lease_sink.lease_count(), 0);
        assert_eq!(lease_sink.subscription_count(), 0);
        assert!(admin_read_model.leases(None).is_empty());
        assert!(subscriber_mailbox.receiver().try_recv().is_err());
    }

    #[tokio::test]
    async fn should_cleanup_real_schedule_subscription_on_close() {
        // Arrange
        let family = RouteFamily::new(1);
        let session_id = 95;
        let schedule_route = "schedule://acme/jobs/nightly/run";
        let schedule_address = RouteAddress::new(family, Route::new(schedule_route));
        let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/95"));

        let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
        let router = Arc::new(crate::runtime::Router::new());
        let admin_read_model = AdminReadModel::new();
        let schedule_sink = Arc::new(ScheduleDomainSink::new(
            store,
            router.clone(),
            admin_read_model.clone(),
        ));
        let subscriber_mailbox = Arc::new(Mailbox::new(8));

        router.register(subscriber_address.clone(), subscriber_mailbox.clone());
        router.register_domain_pattern("schedule", schedule_sink.clone());
        register_fallback_cleanup_domains(&router, DispatchDomain::Schedule);

        let ingress = make_cleanup_ingress(router, admin_read_model);
        let mut session = make_session_info(session_id, TransportKind::WebSocket);
        session.route_family = family;
        ingress.on_open(session).await.unwrap();

        schedule_sink
            .deliver(Envelope::from_route(
                subscriber_address.clone(),
                schedule_address.clone(),
                FrameContext::new(
                    session_id,
                    ChannelId::Sub,
                    MessageType::new(700),
                    encode_schedule_create(schedule_route, "* * * * *", b"nightly"),
                    family,
                ),
            ))
            .expect("create schedule");
        drain_mailbox(&subscriber_mailbox);

        schedule_sink
            .deliver(Envelope::from_route(
                subscriber_address,
                schedule_address,
                FrameContext::new(
                    session_id,
                    ChannelId::Sub,
                    MessageType::new(703),
                    encode_schedule_subscribe(schedule_route),
                    family,
                ),
            ))
            .expect("subscribe schedule");
        drain_mailbox(&subscriber_mailbox);
        assert_eq!(schedule_sink.schedule_count(), 1);
        assert_eq!(schedule_sink.subscription_count(), 1);

        // Act
        ingress.on_close(session_id, CloseReason::ClientClose).await;

        // Assert
        assert_eq!(schedule_sink.schedule_count(), 1);
        assert_eq!(schedule_sink.subscription_count(), 0);
        assert!(subscriber_mailbox.receiver().try_recv().is_err());
    }

    #[tokio::test]
    async fn should_cleanup_real_stream_session_and_subscription_on_close() {
        // Arrange
        let family = RouteFamily::new(1);
        let session_id = 96;
        let stream_route = "stream://acme/logs/events";
        let stream_pattern = "stream://acme/logs/*";
        let source_address = RouteAddress::new(family, Route::new("inbox://session/96"));
        let stream_address = RouteAddress::new(family, Route::new(stream_route));

        let store = crate::benchkit::create_bench_store();
        let router = Arc::new(crate::runtime::Router::new());
        let admin_read_model = AdminReadModel::new();
        let stream_sink = Arc::new(StreamDomainSink::new(
            store,
            router.clone(),
            admin_read_model.clone(),
        ));
        let source_mailbox = Arc::new(Mailbox::new(8));

        router.register(source_address.clone(), source_mailbox.clone());
        router.register_domain_pattern("stream", stream_sink.clone());
        register_fallback_cleanup_domains(&router, DispatchDomain::Stream);

        let ingress = make_cleanup_ingress(router, admin_read_model);
        let mut session = make_session_info(session_id, TransportKind::WebSocket);
        session.route_family = family;
        ingress.on_open(session).await.unwrap();

        let begin_frame = crate::benchkit::build_stream_begin(stream_route);
        let (begin_msg_type, begin_payload) =
            crate::benchkit::extract_single_tlv_field(&begin_frame);
        stream_sink
            .deliver(Envelope::from_route(
                source_address.clone(),
                stream_address.clone(),
                FrameContext::new(
                    session_id,
                    ChannelId::Pub,
                    MessageType::new(begin_msg_type),
                    begin_payload,
                    family,
                ),
            ))
            .expect("begin stream session");

        let begin_response = source_mailbox
            .receiver()
            .try_recv()
            .expect("stream begin response")
            .into_payload::<FrameContext>()
            .expect("stream begin response frame");
        let _stream_session_id =
            crate::benchkit::parse_stream_session_id(begin_response.payload.as_ref())
                .expect("stream session id");

        let subscribe_frame = crate::benchkit::build_stream_subscribe(stream_pattern);
        let (subscribe_msg_type, subscribe_payload) =
            crate::benchkit::extract_single_tlv_field(&subscribe_frame);
        stream_sink
            .deliver(Envelope::from_route(
                source_address,
                stream_address,
                FrameContext::new(
                    session_id,
                    ChannelId::Pub,
                    MessageType::new(subscribe_msg_type),
                    subscribe_payload,
                    family,
                ),
            ))
            .expect("subscribe stream pattern");
        let _subscribe_response = source_mailbox
            .receiver()
            .try_recv()
            .expect("stream subscribe response");

        assert_eq!(stream_sink.append_session_count(), 1);
        assert_eq!(stream_sink.subscription_count(), 1);

        // Act
        ingress.on_close(session_id, CloseReason::ClientClose).await;

        // Assert
        assert_eq!(stream_sink.append_session_count(), 0);
        assert_eq!(stream_sink.subscription_count(), 0);
        assert!(source_mailbox.receiver().try_recv().is_err());
    }

    #[tokio::test]
    async fn should_cleanup_real_kv_transaction_on_close() {
        // Arrange
        let family = RouteFamily::new(1);
        let first_session_id = 97;
        let second_session_id = 98;
        let kv_route = "kv://acme/app/users";
        let kv_address = RouteAddress::new(family, Route::new(kv_route));
        let first_address = RouteAddress::new(family, Route::new("inbox://session/97"));
        let second_address = RouteAddress::new(family, Route::new("inbox://session/98"));

        let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
        let router = Arc::new(crate::runtime::Router::new());
        let admin_read_model = AdminReadModel::new();
        let kv_sink = Arc::new(KvDomainSink::new(
            store,
            router.clone(),
            admin_read_model.clone(),
        ));
        let first_mailbox = Arc::new(Mailbox::new(8));
        let second_mailbox = Arc::new(Mailbox::new(8));

        router.register(first_address.clone(), first_mailbox.clone());
        router.register(second_address.clone(), second_mailbox.clone());
        router.register_domain_pattern("kv", kv_sink.clone());
        register_fallback_cleanup_domains(&router, DispatchDomain::Kv);

        let ingress = make_cleanup_ingress(router, admin_read_model);
        let mut first_session = make_session_info(first_session_id, TransportKind::WebSocket);
        first_session.route_family = family;
        ingress.on_open(first_session).await.unwrap();

        let first_begin = crate::benchkit::build_kv_begin(kv_route, 1, 0);
        let (first_begin_msg_type, first_begin_payload) =
            crate::benchkit::extract_single_tlv_field(&first_begin);
        kv_sink
            .deliver(Envelope::from_route(
                first_address,
                kv_address.clone(),
                FrameContext::new(
                    first_session_id,
                    ChannelId::Pub,
                    MessageType::new(first_begin_msg_type),
                    first_begin_payload,
                    family,
                ),
            ))
            .expect("begin first KV transaction");

        let first_begin_response = first_mailbox
            .receiver()
            .try_recv()
            .expect("first begin response")
            .into_payload::<FrameContext>()
            .expect("first begin response frame");
        let first_tx_id = crate::benchkit::parse_kv_tx_id(first_begin_response.payload.as_ref())
            .expect("first tx id");

        assert_eq!(first_begin_response.payload[0], 0);
        assert!(first_tx_id > 0);
        assert_eq!(kv_sink.active_transaction_count(), 1);

        // Act
        ingress
            .on_close(first_session_id, CloseReason::ClientClose)
            .await;

        // Assert
        assert_eq!(kv_sink.active_transaction_count(), 0);

        let second_begin = crate::benchkit::build_kv_begin(kv_route, 1, 0);
        let (second_begin_msg_type, second_begin_payload) =
            crate::benchkit::extract_single_tlv_field(&second_begin);
        kv_sink
            .deliver(Envelope::from_route(
                second_address,
                kv_address,
                FrameContext::new(
                    second_session_id,
                    ChannelId::Pub,
                    MessageType::new(second_begin_msg_type),
                    second_begin_payload,
                    family,
                ),
            ))
            .expect("begin second KV transaction");

        let second_begin_response = second_mailbox
            .receiver()
            .try_recv()
            .expect("second begin response")
            .into_payload::<FrameContext>()
            .expect("second begin response frame");
        let second_tx_id = crate::benchkit::parse_kv_tx_id(second_begin_response.payload.as_ref())
            .expect("second tx id");

        assert_eq!(second_begin_response.payload[0], 0);
        assert!(second_tx_id > 0);
        assert_eq!(kv_sink.active_transaction_count(), 1);
        assert!(first_mailbox.receiver().try_recv().is_err());
    }

    #[tokio::test]
    async fn should_reject_non_connect_before_auth() {
        let ingress = runtime_ingress_with_jwks_auth();
        let session = make_session_info(4, TransportKind::WebSocket);
        ingress.on_open(session).await.unwrap();

        let decision = ingress
            .on_frame(
                4,
                ChannelId::Pub,
                crate::protocol::tlv::MessageType::new(100),
                Bytes::from("payload"),
            )
            .await;

        assert!(matches!(decision, IngressDecision::Close(_)));
    }

    #[tokio::test]
    async fn should_reject_control_non_connect_before_auth() {
        let ingress = runtime_ingress_with_jwks_auth();
        let session = make_session_info(5, TransportKind::WebSocket);
        ingress.on_open(session).await.unwrap();

        // Control message with wrong type
        let decision = ingress
            .on_frame(
                5,
                ChannelId::Control,
                crate::protocol::tlv::MessageType::new(2),
                Bytes::from("payload"),
            )
            .await;

        assert!(matches!(decision, IngressDecision::Close(_)));
    }

    #[test]
    fn should_retrieve_session_info() {
        // Arrange
        let ingress = runtime_ingress_with_jwks_auth();
        let session = make_session_info(42, TransportKind::Tcp);

        // Act
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            ingress.on_open(session.clone()).await.unwrap();
        });
        let retrieved = ingress.get_session(42);

        // Assert
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().session_id, 42);
    }

    #[test]
    fn should_set_permissions_on_connect_with_valid_token() {
        // Arrange
        let ingress = runtime_ingress_with_jwks_auth().with_route_family_map(&[("acme-prod", 1)]);
        let session = make_session_info(50, TransportKind::Tcp);

        let payload = serde_json::json!({
            "iss": "",
            "aud": "fitz-broker",
            "sub": "user:42",
            "exp": 9999999999u64,
            "tid": "acme-prod",
            "permissions": ["notice://prod/orders/**#read"]
        });
        let jwt = signed_jwks_jwt(payload);

        // Act
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            ingress.on_open(session.clone()).await.unwrap();
            let decision = ingress
                .on_frame(
                    50,
                    ChannelId::Control,
                    crate::protocol::tlv::MessageType::CONNECT,
                    Bytes::from(jwt.clone()),
                )
                .await;

            // Assert
            assert_eq!(decision, IngressDecision::Accept);
        });

        // Assert: permissions snapshot updated
        let retrieved = ingress.get_session(50).unwrap();
        assert!(retrieved.permissions_snapshot.allows(
            &crate::runtime::routing::Route::new("notice://prod/orders/create"),
            crate::auth::Access::Read
        ));
    }

    #[test]
    fn should_set_permissions_on_connect_for_auth0_shape() {
        // Arrange
        let ingress = runtime_ingress_with_jwks_auth()
            .with_auth_claims_config(crate::auth::AuthClaimsConfig::new(
                "org_id",
                None,
                crate::auth::DEFAULT_ROLE_CLAIM,
            ))
            .with_route_family_resolver(crate::auth::RouteFamilyResolverConfig::from_mappings(
                "org_id",
                [("org_acme", 2)],
            ))
            .with_route_families(&[1, 2]);
        let session = make_session_info(57, TransportKind::Tcp);
        let jwt = signed_jwks_jwt(serde_json::json!({
            "iss": "",
            "aud": "fitz-broker",
            "sub": "auth0|user-1",
            "exp": 9999999999u64,
            "org_id": "org_acme",
            "permissions": ["notice://prod/orders/**#read"]
        }));

        // Act
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            ingress.on_open(session).await.unwrap();
            assert_eq!(
                ingress
                    .on_frame(
                        57,
                        ChannelId::Control,
                        crate::protocol::tlv::MessageType::CONNECT,
                        Bytes::from(jwt),
                    )
                    .await,
                IngressDecision::Accept
            );
        });

        // Assert
        let retrieved = ingress.get_session(57).unwrap();
        assert_eq!(retrieved.route_family.id(), 2);
        assert!(retrieved.permissions_snapshot.allows(
            &crate::runtime::routing::Route::new("notice://prod/orders/1"),
            crate::auth::Access::Read
        ));
    }

    #[test]
    fn should_set_permissions_on_connect_for_entra_delegated_shape() {
        // Arrange
        let ingress = runtime_ingress_with_jwks_auth()
            .with_route_family_map(&[("entra-tenant-1", 2)])
            .with_route_families(&[1, 2]);
        let session = make_session_info(58, TransportKind::Tcp);
        let jwt = signed_jwks_jwt(serde_json::json!({
            "iss": "",
            "aud": "fitz-broker",
            "sub": "entra-user-1",
            "exp": 9999999999u64,
            "tid": "entra-tenant-1",
            "scp": "notice.read"
        }));

        // Act
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            ingress.on_open(session).await.unwrap();
            assert_eq!(
                ingress
                    .on_frame(
                        58,
                        ChannelId::Control,
                        crate::protocol::tlv::MessageType::CONNECT,
                        Bytes::from(jwt),
                    )
                    .await,
                IngressDecision::Accept
            );
        });

        // Assert
        let retrieved = ingress.get_session(58).unwrap();
        assert_eq!(retrieved.route_family.id(), 2);
        assert!(retrieved.permissions_snapshot.allows(
            &crate::runtime::routing::Route::new("notice://prod/orders/1"),
            crate::auth::Access::Read
        ));
    }

    #[test]
    fn should_set_permissions_on_connect_for_entra_app_only_shape() {
        // Arrange
        let ingress = runtime_ingress_with_jwks_auth()
            .with_route_family_map(&[("entra-tenant-2", 2)])
            .with_route_families(&[1, 2]);
        let session = make_session_info(59, TransportKind::Tcp);
        let jwt = signed_jwks_jwt(serde_json::json!({
            "iss": "",
            "aud": "fitz-broker",
            "sub": "service-principal-1",
            "exp": 9999999999u64,
            "tid": "entra-tenant-2",
            "roles": ["queue.read"]
        }));

        // Act
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            ingress.on_open(session).await.unwrap();
            assert_eq!(
                ingress
                    .on_frame(
                        59,
                        ChannelId::Control,
                        crate::protocol::tlv::MessageType::CONNECT,
                        Bytes::from(jwt),
                    )
                    .await,
                IngressDecision::Accept
            );
        });

        // Assert
        let retrieved = ingress.get_session(59).unwrap();
        assert_eq!(retrieved.route_family.id(), 2);
        assert!(retrieved.permissions_snapshot.allows(
            &crate::runtime::routing::Route::new("queue://prod/orders/1"),
            crate::auth::Access::Read
        ));
    }

    #[test]
    fn should_set_permissions_on_connect_for_cognito_shape() {
        // Arrange
        let ingress = runtime_ingress_with_jwks_auth()
            .with_auth_claims_config(crate::auth::AuthClaimsConfig::new(
                "custom:tenant_id",
                None,
                crate::auth::DEFAULT_ROLE_CLAIM,
            ))
            .with_route_family_resolver(crate::auth::RouteFamilyResolverConfig::from_mappings(
                "custom:tenant_id",
                [("acme-prod", 2)],
            ))
            .with_route_families(&[1, 2]);
        let session = make_session_info(64, TransportKind::Tcp);
        let jwt = signed_jwks_jwt(serde_json::json!({
            "iss": "",
            "aud": "fitz-broker",
            "sub": "cognito-user-1",
            "exp": 9999999999u64,
            "custom:tenant_id": "acme-prod",
            "scope": "fitz/kv.read"
        }));

        // Act
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            ingress.on_open(session).await.unwrap();
            assert_eq!(
                ingress
                    .on_frame(
                        64,
                        ChannelId::Control,
                        crate::protocol::tlv::MessageType::CONNECT,
                        Bytes::from(jwt),
                    )
                    .await,
                IngressDecision::Accept
            );
        });

        // Assert
        let retrieved = ingress.get_session(64).unwrap();
        assert_eq!(retrieved.route_family.id(), 2);
        assert!(retrieved.permissions_snapshot.allows(
            &crate::runtime::routing::Route::new("kv://prod/orders/1"),
            crate::auth::Access::Read
        ));
    }

    #[test]
    fn should_set_permissions_on_connect_for_okta_shape() {
        // Arrange
        let ingress = runtime_ingress_with_jwks_auth()
            .with_auth_claims_config(crate::auth::AuthClaimsConfig::new(
                "https://fitz.example.com/identity",
                Some("https://fitz.example.com/claims".to_string()),
                crate::auth::DEFAULT_ROLE_CLAIM,
            ))
            .with_route_family_resolver(crate::auth::RouteFamilyResolverConfig::from_mappings(
                "https://fitz.example.com/identity",
                [("okta-acme", 2)],
            ))
            .with_route_families(&[1, 2]);
        let session = make_session_info(65, TransportKind::Tcp);
        let jwt = signed_jwks_jwt(serde_json::json!({
            "iss": "",
            "aud": "fitz-broker",
            "sub": "okta-user-1",
            "exp": 9999999999u64,
            "https://fitz.example.com/identity": "okta-acme",
            "https://fitz.example.com/claims": {
                "permissions": ["notice://prod/orders/**#write"]
            }
        }));

        // Act
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            ingress.on_open(session).await.unwrap();
            assert_eq!(
                ingress
                    .on_frame(
                        65,
                        ChannelId::Control,
                        crate::protocol::tlv::MessageType::CONNECT,
                        Bytes::from(jwt),
                    )
                    .await,
                IngressDecision::Accept
            );
        });

        // Assert
        let retrieved = ingress.get_session(65).unwrap();
        assert_eq!(retrieved.route_family.id(), 2);
        assert!(retrieved.permissions_snapshot.allows(
            &crate::runtime::routing::Route::new("notice://prod/orders/1"),
            crate::auth::Access::Write
        ));
    }

    #[test]
    fn should_assign_route_families_from_verified_claims() {
        // Arrange
        let ingress = runtime_ingress_with_jwks_auth()
            .with_route_families(&[1, 2])
            .with_route_family_map(&[("tenant-a", 2), ("tenant-b", 1)]);
        let session_a = make_session_info(52, TransportKind::Tcp);
        let session_b = make_session_info(53, TransportKind::Tcp);

        let jwt_a = signed_jwks_jwt(serde_json::json!({
            "iss": "",
            "aud": "fitz-broker",
            "sub": "user:a",
            "exp": 9999999999u64,
            "tid": "tenant-a",
            "permissions": ["notice://tenant-a/**#read"]
        }));
        let jwt_b = signed_jwks_jwt(serde_json::json!({
            "iss": "",
            "aud": "fitz-broker",
            "sub": "user:b",
            "exp": 9999999999u64,
            "tid": "tenant-b",
            "permissions": ["notice://tenant-b/**#read"]
        }));

        // Act
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            ingress.on_open(session_a).await.unwrap();
            ingress.on_open(session_b).await.unwrap();

            assert_eq!(
                ingress
                    .on_frame(
                        52,
                        ChannelId::Control,
                        crate::protocol::tlv::MessageType::CONNECT,
                        Bytes::from(jwt_a),
                    )
                    .await,
                IngressDecision::Accept
            );
            assert_eq!(
                ingress
                    .on_frame(
                        53,
                        ChannelId::Control,
                        crate::protocol::tlv::MessageType::CONNECT,
                        Bytes::from(jwt_b),
                    )
                    .await,
                IngressDecision::Accept
            );
        });

        // Assert
        let session_a = ingress.get_session(52).unwrap();
        let session_b = ingress.get_session(53).unwrap();
        assert_ne!(session_a.route_family, session_b.route_family);
        assert_eq!(session_a.route_family.id(), 2);
        assert_eq!(session_b.route_family.id(), 1);
    }

    #[test]
    fn should_reject_connect_with_unprovisioned_resolved_route_family() {
        // Arrange
        let ingress = runtime_ingress_with_jwks_auth()
            .with_route_families(&[1])
            .with_route_family_map(&[("acme-prod", 2)]);
        let session = make_session_info(55, TransportKind::Tcp);
        let jwt = signed_jwks_jwt(serde_json::json!({
            "iss": "",
            "aud": "fitz-broker",
            "sub": "user:55",
            "exp": 9999999999u64,
            "tid": "acme-prod",
            "permissions": ["notice://prod/orders/**#read"]
        }));

        // Act
        let rt = tokio::runtime::Runtime::new().unwrap();
        let decision = rt.block_on(async {
            ingress.on_open(session).await.unwrap();
            ingress
                .on_frame(
                    55,
                    ChannelId::Control,
                    crate::protocol::tlv::MessageType::CONNECT,
                    Bytes::from(jwt),
                )
                .await
        });

        // Assert
        assert!(matches!(decision, IngressDecision::Close(_)));
    }

    #[test]
    fn should_reject_connect_with_unmapped_identity_claim() {
        // Arrange
        let ingress = runtime_ingress_with_jwks_auth().with_route_family_map(&[("mapped", 1)]);
        let session = make_session_info(56, TransportKind::Tcp);
        let jwt = signed_jwks_jwt(serde_json::json!({
            "iss": "",
            "aud": "fitz-broker",
            "sub": "user:56",
            "exp": 9999999999u64,
            "tid": "unmapped",
            "permissions": ["notice://prod/orders/**#read"]
        }));

        // Act
        let rt = tokio::runtime::Runtime::new().unwrap();
        let decision = rt.block_on(async {
            ingress.on_open(session).await.unwrap();
            ingress
                .on_frame(
                    56,
                    ChannelId::Control,
                    crate::protocol::tlv::MessageType::CONNECT,
                    Bytes::from(jwt),
                )
                .await
        });

        // Assert
        assert!(matches!(decision, IngressDecision::Close(_)));
    }

    #[test]
    fn should_preserve_resolved_route_families_when_sessions_reconnect_in_reverse_order() {
        // Arrange
        let jwt_for = |subject: &str| {
            signed_jwks_jwt(serde_json::json!({
                "iss": "",
                "aud": "fitz-broker",
                "sub": subject,
                "exp": 9999999999u64,
                "tid": subject,
                "permissions": ["kv://shared/data/item#read"]
            }))
        };
        let rt = tokio::runtime::Runtime::new().unwrap();
        let first = runtime_ingress_with_jwks_auth()
            .with_route_families(&[1, 2])
            .with_route_family_map(&[("tenant-a", 1), ("tenant-b", 2)]);
        let second = runtime_ingress_with_jwks_auth()
            .with_route_families(&[1, 2])
            .with_route_family_map(&[("tenant-a", 1), ("tenant-b", 2)]);

        // Act
        rt.block_on(async {
            first
                .on_open(make_session_info(60, TransportKind::Tcp))
                .await
                .unwrap();
            first
                .on_open(make_session_info(61, TransportKind::Tcp))
                .await
                .unwrap();
            assert_eq!(
                first
                    .on_frame(
                        60,
                        ChannelId::Control,
                        crate::protocol::tlv::MessageType::CONNECT,
                        Bytes::from(jwt_for("tenant-a")),
                    )
                    .await,
                IngressDecision::Accept
            );
            assert_eq!(
                first
                    .on_frame(
                        61,
                        ChannelId::Control,
                        crate::protocol::tlv::MessageType::CONNECT,
                        Bytes::from(jwt_for("tenant-b")),
                    )
                    .await,
                IngressDecision::Accept
            );

            second
                .on_open(make_session_info(62, TransportKind::Tcp))
                .await
                .unwrap();
            second
                .on_open(make_session_info(63, TransportKind::Tcp))
                .await
                .unwrap();
            assert_eq!(
                second
                    .on_frame(
                        62,
                        ChannelId::Control,
                        crate::protocol::tlv::MessageType::CONNECT,
                        Bytes::from(jwt_for("tenant-b")),
                    )
                    .await,
                IngressDecision::Accept
            );
            assert_eq!(
                second
                    .on_frame(
                        63,
                        ChannelId::Control,
                        crate::protocol::tlv::MessageType::CONNECT,
                        Bytes::from(jwt_for("tenant-a")),
                    )
                    .await,
                IngressDecision::Accept
            );
        });

        // Assert
        assert_eq!(first.get_session(60).unwrap().route_family.id(), 1);
        assert_eq!(first.get_session(61).unwrap().route_family.id(), 2);
        assert_eq!(second.get_session(62).unwrap().route_family.id(), 2);
        assert_eq!(second.get_session(63).unwrap().route_family.id(), 1);
    }

    #[test]
    fn should_reject_connect_with_malformed_permissions() {
        // Arrange
        let ingress = runtime_ingress_with_jwks_auth().with_route_family_map(&[("acme-prod", 1)]);
        let session = make_session_info(51, TransportKind::Tcp);

        let payload = serde_json::json!({
            "iss": "",
            "aud": "fitz-broker",
            "sub": "user:42",
            "exp": 9999999999u64,
            "tid": "acme-prod",
            "permissions": ["badperm#oops"]
        });
        let jwt = signed_jwks_jwt(payload);

        // Act
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            ingress.on_open(session.clone()).await.unwrap();
            let decision = ingress
                .on_frame(
                    51,
                    ChannelId::Control,
                    crate::protocol::tlv::MessageType::CONNECT,
                    Bytes::from(jwt.clone()),
                )
                .await;

            // Assert
            assert!(matches!(decision, IngressDecision::Close(_)));
        });
    }

    #[test]
    fn should_reject_connect_when_issuer_cannot_derive_jwks() {
        // Arrange
        let ingress = runtime_ingress_with_jwks_auth();
        let session = make_session_info(54, TransportKind::Tcp);
        let jwt = signed_jwks_jwt(serde_json::json!({
            "iss": "not-a-valid-issuer",
            "aud": "fitz-broker",
            "sub": "user:54",
            "exp": 9999999999u64,
            "tid": "acme-prod",
            "permissions": ["notice://prod/orders/**#read"]
        }));

        // Act
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            ingress.on_open(session).await.unwrap();
            let decision = ingress
                .on_frame(
                    54,
                    ChannelId::Control,
                    crate::protocol::tlv::MessageType::CONNECT,
                    Bytes::from(jwt),
                )
                .await;

            // Assert
            assert!(matches!(decision, IngressDecision::Close(_)));
        });
    }

    #[test]
    fn should_set_permissions_on_connect_with_issuer_valid_signature() {
        // Arrange
        use base64::Engine;
        use jsonwebtoken::{EncodingKey, Header};
        let issuer = "https://idp.example/jwks-valid";
        let jwks_url = "https://idp.example/jwks-valid/.well-known/jwks.json";

        let ingress = runtime_ingress_with_jwks_auth()
            .with_auth_config(crate::auth::AuthConfig::jwks(
                vec!["fitz-broker".to_string()],
                vec![crate::auth::JwksIssuerConfig {
                    issuer: issuer.to_string(),
                    jwks_url: jwks_url.to_string(),
                }],
            ))
            .with_route_family_map(&[("acme-prod", 1)]);
        let session = make_session_info(80, TransportKind::Tcp);

        // Build a signed HS256 token and cache a matching oct key under the issuer's derived JWKS URL
        let payload = serde_json::json!({
            "iss": issuer,
            "aud": "fitz-broker",
            "sub": "user:80",
            "exp": 9999999999u64,
            "tid": "acme-prod",
            "permissions": ["notice://prod/orders/**#write"]
        });

        let secret = b"supersecretkey".to_vec();
        let header = Header::new(jsonwebtoken::Algorithm::HS256);
        let jwt = jsonwebtoken::encode(
            &header,
            &payload,
            &EncodingKey::from_secret(secret.as_slice()),
        )
        .unwrap();

        // Cache JWKS for the derived URL
        let k_b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&secret);
        let jwks =
            serde_json::json!({ "keys": [ { "kty": "oct", "kid": "", "k": k_b64 } ] }).to_string();
        crate::auth::cache_jwks_from_json(jwks_url, &jwks).unwrap();

        // Act
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            ingress.on_open(session.clone()).await.unwrap();
            let decision = ingress
                .on_frame(
                    80,
                    ChannelId::Control,
                    crate::protocol::tlv::MessageType::CONNECT,
                    Bytes::from(jwt.clone()),
                )
                .await;

            // Assert
            assert_eq!(decision, IngressDecision::Accept);
        });

        // Assert: actor authorizes write
        let actor_ref = ingress.session_actors.get(&80).unwrap();
        let actor = actor_ref.value();
        assert!(actor.authorize(
            &crate::runtime::routing::Route::new("notice://prod/orders/create"),
            crate::auth::Access::Write
        ));
    }

    #[tokio::test]
    async fn should_not_block_unrelated_sessions_while_jwks_fetch_is_pending() {
        // Arrange
        use jsonwebtoken::{Algorithm, EncodingKey, Header};
        use tokio::sync::oneshot;
        use tokio::time::{sleep, timeout, Duration};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let issuer = format!("https://{}", listener.local_addr().unwrap());
        let jwks_url = format!("{issuer}/jwks");
        let secret = b"delayed-secret";
        let (release_response_tx, release_response_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (_socket, _) = listener.accept().await.unwrap();
            release_response_rx.await.unwrap();
        });
        let ingress = Arc::new(
            runtime_ingress_with_jwks_auth()
                .with_auth_config(crate::auth::AuthConfig::jwks(
                    vec!["fitz-broker".to_string()],
                    vec![crate::auth::JwksIssuerConfig {
                        issuer: issuer.clone(),
                        jwks_url,
                    }],
                ))
                .with_route_family_map(&[("acme-prod", 1)]),
        );
        ingress
            .on_open(make_session_info(82, TransportKind::Tcp))
            .await
            .unwrap();
        ingress
            .on_open(make_session_info(83, TransportKind::Tcp))
            .await
            .unwrap();
        let mut active_session = make_session_info(84, TransportKind::Tcp);
        active_session.authenticated = true;
        active_session.route_family = crate::runtime::routing::RouteFamily::new(1);
        ingress.on_open(active_session).await.unwrap();
        let jwt = jsonwebtoken::encode(
            &Header::new(Algorithm::HS256),
            &serde_json::json!({
                "iss": issuer,
                "aud": "fitz-broker",
                "sub": "user:82",
                "exp": 9999999999u64,
                "tid": "acme-prod",
                "permissions": ["notice://prod/orders/**#write"]
            }),
            &EncodingKey::from_secret(secret),
        )
        .unwrap();

        // Act
        let pending_ingress = Arc::clone(&ingress);
        let connect = tokio::spawn(async move {
            pending_ingress
                .on_frame(
                    82,
                    ChannelId::Control,
                    crate::protocol::tlv::MessageType::CONNECT,
                    Bytes::from(jwt),
                )
                .await
        });
        sleep(Duration::from_millis(100)).await;
        timeout(
            Duration::from_millis(100),
            ingress.on_close(83, CloseReason::ClientClose),
        )
        .await
        .expect("unrelated close should not wait for JWKS");
        let frame_decision = timeout(
            Duration::from_millis(100),
            ingress.on_frame(
                84,
                ChannelId::Control,
                crate::protocol::tlv::MessageType::new(2),
                Bytes::new(),
            ),
        )
        .await
        .expect("unrelated frame should not wait for JWKS");

        // Assert
        assert!(!connect.is_finished());
        assert!(ingress.get_session(83).is_none());
        assert_eq!(frame_decision, IngressDecision::Accept);
        release_response_tx.send(()).unwrap();
        assert!(matches!(
            connect.await.unwrap(),
            IngressDecision::Close(reason) if reason.contains("connect failed")
        ));
        server.await.unwrap();
    }

    #[test]
    fn should_reject_connect_with_issuer_invalid_signature() {
        // Arrange
        use base64::Engine;
        use jsonwebtoken::{EncodingKey, Header};
        let issuer = "https://idp.example/jwks-invalid";
        let jwks_url = "https://idp.example/jwks-invalid/.well-known/jwks.json";

        let ingress =
            runtime_ingress_with_jwks_auth().with_auth_config(crate::auth::AuthConfig::jwks(
                vec!["fitz-broker".to_string()],
                vec![crate::auth::JwksIssuerConfig {
                    issuer: issuer.to_string(),
                    jwks_url: jwks_url.to_string(),
                }],
            ));
        let session = make_session_info(81, TransportKind::Tcp);

        // Create a token signed with a secret NOT in the JWKS cache
        let payload = serde_json::json!({
            "iss": issuer,
            "aud": "fitz-broker",
            "sub": "user:81",
            "exp": 9999999999u64,
            "tid": "acme-prod",
            "permissions": ["notice://prod/orders/**#write"]
        });

        let signing_secret = b"othersecret";
        let header = Header::new(jsonwebtoken::Algorithm::HS256);
        let jwt =
            jsonwebtoken::encode(&header, &payload, &EncodingKey::from_secret(signing_secret))
                .unwrap();

        // Cache a different secret under the JWKS URL
        let k_b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"supersecretkey");
        let jwks =
            serde_json::json!({ "keys": [ { "kty": "oct", "kid": "", "k": k_b64 } ] }).to_string();
        crate::auth::cache_jwks_from_json(jwks_url, &jwks).unwrap();

        // Act
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            ingress.on_open(session.clone()).await.unwrap();
            let decision = ingress
                .on_frame(
                    81,
                    ChannelId::Control,
                    crate::protocol::tlv::MessageType::CONNECT,
                    Bytes::from(jwt.clone()),
                )
                .await;

            // Assert
            assert!(matches!(decision, IngressDecision::Close(_)));
        });
    }

    #[test]
    fn should_create_session_actor_on_open() {
        // Arrange
        let ingress = runtime_ingress_with_jwks_auth();
        let session = make_session_info(60, TransportKind::Tcp);

        // Act
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            ingress.on_open(session.clone()).await.unwrap();
        });

        // Assert: Actor should exist but have no permissions
        assert!(ingress.session_actors.contains_key(&60));
        let actor_ref = ingress.session_actors.get(&60).unwrap();
        let actor = actor_ref.value();
        assert!(!actor.authorize(
            &crate::runtime::routing::Route::new("notice://prod/orders/create"),
            crate::auth::Access::Write
        ));
    }

    #[test]
    fn should_update_session_actor_on_connect() {
        // Arrange
        let ingress = runtime_ingress_with_jwks_auth().with_route_family_map(&[("acme-prod", 1)]);
        let session = make_session_info(61, TransportKind::Tcp);

        let payload = serde_json::json!({
            "iss": "",
            "aud": "fitz-broker",
            "sub": "user:42",
            "exp": 9999999999u64,
            "tid": "acme-prod",
            "permissions": ["notice://prod/orders/**#write"]
        });
        let jwt = signed_jwks_jwt(payload);

        // Act
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            ingress.on_open(session.clone()).await.unwrap();
            let decision = ingress
                .on_frame(
                    61,
                    ChannelId::Control,
                    crate::protocol::tlv::MessageType::CONNECT,
                    Bytes::from(jwt.clone()),
                )
                .await;

            // Assert
            assert_eq!(decision, IngressDecision::Accept);
        });

        // Actor should now allow write on the route
        let actor_ref = ingress.session_actors.get(&61).unwrap();
        let actor = actor_ref.value();
        assert!(actor.authorize(
            &crate::runtime::routing::Route::new("notice://prod/orders/create"),
            crate::auth::Access::Write
        ));
    }

    #[test]
    fn should_allow_stream_followup_after_begin_without_global_stream_write_permission() {
        // Arrange
        let ingress = runtime_ingress_with_jwks_auth().with_route_family_map(&[("acme-prod", 1)]);
        let session = make_session_info(62, TransportKind::Tcp);

        let payload = serde_json::json!({
            "iss": "",
            "aud": "fitz-broker",
            "sub": "user:62",
            "exp": 9999999999u64,
            "tid": "acme-prod",
            "permissions": ["stream://acme/logs/**#write"]
        });
        let jwt = signed_jwks_jwt(payload);

        // Act
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            ingress.on_open(session.clone()).await.unwrap();

            let connect_decision = ingress
                .on_frame(
                    62,
                    ChannelId::Control,
                    crate::protocol::tlv::MessageType::CONNECT,
                    Bytes::from(jwt),
                )
                .await;
            assert_eq!(connect_decision, IngressDecision::Accept);

            let begin_frame =
                crate::benchkit::build_stream_begin("stream://acme/logs/events/append");
            let (begin_msg_type, begin_payload) =
                crate::benchkit::extract_single_tlv_field(&begin_frame);
            let begin_decision = ingress
                .on_frame(
                    62,
                    ChannelId::Pub,
                    crate::protocol::tlv::MessageType::new(begin_msg_type),
                    begin_payload,
                )
                .await;
            assert_eq!(begin_decision, IngressDecision::Accept);

            let append_frame = crate::benchkit::build_stream_append(7, 0, b"event-1");
            let (append_msg_type, append_payload) =
                crate::benchkit::extract_single_tlv_field(&append_frame);
            let append_decision = ingress
                .on_frame(
                    62,
                    ChannelId::Pub,
                    crate::protocol::tlv::MessageType::new(append_msg_type),
                    append_payload,
                )
                .await;
            assert_eq!(append_decision, IngressDecision::Accept);

            let commit_frame = crate::benchkit::build_stream_commit(7, 1);
            let (commit_msg_type, commit_payload) =
                crate::benchkit::extract_single_tlv_field(&commit_frame);
            let commit_decision = ingress
                .on_frame(
                    62,
                    ChannelId::Pub,
                    crate::protocol::tlv::MessageType::new(commit_msg_type),
                    commit_payload,
                )
                .await;
            assert_eq!(commit_decision, IngressDecision::Accept);
        });

        // Assert
        assert_eq!(ingress.session_count(), 1);
    }

    #[test]
    fn should_deny_e2e_notification_publish_via_ingress_snapshot() {
        // Arrange
        use crate::domains::notice::session as notice_session;
        use crate::domains::notice::NoticeRouteActor;
        use crate::runtime::actor::Context;
        use crate::runtime::router::Router;
        use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
        use bytes::Bytes;

        let ingress = runtime_ingress_with_jwks_auth().with_route_family_map(&[("acme-prod", 1)]);
        let session = make_session_info(70, TransportKind::Tcp);

        let payload = serde_json::json!({
            "iss": "",
            "aud": "fitz-broker",
            "sub": "user:70",
            "exp": 9999999999u64,
            "tid": "acme-prod",
            "permissions": ["notice://prod/orders/**#read"]
        });
        let jwt = signed_jwks_jwt(payload);

        // Act
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            ingress.on_open(session.clone()).await.unwrap();
            let decision = ingress
                .on_frame(
                    70,
                    ChannelId::Control,
                    crate::protocol::tlv::MessageType::CONNECT,
                    Bytes::from(jwt.clone()),
                )
                .await;
            assert_eq!(decision, IngressDecision::Accept);
        });

        // Build a notice route actor and session wrapper from ingress snapshot
        let router = Router::new();
        let subscriber =
            RouteAddress::new(RouteFamily::new(1), Route::new("notice://realm/subscriber"));
        let mut actor = NoticeRouteActor::new(RouteFamily::new(1));
        let mut ctx = Context::new(subscriber.clone(), std::sync::Arc::new(router));

        let actor_ref = ingress.session_actors.get(&70).unwrap();
        let session_perms = actor_ref.value().permissions.clone();
        let session_actor = notice_session::SessionActor::new(
            crate::session::session::SessionId(70),
            (*session_perms).clone(),
        );

        // Act: Publish should be rejected because session only has read
        let res = session_actor.publish(
            RouteFamily::new(1),
            Route::new("notice://prod/orders/create"),
            Bytes::from("hi"),
            &mut actor,
            &mut ctx,
        );

        // Assert
        assert!(res.is_err());
        assert_eq!(actor.subscription_count(), 0);
    }

    #[test]
    fn should_allow_e2e_notification_publish_via_ingress_snapshot() {
        // Arrange
        use crate::domains::notice::session as notice_session;
        use crate::domains::notice::NoticeRouteActor;
        use crate::runtime::actor::Context;
        use crate::runtime::router::Router;
        use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
        use bytes::Bytes;

        let ingress = runtime_ingress_with_jwks_auth().with_route_family_map(&[("acme-prod", 1)]);
        let session = make_session_info(71, TransportKind::Tcp);

        let payload = serde_json::json!({
            "iss": "",
            "aud": "fitz-broker",
            "sub": "user:71",
            "exp": 9999999999u64,
            "tid": "acme-prod",
            "permissions": ["notice://prod/orders/**#write"]
        });
        let jwt = signed_jwks_jwt(payload);

        // Act
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            ingress.on_open(session.clone()).await.unwrap();
            let decision = ingress
                .on_frame(
                    71,
                    ChannelId::Control,
                    crate::protocol::tlv::MessageType::CONNECT,
                    Bytes::from(jwt.clone()),
                )
                .await;
            assert_eq!(decision, IngressDecision::Accept);
        });

        // Build a notice route actor and session wrapper from ingress snapshot
        let router = Router::new();
        let subscriber =
            RouteAddress::new(RouteFamily::new(1), Route::new("notice://realm/subscriber"));
        let mut actor = NoticeRouteActor::new(RouteFamily::new(1));
        let mut ctx = Context::new(subscriber.clone(), std::sync::Arc::new(router));

        let actor_ref = ingress.session_actors.get(&71).unwrap();
        let session_perms = actor_ref.value().permissions.clone();
        let session_actor = notice_session::SessionActor::new(
            crate::session::session::SessionId(71),
            (*session_perms).clone(),
        );

        // Act: Publish should succeed because session now has write
        let res = session_actor.publish(
            RouteFamily::new(1),
            Route::new("notice://prod/orders/create"),
            Bytes::from("hello"),
            &mut actor,
            &mut ctx,
        );

        // Assert
        assert!(res.is_ok());
        // No subscriptions yet, but publish succeeded (no panic)
        assert_eq!(actor.subscription_count(), 0);
    }

    #[test]
    fn should_surface_router_backpressure_in_ingress_decision() {
        // Arrange
        use crate::runtime::envelope::Envelope;
        use crate::runtime::router::{DeliveryError, MailboxSink};

        let metrics = crate::observability::metrics();
        let backpressure_before = metrics.counter_get(obs::METRIC_ROUTER_BACKPRESSURE);

        struct BackpressuredSink;

        impl MailboxSink for BackpressuredSink {
            fn deliver(&self, _envelope: Envelope) -> Result<(), DeliveryError> {
                Err(DeliveryError::MailboxFull {
                    capacity: 1,
                    current_len: 1,
                })
            }

            fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
                self.deliver(envelope)
            }
        }

        let router = Arc::new(crate::runtime::Router::new());
        router.register_domain_pattern("kv", Arc::new(BackpressuredSink));

        let ingress = RuntimeIngress::new(false).with_router(router);
        let session = make_session_info(90, TransportKind::Tcp);

        // Act
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            ingress.on_open(session).await.unwrap();
            let frame = crate::benchkit::transport::build_kv_begin("kv://test/app/users", 1, 0);
            let payload = Bytes::from(frame[3..].to_vec());

            let decision = ingress
                .on_frame(
                    90,
                    ChannelId::Pub,
                    crate::protocol::tlv::MessageType::new(100),
                    payload,
                )
                .await;

            // Assert
            assert_eq!(decision, IngressDecision::Backpressure);
            assert!(
                metrics.counter_get(obs::METRIC_ROUTER_BACKPRESSURE) > backpressure_before,
                "expected router backpressure metric to increase"
            );
        });
    }

    #[test]
    fn should_list_sessions() {
        // Arrange
        let ingress = runtime_ingress_with_jwks_auth();
        let rt = tokio::runtime::Runtime::new().unwrap();

        // Act
        rt.block_on(async {
            for i in 1..=3 {
                let session = make_session_info(i, TransportKind::WebSocket);
                ingress.on_open(session).await.unwrap();
            }
        });

        // Assert
        assert_eq!(ingress.session_count(), 3);
    }

    #[tokio::test]
    async fn should_allow_anonymous_access_when_auth_not_required() {
        // Arrange - Create ingress with auth_required=false
        let ingress = RuntimeIngress::new(false);
        let session = make_session_info(1, TransportKind::WebSocket);
        ingress.on_open(session).await.unwrap();

        // Build a CONNECT frame with empty JWT
        let jwt = "eyJhbGciOiJub25lIn0.e30."; // header.payload.sig (all empty)

        // Act - Send CONNECT frame without valid JWT
        let result = ingress
            .on_frame(
                1,
                ChannelId::Control,
                crate::protocol::tlv::MessageType::CONNECT,
                Bytes::from(jwt),
            )
            .await;

        // Assert - Should accept (anonymous mode)
        assert!(
            matches!(result, IngressDecision::Accept),
            "Expected Accept in anonymous mode, got {:?}",
            result
        );

        // Verify session has full permissions
        let session_actor = ingress.session_actors.get(&1).unwrap();
        let perms = &session_actor.permissions;
        assert_eq!(ingress.get_session(1).unwrap().route_family.id(), 1);

        // Should have access to all domains
        assert!(perms.allows(&Route::new("kv://test/area/resource"), Access::Write));
        assert!(perms.allows(&Route::new("notice://test/area/resource"), Access::Write));
        assert!(perms.allows(&Route::new("rpc://test/area/resource"), Access::Write));
    }

    #[test]
    fn should_canonicalize_scheme_less_domain_routes_for_authorization() {
        // Arrange
        // Act
        let queue_route = RuntimeIngress::canonicalize_domain_route(
            DispatchDomain::Queue,
            Route::new("realm/area/tasks/receive"),
        )
        .expect("canonical queue route");
        let notice_route = RuntimeIngress::canonicalize_domain_route(
            DispatchDomain::Notice,
            Route::new("patterns/*"),
        )
        .expect("canonical notice route");
        let stream_route = RuntimeIngress::canonicalize_domain_route(
            DispatchDomain::Stream,
            Route::new("realm/area/stream-data/append"),
        )
        .expect("canonical stream route");
        let existing_notice_route = RuntimeIngress::canonicalize_domain_route(
            DispatchDomain::Notice,
            Route::new("notice://test/notifications/**"),
        )
        .expect("canonical existing notice route");

        // Assert
        assert_eq!(queue_route.as_str(), "queue://realm/area/tasks");
        assert_eq!(notice_route.as_str(), "notice://patterns/*");
        assert_eq!(stream_route.as_str(), "stream://realm/area/stream-data");
        assert_eq!(
            existing_notice_route.as_str(),
            "notice://test/notifications/**"
        );
    }

    #[test]
    fn should_derive_canonical_routes_for_scheme_less_domain_payloads() {
        // Arrange
        let ingress = RuntimeIngress::new(false);
        let mut session = make_session_info(91, TransportKind::Tcp);
        session.route_family = crate::runtime::routing::RouteFamily::new(1);

        let mut queue_payload = Vec::new();
        let queue_wire_route = b"realm/area/tasks/receive";
        queue_payload.extend_from_slice(&(queue_wire_route.len() as u32).to_be_bytes());
        queue_payload.extend_from_slice(queue_wire_route);
        queue_payload.extend_from_slice(&(1_u32).to_be_bytes());
        queue_payload.extend_from_slice(b"x");
        queue_payload.push(0);

        // Act
        let queue_route = ingress
            .derive_route_for_frame(
                &session,
                crate::protocol::tlv::MessageType::new(200),
                &Bytes::from(queue_payload),
            )
            .unwrap()
            .unwrap();

        let pattern = b"patterns/*";
        let mut notice_payload = Vec::new();
        notice_payload.extend_from_slice(&(pattern.len() as u32).to_be_bytes());
        notice_payload.extend_from_slice(pattern);
        let notice_route = ingress
            .derive_route_for_frame(
                &session,
                crate::protocol::tlv::MessageType::new(501),
                &Bytes::from(notice_payload),
            )
            .unwrap()
            .unwrap();

        // Assert
        assert_eq!(queue_route.as_str(), "queue://realm/area/tasks");
        assert_eq!(notice_route.as_str(), "notice://patterns/*");

        let stream_name = b"realm/area/stream-data/read";
        let mut stream_payload = Vec::new();
        stream_payload.extend_from_slice(&(stream_name.len() as u32).to_be_bytes());
        stream_payload.extend_from_slice(stream_name);
        stream_payload.extend_from_slice(&0_u64.to_be_bytes());
        stream_payload.extend_from_slice(&1000_u64.to_be_bytes());
        stream_payload.push(0);
        let stream_route = ingress
            .derive_route_for_frame(
                &session,
                crate::protocol::tlv::MessageType::new(604),
                &Bytes::from(stream_payload),
            )
            .unwrap()
            .unwrap();
        assert_eq!(stream_route.as_str(), "stream://realm/area/stream-data");
    }

    #[test]
    fn should_map_notice_authorization_policies() {
        // Arrange
        let publish = auth_spec(500);
        let subscribe = auth_spec(501);
        let unsubscribe = auth_spec(502);

        // Act
        let publish_policy = publish.policy;
        let subscribe_policy = subscribe.policy;
        let unsubscribe_policy = unsubscribe.policy;

        // Assert
        assert_eq!(publish.domain, DispatchDomain::Notice);
        assert_eq!(
            publish_policy,
            AuthorizationPolicy::RouteScoped(Access::Write)
        );
        assert_eq!(subscribe.domain, DispatchDomain::Notice);
        assert_eq!(
            subscribe_policy,
            AuthorizationPolicy::RouteScoped(Access::Read)
        );
        assert_eq!(unsubscribe.domain, DispatchDomain::Notice);
        assert_eq!(unsubscribe_policy, AuthorizationPolicy::SessionOwned);
    }

    #[test]
    fn should_map_rpc_authorization_policies() {
        // Arrange
        let register = auth_spec(300);
        let unregister = auth_spec(301);
        let call = auth_spec(302);
        let response = auth_spec(303);
        let ack = auth_spec(304);

        // Act
        let register_policy = register.policy;
        let unregister_policy = unregister.policy;
        let call_policy = call.policy;

        // Assert
        assert_eq!(register.domain, DispatchDomain::Rpc);
        assert_eq!(
            register_policy,
            AuthorizationPolicy::RouteScoped(Access::All)
        );
        assert_eq!(
            unregister_policy,
            AuthorizationPolicy::RouteScoped(Access::All)
        );
        assert_eq!(call_policy, AuthorizationPolicy::RouteScoped(Access::Write));
        assert_eq!(response.policy, AuthorizationPolicy::SessionOwned);
        assert_eq!(ack.policy, AuthorizationPolicy::SessionOwned);
    }

    #[test]
    fn should_map_kv_authorization_policies() {
        // Arrange
        let route = "kv://acme/app/users";
        let read_only_frame = crate::benchkit::build_kv_begin(route, 0, 0);
        let read_write_frame = crate::benchkit::build_kv_begin(route, 1, 0);
        let (_, read_only_payload) = crate::benchkit::extract_single_tlv_field(&read_only_frame);
        let (_, read_write_payload) = crate::benchkit::extract_single_tlv_field(&read_write_frame);

        // Act
        let (read_only_targets, read_only_access) = RuntimeIngress::resolve_authorization_targets(
            DispatchDomain::Kv,
            MessageType::new(100),
            read_only_payload.as_ref(),
            auth_spec(100).policy,
        )
        .expect("resolve read-only begin auth");
        let (read_write_targets, read_write_access) =
            RuntimeIngress::resolve_authorization_targets(
                DispatchDomain::Kv,
                MessageType::new(100),
                read_write_payload.as_ref(),
                auth_spec(100).policy,
            )
            .expect("resolve read-write begin auth");
        let (put_targets, _) = RuntimeIngress::resolve_authorization_targets(
            DispatchDomain::Kv,
            MessageType::new(104),
            b"",
            auth_spec(104).policy,
        )
        .expect("resolve put auth");

        // Assert
        assert_eq!(read_only_access, Access::Read);
        assert_eq!(read_only_targets.span_target(), (route, 1));
        assert_eq!(read_write_access, Access::Write);
        assert_eq!(read_write_targets.span_target(), (route, 1));
        assert_eq!(put_targets.span_target(), ("<session-owned>", 1));
        assert_eq!(
            auth_spec(109).policy,
            AuthorizationPolicy::RouteScoped(Access::Read)
        );
    }

    #[test]
    fn should_require_explicit_wildcard_policy_for_schedule_list_authorization() {
        // Arrange
        let payload = [];

        // Act
        let wildcard = RuntimeIngress::resolve_authorization_targets(
            DispatchDomain::Schedule,
            MessageType::new(702),
            &payload,
            AuthorizationPolicy::WildcardScoped(Access::Read),
        )
        .expect("resolve schedule list wildcard auth");
        let missing_route = RuntimeIngress::resolve_authorization_targets(
            DispatchDomain::Schedule,
            MessageType::new(702),
            &payload,
            AuthorizationPolicy::RouteScoped(Access::Read),
        );

        // Assert
        assert_eq!(wildcard.0.span_target(), ("schedule://**", 1));
        assert_eq!(wildcard.1, Access::Read);
        assert!(missing_route.is_err());
    }

    #[test]
    fn should_canonicalize_domain_identity_routes_for_authorization() {
        // Arrange
        let kv_begin = crate::benchkit::build_kv_begin("kv://acme/app/users/extra", 0, 0);
        let (_, kv_payload) = crate::benchkit::extract_single_tlv_field(&kv_begin);
        let queue_payload = encode_queue_send("queue://acme/app/jobs/process", b"job");
        let lease_payload = encode_lease_subscribe("lease://acme/locks/db/migration");
        let stream_begin = crate::benchkit::build_stream_begin("stream://acme/logs/events/append");
        let (_, stream_payload) = crate::benchkit::extract_single_tlv_field(&stream_begin);

        // Act
        let kv_result = RuntimeIngress::resolve_authorization_targets(
            DispatchDomain::Kv,
            MessageType::new(100),
            kv_payload.as_ref(),
            auth_spec(100).policy,
        );
        let queue_targets = RuntimeIngress::resolve_authorization_targets(
            DispatchDomain::Queue,
            MessageType::new(200),
            queue_payload.as_ref(),
            auth_spec(200).policy,
        )
        .expect("resolve queue auth");
        let lease_targets = RuntimeIngress::resolve_authorization_targets(
            DispatchDomain::Lease,
            MessageType::new(407),
            lease_payload.as_ref(),
            auth_spec(407).policy,
        )
        .expect("resolve lease auth");
        let stream_targets = RuntimeIngress::resolve_authorization_targets(
            DispatchDomain::Stream,
            MessageType::new(600),
            stream_payload.as_ref(),
            auth_spec(600).policy,
        )
        .expect("resolve stream auth");

        // Assert
        assert!(kv_result.is_err());
        assert_eq!(queue_targets.0.span_target(), ("queue://acme/app/jobs", 1));
        assert_eq!(lease_targets.0.span_target(), ("lease://acme/locks/db", 1));
        assert_eq!(
            stream_targets.0.span_target(),
            ("stream://acme/logs/events", 1)
        );
    }

    #[test]
    fn should_keep_authorization_policies_for_unaffected_domains() {
        // Arrange
        let queue_send = auth_spec(200);
        let queue_watch = auth_spec(207);
        let lease_acquire = auth_spec(400);
        let lease_query = auth_spec(403);
        let stream_begin = auth_spec(600);
        let stream_append = auth_spec(601);
        let stream_read = auth_spec(604);
        let schedule_create = auth_spec(700);
        let schedule_list = auth_spec(702);
        let schedule_batch = auth_spec(706);

        // Act
        let policies = [
            queue_send.policy,
            queue_watch.policy,
            lease_acquire.policy,
            lease_query.policy,
            stream_begin.policy,
            stream_append.policy,
            stream_read.policy,
            schedule_create.policy,
            schedule_list.policy,
            schedule_batch.policy,
        ];

        // Assert
        assert_eq!(queue_send.domain, DispatchDomain::Queue);
        assert_eq!(policies[0], AuthorizationPolicy::RouteScoped(Access::Write));
        assert_eq!(policies[1], AuthorizationPolicy::RouteScoped(Access::Read));
        assert_eq!(lease_acquire.domain, DispatchDomain::Lease);
        assert_eq!(policies[2], AuthorizationPolicy::RouteScoped(Access::Write));
        assert_eq!(policies[3], AuthorizationPolicy::RouteScoped(Access::Read));
        assert_eq!(stream_begin.domain, DispatchDomain::Stream);
        assert_eq!(policies[4], AuthorizationPolicy::RouteScoped(Access::Write));
        assert_eq!(policies[5], AuthorizationPolicy::SessionOwned);
        assert_eq!(policies[6], AuthorizationPolicy::RouteScoped(Access::Read));
        assert_eq!(schedule_create.domain, DispatchDomain::Schedule);
        assert_eq!(policies[7], AuthorizationPolicy::RouteScoped(Access::Write));
        assert_eq!(
            policies[8],
            AuthorizationPolicy::WildcardScoped(Access::Read)
        );
        assert_eq!(
            policies[9],
            AuthorizationPolicy::MultiRouteScoped(Access::Write)
        );
    }

    #[tokio::test]
    async fn should_return_notice_unauthorized_response_without_closing_session() {
        // Arrange
        let family = RouteFamily::new(1);
        let session_id = 501;
        let router = Arc::new(crate::runtime::Router::new());
        let domain_mailbox = Arc::new(Mailbox::new(8));
        let inbox_mailbox = Arc::new(Mailbox::new(8));
        router.register_domain_pattern("notice", domain_mailbox.clone());
        router.register(
            RouteAddress::new(family, Route::new("inbox://session/501")),
            inbox_mailbox.clone(),
        );
        let ingress = runtime_ingress_with_jwks_auth().with_router(router);
        let session = make_authenticated_session_info(
            session_id,
            TransportKind::Tcp,
            family,
            &["notice://prod/orders/**#read"],
        );
        ingress.on_open(session).await.unwrap();

        // Act
        let subscribe_decision = ingress
            .on_frame(
                session_id,
                ChannelId::Sub,
                MessageType::new(501),
                encode_notice_subscribe("notice://prod/orders/**"),
            )
            .await;
        let subscribe_frame = receive_frame(&domain_mailbox, "notice subscribe dispatch");
        let publish_decision = ingress
            .on_frame(
                session_id,
                ChannelId::Pub,
                MessageType::new(500),
                encode_notice_publish("notice://prod/orders/create", b"hello"),
            )
            .await;
        let unauthorized_frame = receive_frame(&inbox_mailbox, "notice unauthorized response");

        // Assert
        assert_eq!(subscribe_decision, IngressDecision::Accept);
        assert_eq!(subscribe_frame.msg_type, MessageType::new(501));
        assert_eq!(publish_decision, IngressDecision::Accept);
        assert_eq!(unauthorized_frame.msg_type, MessageType::new(500));
        assert_eq!(unauthorized_frame.channel_id, ChannelId::Pub);
        assert_eq!(
            decode_domain_error_code(unauthorized_frame.payload.as_ref()),
            crate::protocol::error_codes::notice::ERR_UNAUTHORIZED
        );
        assert_eq!(ingress.session_count(), 1);
        assert!(domain_mailbox.receiver().try_recv().is_err());
    }

    #[tokio::test]
    async fn should_require_all_for_rpc_worker_registration_at_ingress() {
        // Arrange
        let family = RouteFamily::new(1);
        let router = Arc::new(crate::runtime::Router::new());
        let domain_mailbox = Arc::new(Mailbox::new(8));
        let write_inbox = Arc::new(Mailbox::new(8));
        let all_inbox = Arc::new(Mailbox::new(8));
        router.register_domain_pattern("rpc", domain_mailbox.clone());
        router.register(
            RouteAddress::new(family, Route::new("inbox://session/610")),
            write_inbox.clone(),
        );
        router.register(
            RouteAddress::new(family, Route::new("inbox://session/611")),
            all_inbox.clone(),
        );
        let ingress = runtime_ingress_with_jwks_auth().with_router(router);
        let write_session = make_authenticated_session_info(
            610,
            TransportKind::Tcp,
            family,
            &["rpc://acme/tasks/**#write"],
        );
        let all_session = make_authenticated_session_info(
            611,
            TransportKind::Tcp,
            family,
            &["rpc://acme/tasks/**#*"],
        );
        ingress.on_open(write_session).await.unwrap();
        ingress.on_open(all_session).await.unwrap();

        // Act
        let register_frame = crate::benchkit::build_rpc_subscribe("rpc://acme/tasks/worker");
        let (_, register_payload) = crate::benchkit::extract_single_tlv_field(&register_frame);
        let denied_decision = ingress
            .on_frame(
                610,
                ChannelId::Rpc,
                MessageType::new(300),
                register_payload.clone(),
            )
            .await;
        let denied_frame = receive_frame(&write_inbox, "rpc unauthorized response");
        let allowed_decision = ingress
            .on_frame(611, ChannelId::Rpc, MessageType::new(300), register_payload)
            .await;
        let allowed_frame = receive_frame(&domain_mailbox, "rpc register dispatch");

        // Assert
        assert_eq!(denied_decision, IngressDecision::Accept);
        assert_eq!(
            decode_domain_error_code(denied_frame.payload.as_ref()),
            crate::protocol::error_codes::rpc::ERR_UNAUTHORIZED
        );
        assert_eq!(allowed_decision, IngressDecision::Accept);
        assert_eq!(allowed_frame.msg_type, MessageType::new(300));
        assert!(all_inbox.receiver().try_recv().is_err());
    }

    #[tokio::test]
    async fn should_authorize_kv_begin_by_mode_while_keeping_tx_ops_session_owned_at_ingress() {
        // Arrange
        let family = RouteFamily::new(1);
        let session_id = 620;
        let route = "kv://acme/app/users";
        let router = Arc::new(crate::runtime::Router::new());
        let domain_mailbox = Arc::new(Mailbox::new(8));
        let inbox_mailbox = Arc::new(Mailbox::new(8));
        router.register_domain_pattern("kv", domain_mailbox.clone());
        router.register(
            RouteAddress::new(family, Route::new("inbox://session/620")),
            inbox_mailbox.clone(),
        );
        let ingress = runtime_ingress_with_jwks_auth().with_router(router);
        let session = make_authenticated_session_info(
            session_id,
            TransportKind::Tcp,
            family,
            &["kv://acme/app/users#read"],
        );
        ingress.on_open(session).await.unwrap();

        // Act
        let read_only_begin = crate::benchkit::build_kv_begin(route, 0, 0);
        let (_, read_only_payload) = crate::benchkit::extract_single_tlv_field(&read_only_begin);
        let read_only_decision = ingress
            .on_frame(
                session_id,
                ChannelId::Pub,
                MessageType::new(100),
                read_only_payload,
            )
            .await;
        let read_only_frame = receive_frame(&domain_mailbox, "kv read-only begin dispatch");

        let read_write_begin = crate::benchkit::build_kv_begin(route, 1, 0);
        let (_, read_write_payload) = crate::benchkit::extract_single_tlv_field(&read_write_begin);
        let read_write_decision = ingress
            .on_frame(
                session_id,
                ChannelId::Pub,
                MessageType::new(100),
                read_write_payload,
            )
            .await;
        let denied_frame = receive_frame(&inbox_mailbox, "kv unauthorized response");

        let put_frame = crate::benchkit::build_kv_put(7, route, b"name", b"ada");
        let (_, put_payload) = crate::benchkit::extract_single_tlv_field(&put_frame);
        let put_decision = ingress
            .on_frame(
                session_id,
                ChannelId::Pub,
                MessageType::new(104),
                put_payload,
            )
            .await;
        let put_dispatch = receive_frame(&domain_mailbox, "kv put dispatch");

        // Assert
        assert_eq!(read_only_decision, IngressDecision::Accept);
        assert_eq!(read_only_frame.msg_type, MessageType::new(100));
        assert_eq!(read_write_decision, IngressDecision::Accept);
        assert_eq!(
            decode_domain_error_code(denied_frame.payload.as_ref()),
            crate::protocol::error_codes::kv::ERR_UNAUTHORIZED
        );
        assert_eq!(put_decision, IngressDecision::Accept);
        assert_eq!(put_dispatch.msg_type, MessageType::new(104));
    }

    #[tokio::test]
    async fn should_deny_expired_session_owned_frame_without_closing_session() {
        // Arrange
        let family = RouteFamily::new(1);
        let session_id = 621;
        let router = Arc::new(crate::runtime::Router::new());
        let domain_mailbox = Arc::new(Mailbox::new(8));
        let inbox_mailbox = Arc::new(Mailbox::new(8));
        router.register_domain_pattern("kv", domain_mailbox.clone());
        router.register(
            RouteAddress::new(family, Route::new("inbox://session/621")),
            inbox_mailbox.clone(),
        );
        let ingress = runtime_ingress_with_jwks_auth().with_router(router);
        let session = make_authenticated_session_info(
            session_id,
            TransportKind::Tcp,
            family,
            &["kv://acme/app/users#*"],
        );
        ingress.on_open(session).await.unwrap();
        install_expired_session_actor(&ingress, session_id, &["kv://acme/app/users#*"]);

        // Act
        let put_frame = crate::benchkit::build_kv_put(7, "kv://acme/app/users", b"name", b"ada");
        let (_, put_payload) = crate::benchkit::extract_single_tlv_field(&put_frame);
        let decision = ingress
            .on_frame(
                session_id,
                ChannelId::Pub,
                MessageType::new(104),
                put_payload,
            )
            .await;
        let denied_frame = receive_frame(&inbox_mailbox, "expired session-owned denial");

        // Assert
        assert_eq!(decision, IngressDecision::Accept);
        assert_eq!(denied_frame.channel_id, ChannelId::Pub);
        assert_eq!(denied_frame.msg_type, MessageType::new(104));
        assert_eq!(
            decode_domain_error_code(denied_frame.payload.as_ref()),
            crate::protocol::error_codes::kv::ERR_UNAUTHORIZED
        );
        assert_eq!(ingress.session_count(), 1);
        assert!(domain_mailbox.receiver().try_recv().is_err());
    }

    #[test]
    fn should_reject_unassigned_notice_message_types() {
        // Arrange
        let msg_type = crate::protocol::tlv::MessageType::new(505);

        // Act
        let result = RuntimeIngress::domain_dispatch_for_msg_type(msg_type);

        // Assert
        assert!(result.is_err());
    }
}
