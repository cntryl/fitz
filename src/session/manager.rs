// LAYER: SESSION (Async â†’ Sync Bridge)
//! Ingress trait and reference implementation for the async â†’ sync boundary
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

fn canonicalize_dispatch_route_str<'a>(domain: DispatchDomain, route: &'a str) -> Cow<'a, str> {
    RuntimeIngress::canonicalize_domain_route_str(domain.as_str(), route)
}

fn extract_auth_route_for_domain<'a>(
    domain: DispatchDomain,
    msg_type: u16,
    payload: &'a [u8],
) -> Result<Option<Cow<'a, str>>, String> {
    match domain {
        DispatchDomain::Kv => crate::protocol::kv_codec::extract_auth_route(msg_type, payload)
            .map(|route| route.map(|route| canonicalize_dispatch_route_str(domain, route))),
        DispatchDomain::Queue => crate::protocol::queue_codec::extract_auth_route(msg_type, payload)
            .map(|route| route.map(|route| canonicalize_dispatch_route_str(domain, route))),
        DispatchDomain::Rpc => crate::protocol::rpc_codec::extract_auth_route(msg_type, payload)
            .map(|route| route.map(|route| canonicalize_dispatch_route_str(domain, route))),
        DispatchDomain::Lease => crate::protocol::lease_codec::extract_auth_route(msg_type, payload)
            .map(|route| route.map(|route| canonicalize_dispatch_route_str(domain, route))),
        DispatchDomain::Notice => crate::protocol::notice_codec::extract_auth_route(msg_type, payload)
            .map(|route| route.map(|route| canonicalize_dispatch_route_str(domain, route))),
        DispatchDomain::Stream => crate::protocol::stream_codec::extract_auth_route(msg_type, payload)
            .map(|route| route.map(|route| canonicalize_dispatch_route_str(domain, route))),
        DispatchDomain::Schedule => crate::protocol::schedule_codec::extract_auth_route(msg_type, payload)
            .map(|route| route.map(|route| canonicalize_dispatch_route_str(domain, route))),
    }
}

enum AuthorizationTargets<'a> {
    SessionOwned,
    Single(Cow<'a, str>),
    Multiple(Vec<Cow<'a, str>>),
}

struct DomainDispatchRequest<'a> {
    router: &'a crate::runtime::Router,
    session_id: u64,
    route_family: crate::runtime::routing::RouteFamily,
    domain: DispatchDomain,
    access: crate::auth::Access,
    msg_type: crate::protocol::tlv::MessageType,
    preserve_payload_for_handler: bool,
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
            Self::SessionOwned => (true, "<session-owned>", 0),
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
    /// Optional router for dispatching frames to domain sinks
    router: Option<Arc<crate::runtime::Router>>,
    /// Optional callback for session events (for routing to handlers)
    event_handler: Option<Arc<dyn Fn(SessionEvent) + Send + Sync>>,
    /// Control-plane-backed route family resolver
    control_plane: Arc<crate::session::tenant::ControlPlaneStub>,
    /// Storage engine used to ensure RouteFamily -> ColumnFamily alignment
    store: Option<Arc<cntryl_midge::Engine>>,
    /// Whether authentication is required (if false, JWT is ignored and full access granted)
    auth_required: bool,
    /// Passive admin snapshot mirror for session lifecycle
    admin_read_model: Option<Arc<crate::api::admin::read_model::AdminReadModel>>,

    /// Explicit auth configuration used for CONNECT verification when present.
    auth_config: Option<crate::auth::AuthConfig>,
}

impl RuntimeIngress {
    /// Create a new ingress implementation
    pub fn new(auth_required: bool) -> Self {
        Self {
            sessions: Arc::new(DashMap::new()),
            session_actors: Arc::new(DashMap::new()),
            session_inbox_routes: Arc::new(DashMap::new()),
            router: None,
            event_handler: None,
            control_plane: Arc::new(crate::session::tenant::ControlPlaneStub::new()),
            store: None,
            auth_required,
            admin_read_model: None,
            auth_config: None,
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

    /// Attach storage for dynamic RouteFamily column-family creation.
    pub fn with_store(mut self, store: Arc<cntryl_midge::Engine>) -> Self {
        self.store = Some(store);
        self
    }

    pub fn with_auth_config(mut self, auth_config: crate::auth::AuthConfig) -> Self {
        self.auth_config = Some(auth_config);
        self
    }

    /// Attach a control plane resolver.
    pub fn with_control_plane(
        mut self,
        control_plane: Arc<crate::session::tenant::ControlPlaneStub>,
    ) -> Self {
        self.control_plane = control_plane;
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

    fn ensure_route_family_storage(
        &self,
        route_family: crate::runtime::routing::RouteFamily,
    ) -> Result<(), String> {
        if let Some(store) = &self.store {
            crate::boot::storage::ensure_route_family(store.as_ref(), route_family)
                .map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    fn resolve_authenticated_route_family(
        &self,
        compact_jwt: &str,
    ) -> Result<crate::runtime::routing::RouteFamily, String> {
        let assignment = self.control_plane.assign_route_family(compact_jwt);
        if assignment.created {
            self.ensure_route_family_storage(assignment.family)?;
        }
        Ok(assignment.family)
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
    }

    #[cfg_attr(not(test), allow(dead_code))]
    fn canonicalize_domain_route(
        domain: &str,
        route: crate::runtime::routing::Route,
    ) -> crate::runtime::routing::Route {
        let path = route.as_str();
        if path.contains("://") {
            return route;
        }

        crate::runtime::routing::Route::new(format!("{domain}://{}", path.trim_start_matches('/')))
    }

    fn canonicalize_domain_route_str<'a>(domain: &str, route: &'a str) -> Cow<'a, str> {
        if route.contains("://") {
            return Cow::Borrowed(route);
        }

        let trimmed = route.trim_start_matches('/');
        let mut canonical = String::with_capacity(domain.len() + 3 + trimmed.len());
        canonical.push_str(domain);
        canonical.push_str("://");
        canonical.push_str(trimmed);
        Cow::Owned(canonical)
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
    ) -> Result<Option<(DispatchDomain, crate::auth::Access)>, &'static str> {
        let mt = msg_type.as_u16();

        match mt {
            100 | 101 | 102 | 104 | 105 | 106 | 107 => {
                Ok(Some((DispatchDomain::Kv, crate::auth::Access::Write)))
            }
            103 | 108 | 109 | 110 => Ok(Some((DispatchDomain::Kv, crate::auth::Access::Read))),
            111 => Err("invalid message type: 111 is server-to-client only"),
            200..=204 => Ok(Some((DispatchDomain::Queue, crate::auth::Access::Write))),
            207 | 208 => Ok(Some((DispatchDomain::Queue, crate::auth::Access::Read))),
            209 => Err("invalid message type: 209 is server-to-client only"),
            205 | 206 | 210..=299 => Err("invalid message type: unsupported queue operation"),
            300..=304 => Ok(Some((DispatchDomain::Rpc, crate::auth::Access::Write))),
            305..=399 => Ok(Some((DispatchDomain::Rpc, crate::auth::Access::Read))),
            400..=402 => Ok(Some((DispatchDomain::Lease, crate::auth::Access::Write))),
            403 => Ok(Some((DispatchDomain::Lease, crate::auth::Access::Read))),
            407 | 408 => Ok(Some((DispatchDomain::Lease, crate::auth::Access::Read))),
            409 => Err("invalid message type: 409 is server-to-client only"),
            404..=406 | 410..=499 => Err("invalid message type: unsupported lease operation"),
            500..=503 => Ok(Some((DispatchDomain::Notice, crate::auth::Access::Write))),
            504 => Err("invalid message type: 504 is server-to-client only"),
            505..=599 => Err("invalid message type: 505-599 are unsupported notice operations"),
            600..=603 => Ok(Some((DispatchDomain::Stream, crate::auth::Access::Write))),
            604..=608 => Ok(Some((DispatchDomain::Stream, crate::auth::Access::Read))),
            609 => Err("invalid message type: 609 is server-to-client only"),
            700 | 701 | 706 => Ok(Some((DispatchDomain::Schedule, crate::auth::Access::Write))),
            702..=704 => Ok(Some((DispatchDomain::Schedule, crate::auth::Access::Read))),
            705 => Err("invalid message type: 705 is server-to-client only"),
            _ => Ok(None),
        }
    }

    fn skips_route_authorization(msg_type: crate::protocol::tlv::MessageType) -> bool {
        matches!(msg_type.as_u16(), 502 | 503)
    }

    fn resolve_authorization_targets<'a>(
        domain: DispatchDomain,
        msg_type: crate::protocol::tlv::MessageType,
        payload: &'a [u8],
    ) -> Result<AuthorizationTargets<'a>, String> {
        if domain == DispatchDomain::Schedule && msg_type.as_u16() == 706 {
            let routes = crate::protocol::schedule_codec::extract_batch_auth_routes(payload)?
                .into_iter()
                .map(|route| canonicalize_dispatch_route_str(domain, route))
                .collect();
            return Ok(AuthorizationTargets::Multiple(routes));
        }

        match Self::derive_auth_route_for_frame(domain, msg_type, payload)? {
            Some(route) => Ok(AuthorizationTargets::Single(route)),
            None if Self::skips_route_authorization(msg_type) => {
                Ok(AuthorizationTargets::SessionOwned)
            }
            None => Ok(AuthorizationTargets::Single(Cow::Borrowed(
                domain.wildcard_route(),
            ))),
        }
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
    ) -> Result<(), IngressDecision> {
        let Some(actor_ref) = self.get_session_actor(session_id) else {
            warn!(
                session_id = session_id,
                "Ingress: missing session actor for authorization"
            );
            return Err(IngressDecision::Close(
                "unauthorized: session actor missing".to_string(),
            ));
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

        if let Ok(collector) = std::panic::catch_unwind(crate::boot::observability::metrics) {
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

            if let Ok(collector) = std::panic::catch_unwind(crate::boot::observability::metrics) {
                collector.counter_inc(obs::METRIC_AUTH_FAILURES);
            }

            return Err(IngressDecision::Close(
                "unauthorized: permission denied".to_string(),
            ));
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
            crate::protocol::frame::ChannelId::Pub,
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
        if let Ok(collector) = std::panic::catch_unwind(crate::boot::observability::metrics) {
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
        ) {
            Ok(targets) => targets,
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

        if let Ok(collector) = std::panic::catch_unwind(crate::boot::observability::metrics) {
            collector.histogram_observe_us(
                obs::METRIC_INGRESS_AUTH_ROUTE_LATENCY,
                auth_route_start.elapsed().as_micros() as u64,
            );
        }

        self.authorize_domain_targets(
            dispatch.session_id,
            dispatch.msg_type,
            dispatch.domain,
            dispatch.access,
            &targets,
        )?;
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
        let session_id = session.session_id;

        // Record session opened counter
        if let Ok(collector) = std::panic::catch_unwind(crate::boot::observability::metrics) {
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
        let _ingress_latency = crate::boot::observability::ScopedHistogramUs::new(
            obs::METRIC_INGRESS_FRAME_TOTAL_LATENCY,
        );
        // Record frame received counter
        if let Ok(collector) = std::panic::catch_unwind(crate::boot::observability::metrics) {
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
        // We'll set authenticated=true while holding the map write guard, but
        // perform handler notification after dropping the guard to avoid lock reentrancy.
        let mut notify_frame: Option<SessionFrame> = None;
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
                    if channel_id != ChannelId::Control
                        || msg_type != crate::protocol::tlv::MessageType::CONNECT
                    {
                        warn!(session_id = session_id, channel = ?channel_id, msg_type = msg_type.as_u16(), "Ingress: unauthenticated, CONNECT required");
                        return IngressDecision::Close(
                            "unauthenticated: connect required".to_string(),
                        );
                    }

                    let compact =
                        std::str::from_utf8(message_payload.as_ref().unwrap()).unwrap_or("");
                    debug!(
                        session_id = session_id,
                        jwt_len = compact.len(),
                        "Ingress: verifying CONNECT JWT"
                    );

                    let auth_config = self
                        .auth_config
                        .clone()
                        .unwrap_or_else(|| crate::auth::AuthConfig::from_env(true));

                    match crate::auth::permissions_from_verified_jwt(compact, &auth_config).await {
                        Ok((snapshot, claims)) => {
                            let route_family =
                                match self.resolve_authenticated_route_family(compact) {
                                    Ok(route_family) => route_family,
                                    Err(e) => {
                                        error!(
                                            session_id = session_id,
                                            error = %e,
                                            "Ingress: CONNECT failed (route family resolution)"
                                        );
                                        return IngressDecision::Close(format!(
                                            "connect failed: {}",
                                            e
                                        ));
                                    }
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
                    // If auth is not required, grant full anonymous access
                    let snapshot = crate::auth::default_anonymous_permissions();
                    entry.permissions_snapshot = snapshot.clone();
                    entry.authenticated = true;

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
                Ok(Some((domain, access))) => {
                    let dispatch = DomainDispatchRequest {
                        router,
                        session_id,
                        route_family,
                        domain,
                        access,
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

    async fn on_close(&self, session_id: u64, reason: CloseReason) {
        // Record session closed counter
        if let Ok(collector) = std::panic::catch_unwind(crate::boot::observability::metrics) {
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
                        if let Ok(collector) =
                            std::panic::catch_unwind(crate::boot::observability::metrics)
                        {
                            collector.counter_add(
                                obs::METRIC_SESSION_CLEANUP_FAILURES,
                                failed_domains.len() as u64,
                            );
                        }
                        tracing::warn!(
                            session_id = session_id,
                            route_family = route_family.id(),
                            failed_domains = ?failed_domains,
                            "Ingress: session cleanup incomplete"
                        );
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        session_id = session_id,
                        error = %e,
                        "Ingress: cleanup worker task failed"
                    );
                }
            }
        }

        // Remove session state after domain cleanup completes.
        self.sessions.remove(&session_id);
        self.session_actors.remove(&session_id);
        self.session_inbox_routes.remove(&session_id);
        if let Some(admin_read_model) = &self.admin_read_model {
            admin_read_model.record_session_close(session_id);
        }

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

        let _realm = session_info
            .claims
            .as_ref()
            .map(|c| c.tenant.clone())
            .unwrap_or_default();

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
                        .map(|route| {
                            route.map(|route| {
                                Route::new(Self::canonicalize_domain_route_str("kv", route))
                            })
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
                    Self::canonicalize_domain_route("notice", p.route.clone()),
                )),
                Ok(crate::domains::notice::protocol::NotificationMessage::Subscribe(s)) => Ok(
                    Some(Self::canonicalize_domain_route("notice", s.pattern.clone())),
                ),
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
            .map(|route| {
                route.map(|value| {
                    let canonical = Self::canonicalize_domain_route_str("queue", value);
                    crate::runtime::routing::Route::new(canonical.as_ref())
                })
            }),
            400..=499 => crate::protocol::lease_codec::extract_auth_route(
                msg_type.as_u16(),
                payload.as_ref(),
            )
            .map(|route| {
                route.map(|value| {
                    let canonical = Self::canonicalize_domain_route_str("lease", value);
                    crate::runtime::routing::Route::new(canonical.as_ref())
                })
            }),
            600..=699 => {
                match crate::protocol::stream_codec::extract_auth_route(
                    ctx.msg_type.0,
                    payload.as_ref(),
                ) {
                    Ok(Some(route_str)) => Ok(Some(Self::canonicalize_domain_route(
                        "stream",
                        Route::new(route_str),
                    ))),
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
                        "schedule",
                        Route::new(route),
                    ))),
                    Ok(crate::domains::schedule::ScheduleMessage::Subscribe { route, .. }) => Ok(
                        Some(Self::canonicalize_domain_route("schedule", route.clone())),
                    ),
                    Ok(crate::domains::schedule::ScheduleMessage::Unsubscribe {
                        route, ..
                    }) => Ok(Some(Self::canonicalize_domain_route(
                        "schedule",
                        route.clone(),
                    ))),
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
    use std::sync::{Arc, Mutex};

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

    fn signed_hmac_jwt(payload: serde_json::Value) -> String {
        use jsonwebtoken::{Algorithm, EncodingKey, Header};

        std::env::set_var("FITZ_JWT_HMAC_SECRET", "test-secret-key");
        jsonwebtoken::encode(
            &Header::new(Algorithm::HS256),
            &payload,
            &EncodingKey::from_secret(b"test-secret-key"),
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
        RuntimeIngress::new(true)
            .with_router(router)
            .with_admin_read_model(admin_read_model)
    }

    #[tokio::test]
    async fn should_open_session() {
        let ingress = RuntimeIngress::new(true);
        let session = make_session_info(1, TransportKind::WebSocket);

        let result = ingress.on_open(session).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1);
        assert_eq!(ingress.session_count(), 1);
    }

    #[test]
    fn should_process_frame() {
        // Arrange
        let ingress = RuntimeIngress::new(true);
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
                "fitz": { "permissions": ["notice://prod/orders/**#read"] }
            });
            let jwt = signed_hmac_jwt(payload);

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
        let ingress = RuntimeIngress::new(true);

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
        let ingress = RuntimeIngress::new(true).with_event_handler(move |_event| {
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
                "fitz": { "permissions": ["notice://prod/orders/**#read"] }
            });
            let jwt = signed_hmac_jwt(payload);

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
        assert_eq!(admin_read_model.sessions(None).len(), 1);

        // Act
        ingress.on_close(session_id, CloseReason::ClientClose).await;

        // Assert
        assert_eq!(ingress.session_count(), 0);
        assert!(ingress.get_session(session_id).is_none());
        assert!(ingress.get_session_actor(session_id).is_none());
        assert!(admin_read_model.sessions(None).is_empty());
        for sink in sinks {
            assert_eq!(sink.recorded_sessions(), vec![session_id]);
        }
    }

    #[tokio::test]
    async fn should_record_cleanup_failures_when_on_close_cannot_reach_all_domains() {
        // Arrange
        let collector = crate::boot::observability::metrics();

        let router = Arc::new(crate::runtime::Router::new());
        let admin_read_model = AdminReadModel::new();
        let ingress = make_cleanup_ingress(router.clone(), admin_read_model);
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
                    Bytes::from(register_payload),
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
            Bytes::from(request_payload.clone()),
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
        let ingress = RuntimeIngress::new(true);
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
        let ingress = RuntimeIngress::new(true);
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
        let ingress = RuntimeIngress::new(true);
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
        let ingress = RuntimeIngress::new(true);
        let session = make_session_info(50, TransportKind::Tcp);

        let payload = serde_json::json!({
            "iss": "",
            "aud": "fitz-broker",
            "sub": "user:42",
            "exp": 9999999999u64,
            "tid": "acme-prod",
            "fitz": { "permissions": ["notice://prod/orders/**#read"] }
        });
        let jwt = signed_hmac_jwt(payload);

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
    fn should_assign_distinct_route_families_per_tenant() {
        // Arrange
        let ingress = RuntimeIngress::new(true);
        let session_a = make_session_info(52, TransportKind::Tcp);
        let session_b = make_session_info(53, TransportKind::Tcp);

        let jwt_a = signed_hmac_jwt(serde_json::json!({
            "iss": "",
            "aud": "fitz-broker",
            "sub": "user:a",
            "exp": 9999999999u64,
            "tid": "tenant-a",
            "fitz": { "permissions": ["notice://tenant-a/**#read"] }
        }));
        let jwt_b = signed_hmac_jwt(serde_json::json!({
            "iss": "",
            "aud": "fitz-broker",
            "sub": "user:b",
            "exp": 9999999999u64,
            "tid": "tenant-b",
            "fitz": { "permissions": ["notice://tenant-b/**#read"] }
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
        assert!(session_a.route_family.id() >= 2);
        assert!(session_b.route_family.id() >= 2);
    }

    #[test]
    fn should_reject_connect_with_malformed_permissions() {
        // Arrange
        let ingress = RuntimeIngress::new(true);
        let session = make_session_info(51, TransportKind::Tcp);

        let payload = serde_json::json!({
            "iss": "",
            "aud": "fitz-broker",
            "sub": "user:42",
            "exp": 9999999999u64,
            "tid": "acme-prod",
            "fitz": { "permissions": ["badperm#oops"] }
        });
        let jwt = signed_hmac_jwt(payload);

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
        let ingress = RuntimeIngress::new(true);
        let session = make_session_info(54, TransportKind::Tcp);
        let jwt = signed_hmac_jwt(serde_json::json!({
            "iss": "not-a-valid-issuer",
            "aud": "fitz-broker",
            "sub": "user:54",
            "exp": 9999999999u64,
            "tid": "acme-prod",
            "fitz": { "permissions": ["notice://prod/orders/**#read"] }
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

        let ingress = RuntimeIngress::new(true).with_auth_config(crate::auth::AuthConfig::jwks(
            vec!["fitz-broker".to_string()],
            vec![crate::auth::JwksIssuerConfig {
                issuer: "https://idp.example".to_string(),
                jwks_url: "https://idp.example/.well-known/jwks.json".to_string(),
            }],
        ));
        let session = make_session_info(80, TransportKind::Tcp);

        // Build a signed HS256 token and cache a matching oct key under the issuer's derived JWKS URL
        let iss = "https://idp.example";
        let jwks_url = crate::auth::derive_jwks_url_from_issuer(iss).unwrap();

        let payload = serde_json::json!({
            "iss": iss,
            "aud": "fitz-broker",
            "sub": "user:80",
            "exp": 9999999999u64,
            "tid": "acme-prod",
            "fitz": { "permissions": ["notice://prod/orders/**#write"] }
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
        crate::auth::cache_jwks_from_json(&jwks_url, &jwks).unwrap();

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

    #[test]
    fn should_reject_connect_with_issuer_invalid_signature() {
        // Arrange
        use base64::Engine;
        use jsonwebtoken::{EncodingKey, Header};

        let ingress = RuntimeIngress::new(true).with_auth_config(crate::auth::AuthConfig::jwks(
            vec!["fitz-broker".to_string()],
            vec![crate::auth::JwksIssuerConfig {
                issuer: "https://idp.example".to_string(),
                jwks_url: "https://idp.example/.well-known/jwks.json".to_string(),
            }],
        ));
        let session = make_session_info(81, TransportKind::Tcp);

        let iss = "https://idp.example";
        let jwks_url = crate::auth::derive_jwks_url_from_issuer(iss).unwrap();

        // Create a token signed with a secret NOT in the JWKS cache
        let payload = serde_json::json!({
            "iss": iss,
            "aud": "fitz-broker",
            "sub": "user:81",
            "exp": 9999999999u64,
            "tid": "acme-prod",
            "fitz": { "permissions": ["notice://prod/orders/**#write"] }
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
        crate::auth::cache_jwks_from_json(&jwks_url, &jwks).unwrap();

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
        let ingress = RuntimeIngress::new(true);
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
        let ingress = RuntimeIngress::new(true);
        let session = make_session_info(61, TransportKind::Tcp);

        let payload = serde_json::json!({
            "iss": "",
            "aud": "fitz-broker",
            "sub": "user:42",
            "exp": 9999999999u64,
            "tid": "acme-prod",
            "fitz": { "permissions": ["notice://prod/orders/**#write"] }
        });
        let jwt = signed_hmac_jwt(payload);

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
    fn should_deny_e2e_notification_publish_via_ingress_snapshot() {
        // Arrange
        use crate::domains::notice::session as notice_session;
        use crate::domains::notice::NoticeRouteActor;
        use crate::runtime::actor::Context;
        use crate::runtime::router::Router;
        use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
        use bytes::Bytes;

        let ingress = RuntimeIngress::new(true);
        let session = make_session_info(70, TransportKind::Tcp);

        let payload = serde_json::json!({
            "iss": "",
            "aud": "fitz-broker",
            "sub": "user:70",
            "exp": 9999999999u64,
            "tid": "acme-prod",
            "fitz": { "permissions": ["notice://prod/orders/**#read"] }
        });
        let jwt = signed_hmac_jwt(payload);

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

        let ingress = RuntimeIngress::new(true);
        let session = make_session_info(71, TransportKind::Tcp);

        let payload = serde_json::json!({
            "iss": "",
            "aud": "fitz-broker",
            "sub": "user:71",
            "exp": 9999999999u64,
            "tid": "acme-prod",
            "fitz": { "permissions": ["notice://prod/orders/**#write"] }
        });
        let jwt = signed_hmac_jwt(payload);

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

        let metrics = crate::boot::observability::metrics();
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
        let ingress = RuntimeIngress::new(true);
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

        // Should have access to all domains
        assert!(perms.allows(&Route::new("kv://test/area/resource"), Access::Write));
        assert!(perms.allows(&Route::new("notice://test/area/resource"), Access::Write));
        assert!(perms.allows(&Route::new("rpc://test/area/resource"), Access::Write));
    }

    #[test]
    fn should_canonicalize_scheme_less_domain_routes_for_authorization() {
        // Arrange
        // Act
        let queue_route = RuntimeIngress::canonicalize_domain_route("queue", Route::new("tasks"));
        let notice_route =
            RuntimeIngress::canonicalize_domain_route("notice", Route::new("patterns/*"));
        let stream_route =
            RuntimeIngress::canonicalize_domain_route("stream", Route::new("stream-data"));
        let existing_notice_route = RuntimeIngress::canonicalize_domain_route(
            "notice",
            Route::new("notice://test/notifications/**"),
        );

        // Assert
        assert_eq!(queue_route.as_str(), "queue://tasks");
        assert_eq!(notice_route.as_str(), "notice://patterns/*");
        assert_eq!(stream_route.as_str(), "stream://stream-data");
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
        queue_payload.extend_from_slice(&(5_u32).to_be_bytes());
        queue_payload.extend_from_slice(b"tasks");
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
        assert_eq!(queue_route.as_str(), "queue://tasks");
        assert_eq!(notice_route.as_str(), "notice://patterns/*");

        let stream_name = b"stream-data";
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
        assert_eq!(stream_route.as_str(), "stream://stream-data");
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
