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
use crate::session::{CloseReason, SessionInfo, SessionPermissions};
use bytes::Bytes;
use dashmap::DashMap;
use once_cell::sync::Lazy;
use std::borrow::Cow;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, error, info, trace, warn};

fn dispatch_session_cleanup(
    router: &crate::runtime::Router,
    route_family: crate::runtime::routing::RouteFamily,
    session_id: u64,
) {
    let cleanup = crate::runtime::SessionCleanup { session_id };

    let kv_addr = crate::runtime::routing::RouteAddress::new(
        route_family,
        crate::runtime::routing::Route::new("kv://cleanup"),
    );
    let kv_envelope = crate::runtime::Envelope::new(kv_addr, cleanup.clone());
    let _ = router.route(kv_envelope);

    let notice_addr = crate::runtime::routing::RouteAddress::new(
        route_family,
        crate::runtime::routing::Route::new("notice://cleanup"),
    );
    let notice_envelope = crate::runtime::Envelope::new(notice_addr, cleanup.clone());
    let _ = router.route(notice_envelope);

    let rpc_addr = crate::runtime::routing::RouteAddress::new(
        route_family,
        crate::runtime::routing::Route::new("rpc://cleanup"),
    );
    let rpc_envelope = crate::runtime::Envelope::new(rpc_addr, cleanup.clone());
    let _ = router.route(rpc_envelope);

    let stream_addr = crate::runtime::routing::RouteAddress::new(
        route_family,
        crate::runtime::routing::Route::new("stream://cleanup"),
    );
    let stream_envelope = crate::runtime::Envelope::new(stream_addr, cleanup.clone());
    let _ = router.route(stream_envelope);

    let schedule_addr = crate::runtime::routing::RouteAddress::new(
        route_family,
        crate::runtime::routing::Route::new("schedule://cleanup"),
    );
    let schedule_envelope = crate::runtime::Envelope::new(schedule_addr, cleanup.clone());
    let _ = router.route(schedule_envelope);

    let lease_addr = crate::runtime::routing::RouteAddress::new(
        route_family,
        crate::runtime::routing::Route::new("lease://cleanup"),
    );
    let lease_envelope = crate::runtime::Envelope::new(lease_addr, cleanup.clone());
    let _ = router.route(lease_envelope);

    let queue_addr = crate::runtime::routing::RouteAddress::new(
        route_family,
        crate::runtime::routing::Route::new("queue://cleanup"),
    );
    let queue_envelope = crate::runtime::Envelope::new(queue_addr, cleanup);
    let _ = router.route(queue_envelope);
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

    fn wildcard_route_for_domain(domain: &str) -> &'static str {
        match domain {
            "kv" => "kv://**",
            "queue" => "queue://**",
            "rpc" => "rpc://**",
            "lease" => "lease://**",
            "notice" => "notice://**",
            "stream" => "stream://**",
            "schedule" => "schedule://**",
            _ => "**",
        }
    }

    fn inbound_route_for_domain_cached(domain: &str) -> &'static crate::runtime::routing::Route {
        static KV: Lazy<crate::runtime::routing::Route> =
            Lazy::new(|| crate::runtime::routing::Route::new("kv://inbound"));
        static QUEUE: Lazy<crate::runtime::routing::Route> =
            Lazy::new(|| crate::runtime::routing::Route::new("queue://inbound"));
        static RPC: Lazy<crate::runtime::routing::Route> =
            Lazy::new(|| crate::runtime::routing::Route::new("rpc://inbound"));
        static LEASE: Lazy<crate::runtime::routing::Route> =
            Lazy::new(|| crate::runtime::routing::Route::new("lease://inbound"));
        static NOTICE: Lazy<crate::runtime::routing::Route> =
            Lazy::new(|| crate::runtime::routing::Route::new("notice://inbound"));
        static STREAM: Lazy<crate::runtime::routing::Route> =
            Lazy::new(|| crate::runtime::routing::Route::new("stream://inbound"));
        static SCHEDULE: Lazy<crate::runtime::routing::Route> =
            Lazy::new(|| crate::runtime::routing::Route::new("schedule://inbound"));
        static DEFAULT: Lazy<crate::runtime::routing::Route> =
            Lazy::new(|| crate::runtime::routing::Route::new("inbox://inbound"));

        match domain {
            "kv" => &KV,
            "queue" => &QUEUE,
            "rpc" => &RPC,
            "lease" => &LEASE,
            "notice" => &NOTICE,
            "stream" => &STREAM,
            "schedule" => &SCHEDULE,
            _ => &DEFAULT,
        }
    }

    fn cached_session_inbox_route(&self, session_id: u64) -> crate::runtime::routing::Route {
        self.session_inbox_routes
            .get(&session_id)
            .map(|entry| entry.value().clone())
            .unwrap_or_else(|| crate::runtime::routing::Route::new(format!("inbox://session/{session_id}")))
    }

    fn domain_dispatch_for_msg_type(
        msg_type: crate::protocol::tlv::MessageType,
    ) -> Result<Option<(&'static str, crate::auth::Access)>, &'static str> {
        let mt = msg_type.as_u16();

        match mt {
            100 | 101 | 102 | 104 | 105 | 106 | 107 => {
                Ok(Some(("kv", crate::auth::Access::Write)))
            }
            103 | 108 => Ok(Some(("kv", crate::auth::Access::Read))),
            200..=204 => Ok(Some(("queue", crate::auth::Access::Write))),
            205..=299 => Ok(Some(("queue", crate::auth::Access::Read))),
            300..=304 => Ok(Some(("rpc", crate::auth::Access::Write))),
            305..=399 => Ok(Some(("rpc", crate::auth::Access::Read))),
            400..=402 => Ok(Some(("lease", crate::auth::Access::Write))),
            403 => Ok(Some(("lease", crate::auth::Access::Read))),
            404..=499 => Ok(Some(("lease", crate::auth::Access::Write))),
            500..=504 => Ok(Some(("notice", crate::auth::Access::Write))),
            505..=599 => Ok(Some(("notice", crate::auth::Access::Read))),
            600..=603 => Ok(Some(("stream", crate::auth::Access::Write))),
            604..=608 => Ok(Some(("stream", crate::auth::Access::Read))),
            609 => Err("invalid message type: 609 is server-to-client only"),
            700 | 701 => Ok(Some(("schedule", crate::auth::Access::Write))),
            702..=704 => Ok(Some(("schedule", crate::auth::Access::Read))),
            705 => Err("invalid message type: 705 is server-to-client only"),
            _ => Ok(None),
        }
    }

    fn derive_auth_route_for_frame<'a>(
        &self,
        msg_type: crate::protocol::tlv::MessageType,
        payload: &'a [u8],
    ) -> Result<Option<Cow<'a, str>>, String> {
        let mt = msg_type.as_u16();

        match mt {
            100..=108 => crate::protocol::kv_codec::extract_auth_route(mt, payload)
                .map(|route| route.map(|route| Self::canonicalize_domain_route_str("kv", route))),
            200..=299 => {
                crate::protocol::queue_codec::extract_auth_route(mt, payload).map(|route| {
                    route.map(|route| Self::canonicalize_domain_route_str("queue", route))
                })
            }
            300..=399 => crate::protocol::rpc_codec::extract_auth_route(mt, payload)
                .map(|route| route.map(|route| Self::canonicalize_domain_route_str("rpc", route))),
            400..=499 => {
                crate::protocol::lease_codec::extract_auth_route(mt, payload).map(|route| {
                    route.map(|route| Self::canonicalize_domain_route_str("lease", route))
                })
            }
            500..=599 => {
                crate::protocol::notice_codec::extract_auth_route(mt, payload).map(|route| {
                    route.map(|route| Self::canonicalize_domain_route_str("notice", route))
                })
            }
            600..=699 => {
                crate::protocol::stream_codec::extract_auth_route(mt, payload).map(|route| {
                    route.map(|route| Self::canonicalize_domain_route_str("stream", route))
                })
            }
            700..=799 => {
                crate::protocol::schedule_codec::extract_auth_route(mt, payload).map(|route| {
                    route.map(|route| Self::canonicalize_domain_route_str("schedule", route))
                })
            }
            _ => Ok(None),
        }
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
                    warn!(session_id = session_id, msg_type = msg_type.as_u16(), reason = reason, "Ingress: client sent server-to-client-only message type");
                    return IngressDecision::Close(reason.to_string());
                }
                Ok(Some((domain, access))) => {
                debug!(
                    session_id = session_id,
                    msg_type = msg_type.as_u16(),
                    domain = domain,
                    "Ingress: resolved domain for msg_type"
                );
                    // Attempt to derive a fine-grained route from the payload for better authorization.
                    let auth_route_start = Instant::now();
                    let auth_route = match self
                        .derive_auth_route_for_frame(msg_type, message_payload.as_ref().unwrap())
                    {
                        Ok(Some(r)) => r,
                        Ok(None) => Cow::Borrowed(Self::wildcard_route_for_domain(domain)),
                        Err(e) => {
                            warn!(session_id = session_id, error = %e, domain = domain, "Ingress: failed to derive route for authorization");
                            return IngressDecision::Close(format!(
                                "authorization parse failed: {}",
                                e
                            ));
                        }
                    };

                    if let Ok(collector) =
                        std::panic::catch_unwind(crate::boot::observability::metrics)
                    {
                        collector.histogram_observe_us(
                            obs::METRIC_INGRESS_AUTH_ROUTE_LATENCY,
                            auth_route_start.elapsed().as_micros() as u64,
                        );
                    }

                    // Check session actor's authorization
                    if let Some(actor_ref) = self.get_session_actor(session_id) {
                        // 100% sample: authorization is critical path
                        let _span = tracing::debug_span!(
                            obs::SPAN_PERMISSION_CHECK,
                            session_id = session_id,
                            route = %auth_route.as_ref(),
                            access = ?access,
                        );
                        let _guard = _span.enter();
                        let start = Instant::now();

                        let authorized = actor_ref.authorize_route(auth_route.as_ref(), access);

                        // Record latency
                        if let Ok(collector) =
                            std::panic::catch_unwind(crate::boot::observability::metrics)
                        {
                            let elapsed_us = start.elapsed().as_micros() as u64;
                            collector.histogram_observe_us(
                                obs::METRIC_PERMISSION_CHECK_LATENCY,
                                elapsed_us,
                            );
                        }

                        if !authorized {
                            warn!(
                                session_id = session_id,
                                msg_type = msg_type.as_u16(),
                                route = auth_route.as_ref(),
                                access = ?access,
                                "Ingress: authorization DENIED"
                            );

                            // Counter: auth failures
                            if let Ok(collector) =
                                std::panic::catch_unwind(crate::boot::observability::metrics)
                            {
                                collector.counter_inc(obs::METRIC_AUTH_FAILURES);
                            }

                            return IngressDecision::Close(
                                "unauthorized: permission denied".to_string(),
                            );
                        }
                    } else {
                        warn!(
                            session_id = session_id,
                            "Ingress: missing session actor for authorization"
                        );
                        return IngressDecision::Close(
                            "unauthorized: session actor missing".to_string(),
                        );
                    }

                    let route = Self::inbound_route_for_domain_cached(domain).clone();
                    let addr = crate::runtime::routing::RouteAddress::new(route_family, route);
                    let dispatch_payload = if should_notify_handler && notify_frame.is_none() {
                        message_payload.as_ref().unwrap().clone()
                    } else {
                        message_payload.take().unwrap()
                    };
                    let ctx = crate::protocol::frame_context::FrameContext::new(
                        session_id,
                        crate::protocol::frame::ChannelId::Pub,
                        msg_type,
                        dispatch_payload,
                        route_family,
                    );
                    // Set source to the session's inbox so domain sinks can route responses back
                    let source = crate::runtime::routing::RouteAddress::new(
                        route_family,
                        self.cached_session_inbox_route(session_id),
                    );
                    let envelope =
                        crate::runtime::envelope::Envelope::from_route(source, addr, ctx);
                    debug!(
                        session_id = session_id,
                        domain = domain,
                        msg_type = msg_type.as_u16(),
                        route = %envelope.destination(),
                        source = ?envelope.source(),
                        "Ingress: routing envelope to domain"
                    );
                    let dispatch_start = Instant::now();
                    let dispatch_result = router.route_to_domain(domain, envelope);
                    if let Ok(collector) =
                        std::panic::catch_unwind(crate::boot::observability::metrics)
                    {
                        collector.histogram_observe_us(
                            obs::METRIC_INGRESS_DOMAIN_DISPATCH_LATENCY,
                            dispatch_start.elapsed().as_micros() as u64,
                        );
                    }

                    match dispatch_result {
                        Ok(()) => {}
                        Err(crate::runtime::router::RouteError::DeliveryFailed(
                            _,
                            crate::runtime::router::DeliveryError::MailboxFull { .. }
                            | crate::runtime::router::DeliveryError::HighLaneFull { .. },
                        )) => {
                            warn!(
                                session_id = session_id,
                                domain = domain,
                                "Ingress: domain dispatch backpressure"
                            );
                            return IngressDecision::Backpressure;
                        }
                        Err(e) => {
                            error!(
                                session_id = session_id,
                                domain = domain,
                                error = %e,
                                "Ingress: router.route failed for domain dispatch"
                            );
                            return IngressDecision::Close(format!("route delivery failed: {}", e));
                        }
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
                Ok(()) => {
                    tracing::debug!(
                        session_id = session_id,
                        route_family = route_family.id(),
                        "Ingress: dispatched cleanup to KV, Notice, RPC, Stream, Schedule, Lease, and Queue domains"
                    );
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
            100..=108 => {
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
            500..=599 => match crate::protocol::notice_codec::parse_request(
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
                Ok(crate::domains::rpc::protocol::RpcMessage::Subscribe { worker_addr }) => {
                    Ok(Some(worker_addr.route().clone()))
                }
                Ok(crate::domains::rpc::protocol::RpcMessage::Unsubscribe { worker_addr }) => {
                    Ok(Some(worker_addr.route().clone()))
                }
                Ok(_) => Ok(None),
                Err(e) => Err(e),
            },
            200..=299 => {
                let mt = msg_type.as_u16();
                if mt == crate::protocol::queue_codec::msg_type::SUBSCRIBE {
                    match crate::protocol::queue_codec::parse_subscribe(
                        session_info.route_family,
                        payload.as_ref(),
                        session_info.session_id,
                        crate::runtime::routing::RouteAddress::new(
                            session_info.route_family,
                            Route::new(""),
                        ),
                    ) {
                        Ok(crate::domains::queue::QueueMessage::Subscribe { pattern, .. }) => Ok(
                            Some(Self::canonicalize_domain_route("queue", pattern.clone())),
                        ),
                        Err(e) => Err(e),
                        Ok(_) => Err("parse_subscribe returned unexpected variant".to_string()),
                    }
                } else if mt == crate::protocol::queue_codec::msg_type::UNSUBSCRIBE {
                    match crate::protocol::queue_codec::parse_unsubscribe(
                        session_info.route_family,
                        payload.as_ref(),
                        session_info.session_id,
                        crate::runtime::routing::RouteAddress::new(
                            session_info.route_family,
                            Route::new(""),
                        ),
                    ) {
                        Ok(crate::domains::queue::QueueMessage::Unsubscribe {
                            pattern, ..
                        }) => Ok(Some(Self::canonicalize_domain_route(
                            "queue",
                            pattern.clone(),
                        ))),
                        Err(e) => Err(e),
                        Ok(_) => Err("parse_unsubscribe returned unexpected variant".to_string()),
                    }
                } else {
                    match crate::protocol::queue_codec::parse_request(
                        mt,
                        session_info.route_family,
                        payload.as_ref(),
                    ) {
                        Ok(crate::domains::queue::QueueMessage::Send { route, .. }) => Ok(Some(
                            Self::canonicalize_domain_route("queue", route.clone()),
                        )),
                        Ok(crate::domains::queue::QueueMessage::Receive { route, .. }) => Ok(Some(
                            Self::canonicalize_domain_route("queue", route.clone()),
                        )),
                        Ok(crate::domains::queue::QueueMessage::Extend { route, .. }) => Ok(Some(
                            Self::canonicalize_domain_route("queue", route.clone()),
                        )),
                        Ok(crate::domains::queue::QueueMessage::Ack { route, .. }) => Ok(Some(
                            Self::canonicalize_domain_route("queue", route.clone()),
                        )),
                        Ok(_) => Ok(None),
                        Err(e) => Err(e),
                    }
                }
            }
            400..=499 => {
                match crate::protocol::lease_codec::parse_request(
                    &ctx,
                    payload.as_ref(),
                    session_info.route_family,
                ) {
                    Ok(crate::domains::lease::protocol::LeaseMessage::Acquire {
                        route, ..
                    }) => Ok(Some(route.clone())),
                    Ok(crate::domains::lease::protocol::LeaseMessage::Extend { route, .. }) => {
                        Ok(Some(route.clone()))
                    }
                    Ok(crate::domains::lease::protocol::LeaseMessage::Release {
                        route, ..
                    }) => Ok(Some(route.clone())),
                    Ok(crate::domains::lease::protocol::LeaseMessage::Query { route, .. }) => {
                        Ok(Some(route.clone()))
                    }
                    Ok(_) => Ok(None),
                    Err(e) => Err(e),
                }
            }
            600..=699 => match crate::protocol::stream_codec::parse_request(
                &ctx,
                payload.as_ref(),
                session_info.route_family,
                crate::session::SessionId(session_info.session_id),
                crate::runtime::routing::RouteAddress::new(
                    session_info.route_family,
                    Route::new(""),
                ),
            ) {
                Ok(crate::domains::stream::protocol::StreamMessage::Begin { route, .. }) => Ok(
                    Some(Self::canonicalize_domain_route("stream", route.clone())),
                ),
                Ok(crate::domains::stream::protocol::StreamMessage::Read { route, .. }) => Ok(
                    Some(Self::canonicalize_domain_route("stream", route.clone())),
                ),
                Ok(crate::domains::stream::protocol::StreamMessage::Last { route, .. }) => Ok(
                    Some(Self::canonicalize_domain_route("stream", route.clone())),
                ),
                Ok(crate::domains::stream::protocol::StreamMessage::GetMetadata {
                    route, ..
                }) => Ok(Some(Self::canonicalize_domain_route(
                    "stream",
                    route.clone(),
                ))),
                Ok(crate::domains::stream::protocol::StreamMessage::Subscribe {
                    pattern, ..
                }) => Ok(Some(Self::canonicalize_domain_route(
                    "stream",
                    pattern.clone(),
                ))),
                Ok(crate::domains::stream::protocol::StreamMessage::Unsubscribe {
                    pattern,
                    ..
                }) => Ok(Some(Self::canonicalize_domain_route(
                    "stream",
                    pattern.clone(),
                ))),
                Ok(_) => Ok(None),
                Err(e) => Err(e),
            },
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
                    Ok(crate::domains::schedule::ScheduleMessage::Subscribe {
                        pattern, ..
                    }) => Ok(Some(Self::canonicalize_domain_route(
                        "schedule",
                        pattern.clone(),
                    ))),
                    Ok(crate::domains::schedule::ScheduleMessage::Unsubscribe {
                        pattern, ..
                    }) => Ok(Some(Self::canonicalize_domain_route(
                        "schedule",
                        pattern.clone(),
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
    use crate::auth::Access;
    use crate::protocol::frame::ChannelId;
    use crate::runtime::routing::Route;
    use crate::session::{SessionInfo, SessionMetadata, SessionPermissions, TransportKind};
    use bytes::Bytes;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

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
}
