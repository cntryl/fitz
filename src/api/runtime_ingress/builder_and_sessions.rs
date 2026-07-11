use super::{DispatchDomain, RuntimeIngress, SessionEvent, SessionInfo};
use dashmap::DashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

impl RuntimeIngress {
    /// Create a new ingress implementation
    #[must_use]
    pub fn new(auth_required: bool) -> Self {
        Self {
            sessions: Arc::new(DashMap::new()),
            session_actors: Arc::new(DashMap::new()),
            session_inbox_routes: Arc::new(DashMap::new()),
            pending_session_cleanups: Arc::new(DashMap::new()),
            cleanup_wake: Arc::new(tokio::sync::Notify::new()),
            cleanup_worker_started: Arc::new(AtomicBool::new(false)),
            cleanup_shutdown: Arc::new(AtomicBool::new(false)),
            closed_sessions: Arc::new(DashMap::new()),
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
    #[must_use]
    pub fn with_event_handler<F>(mut self, handler: F) -> Self
    where
        F: Fn(SessionEvent) + Send + Sync + 'static,
    {
        self.event_handler = Some(Arc::new(handler));
        self
    }

    /// Attach a router reference for dispatching frames directly from ingress
    #[must_use]
    pub fn with_router(mut self, router: Arc<crate::runtime::Router>) -> Self {
        self.router = Some(router);
        self
    }

    #[must_use]
    pub fn with_admin_read_model(
        mut self,
        admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
    ) -> Self {
        self.admin_read_model = Some(admin_read_model);
        self
    }

    #[must_use]
    pub fn with_auth_config(mut self, auth_config: crate::auth::AuthConfig) -> Self {
        self.auth_config = Some(auth_config);
        self
    }

    #[must_use]
    pub fn with_auth_claims_config(
        mut self,
        auth_claims_config: crate::auth::AuthClaimsConfig,
    ) -> Self {
        self.auth_claims_config = auth_claims_config;
        self
    }

    #[must_use]
    pub fn with_route_family_resolver(
        mut self,
        route_family_resolver: crate::auth::RouteFamilyResolverConfig,
    ) -> Self {
        self.route_family_resolver = route_family_resolver;
        self
    }

    #[cfg(test)]
    pub(super) fn with_route_family_map(mut self, mappings: &[(&str, u32)]) -> Self {
        self.route_family_resolver = crate::auth::RouteFamilyResolverConfig::from_mappings(
            crate::auth::DEFAULT_ROUTE_FAMILY_CLAIM,
            mappings
                .iter()
                .map(|(identity, family)| (*identity, *family)),
        );
        self
    }

    #[must_use]
    pub fn with_route_families(mut self, route_families: &[u32]) -> Self {
        self.route_families = Arc::new(route_families.iter().copied().collect());
        self
    }

    /// Get a clone of the session actor for authorization checks
    #[must_use]
    pub fn get_session_actor(
        &self,
        session_id: u64,
    ) -> Option<crate::session::actor::SessionActor> {
        self.session_registry().session_actor(session_id)
    }

    /// Get a session by ID
    #[must_use]
    pub fn get_session(&self, session_id: u64) -> Option<SessionInfo> {
        self.session_registry().session(session_id)
    }

    /// Get all active sessions
    #[must_use]
    pub fn active_sessions(&self) -> Vec<SessionInfo> {
        self.session_registry().active_sessions()
    }

    /// Get session count
    #[must_use]
    pub fn session_count(&self) -> usize {
        self.session_registry().session_count()
    }

    #[allow(dead_code)]
    pub(super) fn finalize_session_close(&self, session_id: u64) {
        self.session_registry().finalize_close(session_id);
    }

    #[allow(dead_code)]
    pub(super) fn record_cleanup_failure(
        &self,
        session_id: u64,
        route_family: crate::runtime::routing::RouteFamily,
        failed_domains: &[DispatchDomain],
        store_retry_ticket: bool,
    ) {
        self.session_cleanup_coordinator().record_failure(
            session_id,
            route_family,
            failed_domains,
            store_retry_ticket,
        );
    }

    #[allow(dead_code)]
    pub(super) fn retry_pending_session_cleanups(&self) {
        self.session_cleanup_coordinator().retry_pending();
    }

    /// Drain the dedicated cleanup worker before runtime teardown.
    pub async fn drain_session_cleanups(&self) {
        self.drain_cleanup_tickets().await;
    }
}
