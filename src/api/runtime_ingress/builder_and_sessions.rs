use super::{RuntimeIngress, SessionEvent, SessionInfo};
use dashmap::DashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

impl RuntimeIngress {
    /// Create a new ingress implementation
    #[must_use]
    pub fn new(auth_required: bool) -> Self {
        let registry = super::session_registry::SessionRegistry {
            accepting_sessions: Arc::new(AtomicBool::new(true)),
            sessions: Arc::new(DashMap::new()),
            session_actors: Arc::new(DashMap::new()),
            session_inbox_routes: Arc::new(DashMap::new()),
            closing_sessions: Arc::new(DashMap::new()),
            event_handler: None,
            admin_read_model: None,
            auth_required,
        };
        let authenticator = super::session_authenticator::SessionAuthenticator {
            registry: registry.clone(),
            route_families: Arc::new(std::iter::once(1).collect()),
            auth_required,
            auth_config: None,
            auth_claims_config: crate::auth::AuthClaimsConfig::default(),
            route_family_resolver: crate::auth::RouteFamilyResolverConfig::default(),
            connect_diagnostics_budget: Arc::default(),
        };
        let cleanup = super::session_cleanup_coordinator::SessionCleanupCoordinator {
            pending_session_cleanups: Arc::new(DashMap::new()),
            cleanup_wake: Arc::new(tokio::sync::Notify::new()),
            cleanup_worker_started: Arc::new(AtomicBool::new(false)),
            cleanup_shutdown: Arc::new(AtomicBool::new(false)),
            cleanup_permits: Arc::new(tokio::sync::Semaphore::new(
                super::session_cleanup_coordinator::SESSION_CLEANUP_CONCURRENCY,
            )),
            router: None,
        };
        let dispatcher = super::domain_frame_dispatcher::DomainFrameDispatcher {
            router: None,
            registry: registry.clone(),
            auth_required,
        };
        Self {
            registry,
            authenticator,
            cleanup,
            dispatcher,
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
        let handler = Arc::new(handler);
        self.registry.event_handler = Some(handler.clone());
        self.authenticator.registry.event_handler = Some(handler.clone());
        self.dispatcher.registry.event_handler = Some(handler);
        self
    }

    /// Attach a router reference for dispatching frames directly from ingress
    #[must_use]
    pub fn with_router(mut self, router: Arc<crate::runtime::Router>) -> Self {
        self.cleanup.router = Some(router.clone());
        self.dispatcher.router = Some(router);
        self
    }

    #[must_use]
    pub fn with_admin_read_model(
        mut self,
        admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
    ) -> Self {
        self.registry.admin_read_model = Some(admin_read_model.clone());
        self.authenticator.registry.admin_read_model = Some(admin_read_model.clone());
        self.dispatcher.registry.admin_read_model = Some(admin_read_model);
        self
    }

    #[must_use]
    pub fn with_auth_config(mut self, auth_config: crate::auth::AuthConfig) -> Self {
        self.authenticator.auth_config = Some(auth_config);
        self
    }

    #[must_use]
    pub fn with_auth_claims_config(
        mut self,
        auth_claims_config: crate::auth::AuthClaimsConfig,
    ) -> Self {
        self.authenticator.auth_claims_config = auth_claims_config;
        self
    }

    #[must_use]
    pub fn with_route_family_resolver(
        mut self,
        route_family_resolver: crate::auth::RouteFamilyResolverConfig,
    ) -> Self {
        self.authenticator.route_family_resolver = route_family_resolver;
        self
    }

    #[cfg(test)]
    pub(super) fn with_route_family_map(mut self, mappings: &[(&str, u32)]) -> Self {
        self.authenticator.route_family_resolver =
            crate::auth::RouteFamilyResolverConfig::from_mappings(
                crate::auth::DEFAULT_ROUTE_FAMILY_CLAIM,
                mappings
                    .iter()
                    .map(|(identity, family)| (*identity, *family)),
            );
        self
    }

    #[must_use]
    pub fn with_route_families(mut self, route_families: &[u32]) -> Self {
        self.authenticator.route_families = Arc::new(route_families.iter().copied().collect());
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

    /// Drain the dedicated cleanup worker before runtime teardown.
    pub async fn drain_session_cleanups(&self) {
        self.drain_cleanup_tickets().await;
    }
}
