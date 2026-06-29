use super::*;

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
        admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
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
    pub(super) fn with_route_family_map(mut self, mappings: &[(&str, u32)]) -> Self {
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

    pub(super) fn finalize_session_close(&self, session_id: u64) {
        self.sessions.remove(&session_id);
        self.session_actors.remove(&session_id);
        self.session_inbox_routes.remove(&session_id);
        if let Some(admin_read_model) = &self.admin_read_model {
            admin_read_model.record_session_close(session_id);
        }
    }

    pub(super) fn record_cleanup_failure(
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

    pub(super) async fn retry_pending_session_cleanups(&self) {
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
}
