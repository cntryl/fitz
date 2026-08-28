use super::{
    BrokerLifecycleState, Runtime, DEFAULT_DRAIN_CLOSE_REASON, DEFAULT_DRAIN_GRACE_SECONDS,
    LIFECYCLE_DRAINING, LIFECYCLE_RUNNING, LIFECYCLE_SHUTTING_DOWN,
};
use crate::api::runtime_ingress::RuntimeIngress;
use crate::boot::domains::DomainHandles;
use crate::runtime::Router;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

fn current_epoch_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX))
        .try_into()
        .unwrap_or(u64::MAX)
}

fn u64_to_f64(value: u64) -> f64 {
    let high = u32::try_from(value >> 32).unwrap_or(u32::MAX);
    let low = u32::try_from(value & u64::from(u32::MAX)).unwrap_or(u32::MAX);
    f64::from(high) * 4_294_967_296.0 + f64::from(low)
}

impl Runtime {
    /// Create a new runtime statistics tracker.
    #[must_use]
    pub fn new(router: Arc<Router>) -> Self {
        Self::with_admin_read_model(
            router,
            crate::control::admin::read_model::AdminReadModel::new(),
        )
    }

    pub fn with_admin_read_model(
        router: Arc<Router>,
        admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
    ) -> Self {
        let now = std::time::Instant::now();
        Self {
            router,
            startup_time: now,
            storage_ready: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            domains_ready: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            auth_config_ready: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            startup_complete: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            lifecycle_state: Arc::new(std::sync::atomic::AtomicU8::new(LIFECYCLE_RUNNING)),
            drain_grace_seconds: Arc::new(std::sync::atomic::AtomicU64::new(
                DEFAULT_DRAIN_GRACE_SECONDS,
            )),
            drain_started_epoch_ms: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            drain_deadline_epoch_ms: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            drain_close_reason: Arc::new(parking_lot::RwLock::new(
                DEFAULT_DRAIN_CLOSE_REASON.to_string(),
            )),
            connection_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            session_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            family_actor_shards: Arc::new(std::sync::atomic::AtomicUsize::new(1)),
            family_actor_ingress: Arc::new(parking_lot::RwLock::new(None)),
            fatal_domain_failure: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            messages_received: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            messages_sent: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            admin_auth: Arc::new(crate::api::admin::auth::AdminAuth::from_env()),
            admin_read_model,
            ingress: Arc::new(parking_lot::RwLock::new(None)),
            domains: Arc::new(parking_lot::RwLock::new(None)),
            auth_config: Arc::new(parking_lot::RwLock::new(crate::auth::AuthConfig::Disabled)),
            assume_external_tls: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            admin_blocking_slots: Arc::new(std::sync::atomic::AtomicUsize::new(
                super::ADMIN_BLOCKING_EXECUTOR_CAPACITY,
            )),
        }
    }

    #[must_use]
    pub fn admin_auth(&self) -> Arc<crate::api::admin::auth::AdminAuth> {
        self.admin_auth.clone()
    }

    #[must_use]
    pub fn admin_read_model(&self) -> Arc<crate::control::admin::read_model::AdminReadModel> {
        self.admin_read_model.clone()
    }

    pub fn attach_ingress(&self, ingress: Arc<RuntimeIngress>) {
        *self.ingress.write() = Some(ingress);
    }

    ///
    /// # Panics
    ///
    /// Panics if boot configuration validation allowed an invalid or empty
    /// route-family set through to runtime initialization.
    pub fn configure_route_families(&self, route_families: &[u32]) {
        self.admin_auth
            .set_provisioned_route_families(route_families);
        let families = route_families
            .iter()
            .copied()
            .map(crate::runtime::routing::RouteFamily::new)
            .collect::<Vec<_>>();
        let pool = crate::runtime::FamilyActorPool::<()>::new(&families)
            .expect("validated route families must provision a family actor pool");
        self.family_actor_shards
            .store(pool.shard_count(), Ordering::Release);
        *self.family_actor_ingress.write() = Some(pool.ingress());
    }

    #[must_use]
    pub fn family_actor_shard_count(&self) -> usize {
        self.family_actor_shards.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn family_actor_shard_for(
        &self,
        family: crate::runtime::routing::RouteFamily,
    ) -> Option<usize> {
        self.family_actor_ingress
            .read()
            .as_ref()
            .and_then(|ingress| ingress.shard_for_family(family))
    }

    pub fn attach_domains(&self, domains: Arc<DomainHandles>) {
        *self.domains.write() = Some(domains);
    }

    #[must_use]
    ///
    /// # Panics
    ///
    /// Panics if domain handles have not been attached yet.
    pub fn domains(&self) -> Arc<DomainHandles> {
        self.domains
            .read()
            .clone()
            .expect("domain handles must be attached before health monitoring")
    }

    #[cfg(test)]
    /// Build a runtime with the standard test domain fixture attached.
    ///
    /// # Panics
    ///
    /// Panics if the in-memory test domain fixture cannot be initialized.
    #[must_use]
    pub fn with_test_domains_for_tests() -> Self {
        let router = Arc::new(Router::new());
        let runtime = Self::new(router.clone());
        let store = crate::testkit::midge::create_test_engine_with_cfs(vec![1, 2, 3, 4, 5, 6, 7]);
        let domains = crate::boot::domains::setup(
            &router,
            &store,
            &runtime.admin_read_model(),
            &crate::boot::domains::DomainSetupOptions {
                route_families: vec![1, 2, 3, 4, 5, 6, 7],
                schedule_write_options: cntryl_midge::WriteOptions::best_effort(),
                queue_write_options: cntryl_midge::WriteOptions::best_effort(),
                queue_fast_flush_interval: Some(Duration::from_millis(100)),
                request_sync_write_options: cntryl_midge::WriteOptions::sync(),
                request_buffered_write_options: cntryl_midge::WriteOptions::buffered(),
                rpc_request_timeout: None,
                stream_storage_layout: crate::domains::stream::StreamStorageLayout::default(),
                kv_idle_transaction_ttl: Duration::from_mins(5),
                schedule_preload_timeout:
                    crate::domains::schedule::sink::DEFAULT_SCHEDULE_PRELOAD_TIMEOUT,
            },
        )
        .expect("setup domains");
        runtime.attach_domains(domains);
        runtime
    }

    #[cfg(test)]
    pub fn mark_kv_domain_permanently_failed_for_tests(&self) {
        if let Some(domains) = self.domains.read().as_ref() {
            domains.mark_kv_permanently_failed_for_tests();
        }
    }

    #[cfg(test)]
    pub fn panic_all_domain_actors_for_tests(&self) {
        if let Some(domains) = self.domains.read().as_ref() {
            domains.panic_all_domain_actors_for_tests();
        }
    }

    pub(crate) fn panic_notice_actor_for_failpoint(&self) {
        if let Some(domains) = self.domains.read().as_ref() {
            domains.panic_notice_actor_for_failpoint();
        }
    }

    pub(crate) fn panic_queue_actor_for_failpoint(&self) {
        if let Some(domains) = self.domains.read().as_ref() {
            domains.panic_queue_actor_for_failpoint();
        }
    }

    #[must_use]
    pub fn detach_domains(&self) -> Option<Arc<DomainHandles>> {
        self.domains.write().take()
    }

    #[must_use]
    pub fn detach_ingress(&self) -> Option<Arc<RuntimeIngress>> {
        self.ingress.write().take()
    }

    pub fn attach_auth_config(&self, auth_config: crate::auth::AuthConfig) {
        *self.auth_config.write() = auth_config;
    }

    #[must_use]
    pub fn auth_config(&self) -> crate::auth::AuthConfig {
        self.auth_config.read().clone()
    }

    pub fn set_assume_external_tls(&self, assume_external_tls: bool) {
        self.assume_external_tls
            .store(assume_external_tls, Ordering::SeqCst);
    }

    pub(crate) fn try_acquire_admin_blocking_permit(&self) -> Option<super::AdminBlockingPermit> {
        let mut available = self.admin_blocking_slots.load(Ordering::Acquire);
        loop {
            if available == 0 {
                return None;
            }
            match self.admin_blocking_slots.compare_exchange_weak(
                available,
                available - 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Some(super::AdminBlockingPermit {
                        slots: self.admin_blocking_slots.clone(),
                    });
                }
                Err(observed) => available = observed,
            }
        }
    }

    #[must_use]
    pub fn assume_external_tls(&self) -> bool {
        self.assume_external_tls.load(Ordering::SeqCst)
    }

    pub fn configure_drain(&self, grace_seconds: u64, close_reason: String) {
        self.drain_grace_seconds
            .store(grace_seconds, Ordering::SeqCst);
        *self.drain_close_reason.write() = close_reason;
    }

    pub fn mark_storage_ready(&self) {
        self.storage_ready.store(1, Ordering::SeqCst);
    }

    pub fn mark_storage_unavailable(&self) {
        self.storage_ready.store(0, Ordering::SeqCst);
    }

    #[must_use]
    pub fn is_storage_ready(&self) -> bool {
        self.storage_ready.load(Ordering::SeqCst) == 1
    }

    pub fn mark_domains_ready(&self) {
        self.domains_ready.store(1, Ordering::SeqCst);
    }

    #[must_use]
    pub fn are_domains_ready(&self) -> bool {
        self.domains_ready.load(Ordering::SeqCst) == 1
    }

    #[must_use]
    pub fn domain_health_snapshots(&self) -> Vec<crate::boot::domains::DomainHealthSnapshot> {
        self.domains
            .read()
            .as_ref()
            .map_or_else(Vec::new, |domains| domains.health_snapshots())
    }

    #[must_use]
    pub fn has_permanently_failed_domain(&self) -> bool {
        self.domains.read().as_ref().is_some_and(|domains| {
            self.are_domains_ready() && domains.has_permanently_failed_domain()
        })
    }

    pub fn mark_fatal_domain_failure(&self) {
        self.fatal_domain_failure.store(true, Ordering::SeqCst);
    }

    #[must_use]
    pub fn has_fatal_domain_failure(&self) -> bool {
        self.fatal_domain_failure.load(Ordering::SeqCst)
    }

    pub fn mark_auth_config_ready(&self) {
        self.auth_config_ready.store(1, Ordering::SeqCst);
    }

    #[must_use]
    pub fn is_auth_config_ready(&self) -> bool {
        self.auth_config_ready.load(Ordering::SeqCst) == 1
    }

    pub fn mark_startup_complete(&self) {
        self.startup_complete.store(1, Ordering::SeqCst);
    }

    #[must_use]
    pub fn is_startup_complete(&self) -> bool {
        self.startup_complete.load(Ordering::SeqCst) == 1
    }

    #[must_use]
    pub fn lifecycle_state(&self) -> BrokerLifecycleState {
        BrokerLifecycleState::from_u8(self.lifecycle_state.load(Ordering::SeqCst))
    }

    pub fn begin_drain(&self) {
        let previous = self
            .lifecycle_state
            .compare_exchange(
                LIFECYCLE_RUNNING,
                LIFECYCLE_DRAINING,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .unwrap_or_else(|state| state);

        if previous == LIFECYCLE_RUNNING {
            let started = current_epoch_ms();
            let grace_ms = self.drain_grace_seconds() * 1_000;
            self.drain_started_epoch_ms.store(started, Ordering::SeqCst);
            self.drain_deadline_epoch_ms
                .store(started.saturating_add(grace_ms), Ordering::SeqCst);
        }
    }

    pub fn begin_shutdown(&self) {
        self.lifecycle_state
            .store(LIFECYCLE_SHUTTING_DOWN, Ordering::SeqCst);
    }

    #[must_use]
    pub fn is_draining(&self) -> bool {
        self.lifecycle_state() == BrokerLifecycleState::Draining
    }

    #[must_use]
    pub fn is_shutting_down(&self) -> bool {
        self.lifecycle_state() == BrokerLifecycleState::ShuttingDown
    }

    #[must_use]
    pub fn is_accepting_traffic(&self) -> bool {
        self.lifecycle_state() == BrokerLifecycleState::Running
    }

    #[must_use]
    pub fn is_ready_for_traffic(&self) -> bool {
        self.is_storage_ready()
            && self.are_domains_ready()
            && self.is_auth_config_ready()
            && self.is_startup_complete()
            && self.is_accepting_traffic()
            && !self.has_permanently_failed_domain()
    }

    #[must_use]
    pub fn traffic_status(&self) -> &'static str {
        match self.lifecycle_state() {
            BrokerLifecycleState::Running => "ok",
            BrokerLifecycleState::Draining => "draining",
            BrokerLifecycleState::ShuttingDown => "not_ready",
        }
    }

    #[must_use]
    pub fn drain_grace_seconds(&self) -> u64 {
        self.drain_grace_seconds.load(Ordering::SeqCst)
    }

    #[must_use]
    pub fn drain_grace(&self) -> Duration {
        Duration::from_secs(self.drain_grace_seconds())
    }

    #[must_use]
    pub fn drain_close_reason(&self) -> String {
        self.drain_close_reason.read().clone()
    }

    #[must_use]
    pub fn drain_started_epoch_ms(&self) -> Option<u64> {
        let value = self.drain_started_epoch_ms.load(Ordering::SeqCst);
        (value != 0).then_some(value)
    }

    #[must_use]
    pub fn drain_deadline_epoch_ms(&self) -> Option<u64> {
        let value = self.drain_deadline_epoch_ms.load(Ordering::SeqCst);
        (value != 0).then_some(value)
    }

    #[must_use]
    pub fn remaining_drain_grace(&self) -> Duration {
        let Some(deadline) = self.drain_deadline_epoch_ms() else {
            return self.drain_grace();
        };
        let now = current_epoch_ms();
        if deadline <= now {
            Duration::ZERO
        } else {
            Duration::from_millis(deadline - now)
        }
    }

    #[must_use]
    pub fn startup_duration(&self) -> Duration {
        self.startup_time.elapsed()
    }

    #[must_use]
    pub fn uptime(&self) -> Duration {
        self.startup_time.elapsed()
    }

    #[must_use]
    pub fn uptime_seconds(&self) -> u64 {
        self.uptime().as_secs()
    }

    #[must_use]
    pub fn connection_count(&self) -> usize {
        self.connection_count.load(Ordering::Relaxed)
    }

    pub fn increment_connections(&self) {
        self.connection_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn decrement_connections(&self) {
        self.connection_count.fetch_sub(1, Ordering::Relaxed);
    }

    #[must_use]
    pub fn session_count(&self) -> usize {
        self.ingress.read().as_ref().map_or_else(
            || self.session_count.load(Ordering::Relaxed),
            |ingress| ingress.session_count(),
        )
    }

    pub fn increment_sessions(&self) {
        self.session_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn decrement_sessions(&self) {
        self.session_count.fetch_sub(1, Ordering::Relaxed);
    }

    #[must_use]
    pub fn messages_received(&self) -> u64 {
        self.messages_received.load(Ordering::Relaxed)
    }

    pub fn increment_messages_received(&self) {
        self.messages_received.fetch_add(1, Ordering::Relaxed);
    }

    #[must_use]
    pub fn messages_sent(&self) -> u64 {
        self.messages_sent.load(Ordering::Relaxed)
    }

    pub fn increment_messages_sent(&self) {
        self.messages_sent.fetch_add(1, Ordering::Relaxed);
    }

    #[must_use]
    pub fn registered_route_count(&self) -> usize {
        self.router.len()
    }

    #[must_use]
    pub fn router(&self) -> Arc<Router> {
        self.router.clone()
    }

    #[must_use]
    pub fn authenticated_session_count(&self) -> usize {
        self.ingress.read().as_ref().map_or(0, |ingress| {
            ingress
                .active_sessions()
                .into_iter()
                .filter(|session| session.authenticated)
                .count()
        })
    }

    #[must_use]
    pub fn active_realms(&self) -> Vec<String> {
        Vec::new()
    }

    #[must_use]
    pub fn messages_per_second(&self) -> f64 {
        let uptime_secs = u64_to_f64(self.uptime_seconds());
        if uptime_secs < 0.001 {
            return 0.0;
        }
        let total_messages = self.messages_received() + self.messages_sent();
        u64_to_f64(total_messages) / uptime_secs
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::routing::RouteFamily;

    #[test]
    fn should_track_storage_readiness() {
        // Arrange
        let router = Arc::new(Router::new());
        let runtime = Runtime::new(router);

        // Act
        assert!(!runtime.is_storage_ready());
        runtime.mark_storage_ready();

        // Assert
        assert!(runtime.is_storage_ready());
    }

    #[test]
    fn should_withdraw_traffic_readiness_when_storage_becomes_unavailable() {
        // Arrange
        let router = Arc::new(Router::new());
        let runtime = Runtime::new(router);
        runtime.mark_storage_ready();
        runtime.mark_domains_ready();
        runtime.mark_auth_config_ready();
        runtime.mark_startup_complete();

        // Act
        runtime.mark_storage_unavailable();

        // Assert
        assert!(!runtime.is_storage_ready());
        assert!(!runtime.is_ready_for_traffic());
    }

    #[test]
    fn should_track_domains_readiness() {
        // Arrange
        let router = Arc::new(Router::new());
        let runtime = Runtime::new(router);

        // Act
        assert!(!runtime.are_domains_ready());
        runtime.mark_domains_ready();

        // Assert
        assert!(runtime.are_domains_ready());
    }

    #[test]
    fn should_track_auth_configuration_readiness() {
        // Arrange
        let router = Arc::new(Router::new());
        let runtime = Runtime::new(router);

        // Act
        assert!(!runtime.is_auth_config_ready());
        runtime.mark_auth_config_ready();

        // Assert
        assert!(runtime.is_auth_config_ready());
    }

    #[test]
    fn should_track_startup_completion() {
        // Arrange
        let router = Arc::new(Router::new());
        let runtime = Runtime::new(router);

        // Act
        assert!(!runtime.is_startup_complete());
        runtime.mark_startup_complete();

        // Assert
        assert!(runtime.is_startup_complete());
    }

    #[test]
    fn should_track_connections() {
        // Arrange
        let router = Arc::new(Router::new());
        let runtime = Runtime::new(router);

        // Act
        assert_eq!(runtime.connection_count(), 0);
        runtime.increment_connections();
        assert_eq!(runtime.connection_count(), 1);
        runtime.decrement_connections();

        // Assert
        assert_eq!(runtime.connection_count(), 0);
    }

    #[test]
    fn should_track_sessions() {
        // Arrange
        let router = Arc::new(Router::new());
        let runtime = Runtime::new(router);

        // Act
        assert_eq!(runtime.session_count(), 0);
        runtime.increment_sessions();
        assert_eq!(runtime.session_count(), 1);
        runtime.decrement_sessions();

        // Assert
        assert_eq!(runtime.session_count(), 0);
    }

    #[test]
    fn should_track_messages() {
        // Arrange
        let router = Arc::new(Router::new());
        let runtime = Runtime::new(router);
        assert_eq!(runtime.messages_received(), 0);
        assert_eq!(runtime.messages_sent(), 0);

        // Act
        runtime.increment_messages_received();
        runtime.increment_messages_sent();

        // Assert
        assert_eq!(runtime.messages_received(), 1);
        assert_eq!(runtime.messages_sent(), 1);
    }

    #[test]
    fn should_calculate_messages_per_second() {
        // Arrange
        let router = Arc::new(Router::new());
        let runtime = Runtime::new(router);

        assert!(runtime.messages_per_second().abs() < f64::EPSILON);

        // Act
        runtime.increment_messages_received();
        runtime.increment_messages_sent();
        std::thread::sleep(std::time::Duration::from_millis(10));

        // Assert
        let mps = runtime.messages_per_second();
        assert!(mps >= 0.0);
    }

    #[test]
    fn should_report_uptime() {
        // Arrange
        let router = Arc::new(Router::new());
        let runtime = Runtime::new(router);
        std::thread::sleep(std::time::Duration::from_millis(50));

        // Act
        let uptime = runtime.uptime();

        // Assert
        assert!(uptime.as_millis() >= 50);
        let _uptime_secs = runtime.uptime_seconds();
    }

    #[test]
    fn should_report_ready_for_traffic_given_all_readiness_checks_pass() {
        // Arrange
        let router = Arc::new(Router::new());
        let runtime = Runtime::new(router);

        // Act
        runtime.mark_storage_ready();
        runtime.mark_domains_ready();
        runtime.mark_auth_config_ready();
        runtime.mark_startup_complete();

        // Assert
        assert!(runtime.is_ready_for_traffic());
    }

    #[test]
    fn should_stop_reporting_ready_for_traffic_when_shutdown_begins() {
        // Arrange
        let router = Arc::new(Router::new());
        let runtime = Runtime::new(router);
        runtime.mark_storage_ready();
        runtime.mark_domains_ready();
        runtime.mark_auth_config_ready();
        runtime.mark_startup_complete();

        // Act
        runtime.begin_shutdown();

        // Assert
        assert!(runtime.is_shutting_down());
        assert!(!runtime.is_ready_for_traffic());
    }

    #[test]
    fn should_stop_accepting_traffic_when_drain_begins() {
        // Arrange
        let router = Arc::new(Router::new());
        let runtime = Runtime::new(router);
        runtime.mark_storage_ready();
        runtime.mark_domains_ready();
        runtime.mark_auth_config_ready();
        runtime.mark_startup_complete();

        // Act
        runtime.begin_drain();

        // Assert
        assert!(runtime.is_draining());
        assert!(!runtime.is_shutting_down());
        assert!(!runtime.is_accepting_traffic());
        assert!(!runtime.is_ready_for_traffic());
        assert_eq!(runtime.traffic_status(), "draining");
        assert_eq!(runtime.lifecycle_state().as_str(), "draining");
        assert!(runtime.drain_started_epoch_ms().is_some());
        assert!(runtime.drain_deadline_epoch_ms().is_some());
    }

    #[test]
    fn should_keep_original_drain_deadline_given_duplicate_drain_request() {
        // Arrange
        let router = Arc::new(Router::new());
        let runtime = Runtime::new(router);
        runtime.configure_drain(5, "first drain".to_string());
        runtime.begin_drain();
        let first_deadline = runtime.drain_deadline_epoch_ms();

        // Act
        runtime.configure_drain(10, "second drain".to_string());
        runtime.begin_drain();

        // Assert
        assert_eq!(runtime.drain_deadline_epoch_ms(), first_deadline);
        assert_eq!(runtime.drain_close_reason(), "second drain");
    }

    #[test]
    fn should_provision_family_affinity_for_configured_route_families_only() {
        // Arrange
        let router = Arc::new(Router::new());
        let runtime = Runtime::new(router);

        // Act
        runtime.configure_route_families(&[1, 3]);

        // Assert
        assert_eq!(runtime.family_actor_shard_count(), 2);
        assert!(runtime
            .family_actor_shard_for(RouteFamily::new(1))
            .is_some());
        assert!(runtime
            .family_actor_shard_for(RouteFamily::new(3))
            .is_some());
        assert_eq!(runtime.family_actor_shard_for(RouteFamily::new(2)), None);
    }
}
