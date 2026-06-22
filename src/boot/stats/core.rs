use super::{
    BrokerLifecycleState, Runtime, DEFAULT_DRAIN_CLOSE_REASON, DEFAULT_DRAIN_GRACE_SECONDS,
    LIFECYCLE_DRAINING, LIFECYCLE_RUNNING, LIFECYCLE_SHUTTING_DOWN,
};
use crate::boot::domains::DomainHandles;
use crate::runtime::Router;
use crate::session::manager::RuntimeIngress;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

fn current_epoch_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

impl Runtime {
    /// Create a new runtime statistics tracker.
    pub fn new(router: Arc<Router>) -> Self {
        Self::with_admin_read_model(router, crate::api::admin::read_model::AdminReadModel::new())
    }

    pub fn with_admin_read_model(
        router: Arc<Router>,
        admin_read_model: Arc<crate::api::admin::read_model::AdminReadModel>,
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
            messages_received: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            messages_sent: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            admin_auth: Arc::new(crate::api::admin::auth::AdminAuth::from_env()),
            admin_read_model,
            ingress: Arc::new(parking_lot::RwLock::new(None)),
            domains: Arc::new(parking_lot::RwLock::new(None)),
            auth_config: Arc::new(parking_lot::RwLock::new(crate::auth::AuthConfig::Disabled)),
            assume_external_tls: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    pub fn admin_auth(&self) -> Arc<crate::api::admin::auth::AdminAuth> {
        self.admin_auth.clone()
    }

    pub fn admin_read_model(&self) -> Arc<crate::api::admin::read_model::AdminReadModel> {
        self.admin_read_model.clone()
    }

    pub fn attach_ingress(&self, ingress: Arc<RuntimeIngress>) {
        *self.ingress.write() = Some(ingress);
    }

    pub fn attach_domains(&self, domains: Arc<DomainHandles>) {
        *self.domains.write() = Some(domains);
    }

    pub fn detach_domains(&self) -> Option<Arc<DomainHandles>> {
        self.domains.write().take()
    }

    pub fn detach_ingress(&self) -> Option<Arc<RuntimeIngress>> {
        self.ingress.write().take()
    }

    pub fn attach_auth_config(&self, auth_config: crate::auth::AuthConfig) {
        *self.auth_config.write() = auth_config;
    }

    pub fn auth_config(&self) -> crate::auth::AuthConfig {
        self.auth_config.read().clone()
    }

    pub fn set_assume_external_tls(&self, assume_external_tls: bool) {
        self.assume_external_tls
            .store(assume_external_tls, Ordering::SeqCst);
    }

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

    pub fn is_storage_ready(&self) -> bool {
        self.storage_ready.load(Ordering::SeqCst) == 1
    }

    pub fn mark_domains_ready(&self) {
        self.domains_ready.store(1, Ordering::SeqCst);
    }

    pub fn are_domains_ready(&self) -> bool {
        self.domains_ready.load(Ordering::SeqCst) == 1
    }

    pub fn mark_auth_config_ready(&self) {
        self.auth_config_ready.store(1, Ordering::SeqCst);
    }

    pub fn is_auth_config_ready(&self) -> bool {
        self.auth_config_ready.load(Ordering::SeqCst) == 1
    }

    pub fn mark_startup_complete(&self) {
        self.startup_complete.store(1, Ordering::SeqCst);
    }

    pub fn is_startup_complete(&self) -> bool {
        self.startup_complete.load(Ordering::SeqCst) == 1
    }

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

    pub fn is_draining(&self) -> bool {
        self.lifecycle_state() == BrokerLifecycleState::Draining
    }

    pub fn is_shutting_down(&self) -> bool {
        self.lifecycle_state() == BrokerLifecycleState::ShuttingDown
    }

    pub fn is_accepting_traffic(&self) -> bool {
        self.lifecycle_state() == BrokerLifecycleState::Running
    }

    pub fn is_ready_for_traffic(&self) -> bool {
        self.is_storage_ready()
            && self.are_domains_ready()
            && self.is_auth_config_ready()
            && self.is_startup_complete()
            && self.is_accepting_traffic()
    }

    pub fn traffic_status(&self) -> &'static str {
        match self.lifecycle_state() {
            BrokerLifecycleState::Running => "ok",
            BrokerLifecycleState::Draining => "draining",
            BrokerLifecycleState::ShuttingDown => "not_ready",
        }
    }

    pub fn drain_grace_seconds(&self) -> u64 {
        self.drain_grace_seconds.load(Ordering::SeqCst)
    }

    pub fn drain_grace(&self) -> Duration {
        Duration::from_secs(self.drain_grace_seconds())
    }

    pub fn drain_close_reason(&self) -> String {
        self.drain_close_reason.read().clone()
    }

    pub fn drain_started_epoch_ms(&self) -> Option<u64> {
        let value = self.drain_started_epoch_ms.load(Ordering::SeqCst);
        (value != 0).then_some(value)
    }

    pub fn drain_deadline_epoch_ms(&self) -> Option<u64> {
        let value = self.drain_deadline_epoch_ms.load(Ordering::SeqCst);
        (value != 0).then_some(value)
    }

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

    pub fn startup_duration(&self) -> Duration {
        self.startup_time.elapsed()
    }

    pub fn uptime(&self) -> Duration {
        self.startup_time.elapsed()
    }

    pub fn uptime_seconds(&self) -> u64 {
        self.uptime().as_secs()
    }

    pub fn connection_count(&self) -> usize {
        self.connection_count.load(Ordering::Relaxed)
    }

    pub fn increment_connections(&self) {
        self.connection_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn decrement_connections(&self) {
        self.connection_count.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn session_count(&self) -> usize {
        self.ingress
            .read()
            .as_ref()
            .map(|ingress| ingress.session_count())
            .unwrap_or_else(|| self.session_count.load(Ordering::Relaxed))
    }

    pub fn increment_sessions(&self) {
        self.session_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn decrement_sessions(&self) {
        self.session_count.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn messages_received(&self) -> u64 {
        self.messages_received.load(Ordering::Relaxed)
    }

    pub fn increment_messages_received(&self) {
        self.messages_received.fetch_add(1, Ordering::Relaxed);
    }

    pub fn messages_sent(&self) -> u64 {
        self.messages_sent.load(Ordering::Relaxed)
    }

    pub fn increment_messages_sent(&self) {
        self.messages_sent.fetch_add(1, Ordering::Relaxed);
    }

    pub fn registered_route_count(&self) -> usize {
        self.router.len()
    }

    pub fn router(&self) -> Arc<Router> {
        self.router.clone()
    }

    pub fn authenticated_session_count(&self) -> usize {
        self.ingress
            .read()
            .as_ref()
            .map(|ingress| {
                ingress
                    .active_sessions()
                    .into_iter()
                    .filter(|session| session.authenticated)
                    .count()
            })
            .unwrap_or(0)
    }

    pub fn active_realms(&self) -> Vec<String> {
        Vec::new()
    }

    pub fn messages_per_second(&self) -> f64 {
        let uptime_secs = self.uptime_seconds() as f64;
        if uptime_secs < 0.001 {
            return 0.0;
        }
        let total_messages = self.messages_received() + self.messages_sent();
        total_messages as f64 / uptime_secs
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

        assert_eq!(runtime.messages_per_second(), 0.0);

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
}
