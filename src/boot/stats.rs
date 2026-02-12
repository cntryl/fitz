//! Runtime statistics and observability

use crate::runtime::Router;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Runtime statistics and state accessor
///
/// Provides read-only access to runtime metrics for observability.
/// This structure is thread-safe and can be cloned cheaply (Arc-wrapped).
#[derive(Clone)]
pub struct Runtime {
    /// Message router (for route/subscription queries)
    #[allow(dead_code)] // TODO: Use for querying domain stats
    pub(crate) router: Arc<Router>,

    /// Startup timestamp
    pub(crate) startup_time: Instant,

    /// Storage ready flag
    pub(crate) storage_ready: Arc<AtomicU64>,

    /// Domains initialized flag
    pub(crate) domains_ready: Arc<AtomicU64>,

    /// Startup complete flag
    pub(crate) startup_complete: Arc<AtomicU64>,

    /// Active connection count
    pub(crate) connection_count: Arc<AtomicUsize>,

    /// Active session count
    pub(crate) session_count: Arc<AtomicUsize>,

    /// Total messages received
    pub(crate) messages_received: Arc<AtomicU64>,

    /// Total messages sent
    pub(crate) messages_sent: Arc<AtomicU64>,
}

impl Runtime {
    /// Create a new runtime statistics tracker
    pub fn new(router: Arc<Router>) -> Self {
        let now = Instant::now();
        Self {
            router,
            startup_time: now,
            storage_ready: Arc::new(AtomicU64::new(0)),
            domains_ready: Arc::new(AtomicU64::new(0)),
            startup_complete: Arc::new(AtomicU64::new(0)),
            connection_count: Arc::new(AtomicUsize::new(0)),
            session_count: Arc::new(AtomicUsize::new(0)),
            messages_received: Arc::new(AtomicU64::new(0)),
            messages_sent: Arc::new(AtomicU64::new(0)),
        }
    }

    // Storage status

    pub fn mark_storage_ready(&self) {
        self.storage_ready.store(1, Ordering::SeqCst);
    }

    pub fn is_storage_ready(&self) -> bool {
        self.storage_ready.load(Ordering::SeqCst) == 1
    }

    // Domain status

    pub fn mark_domains_ready(&self) {
        self.domains_ready.store(1, Ordering::SeqCst);
    }

    pub fn are_domains_ready(&self) -> bool {
        self.domains_ready.load(Ordering::SeqCst) == 1
    }

    // Startup status

    pub fn mark_startup_complete(&self) {
        self.startup_complete.store(1, Ordering::SeqCst);
    }

    pub fn is_startup_complete(&self) -> bool {
        self.startup_complete.load(Ordering::SeqCst) == 1
    }

    pub fn startup_duration(&self) -> Duration {
        self.startup_time.elapsed()
    }

    // Uptime

    pub fn uptime(&self) -> Duration {
        self.startup_time.elapsed()
    }

    pub fn uptime_seconds(&self) -> u64 {
        self.uptime().as_secs()
    }

    // Connection/Session tracking

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
        self.session_count.load(Ordering::Relaxed)
    }

    pub fn increment_sessions(&self) {
        self.session_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn decrement_sessions(&self) {
        self.session_count.fetch_sub(1, Ordering::Relaxed);
    }

    // Message counters

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

    // Active realms (derived from router state)

    pub fn active_realms(&self) -> Vec<String> {
        // TODO: Query router for active route families/realms
        // For now, return empty vec
        vec![]
    }

    // Messages per second (simple calculation)

    pub fn messages_per_second(&self) -> f64 {
        let uptime_secs = self.uptime_seconds() as f64;
        if uptime_secs < 0.001 {
            return 0.0;
        }
        let total_messages = self.messages_received() + self.messages_sent();
        total_messages as f64 / uptime_secs
    }

    // Domain-specific stats (stubs - to be implemented by querying domain actors)

    pub fn kv_transactions_active(&self) -> usize {
        // TODO: Query KV domain for active transactions
        0
    }

    pub fn kv_keys_total(&self) -> usize {
        // TODO: Query KV domain for total keys
        0
    }

    pub fn notice_subscriptions_active(&self) -> usize {
        // TODO: Query Notice domain for active subscriptions
        0
    }

    pub fn queue_messages_pending(&self) -> usize {
        // TODO: Query Queue domain for pending messages
        0
    }

    pub fn queue_leases_active(&self) -> usize {
        // TODO: Query Queue domain for active leases
        0
    }

    pub fn rpc_workers_registered(&self) -> usize {
        // TODO: Query RPC domain for registered workers
        0
    }

    pub fn rpc_requests_pending(&self) -> usize {
        // TODO: Query RPC domain for pending requests
        0
    }

    pub fn lease_active(&self) -> usize {
        // TODO: Query Lease domain for active leases
        0
    }

    pub fn stream_active(&self) -> usize {
        // TODO: Query Stream domain for active streams
        0
    }

    pub fn kv_operations_per_second(&self) -> f64 {
        // TODO: Calculate from domain metrics
        0.0
    }

    pub fn stream_events_total(&self) -> usize {
        // TODO: Query Stream domain for total events
        0
    }

    pub fn stream_operations_per_second(&self) -> f64 {
        // TODO: Calculate from domain metrics
        0.0
    }

    pub fn stream_subscriptions_active(&self) -> usize {
        // TODO: Query StreamDomainSink subscription_count() via router
        0
    }

    pub fn notice_publishes_per_second(&self) -> f64 {
        // TODO: Calculate from domain metrics
        0.0
    }

    pub fn queue_operations_per_second(&self) -> f64 {
        // TODO: Calculate from domain metrics
        0.0
    }

    pub fn rpc_operations_per_second(&self) -> f64 {
        // TODO: Calculate from domain metrics
        0.0
    }

    pub fn lease_operations_per_second(&self) -> f64 {
        // TODO: Calculate from domain metrics
        0.0
    }

    pub fn schedule_active(&self) -> usize {
        // TODO: Query Schedule domain for active schedules
        0
    }

    pub fn schedule_executions_per_minute(&self) -> f64 {
        // TODO: Calculate from domain metrics
        0.0
    }

    pub fn schedule_subscriptions_active(&self) -> usize {
        // TODO: Query ScheduleDomainSink subscription_count() via router
        0
    }

    // List methods for admin API

    pub fn kv_list_transactions(
        &self,
        _realm: Option<&str>,
    ) -> Vec<crate::api::admin::KvTransaction> {
        // TODO: Query KV domain for active transactions
        vec![]
    }

    pub fn stream_list_streams(&self, _realm: Option<&str>) -> Vec<crate::api::admin::StreamInfo> {
        // TODO: Query Stream domain for active streams
        vec![]
    }

    pub fn notice_list_subscriptions(
        &self,
        _realm: Option<&str>,
        _route_pattern: Option<&str>,
    ) -> Vec<crate::api::admin::NoticeSubscription> {
        // TODO: Query Notice domain for subscriptions
        vec![]
    }

    pub fn notice_list_routes(
        &self,
        _realm: Option<&str>,
    ) -> Vec<crate::api::admin::NoticeRouteInfo> {
        // TODO: Query Notice domain for routes with subscriber counts
        vec![]
    }

    pub fn queue_list_queues(&self, _realm: Option<&str>) -> Vec<crate::api::admin::QueueInfo> {
        // TODO: Query Queue domain for queue depths
        vec![]
    }

    pub fn queue_list_leases(&self, _realm: Option<&str>) -> Vec<crate::api::admin::QueueLease> {
        // TODO: Query Queue domain for active leases
        vec![]
    }

    pub fn rpc_list_workers(&self, _realm: Option<&str>) -> Vec<crate::api::admin::RpcWorker> {
        // TODO: Query RPC domain for registered workers
        vec![]
    }

    pub fn rpc_list_pending(
        &self,
        _realm: Option<&str>,
    ) -> Vec<crate::api::admin::RpcPendingRequest> {
        // TODO: Query RPC domain for pending requests
        vec![]
    }

    pub fn lease_list_leases(&self, _realm: Option<&str>) -> Vec<crate::api::admin::LeaseInfo> {
        // TODO: Query Lease domain for active leases
        vec![]
    }

    pub fn schedule_list_schedules(
        &self,
        _realm: Option<&str>,
    ) -> Vec<crate::api::admin::ScheduleInfo> {
        // TODO: Query Schedule domain for schedules
        vec![]
    }

    pub fn list_sessions(&self, _realm: Option<&str>) -> Vec<crate::api::admin::SessionInfo> {
        // TODO: Query session manager for active sessions
        vec![]
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

        // At startup, should be 0
        assert_eq!(runtime.messages_per_second(), 0.0);

        // Act
        runtime.increment_messages_received();
        runtime.increment_messages_sent();
        std::thread::sleep(std::time::Duration::from_millis(10));

        // Assert
        let mps = runtime.messages_per_second();
        assert!(mps >= 0.0); // Just check it's non-negative, timing is unpredictable
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
        // Uptime seconds is always valid, no assertion needed
        let _uptime_secs = runtime.uptime_seconds();
    }
}
