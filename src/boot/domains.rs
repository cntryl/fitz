//! Domain actor setup and registration

use crate::boot::runtime::BootResult;
use crate::runtime::{DeliveryError, DomainKind, Envelope, MailboxSink, Router};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::Arc as StdArc;

#[cfg(test)]
use crate::runtime::routing::{Route, RouteAddress, RouteFamily};

pub use crate::domains::kv::sink::KvDomainSink;
pub use crate::domains::lease::sink::LeaseDomainSink;
pub use crate::domains::notice::sink::NoticeDomainSink;
pub use crate::domains::queue::sink::QueueDomainSink;
pub use crate::domains::rpc::sink::RpcDomainSink;
pub use crate::domains::schedule::sink::ScheduleDomainSink;
pub use crate::domains::stream::sink::StreamDomainSink;

/// Generic domain sink: Forwards envelopes to domain actors.
pub struct DomainSink {
    name: &'static str,
    active: AtomicBool,
}

impl DomainSink {
    #[must_use]
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            active: AtomicBool::new(true),
        }
    }

    pub fn stop(&self) {
        self.active.store(false, Ordering::Relaxed);
    }
}

impl MailboxSink for DomainSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        if !self.active.load(Ordering::Relaxed) {
            return Err(DeliveryError::ActorStopped);
        }

        tracing::debug!(
            domain = self.name,
            destination = ?envelope.destination(),
            "Frame received by domain sink"
        );

        Ok(())
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

#[derive(Clone)]
pub struct DomainHandles {
    kv: Arc<KvDomainSink>,
    queue: Arc<QueueDomainSink>,
    notice: Arc<NoticeDomainSink>,
    stream: Arc<StreamDomainSink>,
    rpc: Arc<RpcDomainSink>,
    lease: Arc<LeaseDomainSink>,
    schedule: Arc<ScheduleDomainSink>,
}

impl DomainHandles {
    #[must_use]
    pub fn new(
        kv: Arc<KvDomainSink>,
        queue: Arc<QueueDomainSink>,
        notice: Arc<NoticeDomainSink>,
        stream: Arc<StreamDomainSink>,
        rpc: Arc<RpcDomainSink>,
        lease: Arc<LeaseDomainSink>,
        schedule: Arc<ScheduleDomainSink>,
    ) -> Self {
        Self {
            kv,
            queue,
            notice,
            stream,
            rpc,
            lease,
            schedule,
        }
    }

    pub fn stop(&self) {
        self.kv.stop();
        self.queue.stop();
        self.notice.stop();
        self.stream.stop();
        self.rpc.stop();
        self.lease.stop();
        self.schedule.stop();
    }

    pub(crate) fn queue_is_active(&self) -> bool {
        self.queue.is_active()
    }

    pub(crate) fn queue_sweep_runtime_state(&self) {
        self.queue.sweep_runtime_state();
    }

    pub(crate) fn rpc_is_active(&self) -> bool {
        self.rpc.is_active()
    }

    pub(crate) fn rpc_timeout_sweep_interval(&self) -> std::time::Duration {
        self.rpc.timeout_sweep_interval()
    }

    pub(crate) fn rpc_expire_timed_out_requests(&self) {
        self.rpc.expire_timed_out_requests();
    }

    pub(crate) fn lease_is_active(&self) -> bool {
        self.lease.is_active()
    }

    pub(crate) fn lease_sweep_expired_state(&self) {
        self.lease.sweep_expired_state();
    }

    pub(crate) fn schedule_is_active(&self) -> bool {
        self.schedule.is_active()
    }

    pub(crate) fn schedule_scan_due_schedules(&self) {
        self.schedule.scan_due_schedules();
    }

    pub(crate) fn schedule_force_due_scan_for_tests(&self, ready_count: usize) {
        self.schedule.force_due_scan_for_tests(ready_count);
    }

    pub(crate) fn refresh_queue_admin_snapshot(&self) {
        self.queue.refresh_admin_snapshot_if_dirty();
    }

    pub(crate) fn refresh_rpc_admin_snapshot(&self) {
        self.rpc.refresh_admin_snapshot_if_dirty();
    }

    pub(crate) fn refresh_notice_admin_snapshot(&self) {
        self.notice.refresh_admin_snapshot_if_dirty();
    }

    pub(crate) fn refresh_schedule_admin_snapshot(&self) {
        self.schedule.refresh_admin_snapshot_if_dirty();
    }

    pub(crate) fn refresh_stream_admin_snapshot(&self) {
        self.stream.refresh_admin_snapshot_if_dirty();
    }

    pub(crate) fn kv_active_transaction_count(&self) -> usize {
        self.kv.active_transaction_count()
    }

    pub(crate) fn kv_admin_inventory(
        &self,
        family: Option<crate::runtime::routing::RouteFamily>,
    ) -> Result<Vec<crate::control::admin::KvResourceInventoryEntry>, String> {
        self.kv.admin_inventory(family)
    }

    pub(crate) fn kv_admin_inventory_resource(
        &self,
        family: crate::runtime::routing::RouteFamily,
        realm: &str,
        area: &str,
        resource: &str,
    ) -> Result<Option<crate::control::admin::KvResourceInventoryEntry>, String> {
        self.kv
            .admin_inventory_resource(family, realm, area, resource)
    }

    pub(crate) fn kv_admin_get_committed_value(
        &self,
        family: crate::runtime::routing::RouteFamily,
        realm: &str,
        area: &str,
        resource: &str,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, String> {
        self.kv
            .admin_get_committed_value(family, realm, area, resource, key)
    }

    pub(crate) fn kv_admin_scan_committed_prefix(
        &self,
        family: crate::runtime::routing::RouteFamily,
        realm: &str,
        area: &str,
        resource: &str,
        key_prefix: &[u8],
        limit: usize,
    ) -> Result<crate::domains::kv::sink::AdminKvPrefixScanResult, String> {
        self.kv
            .admin_scan_committed_prefix(family, realm, area, resource, key_prefix, limit)
    }

    pub(crate) fn kv_admin_scan_committed_rows(
        &self,
        request: &crate::domains::kv::sink::AdminKvRowsRequest<'_>,
    ) -> Result<crate::domains::kv::sink::AdminKvRowsResult, String> {
        self.kv.admin_scan_committed_rows(request)
    }

    pub(crate) fn queue_ready_message_count(&self) -> usize {
        self.queue.ready_message_count()
    }

    pub(crate) fn queue_delayed_message_count(&self) -> usize {
        self.queue.delayed_message_count()
    }

    pub(crate) fn queue_pending_message_count(&self) -> usize {
        self.queue.pending_message_count()
    }

    pub(crate) fn queue_dead_letter_count(&self) -> usize {
        self.queue.dead_letter_count()
    }

    pub(crate) fn queue_active_inflight_count(&self) -> usize {
        self.queue.active_inflight_count()
    }

    pub(crate) fn queue_replay_dead_letter(
        &self,
        key: &crate::domains::queue::QueueKey,
        message_id: crate::domains::queue::MessageId,
    ) -> Result<bool, String> {
        self.queue.replay_dead_letter(key, message_id)
    }

    pub(crate) fn queue_purge_dead_letter(
        &self,
        key: &crate::domains::queue::QueueKey,
        message_id: crate::domains::queue::MessageId,
    ) -> Result<bool, String> {
        self.queue.purge_dead_letter(key, message_id)
    }

    pub(crate) fn stream_admin_read_resource_records(
        &self,
        request: crate::domains::stream::sink::AdminStreamReadRequest<'_>,
    ) -> Result<
        (
            Vec<crate::domains::stream::protocol::StreamReadItem>,
            crate::domains::stream::protocol::ReadCursor,
        ),
        String,
    > {
        self.stream.admin_read_resource_records(request)
    }

    pub(crate) fn stream_count(&self) -> usize {
        self.stream.stream_count()
    }

    pub(crate) fn stream_append_session_count(&self) -> usize {
        self.stream.append_session_count()
    }

    pub(crate) fn stream_subscription_count(&self) -> usize {
        self.stream.subscription_count()
    }

    pub(crate) fn rpc_worker_count(&self) -> usize {
        self.rpc.worker_count()
    }

    pub(crate) fn rpc_pending_request_count(&self) -> usize {
        self.rpc.pending_request_count()
    }

    pub(crate) fn lease_count(&self) -> usize {
        self.lease.lease_count()
    }

    pub(crate) fn lease_admin_waiters(&self) -> Vec<crate::control::admin::LeaseWaiterInfo> {
        self.lease.admin_waiters()
    }

    pub(crate) fn schedule_count(&self) -> usize {
        self.schedule.schedule_count()
    }

    pub(crate) fn schedule_executions_per_minute(&self) -> f64 {
        self.schedule.executions_per_minute()
    }

    pub(crate) fn schedule_subscription_count(&self) -> usize {
        self.schedule.subscription_count()
    }

    pub(crate) fn schedule_pending_fire_count(&self) -> usize {
        self.schedule.pending_fire_count()
    }

    pub(crate) fn schedule_pending_ack_retry_count(&self) -> usize {
        self.schedule.pending_ack_retry_count()
    }

    pub(crate) fn schedule_oldest_pending_claim_age_seconds(&self) -> u64 {
        self.schedule.oldest_pending_claim_age_seconds()
    }

    pub(crate) fn schedule_notify_failure_count(&self) -> u64 {
        self.schedule.notify_failure_count()
    }

    pub(crate) fn schedule_ack_failure_count(&self) -> u64 {
        self.schedule.ack_failure_count()
    }

    pub(crate) fn schedule_overdue_normalization_count(&self) -> u64 {
        self.schedule.overdue_normalization_count()
    }

    pub(crate) fn schedule_admin_pending_claims(
        &self,
        family: crate::runtime::routing::RouteFamily,
    ) -> Vec<crate::control::admin::SchedulePendingClaimInfo> {
        self.schedule.admin_pending_claims(family)
    }

    /// Preload persisted schedule families through the schedule domain handle.
    ///
    /// # Errors
    ///
    /// Returns an error when persisted schedule state cannot be loaded into the
    /// schedule actor projection.
    pub fn preload_schedule_families(&self) -> Result<(), String> {
        self.schedule.preload_persisted_families()
    }
}

pub struct DomainSetupOptions {
    pub server_write_options: cntryl_midge::WriteOptions,
    pub queue_write_options: cntryl_midge::WriteOptions,
    pub queue_fast_flush_interval: Option<std::time::Duration>,
    pub request_sync_write_options: cntryl_midge::WriteOptions,
    pub rpc_request_timeout: Option<std::time::Duration>,
    pub stream_storage_layout: crate::domains::stream::StreamStorageLayout,
}

/// Set up all 7 domain actors and register them with the router.
///
/// # Errors
///
/// Returns an error when any domain sink initialization fails or when schedule
/// preload cannot restore persisted schedule families.
pub fn setup(
    router: &StdArc<Router>,
    store: &StdArc<cntryl_midge::Engine>,
    admin_read_model: &Arc<crate::control::admin::read_model::AdminReadModel>,
    options: &DomainSetupOptions,
) -> BootResult<Arc<DomainHandles>> {
    let metrics = (*crate::observability::metrics()).clone();
    let storage = crate::storage::FitzStorageEngine::new(store.clone());

    let kv_sink = Arc::new(
        KvDomainSink::new(store.clone(), router.clone(), admin_read_model.clone())
            .with_sync_write_options(options.request_sync_write_options)
            .with_metrics(metrics.clone()),
    );
    DomainKind::Kv
        .descriptor()
        .register_sink(router, kv_sink.clone() as Arc<dyn MailboxSink>);
    tracing::info!("Registered KV domain (handles kv://* across all route families)");

    let queue_sink = Arc::new(
        QueueDomainSink::try_new_with_storage(
            storage.clone(),
            router.clone(),
            admin_read_model.clone(),
            options.queue_write_options,
            crate::utils::idempotency::default_dedup_store(),
        )?
        .with_fast_flush_interval(options.queue_fast_flush_interval)
        .with_metrics(metrics.clone()),
    );
    DomainKind::Queue
        .descriptor()
        .register_sink(router, queue_sink.clone() as Arc<dyn MailboxSink>);
    tracing::info!("Registered Queue domain (handles queue://* across all route families)");

    let notice_sink = Arc::new(
        NoticeDomainSink::new(router.clone(), admin_read_model.clone())
            .with_metrics(metrics.clone()),
    );
    DomainKind::Notice
        .descriptor()
        .register_sink(router, notice_sink.clone() as Arc<dyn MailboxSink>);
    tracing::info!("Registered Notice domain (handles notice://* across all route families)");

    let stream_sink = Arc::new(
        StreamDomainSink::new_with_storage_layout(
            storage.clone(),
            router.clone(),
            admin_read_model.clone(),
            options.stream_storage_layout,
        )?
        .with_sync_write_options(options.request_sync_write_options)
        .with_metrics(metrics.clone()),
    );
    DomainKind::Stream
        .descriptor()
        .register_sink(router, stream_sink.clone() as Arc<dyn MailboxSink>);
    tracing::info!("Registered Stream domain (handles stream://* across all route families)");

    let rpc_sink = Arc::new(
        RpcDomainSink::new(router.clone(), admin_read_model.clone())
            .with_request_timeout(
                options
                    .rpc_request_timeout
                    .unwrap_or(std::time::Duration::from_secs(30)),
            )
            .with_metrics(metrics.clone()),
    );
    DomainKind::Rpc
        .descriptor()
        .register_sink(router, rpc_sink.clone() as Arc<dyn MailboxSink>);
    tracing::info!("Registered RPC domain (handles rpc://* across all route families)");

    let lease_sink = Arc::new(
        LeaseDomainSink::new(router.clone(), admin_read_model.clone())
            .with_metrics(metrics.clone()),
    );
    DomainKind::Lease
        .descriptor()
        .register_sink(router, lease_sink.clone() as Arc<dyn MailboxSink>);
    tracing::info!(
        "Registered Lease domain (ephemeral, in-memory lease://* across all route families)"
    );

    let schedule_sink = Arc::new(
        ScheduleDomainSink::new_with_storage(storage, router.clone(), admin_read_model.clone())
            .with_write_options(options.server_write_options)
            .with_metrics(metrics.clone()),
    );
    DomainKind::Schedule
        .descriptor()
        .register_sink(router, schedule_sink.clone() as Arc<dyn MailboxSink>);
    schedule_sink
        .preload_persisted_families()
        .map_err(|error| format!("schedule preload failed: {error}"))?;
    tracing::info!("Registered Schedule domain (handles schedule://* across all route families)");

    tracing::info!(
        "All {} domain sinks registered with router",
        DomainKind::ALL.len()
    );

    let handles = Arc::new(DomainHandles::new(
        kv_sink,
        queue_sink,
        notice_sink,
        stream_sink,
        rpc_sink,
        lease_sink,
        schedule_sink,
    ));
    crate::api::background::start_domain_background_tasks(&handles);
    Ok(handles)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_define_domain_setup() {
        // Placeholder: Domain setup structure is well-defined
    }

    #[test]
    fn should_create_domain_sinks() {
        // Arrange
        let kv_sink = DomainSink::new("kv");
        let notice_sink = DomainSink::new("notice");

        // Act
        let kv_active = kv_sink.active.load(Ordering::Relaxed);
        let notice_active = notice_sink.active.load(Ordering::Relaxed);
        kv_sink.stop();
        let kv_stopped = kv_sink.active.load(Ordering::Relaxed);

        // Assert
        assert!(kv_active);
        assert!(notice_active);
        assert!(!kv_stopped);
    }

    #[test]
    fn should_handle_delivery_when_active() {
        // Arrange
        let sink = DomainSink::new("kv");
        let address = RouteAddress::new(RouteFamily::new(1), Route::new("kv"));
        let envelope = Envelope::new(address, vec![0u8; 10]);

        // Act
        let result = sink.deliver(envelope);

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_reject_delivery_when_stopped() {
        // Arrange
        let sink = DomainSink::new("kv");
        sink.stop();

        let address = RouteAddress::new(RouteFamily::new(1), Route::new("kv"));
        let envelope = Envelope::new(address, vec![0u8; 10]);

        // Act
        let result = sink.deliver(envelope);

        // Assert
        assert!(matches!(result, Err(DeliveryError::ActorStopped)));
    }

    #[test]
    fn should_handle_high_priority_delivery() {
        // Arrange
        let sink = DomainSink::new("kv");
        let address = RouteAddress::new(RouteFamily::new(1), Route::new("kv"));
        let envelope = Envelope::new(address, vec![0u8; 10]);

        // Act
        let result = sink.deliver_high_priority(envelope);

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_setup_all_seven_domains() {
        // Arrange
        let store = crate::testkit::midge::create_test_engine_with_cfs(vec![1, 2, 3, 4, 5, 6, 7]);
        let router = Arc::new(Router::new());
        let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();

        // Act
        let result = setup(
            &router,
            &store,
            &admin_read_model,
            &DomainSetupOptions {
                server_write_options: cntryl_midge::WriteOptions::best_effort(),
                queue_write_options: cntryl_midge::WriteOptions::best_effort(),
                queue_fast_flush_interval: Some(std::time::Duration::from_millis(100)),
                request_sync_write_options: cntryl_midge::WriteOptions::sync(),
                rpc_request_timeout: None,
                stream_storage_layout: crate::domains::stream::StreamStorageLayout::default(),
            },
        );

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_register_all_manifest_domains_for_session_cleanup() {
        // Arrange
        let store = crate::testkit::midge::create_test_engine_with_cfs(vec![1, 2, 3, 4, 5, 6, 7]);
        let router = Arc::new(Router::new());
        let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();

        // Act
        setup(
            &router,
            &store,
            &admin_read_model,
            &DomainSetupOptions {
                server_write_options: cntryl_midge::WriteOptions::best_effort(),
                queue_write_options: cntryl_midge::WriteOptions::best_effort(),
                queue_fast_flush_interval: Some(std::time::Duration::from_millis(100)),
                request_sync_write_options: cntryl_midge::WriteOptions::sync(),
                rpc_request_timeout: None,
                stream_storage_layout: crate::domains::stream::StreamStorageLayout::default(),
            },
        )
        .expect("setup domains");

        // Assert
        for domain in DomainKind::ALL {
            let result = router.route(Envelope::new(
                RouteAddress::new(RouteFamily::new(1), domain.cleanup_route()),
                crate::runtime::SessionCleanup { session_id: 42 },
            ));
            assert!(
                result.is_ok(),
                "expected {} cleanup route to be registered",
                domain.as_str()
            );
        }
    }
}
