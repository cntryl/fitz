//! Domain actor setup and registration

use crate::boot::runtime::BootResult;
use crate::runtime::{DomainKind, MailboxSink, Router};
use std::sync::Arc;
use std::sync::Arc as StdArc;

use crate::domains::kv::KvDomain;
use crate::domains::lease::LeaseDomain;
use crate::domains::notice::NoticeDomain;
use crate::domains::queue::QueueDomain;
use crate::domains::rpc::RpcDomain;
use crate::domains::schedule::ScheduleDomain;
use crate::domains::stream::StreamDomain;
use crate::runtime::routing::RouteFamily;

#[derive(Clone)]
pub(crate) struct BrokerDomains {
    kv: Arc<KvDomain>,
    queue: Arc<QueueDomain>,
    notice: Arc<NoticeDomain>,
    stream: Arc<StreamDomain>,
    rpc: Arc<RpcDomain>,
    lease: Arc<LeaseDomain>,
    schedule: Arc<ScheduleDomain>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg(test)]
pub(crate) struct DomainHealthSnapshot {
    pub(crate) domain: &'static str,
    pub(crate) panic_count: u64,
    pub(crate) healthy_families: Vec<RouteFamily>,
    pub(crate) degraded_families: Vec<RouteFamily>,
    pub(crate) failed_families: Vec<RouteFamily>,
}

#[cfg(test)]
impl DomainHealthSnapshot {
    fn new(
        domain: DomainKind,
        health: crate::runtime::family_actor_pool::FamilyActorPoolHealthSnapshot,
    ) -> Self {
        Self {
            domain: domain.as_str(),
            panic_count: health.panic_count,
            healthy_families: health.healthy_families,
            degraded_families: health.degraded_families,
            failed_families: health.failed_families,
        }
    }

    #[must_use]
    pub(crate) fn has_usable_family(&self) -> bool {
        !self.healthy_families.is_empty()
    }
}

impl BrokerDomains {
    pub(crate) fn maintenance_jobs(&self) -> Vec<super::domain_interfaces::MaintenanceJob> {
        let queue_active = self.queue.clone();
        let queue_run = self.queue.clone();
        let rpc_interval = self.rpc.clone();
        let rpc_active = self.rpc.clone();
        let rpc_run = self.rpc.clone();
        let lease_active = self.lease.clone();
        let lease_run = self.lease.clone();
        let schedule_active = self.schedule.clone();
        let schedule_run = self.schedule.clone();
        let stream_active = self.stream.clone();
        let stream_run = self.stream.clone();
        vec![
            super::domain_interfaces::MaintenanceJob::new(
                "queue",
                true,
                || std::time::Duration::from_millis(50),
                move || queue_active.is_active(),
                move || queue_run.sweep_runtime_state(),
            ),
            super::domain_interfaces::MaintenanceJob::new(
                "rpc",
                false,
                move || rpc_interval.timeout_sweep_interval(),
                move || rpc_active.is_active(),
                move || rpc_run.expire_timed_out_requests(),
            ),
            super::domain_interfaces::MaintenanceJob::new(
                "lease",
                true,
                || std::time::Duration::from_millis(50),
                move || lease_active.is_active(),
                move || lease_run.sweep_expired_state(),
            ),
            super::domain_interfaces::MaintenanceJob::new(
                "schedule",
                true,
                || std::time::Duration::from_millis(250),
                move || schedule_active.is_active(),
                move || schedule_run.scan_due_schedules(),
            ),
            super::domain_interfaces::MaintenanceJob::new(
                "stream",
                true,
                || std::time::Duration::from_secs(1),
                move || stream_active.is_active(),
                move || stream_run.run_maintenance_slice(),
            ),
        ]
    }

    #[must_use]
    pub(crate) fn new(
        kv: Arc<KvDomain>,
        queue: Arc<QueueDomain>,
        notice: Arc<NoticeDomain>,
        stream: Arc<StreamDomain>,
        rpc: Arc<RpcDomain>,
        lease: Arc<LeaseDomain>,
        schedule: Arc<ScheduleDomain>,
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

    pub(crate) fn stop(&self) {
        self.kv.stop();
        self.queue.stop();
        self.notice.stop();
        self.stream.stop();
        self.rpc.stop();
        self.lease.stop();
        self.schedule.stop();
    }

    #[must_use]
    #[cfg(test)]
    pub(crate) fn health_snapshots(&self) -> Vec<DomainHealthSnapshot> {
        vec![
            DomainHealthSnapshot::new(DomainKind::Kv, self.kv.family_health_snapshot()),
            DomainHealthSnapshot::new(DomainKind::Queue, self.queue.family_health_snapshot()),
            DomainHealthSnapshot::new(DomainKind::Notice, self.notice.family_health_snapshot()),
            DomainHealthSnapshot::new(DomainKind::Stream, self.stream.family_health_snapshot()),
            DomainHealthSnapshot::new(DomainKind::Rpc, self.rpc.family_health_snapshot()),
            DomainHealthSnapshot::new(DomainKind::Lease, self.lease.family_health_snapshot()),
            DomainHealthSnapshot::new(DomainKind::Schedule, self.schedule.family_health_snapshot()),
        ]
    }

    #[must_use]
    pub(crate) fn has_permanently_failed_domain(&self) -> bool {
        !self.kv.has_usable_family()
            || !self.queue.has_usable_family()
            || !self.notice.has_usable_family()
            || !self.stream.has_usable_family()
            || !self.rpc.has_usable_family()
            || !self.lease.has_usable_family()
            || !self.schedule.has_usable_family()
    }

    #[cfg(test)]
    pub(crate) fn mark_kv_permanently_failed_for_tests(&self) {
        self.kv.mark_actor_permanently_failed_for_tests();
    }

    pub(crate) fn panic_all_domain_actors_for_failpoint(&self) {
        self.kv.panic_actor_for_failpoint();
        self.queue.panic_actor_for_failpoint();
        self.notice.panic_actor_for_failpoint();
        self.stream.panic_actor_for_failpoint();
        self.rpc.panic_actor_for_failpoint();
        self.lease.panic_actor_for_failpoint();
        self.schedule.panic_actor_for_failpoint();
    }

    pub(crate) fn panic_notice_actor_for_failpoint(&self) {
        self.notice.panic_actor_for_failpoint();
    }

    pub(crate) fn panic_queue_actor_for_failpoint(&self) {
        self.queue.panic_actor_for_failpoint();
    }

    pub(crate) fn panic_kv_actor_for_failpoint(&self) {
        self.kv.panic_actor_for_failpoint();
    }

    pub(crate) fn panic_lease_actor_for_failpoint(&self) {
        self.lease.panic_actor_for_failpoint();
    }

    pub(crate) fn panic_schedule_actor_for_failpoint(&self) {
        self.schedule.panic_actor_for_failpoint();
    }

    pub(crate) fn panic_stream_actor_for_failpoint(&self) {
        self.stream.panic_actor_for_failpoint();
    }

    pub(crate) fn panic_rpc_actor_for_failpoint(&self) {
        self.rpc.panic_actor_for_failpoint();
    }

    pub(crate) fn panic_stream_family_actor_for_failpoint(&self, family: RouteFamily) {
        self.stream.panic_family_actor_for_failpoint(family);
    }

    pub(crate) fn panic_rpc_family_actor_for_failpoint(&self, family: RouteFamily) {
        self.rpc.panic_family_actor_for_failpoint(family);
    }

    pub(crate) fn schedule_force_due_scan_for_tests(&self, ready_count: usize) {
        self.schedule.force_due_scan_for_tests(ready_count);
    }

    pub(crate) fn admin_ports(&self) -> DomainAdminPorts {
        DomainAdminPorts {
            kv: self.kv.clone(),
            queue: self.queue.clone(),
            notice: self.notice.clone(),
            stream: self.stream.clone(),
            rpc: self.rpc.clone(),
            lease: self.lease.clone(),
            schedule: self.schedule.clone(),
        }
    }
}

pub(crate) type KvAdmin = Arc<KvDomain>;
pub(crate) type QueueAdmin = Arc<QueueDomain>;
pub(crate) type NoticeAdmin = Arc<NoticeDomain>;
pub(crate) type StreamAdmin = Arc<StreamDomain>;
pub(crate) type RpcAdmin = Arc<RpcDomain>;
pub(crate) type LeaseAdmin = Arc<LeaseDomain>;
pub(crate) type ScheduleAdmin = Arc<ScheduleDomain>;

#[derive(Clone)]
pub(crate) struct DomainAdminPorts {
    kv: KvAdmin,
    queue: QueueAdmin,
    notice: NoticeAdmin,
    stream: StreamAdmin,
    rpc: RpcAdmin,
    lease: LeaseAdmin,
    schedule: ScheduleAdmin,
}

impl DomainAdminPorts {
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

    pub(crate) fn stream_durable_metrics_snapshot(
        &self,
    ) -> crate::domains::stream::metrics::StreamDurableMetricsSnapshot {
        self.stream.durable_metrics_snapshot()
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

    pub(crate) fn kv_admin_inventory_metadata_resource(
        &self,
        family: RouteFamily,
        realm: &str,
        area: &str,
        resource: &str,
    ) -> Result<Option<crate::control::admin::KvResourceInventoryEntry>, String> {
        self.kv
            .admin_inventory_metadata_resource(family, realm, area, resource)
    }

    pub(crate) fn kv_admin_inventory_page(
        &self,
        family: crate::runtime::routing::RouteFamily,
        realm: &str,
        area: Option<&str>,
        after: Option<(&str, &str)>,
        limit: usize,
    ) -> Result<(Vec<crate::control::admin::KvResourceInventoryEntry>, bool), String> {
        self.kv
            .admin_inventory_page(family, realm, area, after, limit)
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
    ) -> Result<crate::domains::kv::admin::AdminKvPrefixScanResult, String> {
        self.kv
            .admin_scan_committed_prefix(family, realm, area, resource, key_prefix, limit)
    }

    pub(crate) fn kv_admin_scan_committed_rows(
        &self,
        request: &crate::domains::kv::admin::AdminKvRowsRequest<'_>,
    ) -> Result<
        crate::domains::kv::admin::AdminKvRowsResult,
        crate::domains::kv::admin::AdminKvRowsError,
    > {
        self.kv.admin_scan_committed_rows(request)
    }

    pub(crate) fn kv_admin_capture_snapshot(
        &self,
        selector: &crate::snapshot::SnapshotSelector,
    ) -> Result<crate::snapshot::SnapshotArtifact, String> {
        self.kv.capture_kv_snapshot(selector)
    }

    pub(crate) fn kv_admin_restore_snapshot(&self, bytes: &[u8]) -> Result<Vec<String>, String> {
        self.kv.restore_kv_snapshot(bytes)
    }

    pub(crate) fn stream_admin_capture_snapshot(
        &self,
        selector: &crate::snapshot::SnapshotSelector,
    ) -> Result<crate::snapshot::SnapshotArtifact, String> {
        self.stream.capture_stream_snapshot(selector)
    }

    pub(crate) fn stream_admin_restore_snapshot(
        &self,
        bytes: &[u8],
    ) -> Result<Vec<String>, String> {
        self.stream.restore_stream_snapshot(bytes)
    }

    pub(crate) fn queue_ready_message_count(&self) -> usize {
        self.queue.counts().ready
    }

    pub(crate) fn queue_delayed_message_count(&self) -> usize {
        self.queue.counts().delayed
    }

    pub(crate) fn queue_pending_message_count(&self) -> usize {
        self.queue.counts().pending
    }

    pub(crate) fn queue_dead_letter_count(&self) -> usize {
        self.queue.counts().dead_letters
    }

    pub(crate) fn queue_active_inflight_count(&self) -> usize {
        self.queue.counts().inflight
    }

    pub(crate) fn queue_inspect_dead_letter(
        &self,
        key: &crate::domains::queue::QueueKey,
        id: crate::domains::queue::MessageId,
    ) -> Result<Option<crate::domains::queue::projection::QueueDeadLetterSnapshot>, String> {
        self.queue.inspect_dead_letter(key, id)
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
        request: &crate::domains::stream::admin::AdminStreamReadRequest<'_>,
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

    pub(crate) fn schedule_run_now(
        &self,
        family: crate::runtime::routing::RouteFamily,
        route: String,
        timeout: std::time::Duration,
    ) -> Result<
        Option<crate::domains::schedule::ScheduleRunNowResult>,
        crate::domains::schedule::ScheduleRunNowError,
    > {
        self.schedule.run_now(family, route, timeout)
    }

    #[cfg(test)]
    pub(crate) fn preload_schedule_families(&self) -> Result<(), String> {
        self.schedule.preload_persisted_families()
    }
}

pub(crate) struct DomainSetupOptions {
    pub(crate) route_families: Vec<u32>,
    pub(crate) schedule_write_policy: crate::domains::WritePolicy,

    pub(crate) queue_fast_flush_interval: Option<std::time::Duration>,
    pub(crate) request_sync_write_policy: crate::domains::WritePolicy,
    pub(crate) request_buffered_write_policy: crate::domains::WritePolicy,
    pub(crate) rpc_request_timeout: Option<std::time::Duration>,
    pub(crate) stream_storage_layout: crate::domains::stream::StreamStorageLayout,
    pub(crate) kv_idle_transaction_ttl: std::time::Duration,
    pub(crate) schedule_preload_timeout: std::time::Duration,
}

fn provisioned_route_families(options: &DomainSetupOptions) -> Vec<RouteFamily> {
    options
        .route_families
        .iter()
        .copied()
        .map(RouteFamily::new)
        .collect()
}

fn create_stream_sink(
    storage: crate::storage::FitzStorageEngine,
    router: &StdArc<Router>,
    admin_read_model: &Arc<crate::control::admin::read_model::AdminReadModel>,
    options: &DomainSetupOptions,
    route_families: &[RouteFamily],
    metrics: &crate::observability::metrics::MetricsCollector,
) -> BootResult<Arc<StreamDomain>> {
    Ok(Arc::new(
        StreamDomain::new_with_storage_layout_and_families(
            storage,
            router.clone(),
            admin_read_model.clone(),
            options.stream_storage_layout,
            Some(route_families),
            crate::domains::stream::StreamStorageWriteOptions::new(
                options.request_sync_write_policy,
                options.request_buffered_write_policy,
            ),
        )?
        .with_metrics(metrics.clone()),
    ))
}

fn create_kv_sink(
    store: &StdArc<cntryl_midge::Engine>,
    router: &StdArc<Router>,
    admin_read_model: &Arc<crate::control::admin::read_model::AdminReadModel>,
    options: &DomainSetupOptions,
    metrics: &crate::observability::metrics::MetricsCollector,
) -> Arc<KvDomain> {
    Arc::new(
        KvDomain::new_with_families(
            store.clone(),
            router.clone(),
            admin_read_model.clone(),
            &options
                .route_families
                .iter()
                .copied()
                .map(RouteFamily::new)
                .collect::<Vec<_>>(),
        )
        .with_idle_transaction_ttl(options.kv_idle_transaction_ttl)
        .with_write_policies(
            options.request_sync_write_policy,
            options.request_buffered_write_policy,
        )
        .with_metrics(metrics.clone()),
    )
}

/// Set up all 7 domain actors and register them with the router.
///
/// # Errors
///
/// Returns an error when any domain sink initialization fails or when schedule
/// preload cannot restore persisted schedule families.
pub(crate) fn setup(
    router: &StdArc<Router>,
    store: &StdArc<cntryl_midge::Engine>,
    admin_read_model: &Arc<crate::control::admin::read_model::AdminReadModel>,
    options: &DomainSetupOptions,
) -> BootResult<Arc<BrokerDomains>> {
    let metrics = (*crate::observability::metrics()).clone();
    let storage = crate::storage::FitzStorageEngine::new(store.clone());
    let route_families = provisioned_route_families(options);
    let rpc_request_timeout = options
        .rpc_request_timeout
        .unwrap_or(std::time::Duration::from_secs(30));

    let kv_sink = create_kv_sink(store, router, admin_read_model, options, &metrics);
    register_domain_sink(DomainKind::Kv, router, kv_sink.clone());

    let queue_sink = Arc::new(
        QueueDomain::try_new_with_storage(
            crate::domains::queue::actor::recovery_store::QueueStore::from(storage.clone()),
            router.clone(),
            admin_read_model.clone(),
            crate::utils::idempotency::default_dedup_store(),
        )?
        .with_fast_flush_interval(options.queue_fast_flush_interval)
        .with_metrics(metrics.clone()),
    );
    register_domain_sink(DomainKind::Queue, router, queue_sink.clone());

    let notice_sink = Arc::new(
        NoticeDomain::new_with_families(router.clone(), admin_read_model.clone(), &route_families)
            .with_metrics(metrics.clone()),
    );
    register_domain_sink(DomainKind::Notice, router, notice_sink.clone());

    let stream_sink = create_stream_sink(
        storage.clone(),
        router,
        admin_read_model,
        options,
        &route_families,
        &metrics,
    )?;
    stream_sink.initialize_admin_snapshot();
    register_domain_sink(DomainKind::Stream, router, stream_sink.clone());

    let rpc_sink = Arc::new(
        RpcDomain::new_with_families(router.clone(), admin_read_model.clone(), &route_families)
            .with_request_timeout(rpc_request_timeout)
            .with_metrics(metrics.clone()),
    );
    register_domain_sink(DomainKind::Rpc, router, rpc_sink.clone());

    let lease_sink = Arc::new(
        LeaseDomain::new_with_families(router.clone(), admin_read_model.clone(), &route_families)
            .with_metrics(metrics.clone()),
    );
    register_domain_sink(DomainKind::Lease, router, lease_sink.clone());

    let schedule_sink = Arc::new(
        ScheduleDomain::new_with_store_and_families(
            crate::domains::schedule::ScheduleStore::new_with_storage(storage),
            router.clone(),
            admin_read_model.clone(),
            &route_families,
        )
        .with_write_policy(options.schedule_write_policy)
        .with_metrics(metrics.clone()),
    );
    register_domain_sink(DomainKind::Schedule, router, schedule_sink.clone());
    schedule_sink
        .preload_persisted_families_with_timeout(options.schedule_preload_timeout)
        .map_err(|error| format!("schedule preload failed: {error}"))?;
    tracing::info!(
        "All {} domain sinks registered with router",
        DomainKind::ALL.len()
    );

    let handles = Arc::new(BrokerDomains::new(
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

fn register_domain_sink<T>(kind: DomainKind, router: &Arc<Router>, sink: Arc<T>)
where
    T: MailboxSink + 'static,
{
    kind.descriptor().register_sink(router, sink);
    tracing::info!(domain = kind.as_str(), "Registered domain sink");
}

#[cfg(test)]
mod tests;
