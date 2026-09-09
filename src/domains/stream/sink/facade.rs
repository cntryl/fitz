#[cfg(test)]
use super::model::StreamReadExecution;
#[cfg(test)]
use super::model::STREAM_ACTOR_REPLY_TIMEOUT;
use super::model::{
    AdminSnapshotState, AdminStreamReadRequest, AdminStreamReadRequestOwned,
    StreamAdminReadCommand, StreamDomain, StreamDomainCommand, StreamDomainConfig,
    StreamFamilyRuntime, StreamFamilyState, StreamLiveCounts, SubscriptionRegistry,
};
use crate::domains::stream::metrics::StreamDurableMetrics;
use crate::domains::stream::{StreamMetrics, StreamReadItem, StreamStorageLayout};
#[cfg(test)]
use crate::runtime::routing::Route;
use crate::runtime::routing::RouteFamily;
use crate::runtime::{CleanedUpSessions, DeliveryError, Router};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

impl StreamFamilyState {
    fn new(config: &StreamDomainConfig) -> Self {
        Self {
            stream_store: config.stream_store.clone(),
            actors: HashMap::new(),
            session_owners: HashMap::new(),
            cleaned_up_sessions: CleanedUpSessions::new(
                crate::domains::DOMAIN_ACTOR_MAILBOX_CAPACITY,
            ),
            subscriptions: SubscriptionRegistry::new(config.next_subscription_id.clone()),
            next_session_id: config.next_session_id.clone(),
            cursor_integrity_key: config.cursor_integrity_key.clone(),
            router: config.router.clone(),
            admin_snapshot: AdminSnapshotState::new(config.admin_read_model.clone(), true),
            sync_write_mode: config.sync_write_mode,
            metrics: config.metrics.clone(),
            durable_metrics: config.durable_metrics.clone(),
            active: config.active.clone(),
        }
    }
}

impl StreamDomain {
    /// Construct a Stream sink using the default storage layout.
    ///
    /// # Errors
    ///
    /// Returns an initialization error when storage activation, persisted
    /// state validation, or cursor-key generation fails.
    #[allow(private_bounds)]
    pub fn try_new(
        store: impl Into<crate::domains::stream::store::StreamDomainStorage>,
        router: Arc<Router>,
        admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
        write_options: super::StreamStorageWriteOptions,
    ) -> Result<Self, super::StreamSinkInitError> {
        Self::new_with_storage_layout(
            store,
            router,
            admin_read_model,
            StreamStorageLayout::default(),
            write_options,
        )
        .map_err(super::StreamSinkInitError::new)
    }

    /// # Errors
    ///
    /// Returns an error if the configured stream storage layout cannot be
    /// initialized or if existing families fail persisted-state validation.
    #[allow(private_bounds)]
    pub fn new_with_layout(
        store: impl Into<crate::domains::stream::store::StreamDomainStorage>,
        router: Arc<Router>,
        admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
        stream_storage_layout: StreamStorageLayout,
        write_options: super::StreamStorageWriteOptions,
    ) -> Result<Self, String> {
        Self::new_with_storage_layout(
            store,
            router,
            admin_read_model,
            stream_storage_layout,
            write_options,
        )
    }

    pub(crate) fn new_with_storage_layout(
        store: impl Into<crate::domains::stream::store::StreamDomainStorage>,
        router: Arc<Router>,
        admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
        stream_storage_layout: StreamStorageLayout,
        write_options: super::StreamStorageWriteOptions,
    ) -> Result<Self, String> {
        Self::new_with_storage_layout_and_families(
            store,
            router,
            admin_read_model,
            stream_storage_layout,
            None,
            write_options,
        )
    }

    pub(crate) fn new_with_storage_layout_and_families(
        store: impl Into<crate::domains::stream::store::StreamDomainStorage>,
        router: Arc<Router>,
        admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
        stream_storage_layout: StreamStorageLayout,
        provisioned_families: Option<&[RouteFamily]>,
        write_options: super::StreamStorageWriteOptions,
    ) -> Result<Self, String> {
        let stream_store = store.into().into_store(
            stream_storage_layout,
            write_options.sync_intent(),
            write_options.buffered_intent(),
        );
        stream_store.ensure_layout_activation_for_existing_families()?;
        stream_store.validate_persisted_state_for_existing_families()?;
        let mut cursor_integrity_key = [0u8; 32];
        getrandom::fill(&mut cursor_integrity_key)
            .map_err(|error| format!("generate Stream cursor integrity key failed: {error}"))?;

        let active = Arc::new(AtomicBool::new(true));
        let config = StreamDomainConfig {
            stream_store,
            next_session_id: Arc::new(AtomicU64::new(1)),
            next_subscription_id: Arc::new(AtomicU64::new(1)),
            cursor_integrity_key: Arc::new(cursor_integrity_key),
            router,
            admin_read_model,
            sync_write_mode: crate::domains::stream::protocol::StreamWriteMode::Sync,
            metrics: None,
            durable_metrics: Arc::new(StreamDurableMetrics::default()),
            active: active.clone(),
        };
        let family_families =
            provisioned_families.map_or_else(|| vec![RouteFamily::new(1)], <[RouteFamily]>::to_vec);
        let family_runtime = Self::spawn_family_runtime(&config, &family_families)?;
        Ok(Self {
            config,
            active,
            family_runtime,
            family_families,
        })
    }

    fn spawn_family_runtime(
        config: &StreamDomainConfig,
        families: &[RouteFamily],
    ) -> Result<crate::runtime::FamilyActorPoolRuntime<StreamDomainCommand>, String> {
        let pool = crate::runtime::FamilyActorPool::new(families)
            .map_err(|error| format!("create Stream family actor pool: {error}"))?;
        let family_config = config.clone();
        Ok(
            crate::runtime::FamilyActorPoolRuntime::spawn_with_family_failed_metric_and_idle(
                pool,
                config.active.clone(),
                move |_family| StreamFamilyRuntime::new(StreamFamilyState::new(&family_config)),
                |state, family, _lane, command| match command {
                    StreamDomainCommand::Deliver(envelope, reply) => {
                        let result = if *envelope.destination().family() == family {
                            state.deliver_envelope(&envelope)
                        } else {
                            Err(DeliveryError::ActorStopped)
                        };
                        let _ = reply.send(result);
                    }
                    StreamDomainCommand::ReadLiveCounts(reply) => {
                        let _ = reply.send(state.core.live_counts());
                    }
                    StreamDomainCommand::ReadResourceRecords(command) => {
                        let request = command.request.as_borrowed();
                        let _ = command
                            .reply
                            .send(state.core.admin_read_resource_records(request));
                    }
                    StreamDomainCommand::RefreshAdminSnapshotIfDirty(reply) => {
                        state.core.refresh_admin_snapshot_if_dirty();
                        let _ = reply.send(());
                    }
                    StreamDomainCommand::RunMaintenance {
                        family: requested_family,
                        reply,
                    } => {
                        if requested_family == family.as_u64() {
                            state.core.run_maintenance_slice(requested_family);
                            state.service_watermark_timers();
                        }
                        if let Some(reply) = reply {
                            let _ = reply.send(());
                        }
                    }
                    #[cfg(test)]
                    StreamDomainCommand::SyncAdminSnapshot(reply) => {
                        state.core.sync_admin_snapshot();
                        let _ = reply.send(());
                    }
                    StreamDomainCommand::PanicForFailpoint => {
                        panic!("injected Stream family actor panic");
                    }
                    #[cfg(test)]
                    StreamDomainCommand::BlockForTests(entered, release) => {
                        let _ = entered.send(());
                        let _ = release.recv();
                    }
                    #[cfg(test)]
                    StreamDomainCommand::InspectForTests(inspect, reply) => {
                        inspect(state);
                        let _ = reply.send(());
                    }
                },
                |state, _family| state.service_watermark_timers(),
                crate::domains::stream::metrics::METRIC_FAMILY_FAILED_CLOSED_TOTAL,
            ),
        )
    }

    fn rebuild_actor(&mut self) {
        self.family_runtime.stop();
        self.family_runtime = Self::spawn_family_runtime(&self.config, &self.family_families)
            .expect("validated Stream family actor pool configuration");
    }

    #[must_use]
    pub fn with_metrics(
        mut self,
        collector: crate::observability::metrics::MetricsCollector,
    ) -> Self {
        self.family_runtime.stop();
        self.config.metrics = Some(StreamMetrics::new(collector));
        self.rebuild_actor();
        self
    }

    pub fn stop(&self) {
        self.active.store(false, Ordering::Relaxed);
        self.family_runtime.stop();
    }

    pub(crate) fn is_active(&self) -> bool {
        self.active.load(Ordering::Relaxed)
    }

    pub(crate) fn run_maintenance_slice(&self) {
        let families: Vec<u64> = self
            .family_families
            .iter()
            .map(RouteFamily::as_u64)
            .collect();
        for family in families {
            if !self.config.stream_store.has_pending_maintenance(family) {
                continue;
            }
            let command = StreamDomainCommand::RunMaintenance {
                family,
                reply: None,
            };
            let Ok(family_id) = u32::try_from(family) else {
                continue;
            };
            let enqueue = self
                .family_runtime
                .try_enqueue(
                    RouteFamily::new(family_id),
                    crate::runtime::FamilyActorLane::Control,
                    command,
                )
                .map_err(|error| error.to_string());
            if let Err(error) = enqueue {
                tracing::warn!(
                    domain = "stream",
                    family,
                    error,
                    "Stream maintenance enqueue failed"
                );
            }
        }
    }

    #[must_use]
    #[cfg(test)]
    pub fn storage_layout(&self) -> StreamStorageLayout {
        self.config.stream_store.storage_layout()
    }

    /// # Errors
    ///
    /// Returns an error if the requested route cannot be read or if the stream
    /// store rejects the read parameters.
    pub fn admin_read_resource_records(
        &self,
        request: &AdminStreamReadRequest<'_>,
    ) -> Result<
        (
            Vec<StreamReadItem>,
            crate::domains::stream::protocol::ReadCursor,
        ),
        String,
    > {
        let family = request.family;
        let owned = AdminStreamReadRequestOwned {
            family,
            realm: request.realm.to_owned(),
            area: request.area.to_owned(),
            resource: request.resource.to_owned(),
            from_offset: request.from_offset,
            limit: request.limit,
            discriminator: request.discriminator.clone(),
        };
        self.dispatch_family_command(Some(family), "admin read", move |reply| {
            StreamDomainCommand::ReadResourceRecords(StreamAdminReadCommand {
                request: owned,
                reply,
            })
        })?
    }

    #[cfg(test)]
    pub(super) fn is_actor_running(&self) -> bool {
        self.family_runtime.is_running()
    }

    #[cfg(test)]
    pub(super) fn run_maintenance_slice_for_tests(&self, family: RouteFamily) {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        let command = StreamDomainCommand::RunMaintenance {
            family: family.as_u64(),
            reply: Some(reply_tx),
        };
        self.family_runtime
            .try_enqueue(family, crate::runtime::FamilyActorLane::Control, command)
            .expect("enqueue test Stream maintenance command");
        reply_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("receive test Stream maintenance reply");
    }

    pub(crate) fn family_health_snapshot(
        &self,
    ) -> crate::runtime::family_actor_pool::FamilyActorPoolHealthSnapshot {
        self.family_runtime.health_snapshot()
    }

    #[cfg(test)]
    pub(super) fn fail_next_promotion_frontier_commit_for_tests(&self) {
        self.config
            .stream_store
            .fail_next_promotion_frontier_commit_for_tests();
    }

    /// Panic every provisioned family's handler. Used by the opt-in failpoint and tests to
    /// drive the pool to full exhaustion; a single family's panic must never
    /// be conflated with domain-wide health, so covering every family here
    /// is required to actually observe pool-wide fail-closed behavior.
    pub(crate) fn panic_actor_for_failpoint(&self) {
        for family in &self.family_families {
            let _ =
                self.dispatch_family_control(Some(*family), StreamDomainCommand::PanicForFailpoint);
        }
    }

    pub(crate) fn panic_family_actor_for_failpoint(&self, family: RouteFamily) {
        let _ = self.dispatch_family_control(Some(family), StreamDomainCommand::PanicForFailpoint);
    }

    #[cfg(test)]
    pub(super) fn stop_actor_for_tests(&self) {
        self.family_runtime.stop();
    }

    #[cfg(test)]
    pub(super) fn block_family_actor_for_tests(
        &self,
        family: RouteFamily,
        entered: crossbeam_channel::Sender<()>,
        release: crossbeam_channel::Receiver<()>,
    ) {
        self.dispatch_family_control(
            Some(family),
            StreamDomainCommand::BlockForTests(entered, release),
        )
        .expect("enqueue Stream family actor test block");
    }

    #[cfg(test)]
    pub(super) fn sync_write_mode_for_tests(
        &self,
    ) -> crate::domains::stream::protocol::StreamWriteMode {
        self.config.sync_write_mode
    }

    #[cfg(test)]
    pub(super) fn read_area_records_for_tests(
        &self,
        family: RouteFamily,
        realm: &str,
        area: &str,
        from_offset: u64,
        limit: u64,
    ) -> Result<Vec<StreamReadItem>, String> {
        self.config
            .stream_store
            .read_area(family.as_u64(), realm, area, from_offset, limit, None)
            .map(|(records, _cursor)| records)
    }

    #[cfg(test)]
    pub(super) fn read_realm_records_for_tests(
        &self,
        family: RouteFamily,
        realm: &str,
        from_offset: u64,
        limit: u64,
    ) -> Result<Vec<StreamReadItem>, String> {
        self.config
            .stream_store
            .read_realm(family.as_u64(), realm, from_offset, limit, None)
            .map(|(records, _cursor)| records)
    }

    #[cfg(test)]
    pub(super) fn get_watermark_for_tests(
        &self,
        family: RouteFamily,
        realm: &str,
        area: &str,
    ) -> Result<u64, String> {
        self.config
            .stream_store
            .get_watermark(family.as_u64(), realm, area)
    }

    #[cfg(test)]
    pub(super) fn get_realm_watermark_for_tests(
        &self,
        family: RouteFamily,
        realm: &str,
    ) -> Result<u64, String> {
        self.config
            .stream_store
            .get_realm_watermark(family.as_u64(), realm)
    }

    #[cfg(test)]
    pub(super) fn delete_compact_resource_page_for_tests(
        &self,
        family: RouteFamily,
        realm: &str,
        area: &str,
        resource: &str,
        page_start_offset: u64,
    ) -> Result<(), String> {
        self.config
            .stream_store
            .delete_compact_resource_page_for_tests(
                family.as_u64(),
                realm,
                area,
                resource,
                page_start_offset,
            )
    }

    #[cfg(test)]
    pub(super) fn encode_metadata_response_data_for_tests(
        &self,
        family: RouteFamily,
        route: &Route,
    ) -> Result<Vec<u8>, String> {
        let route = route.clone();
        self.inspect_family_for_tests(family, move |state| {
            state.core.encode_metadata_response_data(family, &route)
        })
    }

    #[cfg(test)]
    pub(super) fn encode_read_response_data_for_tests(
        &self,
        family: RouteFamily,
        route: &Route,
        from_offset: u64,
        limit: u64,
        max_bytes: Option<usize>,
        filter: Option<&crate::domains::stream::protocol::StreamFilterSet>,
    ) -> Result<Vec<u8>, String> {
        let route = route.clone();
        let filter = filter.cloned();
        self.inspect_family_for_tests(family, move |state| {
            state.core.encode_read_response_data(StreamReadExecution {
                family_id: family,
                route: &route,
                from_offset,
                limit,
                max_bytes,
                filter: filter.as_ref(),
                cursor_fingerprint: None,
                captured_watermark: None,
            })
        })
    }

    #[cfg(test)]
    pub(super) fn inspect_family_for_tests<T: Send + 'static>(
        &self,
        family: RouteFamily,
        inspect: impl FnOnce(&mut StreamFamilyRuntime) -> T + Send + 'static,
    ) -> T {
        let (result_tx, result_rx) = crossbeam_channel::bounded(1);
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        self.dispatch_family_control(
            Some(family),
            StreamDomainCommand::InspectForTests(
                Box::new(move |state| {
                    let _ = result_tx.send(inspect(state));
                }),
                reply_tx,
            ),
        )
        .expect("enqueue Stream family inspection");
        reply_rx
            .recv_timeout(STREAM_ACTOR_REPLY_TIMEOUT)
            .expect("complete Stream family inspection");
        result_rx
            .recv_timeout(STREAM_ACTOR_REPLY_TIMEOUT)
            .expect("receive Stream family inspection")
    }

    pub fn refresh_admin_snapshot_if_dirty(&self) {
        self.send_admin_snapshot_command(
            StreamDomainCommand::RefreshAdminSnapshotIfDirty,
            "refresh_if_dirty",
        );
    }

    pub(crate) fn durable_metrics_snapshot(
        &self,
    ) -> crate::domains::stream::metrics::StreamDurableMetricsSnapshot {
        self.config.durable_metrics.snapshot()
    }

    pub(crate) fn initialize_admin_snapshot(&self) {
        self.refresh_admin_snapshot_if_dirty();
    }

    #[cfg(test)]
    pub(super) fn sync_admin_snapshot(&self) {
        self.send_admin_snapshot_command(StreamDomainCommand::SyncAdminSnapshot, "sync");
    }

    fn send_admin_snapshot_command(
        &self,
        build_command: fn(crossbeam_channel::Sender<()>) -> StreamDomainCommand,
        operation: &'static str,
    ) {
        if let Err(error) = self.dispatch_family_command(None, operation, build_command) {
            tracing::warn!(
                domain = "stream",
                error = %error,
                operation,
                "Stream admin snapshot command failed"
            );
        }
    }

    pub fn append_session_count(&self) -> usize {
        self.live_counts().append_sessions
    }

    pub fn subscription_count(&self) -> usize {
        self.live_counts().subscriptions
    }

    pub fn stream_count(&self) -> usize {
        self.live_counts().streams
    }

    fn live_counts(&self) -> StreamLiveCounts {
        {
            let mut total = StreamLiveCounts::default();
            for family in self.family_families.iter().copied() {
                let counts = self.dispatch_family_command(
                    Some(family),
                    "live-count query",
                    StreamDomainCommand::ReadLiveCounts,
                );
                let counts = match counts {
                    Ok(counts) => counts,
                    Err(error) => {
                        tracing::warn!(
                            domain = "stream",
                            family = family.id(),
                            error,
                            "Stream live-count query failed"
                        );
                        continue;
                    }
                };
                total.streams = total.streams.saturating_add(counts.streams);
                total.append_sessions =
                    total.append_sessions.saturating_add(counts.append_sessions);
                total.subscriptions = total.subscriptions.saturating_add(counts.subscriptions);
            }
            total
        }
    }

    fn dispatch_family_command<T>(
        &self,
        family: Option<RouteFamily>,
        operation: &'static str,
        build_command: impl FnOnce(crossbeam_channel::Sender<T>) -> StreamDomainCommand,
    ) -> Result<T, String> {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        self.dispatch_family_control(family, build_command(reply_tx))
            .map_err(|error| format!("enqueue Stream {operation}: {error}"))?;
        reply_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .map_err(|error| format!("receive Stream {operation}: {error}"))
    }

    fn dispatch_family_control(
        &self,
        family: Option<RouteFamily>,
        command: StreamDomainCommand,
    ) -> Result<(), String> {
        let family = family
            .or_else(|| self.family_families.first().copied())
            .ok_or_else(|| "no Stream route family is provisioned".to_string())?;
        if !self.family_families.contains(&family) {
            return Err("route family is not provisioned".to_string());
        }
        self.family_runtime
            .try_enqueue(family, crate::runtime::FamilyActorLane::Control, command)
            .map_err(|error| error.to_string())
    }
}
