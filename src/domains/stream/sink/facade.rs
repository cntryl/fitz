#[cfg(test)]
use super::model::StreamReadExecution;
use super::model::{
    AdminSnapshotState, AdminStreamReadRequest, AdminStreamReadRequestOwned, Arc, AtomicBool,
    AtomicU64, BTreeMap, CleanedUpSessions, HashMap, Mutex, Ordering, RouteFamily, Router,
    StreamAdminReadCommand, StreamDomainCommand, StreamDomainCore, StreamDomainSink,
    StreamDurableMetrics, StreamLiveCounts, StreamMetrics, StreamReadItem, StreamStorageLayout,
    StreamStore, SubscriptionRegistry, WatermarkCoordinators,
};
#[cfg(test)]
use crate::runtime::routing::Route;
use crate::runtime::DeliveryError;

impl StreamDomainSink {
    /// Construct a Stream sink using the default storage layout.
    ///
    /// # Errors
    ///
    /// Returns an initialization error when storage activation, persisted
    /// state validation, or cursor-key generation fails.
    pub fn try_new(
        store: Arc<cntryl_midge::Engine>,
        router: Arc<Router>,
        admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
        write_options: super::StreamStorageWriteOptions,
    ) -> Result<Self, super::StreamSinkInitError> {
        Self::new_with_storage_layout(
            crate::storage::FitzStorageEngine::new(store),
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
    pub fn new_with_layout(
        store: Arc<cntryl_midge::Engine>,
        router: Arc<Router>,
        admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
        stream_storage_layout: StreamStorageLayout,
        write_options: super::StreamStorageWriteOptions,
    ) -> Result<Self, String> {
        Self::new_with_storage_layout(
            crate::storage::FitzStorageEngine::new(store),
            router,
            admin_read_model,
            stream_storage_layout,
            write_options,
        )
    }

    pub(crate) fn new_with_storage_layout(
        store: crate::storage::FitzStorageEngine,
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
        store: crate::storage::FitzStorageEngine,
        router: Arc<Router>,
        admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
        stream_storage_layout: StreamStorageLayout,
        provisioned_families: Option<&[RouteFamily]>,
        write_options: super::StreamStorageWriteOptions,
    ) -> Result<Self, String> {
        let stream_store = Arc::new(
            StreamStore::with_storage_layout(store.clone(), stream_storage_layout)
                .with_write_options(write_options.sync_intent(), write_options.buffered_intent()),
        );
        stream_store.ensure_layout_activation_for_existing_families()?;
        stream_store.validate_persisted_state_for_existing_families()?;
        let mut cursor_integrity_key = [0u8; 32];
        getrandom::fill(&mut cursor_integrity_key)
            .map_err(|error| format!("generate Stream cursor integrity key failed: {error}"))?;

        let router_for_watermark_actors = router.clone();
        let core = Arc::new(StreamDomainCore {
            stream_store,
            store,
            actors: Mutex::new(HashMap::new()),
            session_owners: Mutex::new(HashMap::new()),
            cleaned_up_sessions: Mutex::new(CleanedUpSessions::new(
                crate::domains::DOMAIN_ACTOR_MAILBOX_CAPACITY,
            )),
            subscriptions: SubscriptionRegistry::new(Arc::new(AtomicU64::new(1))),
            next_session_id: Arc::new(AtomicU64::new(1)),
            cursor_integrity_key: Arc::new(cursor_integrity_key),
            router,
            admin_snapshot: AdminSnapshotState::new(
                admin_read_model,
                Arc::new(AtomicBool::new(true)),
            ),
            sync_write_mode: crate::domains::stream::protocol::StreamWriteMode::Sync,
            metrics: None,
            durable_metrics: Arc::new(StreamDurableMetrics::default()),
            active: Arc::new(AtomicBool::new(true)),
            family_cores: Arc::new(Mutex::new(BTreeMap::new())),
            watermark_coordinators: WatermarkCoordinators {
                area: Arc::new(crate::runtime::KeyedActorPool::new(
                    router_for_watermark_actors.clone(),
                    crate::domains::DOMAIN_ACTOR_MAILBOX_CAPACITY,
                    crate::domains::stream::MAX_WATERMARK_COORDINATORS,
                )),
                realm: Arc::new(crate::runtime::KeyedActorPool::new(
                    router_for_watermark_actors,
                    crate::domains::DOMAIN_ACTOR_MAILBOX_CAPACITY,
                    crate::domains::stream::MAX_WATERMARK_COORDINATORS,
                )),
            },
        });
        let family_families =
            provisioned_families.map_or_else(|| vec![RouteFamily::new(1)], <[RouteFamily]>::to_vec);
        let family_runtime = Self::spawn_family_runtime(&core, &family_families)?;
        let core = Self::primary_family_core(&core, &family_families);
        Ok(Self {
            core,
            family_runtime,
            family_families,
        })
    }

    fn spawn_family_runtime(
        core: &Arc<StreamDomainCore>,
        families: &[RouteFamily],
    ) -> Result<crate::runtime::FamilyActorPoolRuntime<StreamDomainCommand>, String> {
        let pool = crate::runtime::FamilyActorPool::new(families)
            .map_err(|error| format!("create Stream family actor pool: {error}"))?;
        let core_for_factory = core.clone();
        Ok(
            crate::runtime::FamilyActorPoolRuntime::spawn_with_family_failed_metric(
                pool,
                core.active.clone(),
                move |family| Self::family_core_for(&core_for_factory, family),
                |core, family, _lane, command| match command {
                    StreamDomainCommand::Deliver(envelope, reply) => {
                        let result = if *envelope.destination().family() == family {
                            core.deliver_envelope(&envelope)
                        } else {
                            Err(DeliveryError::ActorStopped)
                        };
                        let _ = reply.send(result);
                    }
                    StreamDomainCommand::ReadLiveCounts(reply) => {
                        let _ = reply.send(core.live_counts());
                    }
                    StreamDomainCommand::ReadResourceRecords(command) => {
                        let request = command.request.as_borrowed();
                        let _ = command
                            .reply
                            .send(core.admin_read_resource_records(request));
                    }
                    StreamDomainCommand::RefreshAdminSnapshotIfDirty(reply) => {
                        core.refresh_admin_snapshot_if_dirty();
                        let _ = reply.send(());
                    }
                    StreamDomainCommand::RunMaintenance {
                        family: requested_family,
                        reply,
                    } => {
                        if requested_family == family.as_u64() {
                            core.run_maintenance_slice(requested_family);
                        }
                        if let Some(reply) = reply {
                            let _ = reply.send(());
                        }
                    }
                    #[cfg(test)]
                    StreamDomainCommand::SyncAdminSnapshot(reply) => {
                        core.sync_admin_snapshot();
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
                },
                crate::domains::stream::metrics::METRIC_FAMILY_FAILED_CLOSED_TOTAL,
            ),
        )
    }

    fn family_core_for(
        shared: &Arc<StreamDomainCore>,
        family: RouteFamily,
    ) -> Arc<StreamDomainCore> {
        let family_core = Arc::new(StreamDomainCore {
            store: shared.store.clone(),
            stream_store: shared.stream_store.clone(),
            actors: Mutex::new(HashMap::new()),
            session_owners: Mutex::new(HashMap::new()),
            cleaned_up_sessions: Mutex::new(CleanedUpSessions::new(
                crate::domains::DOMAIN_ACTOR_MAILBOX_CAPACITY,
            )),
            subscriptions: SubscriptionRegistry::new(shared.subscriptions.next_id.clone()),
            next_session_id: shared.next_session_id.clone(),
            cursor_integrity_key: shared.cursor_integrity_key.clone(),
            router: shared.router.clone(),
            admin_snapshot: AdminSnapshotState::new(
                shared.admin_snapshot.read_model.clone(),
                shared.admin_snapshot.dirty.clone(),
            ),
            sync_write_mode: shared.sync_write_mode,
            metrics: shared.metrics.clone(),
            durable_metrics: shared.durable_metrics.clone(),
            active: shared.active.clone(),
            family_cores: shared.family_cores.clone(),
            watermark_coordinators: WatermarkCoordinators {
                area: shared.watermark_coordinators.area.clone(),
                realm: shared.watermark_coordinators.realm.clone(),
            },
        });
        shared
            .family_cores
            .lock()
            .insert(family.as_u64(), Arc::downgrade(&family_core));
        family_core
    }

    fn primary_family_core(
        shared: &Arc<StreamDomainCore>,
        families: &[RouteFamily],
    ) -> Arc<StreamDomainCore> {
        let primary = families
            .first()
            .expect("validated Stream family inventory is non-empty");
        shared
            .family_cores
            .lock()
            .get(&primary.as_u64())
            .and_then(std::sync::Weak::upgrade)
            .expect("Stream primary family core was created with its runtime")
    }

    fn rebuild_actor(&mut self) {
        self.family_runtime.stop();
        self.family_runtime = Self::spawn_family_runtime(&self.core, &self.family_families)
            .expect("validated Stream family actor pool configuration");
        self.core = Self::primary_family_core(&self.core, &self.family_families);
    }

    fn core_for_builder(&mut self) -> &mut StreamDomainCore {
        self.family_runtime.stop();
        if let Some(primary) = self.family_families.first() {
            self.core.family_cores.lock().remove(&primary.as_u64());
        }
        Arc::get_mut(&mut self.core).expect("Stream sink builders must run before sharing the sink")
    }

    #[must_use]
    pub fn with_metrics(
        mut self,
        collector: crate::observability::metrics::MetricsCollector,
    ) -> Self {
        self.core_for_builder().metrics = Some(StreamMetrics::new(collector));
        self.core.refresh_metrics_gauges();
        self.rebuild_actor();
        self
    }

    pub fn stop(&self) {
        self.core.active.store(false, Ordering::Relaxed);
        self.family_runtime.stop();
    }

    pub(crate) fn is_active(&self) -> bool {
        self.core.active.load(Ordering::Relaxed)
    }

    pub(crate) fn run_maintenance_slice(&self) {
        let families: Vec<u64> = self
            .family_families
            .iter()
            .map(RouteFamily::as_u64)
            .collect();
        for family in families {
            if !self.core.stream_store.has_pending_maintenance(family) {
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
    pub fn storage_layout(&self) -> StreamStorageLayout {
        self.core.storage_layout()
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

    pub(crate) fn actor_health_snapshot(&self) -> crate::runtime::ManagedActorHealthSnapshot {
        self.family_runtime.managed_actor_health_snapshot()
    }

    #[cfg(test)]
    pub(super) fn fail_next_promotion_frontier_commit_for_tests(&self) {
        self.core
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
        self.core.sync_write_mode
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
        self.core
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
        self.core
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
        self.core
            .stream_store
            .get_watermark(family.as_u64(), realm, area)
    }

    #[cfg(test)]
    pub(super) fn get_realm_watermark_for_tests(
        &self,
        family: RouteFamily,
        realm: &str,
    ) -> Result<u64, String> {
        self.core
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
        let mut txn = self
            .core
            .store
            .begin_tx(family.id(), cntryl_midge::TransactionMode::ReadWrite)
            .map_err(|error| error.to_string())?;
        txn.delete(
            crate::domains::stream::storage::encode_compact_resource_page_key(
                realm,
                area,
                resource,
                page_start_offset,
            ),
        )
        .map_err(|error| error.to_string())?;
        txn.commit(cntryl_midge::WriteOptions::sync())
            .map_err(|error| error.to_string())
    }

    #[cfg(test)]
    pub(super) fn encode_metadata_response_data_for_tests(
        &self,
        family: RouteFamily,
        route: &Route,
    ) -> Result<Vec<u8>, String> {
        self.core.encode_metadata_response_data(family, route)
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
        self.core.encode_read_response_data(StreamReadExecution {
            family_id: family,
            route,
            from_offset,
            limit,
            max_bytes,
            filter,
            cursor_fingerprint: None,
            captured_watermark: None,
        })
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
        self.core.durable_metrics.snapshot()
    }

    pub(crate) fn initialize_admin_snapshot(&self) {
        self.core.refresh_admin_snapshot_if_dirty();
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
