use super::model::{
    route_triplet, AdminStreamReadRequest, AdminStreamReadRequestOwned, Arc, AtomicBool, AtomicU64,
    BTreeMap, Envelope, HashMap, Mutex, Ordering, PayloadEncoder, PendingStreamNotification,
    ReadResponse, ReadyStreamNotification, Route, RouteAddress, RouteFamily, Router, StreamActor,
    StreamActorKey, StreamAdminReadCommand, StreamAdminRecord, StreamAreaSnapshot,
    StreamClientResponseBody, StreamDomainActor, StreamDomainCommand, StreamDomainCore,
    StreamDomainRuntime, StreamDomainSink, StreamFilteredReason, StreamLiveCounts, StreamMetadata,
    StreamMetrics, StreamNotificationTarget, StreamReadExecution, StreamReadItem,
    StreamRealmSnapshot, StreamRecord, StreamStorageLayout, StreamStore, StreamVisibilityFrontier,
};
#[cfg(test)]
use crate::dispatch::protocol::FrameContext;
use crate::runtime::DeliveryError;

fn u64_to_usize_saturating(value: u64) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX)
}

fn usize_to_u32_saturating(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

fn usize_to_u64_saturating(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

type StreamAdminSnapshotMap =
    BTreeMap<(u64, String, String, String), crate::control::admin::StreamInfo>;
type StreamRealmSnapshotMap = BTreeMap<String, StreamRealmSnapshot>;
type StreamAreaSnapshotMap = BTreeMap<(String, String), StreamAreaSnapshot>;

mod domain_core_impl;

impl StreamDomainActor {
    pub(super) fn new(core: Arc<StreamDomainCore>) -> Self {
        Self { core }
    }

    pub(super) fn route_address() -> RouteAddress {
        RouteAddress::new(RouteFamily::new(0), Route::new("internal://domain/stream"))
    }

    pub(super) fn runtime(&self) -> StreamDomainRuntime<'_> {
        StreamDomainRuntime { core: &self.core }
    }
}

impl StreamDomainSink {
    pub fn new(
        store: Arc<cntryl_midge::Engine>,
        router: Arc<Router>,
        admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
        write_options: super::StreamStorageWriteOptions,
    ) -> Self {
        Self::new_with_storage(
            crate::storage::FitzStorageEngine::new(store),
            router,
            admin_read_model,
            write_options,
        )
    }

    pub(crate) fn new_with_storage(
        store: crate::storage::FitzStorageEngine,
        router: Arc<Router>,
        admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
        write_options: super::StreamStorageWriteOptions,
    ) -> Self {
        Self::new_with_storage_layout(
            store,
            router,
            admin_read_model,
            StreamStorageLayout::default(),
            write_options,
        )
        .expect("create stream domain sink with default stream layout")
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
            families: Mutex::new(HashMap::new()),
            next_sub_id: Arc::new(AtomicU64::new(1)),
            next_session_id: Arc::new(AtomicU64::new(1)),
            cursor_integrity_key: Arc::new(cursor_integrity_key),
            pending_notifications: Mutex::new(Vec::new()),
            router,
            admin_read_model,
            admin_snapshot_dirty: Arc::new(AtomicBool::new(true)),
            sync_write_mode: crate::domains::stream::protocol::StreamWriteMode::Sync,
            metrics: None,
            active: Arc::new(AtomicBool::new(true)),
            family_cores: Arc::new(Mutex::new(BTreeMap::new())),
            area_watermark_actors: Arc::new(crate::runtime::KeyedActorPool::new(
                router_for_watermark_actors.clone(),
                crate::domains::DOMAIN_ACTOR_MAILBOX_CAPACITY,
                crate::domains::stream::MAX_WATERMARK_COORDINATORS,
            )),
            realm_watermark_actors: Arc::new(crate::runtime::KeyedActorPool::new(
                router_for_watermark_actors,
                crate::domains::DOMAIN_ACTOR_MAILBOX_CAPACITY,
                crate::domains::stream::MAX_WATERMARK_COORDINATORS,
            )),
        });
        let actor = Self::spawn_actor(core.clone());
        let family_families = provisioned_families.map(<[RouteFamily]>::to_vec);
        let family_runtime = family_families
            .as_deref()
            .map(|families| Self::spawn_family_runtime(&core, families))
            .transpose()?;
        Ok(Self {
            core,
            actor,
            family_runtime,
            family_families,
        })
    }

    fn spawn_actor(
        core: Arc<StreamDomainCore>,
    ) -> crate::runtime::ManagedActor<StreamDomainCommand> {
        let router = core.router.clone();
        crate::runtime::ManagedActor::spawn_fail_closed(
            router,
            StreamDomainActor::route_address(),
            move || StreamDomainActor::new(core.clone()),
            crate::domains::DOMAIN_ACTOR_MAILBOX_CAPACITY,
        )
    }

    fn spawn_family_runtime(
        core: &Arc<StreamDomainCore>,
        families: &[RouteFamily],
    ) -> Result<crate::runtime::FamilyActorPoolRuntime<StreamDomainCommand>, String> {
        let pool = crate::runtime::FamilyActorPool::new(families)
            .map_err(|error| format!("create Stream family actor pool: {error}"))?;
        let active = core.active.clone();
        let core_for_factory = core.clone();
        Ok(crate::runtime::FamilyActorPoolRuntime::spawn(
            pool,
            active,
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
                #[cfg(test)]
                StreamDomainCommand::SyncAdminSnapshot(reply) => {
                    core.sync_admin_snapshot();
                    let _ = reply.send(());
                }
                #[cfg(test)]
                StreamDomainCommand::PanicForTests => {
                    panic!("test Stream family actor panic");
                }
            },
        ))
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
            families: Mutex::new(HashMap::new()),
            next_sub_id: shared.next_sub_id.clone(),
            next_session_id: shared.next_session_id.clone(),
            cursor_integrity_key: shared.cursor_integrity_key.clone(),
            pending_notifications: Mutex::new(Vec::new()),
            router: shared.router.clone(),
            admin_read_model: shared.admin_read_model.clone(),
            admin_snapshot_dirty: shared.admin_snapshot_dirty.clone(),
            sync_write_mode: shared.sync_write_mode,
            metrics: shared.metrics.clone(),
            active: shared.active.clone(),
            family_cores: shared.family_cores.clone(),
            area_watermark_actors: shared.area_watermark_actors.clone(),
            realm_watermark_actors: shared.realm_watermark_actors.clone(),
        });
        shared
            .family_cores
            .lock()
            .insert(family.as_u64(), Arc::downgrade(&family_core));
        family_core
    }

    fn stop_family_runtime(&mut self) {
        if let Some(runtime) = self.family_runtime.take() {
            runtime.stop();
        }
    }

    fn rebuild_actor(&mut self) {
        self.actor.stop();
        self.stop_family_runtime();
        self.actor = Self::spawn_actor(self.core.clone());
        self.family_runtime = self
            .family_families
            .as_deref()
            .map(|families| Self::spawn_family_runtime(&self.core, families))
            .transpose()
            .expect("validated Stream family actor pool configuration");
    }

    fn core_for_builder(&mut self) -> &mut StreamDomainCore {
        self.stop_family_runtime();
        Arc::get_mut(&mut self.core).expect("Stream sink builders must run before sharing the sink")
    }

    #[must_use]
    pub fn with_metrics(
        mut self,
        collector: crate::observability::metrics::MetricsCollector,
    ) -> Self {
        self.actor.stop();
        self.core_for_builder().metrics = Some(StreamMetrics::new(collector));
        self.core.refresh_metrics_gauges();
        self.rebuild_actor();
        self
    }

    pub fn stop(&self) {
        self.core.active.store(false, Ordering::Relaxed);
        if let Some(runtime) = self.family_runtime.as_ref() {
            runtime.stop();
        }
        self.actor.stop();
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
        request: AdminStreamReadRequest<'_>,
    ) -> Result<
        (
            Vec<StreamReadItem>,
            crate::domains::stream::protocol::ReadCursor,
        ),
        String,
    > {
        if let (Some(runtime), Some(families)) =
            (self.family_runtime.as_ref(), self.family_families.as_ref())
        {
            let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
            let family = request.family;
            let command = StreamDomainCommand::ReadResourceRecords(StreamAdminReadCommand {
                request: AdminStreamReadRequestOwned {
                    family,
                    realm: request.realm.to_owned(),
                    area: request.area.to_owned(),
                    resource: request.resource.to_owned(),
                    from_offset: request.from_offset,
                    limit: request.limit,
                    discriminator: request.discriminator.clone(),
                },
                reply: reply_tx,
            });
            if !families.contains(&family) {
                return Err("route family is not provisioned".to_string());
            }
            runtime
                .try_enqueue(family, crate::runtime::FamilyActorLane::Control, command)
                .map_err(|error| format!("enqueue Stream admin read: {error}"))?;
            return reply_rx
                .recv_timeout(std::time::Duration::from_secs(1))
                .map_err(|error| format!("receive Stream admin read: {error}"))?;
        }

        self.core.admin_read_resource_records(request)
    }

    #[cfg(test)]
    pub(super) fn is_actor_running(&self) -> bool {
        self.actor.is_running()
            && self
                .family_runtime
                .as_ref()
                .is_none_or(crate::runtime::FamilyActorPoolRuntime::is_running)
    }

    pub(crate) fn actor_health_snapshot(&self) -> crate::runtime::ManagedActorHealthSnapshot {
        self.family_runtime.as_ref().map_or_else(
            || self.actor.health_snapshot(),
            crate::runtime::FamilyActorPoolRuntime::health_snapshot,
        )
    }

    #[cfg(test)]
    pub(super) fn fail_next_promotion_frontier_commit_for_tests(&self) {
        self.core
            .stream_store
            .fail_next_promotion_frontier_commit_for_tests();
    }

    #[cfg(test)]
    pub(crate) fn panic_actor_for_tests(&self) {
        if let (Some(runtime), Some(family)) = (
            self.family_runtime.as_ref(),
            self.family_families
                .as_ref()
                .and_then(|families| families.first()),
        ) {
            let _ = runtime.try_enqueue(
                *family,
                crate::runtime::FamilyActorLane::Control,
                StreamDomainCommand::PanicForTests,
            );
            return;
        }
        let _ = self
            .actor
            .try_send_high_priority(StreamDomainCommand::PanicForTests);
    }

    #[cfg(test)]
    pub(super) fn stop_actor_for_tests(&self) {
        self.actor.stop();
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

    #[cfg(test)]
    pub(super) fn sync_admin_snapshot(&self) {
        self.send_admin_snapshot_command(StreamDomainCommand::SyncAdminSnapshot, "sync");
    }

    fn send_admin_snapshot_command(
        &self,
        build_command: fn(crossbeam_channel::Sender<()>) -> StreamDomainCommand,
        operation: &'static str,
    ) {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        if let (Some(runtime), Some(family)) = (
            self.family_runtime.as_ref(),
            self.family_families
                .as_ref()
                .and_then(|families| families.first()),
        ) {
            if let Err(error) = runtime.try_enqueue(
                *family,
                crate::runtime::FamilyActorLane::Control,
                build_command(reply_tx),
            ) {
                tracing::warn!(
                    domain = "stream",
                    error = %error,
                    operation,
                    "Stream admin snapshot command enqueue failed"
                );
                return;
            }
            if let Err(error) = reply_rx.recv_timeout(std::time::Duration::from_secs(1)) {
                tracing::warn!(
                    domain = "stream",
                    error = %error,
                    operation,
                    "Stream admin snapshot command reply failed"
                );
            }
            return;
        }

        if let Err(error) = self.actor.try_send_high_priority(build_command(reply_tx)) {
            tracing::warn!(
                domain = "stream",
                error = %error,
                operation,
                "Stream admin snapshot command enqueue failed"
            );
            return;
        }

        if let Err(error) = reply_rx.recv_timeout(std::time::Duration::from_secs(1)) {
            tracing::warn!(
                domain = "stream",
                error = %error,
                operation,
                "Stream admin snapshot command reply failed"
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
        if let (Some(runtime), Some(families)) =
            (self.family_runtime.as_ref(), self.family_families.as_ref())
        {
            let mut total = StreamLiveCounts::default();
            for family in families.iter().copied() {
                let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
                if let Err(error) = runtime.try_enqueue(
                    family,
                    crate::runtime::FamilyActorLane::Control,
                    StreamDomainCommand::ReadLiveCounts(reply_tx),
                ) {
                    tracing::warn!(
                        domain = "stream",
                        family = family.id(),
                        error = %error,
                        "Stream live-count query enqueue failed"
                    );
                    continue;
                }
                let counts = reply_rx.recv_timeout(std::time::Duration::from_secs(1));
                if let Ok(counts) = counts {
                    total.streams = total.streams.saturating_add(counts.streams);
                    total.append_sessions =
                        total.append_sessions.saturating_add(counts.append_sessions);
                    total.subscriptions = total.subscriptions.saturating_add(counts.subscriptions);
                }
            }
            return total;
        }

        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        if let Err(error) = self
            .actor
            .try_send_high_priority(StreamDomainCommand::ReadLiveCounts(reply_tx))
        {
            tracing::warn!(domain = "stream", error = %error, "Stream live-count query enqueue failed");
            return StreamLiveCounts::default();
        }

        reply_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap_or_default()
    }
}
