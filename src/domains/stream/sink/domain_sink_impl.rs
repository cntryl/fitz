use super::model::{
    route_triplet, AdminStreamReadRequest, Arc, AtomicBool, AtomicU64, BTreeMap, Envelope, HashMap,
    Mutex, Ordering, PayloadEncoder, ReadResponse, Route, RouteAddress, RouteFamily, Router,
    StreamActor, StreamActorKey, StreamAdminRecord, StreamAreaSnapshot, StreamClientResponseBody,
    StreamDomainActor, StreamDomainCommand, StreamDomainCore, StreamDomainRuntime,
    StreamDomainSink, StreamFilteredReason, StreamLiveCounts, StreamMetadata, StreamMetrics,
    StreamReadItem, StreamRealmSnapshot, StreamRecord, StreamStorageLayout, StreamStore,
    StreamSubscription,
};
#[cfg(test)]
use crate::protocol::FrameContext;

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
    ) -> Self {
        Self::new_with_storage(
            crate::storage::FitzStorageEngine::new(store),
            router,
            admin_read_model,
        )
    }

    pub(crate) fn new_with_storage(
        store: crate::storage::FitzStorageEngine,
        router: Arc<Router>,
        admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
    ) -> Self {
        Self::new_with_storage_layout(
            store,
            router,
            admin_read_model,
            StreamStorageLayout::default(),
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
    ) -> Result<Self, String> {
        Self::new_with_storage_layout(
            crate::storage::FitzStorageEngine::new(store),
            router,
            admin_read_model,
            stream_storage_layout,
        )
    }

    pub(crate) fn new_with_storage_layout(
        store: crate::storage::FitzStorageEngine,
        router: Arc<Router>,
        admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
        stream_storage_layout: StreamStorageLayout,
    ) -> Result<Self, String> {
        let stream_store = Arc::new(StreamStore::with_storage_layout(
            store.clone(),
            stream_storage_layout,
        ));
        stream_store.ensure_layout_activation_for_existing_families()?;
        stream_store.validate_persisted_state_for_existing_families()?;

        let core = Arc::new(StreamDomainCore {
            stream_store,
            store,
            actors: Mutex::new(HashMap::new()),
            session_owners: Mutex::new(HashMap::new()),
            families: Mutex::new(HashMap::new()),
            next_sub_id: AtomicU64::new(1),
            next_session_id: AtomicU64::new(1),
            router,
            admin_read_model,
            admin_snapshot_dirty: AtomicBool::new(true),
            sync_write_mode: crate::domains::stream::protocol::StreamWriteMode::Sync,
            metrics: None,
            active: AtomicBool::new(true),
        });
        let actor = Self::spawn_actor(core.clone());
        Ok(Self { core, actor })
    }

    fn spawn_actor(
        core: Arc<StreamDomainCore>,
    ) -> crate::runtime::ManagedActor<StreamDomainCommand> {
        let router = core.router.clone();
        crate::runtime::ManagedActor::spawn_supervised(
            router,
            StreamDomainActor::route_address(),
            move || StreamDomainActor::new(core.clone()),
            1024,
        )
    }

    fn rebuild_actor(&mut self) {
        self.actor.stop();
        self.actor = Self::spawn_actor(self.core.clone());
    }

    fn core_for_builder(&mut self) -> &mut StreamDomainCore {
        Arc::get_mut(&mut self.core).expect("Stream sink builders must run before sharing the sink")
    }

    #[must_use]
    pub fn with_sync_write_options(mut self, write_options: cntryl_midge::WriteOptions) -> Self {
        self.actor.stop();
        self.core_for_builder().sync_write_mode = if write_options.is_cloud_strict() {
            crate::domains::stream::protocol::StreamWriteMode::CloudStrict
        } else {
            crate::domains::stream::protocol::StreamWriteMode::Sync
        };
        self.rebuild_actor();
        self
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
        self.core.admin_read_resource_records(request)
    }

    #[cfg(test)]
    pub(super) fn is_actor_running(&self) -> bool {
        self.actor.is_running()
    }

    pub(crate) fn actor_health_snapshot(&self) -> crate::runtime::ManagedActorHealthSnapshot {
        self.actor.health_snapshot()
    }

    #[cfg(test)]
    pub(crate) fn panic_actor_for_tests(&self) {
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
        self.core
            .encode_read_response_data(family, route, from_offset, limit, max_bytes, filter)
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
