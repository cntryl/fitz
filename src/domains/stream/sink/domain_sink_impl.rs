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

impl StreamDomainCore {
    fn storage_layout(&self) -> StreamStorageLayout {
        self.stream_store.storage_layout()
    }

    pub(super) fn actor_key_for_route(
        family_id: RouteFamily,
        route: &Route,
    ) -> Result<StreamActorKey, String> {
        let parts =
            route_triplet(route.as_str()).ok_or_else(|| "invalid stream route".to_string())?;
        if parts.area == crate::domains::stream::INTERNAL_REALM_SEGMENT {
            return Err(format!(
                "area '{}' is reserved for internal broker use",
                crate::domains::stream::INTERNAL_REALM_SEGMENT
            ));
        }
        Ok(StreamActorKey {
            family_id: family_id.as_u64(),
            realm: parts.realm.to_string(),
            area: parts.area.to_string(),
            resource: parts.resource.to_string(),
        })
    }

    pub(super) fn get_or_create_actor(
        &self,
        key: &StreamActorKey,
    ) -> Result<Arc<Mutex<StreamActor>>, String> {
        use std::collections::hash_map::Entry;

        let mut actors = self.actors.lock();
        match actors.entry(key.clone()) {
            Entry::Occupied(entry) => Ok(entry.get().clone()),
            Entry::Vacant(entry) => {
                let actor = Arc::new(Mutex::new(StreamActor::new(
                    RouteFamily::new(key.family_id),
                    key.realm.clone(),
                    key.area.clone(),
                    key.resource.clone(),
                    self.stream_store.clone(),
                )?));
                entry.insert(actor.clone());
                Ok(actor)
            }
        }
    }

    pub(super) fn mark_admin_snapshot_dirty(&self) {
        self.admin_snapshot_dirty.store(true, Ordering::Relaxed);
        self.refresh_metrics_gauges();
    }

    pub(super) fn refresh_metrics_gauges(&self) {
        let counts = self.live_counts();

        if let Some(metrics) = &self.metrics {
            metrics.set_stream_count(counts.streams);
            metrics.set_subscription_count(counts.subscriptions);
            metrics.set_append_session_count(counts.append_sessions);
        } else {
            crate::observability::gauge_set("fitz_stream_active_gauge", counts.streams as u64);
            crate::observability::gauge_set(
                "fitz_stream_subscriptions_gauge",
                counts.subscriptions as u64,
            );
            crate::observability::gauge_set(
                "fitz_stream_append_sessions_active",
                counts.append_sessions as u64,
            );
        }
    }

    pub(super) fn counter_inc(&self, name: &str) {
        if let Some(metrics) = &self.metrics {
            metrics.counter_inc(name);
        } else {
            crate::observability::counter_inc(name);
        }
    }

    pub(super) fn counter_add(&self, name: &str, amount: u64) {
        if let Some(metrics) = &self.metrics {
            metrics.counter_add(name, amount);
        } else {
            crate::observability::counter_add(name, amount);
        }
    }

    pub(super) fn stream_response_is_failure(response: &StreamClientResponseBody) -> bool {
        matches!(response, StreamClientResponseBody::Error(_))
    }

    pub(super) fn refresh_admin_snapshot_if_dirty(&self) {
        if self.admin_snapshot_dirty.swap(false, Ordering::AcqRel) {
            self.sync_admin_snapshot();
        }
    }

    /// # Errors
    ///
    /// Returns an error if the requested route cannot be read or if the stream
    /// store rejects the read parameters.
    fn admin_read_resource_records(
        &self,
        request: AdminStreamReadRequest<'_>,
    ) -> Result<
        (
            Vec<StreamReadItem>,
            crate::domains::stream::protocol::ReadCursor,
        ),
        String,
    > {
        let filter =
            request
                .discriminator
                .map(|value| crate::domains::stream::protocol::StreamFilterSet {
                    clauses: vec![
                        crate::domains::stream::protocol::StreamFilterClause::Equals(value),
                    ],
                });
        let params = crate::domains::stream::store::ReadResourceParams {
            family: request.family.as_u64(),
            realm: request.realm,
            area: request.area,
            resource: request.resource,
            from_offset: request.from_offset,
            limit: request.limit,
            max_bytes: None,
        };

        self.stream_store
            .read_resource_with_filter(&params, filter.as_ref())
    }

    pub(super) fn sync_admin_snapshot(&self) {
        let (mut streams, realm_snapshots, area_snapshots, committed_events_total) =
            self.collect_committed_stream_snapshots();
        let stream_realm_watermarks = self.collect_stream_realm_watermarks(realm_snapshots);
        let stream_area_watermarks = self.collect_stream_area_watermarks(area_snapshots);
        self.overlay_live_actor_snapshots(&mut streams);
        self.publish_admin_snapshot(
            streams,
            stream_realm_watermarks,
            stream_area_watermarks,
            committed_events_total,
        );
    }

    fn collect_committed_stream_snapshots(
        &self,
    ) -> (
        StreamAdminSnapshotMap,
        StreamRealmSnapshotMap,
        StreamAreaSnapshotMap,
        usize,
    ) {
        let mut streams: StreamAdminSnapshotMap = BTreeMap::new();
        let mut realm_snapshots: StreamRealmSnapshotMap = BTreeMap::new();
        let mut area_snapshots: StreamAreaSnapshotMap = BTreeMap::new();
        let mut committed_events_total = 0usize;

        if let Ok(families) = self.store.list_column_families() {
            for family in families {
                let family_id = u64::from(family.id());
                if let Ok(records) = self.stream_store.list_resource_metadata(family_id) {
                    for StreamAdminRecord {
                        realm,
                        area,
                        resource,
                        next_offset,
                        committed_size_bytes,
                    } in records
                    {
                        committed_events_total = committed_events_total
                            .saturating_add(u64_to_usize_saturating(next_offset));
                        let last_offset = next_offset.saturating_sub(1);
                        streams.insert(
                            (family_id, realm.clone(), area.clone(), resource.clone()),
                            crate::control::admin::StreamInfo::snapshot(
                                crate::control::admin::StreamInfoSnapshot {
                                    route_family: family_id,
                                    realm: &realm,
                                    area: &area,
                                    resource: &resource,
                                    offset: last_offset,
                                    watermark: last_offset,
                                    size_bytes: committed_size_bytes,
                                    sessions_active: 0,
                                },
                            ),
                        );

                        let realm_snapshot = realm_snapshots.entry(realm.clone()).or_default();
                        realm_snapshot.areas.insert(area.clone());
                        realm_snapshot.resource_count =
                            realm_snapshot.resource_count.saturating_add(1);
                        realm_snapshot.families.insert(family_id);

                        let area_snapshot = area_snapshots
                            .entry((realm.clone(), area.clone()))
                            .or_default();
                        area_snapshot.resource_count =
                            area_snapshot.resource_count.saturating_add(1);
                        area_snapshot.families.insert(family_id);
                    }
                }
            }
        }

        (
            streams,
            realm_snapshots,
            area_snapshots,
            committed_events_total,
        )
    }

    fn collect_stream_realm_watermarks(
        &self,
        realm_snapshots: StreamRealmSnapshotMap,
    ) -> Vec<crate::control::admin::StreamRealmWatermarkDetail> {
        realm_snapshots
            .into_iter()
            .map(|(realm, snapshot)| {
                let family_watermarks = snapshot
                    .families
                    .into_iter()
                    .filter_map(|family_id| {
                        self.stream_store
                            .get_realm_watermark(family_id, &realm)
                            .ok()
                            .map(|watermark| {
                                crate::control::admin::StreamRealmWatermark::snapshot(
                                    family_id, watermark,
                                )
                            })
                    })
                    .collect();

                crate::control::admin::StreamRealmWatermarkDetail::snapshot(
                    &realm,
                    snapshot.areas.len(),
                    snapshot.resource_count,
                    family_watermarks,
                )
            })
            .collect()
    }

    fn collect_stream_area_watermarks(
        &self,
        area_snapshots: StreamAreaSnapshotMap,
    ) -> Vec<crate::control::admin::StreamAreaWatermarkDetail> {
        area_snapshots
            .into_iter()
            .map(|((realm, area), snapshot)| {
                let family_watermarks = snapshot
                    .families
                    .into_iter()
                    .filter_map(|family_id| {
                        self.stream_store
                            .get_watermark(family_id, &realm, &area)
                            .ok()
                            .map(|watermark| {
                                crate::control::admin::StreamAreaWatermark::snapshot(
                                    family_id, watermark,
                                )
                            })
                    })
                    .collect();

                crate::control::admin::StreamAreaWatermarkDetail::snapshot(
                    &realm,
                    &area,
                    snapshot.resource_count,
                    family_watermarks,
                )
            })
            .collect()
    }

    fn overlay_live_actor_snapshots(&self, streams: &mut StreamAdminSnapshotMap) {
        let actors = self.actors.lock();
        for (key, actor) in actors.iter() {
            let actor = actor.lock();
            let last_offset = actor
                .metadata()
                .ok()
                .and_then(|response| response.metadata.last_resource_offset);
            let sessions_active = usize::from(actor.has_active_session());
            let stream_key = (
                key.family_id,
                key.realm.clone(),
                key.area.clone(),
                key.resource.clone(),
            );
            let committed_snapshot = streams.get(&stream_key);
            if committed_snapshot.is_none() && last_offset.is_none() {
                continue;
            }
            let committed_size_bytes = committed_snapshot.map_or(0, |item| item.size_bytes);
            let committed_offset = committed_snapshot.map(|item| item.offset);
            let visible_offset = last_offset.or(committed_offset).unwrap_or(0);

            streams.insert(
                stream_key,
                crate::control::admin::StreamInfo::snapshot(
                    crate::control::admin::StreamInfoSnapshot {
                        route_family: key.family_id,
                        realm: &key.realm,
                        area: &key.area,
                        resource: &key.resource,
                        offset: visible_offset,
                        watermark: visible_offset,
                        size_bytes: committed_size_bytes,
                        sessions_active,
                    },
                ),
            );
        }
    }

    fn publish_admin_snapshot(
        &self,
        streams: StreamAdminSnapshotMap,
        stream_realm_watermarks: Vec<crate::control::admin::StreamRealmWatermarkDetail>,
        stream_area_watermarks: Vec<crate::control::admin::StreamAreaWatermarkDetail>,
        committed_events_total: usize,
    ) {
        self.admin_read_model
            .replace_streams(streams.into_values().collect());
        self.admin_read_model
            .replace_stream_realm_watermarks(stream_realm_watermarks);
        self.admin_read_model
            .replace_stream_area_watermarks(stream_area_watermarks);
        self.admin_read_model
            .replace_stream_events_total(committed_events_total);
    }

    pub(super) fn encode_optional_bytes(
        encoder: &mut PayloadEncoder,
        value: Option<&bytes::Bytes>,
    ) {
        match value {
            Some(bytes) => {
                encoder.put_u8(1);
                encoder.put_bytes(bytes.as_ref());
            }
            None => encoder.put_u8(0),
        }
    }

    pub(super) fn encode_stream_record(encoder: &mut PayloadEncoder, record: &StreamRecord) {
        encoder.put_u64(record.resource_offset);
        encoder.put_optional_u64(record.area_offset);
        encoder.put_optional_u64(record.realm_offset);
        encoder.put_bytes(record.body.as_ref());
        Self::encode_optional_bytes(encoder, record.metadata.as_ref());
        encoder.put_u64(record.created_at);
    }

    pub(super) fn encode_stream_filtered_reason(
        encoder: &mut PayloadEncoder,
        reason: Option<&StreamFilteredReason>,
    ) {
        match reason {
            Some(StreamFilteredReason::ServerFilter) => encoder.put_u8(1),
            Some(StreamFilteredReason::Permission) => encoder.put_u8(2),
            Some(StreamFilteredReason::Projection) => encoder.put_u8(3),
            None => encoder.put_u8(0),
        }
    }

    pub(super) fn encode_stream_read_item(encoder: &mut PayloadEncoder, item: &StreamReadItem) {
        match item {
            StreamReadItem::Event(record) => {
                encoder.put_u8(0);
                Self::encode_stream_record(encoder, record);
            }
            StreamReadItem::Filtered { offset, reason } => {
                encoder.put_u8(1);
                encoder.put_u64(*offset);
                Self::encode_stream_filtered_reason(encoder, reason.as_ref());
            }
            StreamReadItem::FilteredRange {
                from_offset,
                to_offset,
                reason,
            } => {
                encoder.put_u8(2);
                encoder.put_u64(*from_offset);
                encoder.put_u64(*to_offset);
                Self::encode_stream_filtered_reason(encoder, reason.as_ref());
            }
        }
    }

    pub(super) fn encode_stream_cursor(
        encoder: &mut PayloadEncoder,
        cursor: &crate::domains::stream::protocol::ReadCursor,
    ) {
        encoder.put_u64(cursor.last_resource_offset);
        encoder.put_optional_u64(cursor.last_area_offset);
        encoder.put_optional_u64(cursor.last_realm_offset);
        encoder.put_u8(u8::from(cursor.has_more));
    }

    pub(super) fn encode_stream_read_data(
        items: &[StreamReadItem],
        cursor: &crate::domains::stream::protocol::ReadCursor,
    ) -> Vec<u8> {
        let mut encoder = PayloadEncoder::new();
        encoder.put_u32(usize_to_u32_saturating(items.len()));
        for item in items {
            Self::encode_stream_read_item(&mut encoder, item);
        }
        Self::encode_stream_cursor(&mut encoder, cursor);
        encoder.finish()
    }

    pub(super) fn encode_stream_last_data(record: &StreamRecord) -> Vec<u8> {
        let mut encoder = PayloadEncoder::new();
        Self::encode_stream_record(&mut encoder, record);
        encoder.finish()
    }

    pub(super) fn encode_stream_metadata_data(metadata: &StreamMetadata) -> Vec<u8> {
        let mut encoder = PayloadEncoder::new();
        encoder.put_optional_u64(metadata.first_resource_offset);
        encoder.put_optional_u64(metadata.last_resource_offset);
        encoder.put_u64(metadata.resource_count);
        encoder.put_u64(usize_to_u64_saturating(metadata.max_batch_events));
        encoder.put_u64(usize_to_u64_saturating(metadata.max_batch_bytes));
        encoder.put_optional_u64(metadata.ttl_seconds);
        encoder.put_u64(metadata.area_watermark);
        encoder.put_u64(metadata.realm_watermark);
        encoder.finish()
    }

    pub(super) fn encode_stream_commit_notify_payload(
        first_resource_offset: u64,
        last_resource_offset: u64,
        first_area_offset: u64,
        last_area_offset: u64,
        first_realm_offset: u64,
        last_realm_offset: u64,
        batch_size: usize,
    ) -> bytes::Bytes {
        bytes::Bytes::from(
            serde_json::json!({
                "event": "committed",
                "first_resource_offset": first_resource_offset,
                "last_resource_offset": last_resource_offset,
                "first_area_offset": first_area_offset,
                "last_area_offset": last_area_offset,
                "first_realm_offset": first_realm_offset,
                "last_realm_offset": last_realm_offset,
                "batch_size": batch_size,
            })
            .to_string(),
        )
    }

    pub(super) fn stream_error_response(error: impl Into<String>) -> StreamClientResponseBody {
        StreamClientResponseBody::Error(error.into())
    }

    pub(super) fn encode_read_response_data(
        &self,
        family_id: RouteFamily,
        route: &Route,
        from_offset: u64,
        limit: u64,
        max_bytes: Option<usize>,
        filter: Option<&crate::domains::stream::protocol::StreamFilterSet>,
    ) -> Result<Vec<u8>, String> {
        let parts =
            route_triplet(route.as_str()).ok_or_else(|| "invalid stream route".to_string())?;

        if limit == 0 {
            let cursor = if parts.area == "*" && parts.resource == "*" {
                crate::domains::stream::protocol::ReadCursor {
                    last_resource_offset: 0,
                    last_area_offset: None,
                    last_realm_offset: Some(from_offset),
                    has_more: false,
                }
            } else if parts.resource == "*" {
                crate::domains::stream::protocol::ReadCursor {
                    last_resource_offset: 0,
                    last_area_offset: Some(from_offset),
                    last_realm_offset: None,
                    has_more: false,
                }
            } else {
                crate::domains::stream::protocol::ReadCursor {
                    last_resource_offset: from_offset,
                    last_area_offset: None,
                    last_realm_offset: None,
                    has_more: false,
                }
            };
            return Ok(Self::encode_stream_read_data(&[], &cursor));
        }

        let read_response = if parts.area == "*" && parts.resource == "*" {
            let (items, cursor) = self.stream_store.read_realm_with_filter(
                family_id.as_u64(),
                parts.realm,
                from_offset,
                limit,
                max_bytes,
                filter,
            )?;
            ReadResponse { items, cursor }
        } else if parts.resource == "*" {
            let (items, cursor) = self.stream_store.read_area_with_filter(
                &crate::domains::stream::store::ReadAreaParams {
                    family: family_id.as_u64(),
                    realm: parts.realm,
                    area: parts.area,
                    from_offset,
                    limit,
                    max_bytes,
                },
                filter,
            )?;
            ReadResponse { items, cursor }
        } else {
            let key = Self::actor_key_for_route(family_id, route)?;
            let actor = self.get_or_create_actor(&key)?;
            let read_response =
                actor
                    .lock()
                    .read_with_filter(from_offset, limit, max_bytes, filter)?;
            read_response
        };

        Ok(Self::encode_stream_read_data(
            &read_response.items,
            &read_response.cursor,
        ))
    }

    pub(super) fn encode_last_response_data(
        &self,
        family_id: RouteFamily,
        route: &Route,
    ) -> Result<Vec<u8>, String> {
        let parts =
            route_triplet(route.as_str()).ok_or_else(|| "invalid stream route".to_string())?;
        if parts.area == "*" || parts.resource == "*" {
            return Ok(Vec::new());
        }

        let key = Self::actor_key_for_route(family_id, route)?;
        let actor = self.get_or_create_actor(&key)?;
        let data = actor
            .lock()
            .last()?
            .record
            .as_ref()
            .map(Self::encode_stream_last_data)
            .unwrap_or_default();
        Ok(data)
    }

    pub(super) fn encode_metadata_response_data(
        &self,
        family_id: RouteFamily,
        route: &Route,
    ) -> Result<Vec<u8>, String> {
        let parts =
            route_triplet(route.as_str()).ok_or_else(|| "invalid stream route".to_string())?;
        if parts.area == "*" || parts.resource == "*" {
            return Ok(Vec::new());
        }

        let key = Self::actor_key_for_route(family_id, route)?;
        let actor = self.get_or_create_actor(&key)?;
        let metadata = actor.lock().metadata()?.metadata;

        Ok(Self::encode_stream_metadata_data(&metadata))
    }

    pub(super) fn handle_domain_publish(&self, event: &crate::runtime::DomainPublishEvent) {
        let family_id = event.family_id.as_u64();
        let families = self.families.lock();
        if let Some(state) = families.get(&family_id) {
            #[cfg(test)]
            let mut payload_encoder = PayloadEncoder::with_capacity(256);
            state.for_each_matching(event, |subscription| {
                #[cfg(test)]
                self.route_commit_notify(subscription, event, &mut payload_encoder);
                #[cfg(not(test))]
                self.route_commit_notify(subscription, event);
            });
        }
    }

    #[cfg(test)]
    pub(super) fn route_commit_notify(
        &self,
        subscription: &StreamSubscription,
        event: &crate::runtime::DomainPublishEvent,
        payload_encoder: &mut PayloadEncoder,
    ) {
        let notify_payload = crate::protocol::stream_codec::encode_notify_into(
            payload_encoder,
            subscription.subscription_id,
            &event.route,
            &event.payload,
        );
        let notify_ctx = FrameContext::new(
            subscription.session_id,
            crate::protocol::frame::ChannelId::Sub,
            crate::protocol::tlv::MessageType::new(609),
            bytes::Bytes::from(notify_payload),
            RouteFamily::from_u32(subscription.subscriber.family().id()),
        );
        let notify_envelope = Envelope::new(subscription.subscriber.clone(), notify_ctx);
        if self.router.route(notify_envelope).is_err() {
            crate::observability::counter_inc("fitz_stream_notify_drops_total");
        }
    }

    #[cfg(not(test))]
    pub(super) fn route_commit_notify(
        &self,
        subscription: &StreamSubscription,
        event: &crate::runtime::DomainPublishEvent,
    ) {
        let notify = crate::domains::stream::StreamClientNotification::new(
            subscription.session_id,
            RouteFamily::from_u32(subscription.subscriber.family().id()),
            subscription.subscription_id,
            event.route.clone(),
            event.payload.clone(),
        );
        let notify_envelope = Envelope::new(subscription.subscriber.clone(), notify);
        if self.router.route(notify_envelope).is_err() {
            crate::observability::counter_inc("fitz_stream_notify_drops_total");
        }
    }

    pub(super) fn unsubscribe_all(&self, session_id: u64) {
        let mut families = self.families.lock();
        for (family_id, state) in families.iter_mut() {
            state.remove_session(RouteFamily::new(*family_id), session_id);
        }
        families.retain(|_, state| !state.is_empty());
        drop(families);
        self.refresh_metrics_gauges();
    }

    pub(super) fn cleanup_session(&self, session_id: u64) {
        self.unsubscribe_all(session_id);

        let actors: Vec<Arc<Mutex<StreamActor>>> = self.actors.lock().values().cloned().collect();
        let mut removed_sessions = Vec::new();
        for actor in actors {
            if let Some(stream_session_id) = actor.lock().cleanup_session(session_id) {
                removed_sessions.push(stream_session_id);
            }
        }

        if !removed_sessions.is_empty() {
            let removed_count = usize_to_u64_saturating(removed_sessions.len());
            let mut session_owners = self.session_owners.lock();
            for stream_session_id in removed_sessions {
                session_owners.remove(&stream_session_id);
            }
            self.counter_add("fitz_stream_append_sessions_ended_total", removed_count);
            self.admin_snapshot_dirty.store(true, Ordering::Relaxed);
        }
    }

    pub(super) fn live_counts(&self) -> StreamLiveCounts {
        let subscriptions = self
            .families
            .lock()
            .values()
            .map(crate::domains::subscription_state::RoutedSubscriptionSet::subscription_count)
            .sum();

        StreamLiveCounts {
            streams: self.actors.lock().len(),
            append_sessions: self.session_owners.lock().len(),
            subscriptions,
        }
    }
}

impl StreamDomainRuntime<'_> {
    pub(super) fn refresh_admin_snapshot_if_dirty(&self) {
        self.core.refresh_admin_snapshot_if_dirty();
    }

    #[cfg(test)]
    pub(super) fn sync_admin_snapshot(&self) {
        self.core.sync_admin_snapshot();
    }

    pub(super) fn live_counts(&self) -> StreamLiveCounts {
        self.core.live_counts()
    }
}
