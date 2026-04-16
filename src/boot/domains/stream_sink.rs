use super::subscription_state::{RoutedSubscription, RoutedSubscriptionSet};
use crate::domains::stream::store::StreamAdminRecord;
use crate::domains::stream::StreamMetrics;
use crate::domains::stream::{
    ReadResponse, StreamActor, StreamMetadata, StreamRecord, StreamStorageLayout, StreamStore,
};
use crate::protocol::frame_context::FrameContext;
use crate::protocol::payload_codec::PayloadEncoder;
use crate::runtime::routing::{route_triplet, Route, RouteAddress, RouteFamily};
use crate::runtime::{DeliveryError, Envelope, MailboxSink, Router};
use parking_lot::Mutex;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

struct StreamSubscription {
    pattern: crate::runtime::matcher::Pattern,
    session_id: u64,
    subscription_id: u64,
    subscriber: RouteAddress,
}

impl RoutedSubscription for StreamSubscription {
    fn pattern(&self) -> &crate::runtime::matcher::Pattern {
        &self.pattern
    }

    fn session_id(&self) -> u64 {
        self.session_id
    }

    fn subscription_id(&self) -> u64 {
        self.subscription_id
    }
}

#[derive(Debug, Clone, Eq, Hash, PartialEq)]
struct StreamActorKey {
    family_id: u64,
    realm: String,
    area: String,
    resource: String,
}

#[derive(Default)]
struct StreamRealmSnapshot {
    areas: BTreeSet<String>,
    resource_count: usize,
    families: BTreeSet<u64>,
}

#[derive(Default)]
struct StreamAreaSnapshot {
    resource_count: usize,
    families: BTreeSet<u64>,
}

const STREAM_OPERATIONS_TOTAL: &str = "fitz_stream_operations_total";

impl StreamActorKey {
    fn resource_route(&self) -> Route {
        Route::new(format!(
            "stream://{}/{}/{}",
            self.realm, self.area, self.resource
        ))
    }
}

pub struct StreamDomainSink {
    store: Arc<cntryl_midge::Engine>,
    stream_store: Arc<StreamStore>,
    actors: Mutex<HashMap<StreamActorKey, Arc<Mutex<StreamActor>>>>,
    session_owners: Mutex<HashMap<u64, StreamActorKey>>,
    families: Mutex<HashMap<u64, RoutedSubscriptionSet<StreamSubscription>>>,
    next_sub_id: AtomicU64,
    next_session_id: AtomicU64,
    router: Arc<Router>,
    admin_read_model: Arc<crate::api::admin::read_model::AdminReadModel>,
    admin_snapshot_dirty: AtomicBool,
    metrics: Option<StreamMetrics>,
    active: AtomicBool,
}

impl StreamDomainSink {
    pub fn new(
        store: Arc<cntryl_midge::Engine>,
        router: Arc<Router>,
        admin_read_model: Arc<crate::api::admin::read_model::AdminReadModel>,
    ) -> Self {
        Self::new_with_layout(
            store,
            router,
            admin_read_model,
            StreamStorageLayout::default(),
        )
        .expect("create stream domain sink with default stream layout")
    }

    pub fn new_with_layout(
        store: Arc<cntryl_midge::Engine>,
        router: Arc<Router>,
        admin_read_model: Arc<crate::api::admin::read_model::AdminReadModel>,
        stream_storage_layout: StreamStorageLayout,
    ) -> Result<Self, String> {
        let stream_store = Arc::new(StreamStore::with_layout(
            store.clone(),
            stream_storage_layout,
        ));
        stream_store.ensure_layout_activation_for_existing_families()?;

        Ok(Self {
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
            metrics: None,
            active: AtomicBool::new(true),
        })
    }

    pub fn with_metrics(
        mut self,
        collector: crate::observability::metrics::MetricsCollector,
    ) -> Self {
        self.metrics = Some(StreamMetrics::new(collector));
        self.refresh_metrics_gauges();
        self
    }

    pub fn stop(&self) {
        self.active.store(false, Ordering::Relaxed);
    }

    pub fn storage_layout(&self) -> StreamStorageLayout {
        self.stream_store.storage_layout()
    }

    fn actor_key_for_route(
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

    fn get_or_create_actor(&self, key: &StreamActorKey) -> Result<Arc<Mutex<StreamActor>>, String> {
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

    fn mark_admin_snapshot_dirty(&self) {
        self.admin_snapshot_dirty.store(true, Ordering::Relaxed);
        self.refresh_metrics_gauges();
    }

    fn refresh_metrics_gauges(&self) {
        if let Some(metrics) = &self.metrics {
            metrics.set_stream_count(self.stream_count());
            metrics.set_subscription_count(self.subscription_count());
        }
    }

    fn stream_response_is_failure(
        response: &crate::protocol::stream_codec::StreamResponse,
    ) -> bool {
        matches!(
            response,
            crate::protocol::stream_codec::StreamResponse::Error(_)
        )
    }

    pub fn refresh_admin_snapshot_if_dirty(&self) {
        if self.admin_snapshot_dirty.swap(false, Ordering::AcqRel) {
            self.sync_admin_snapshot();
        }
    }

    fn sync_admin_snapshot(&self) {
        let mut streams: BTreeMap<(u64, String, String, String), crate::api::admin::StreamInfo> =
            BTreeMap::new();
        let mut realm_snapshots: BTreeMap<String, StreamRealmSnapshot> = BTreeMap::new();
        let mut area_snapshots: BTreeMap<(String, String), StreamAreaSnapshot> = BTreeMap::new();
        let mut committed_events_total = 0usize;

        if let Ok(families) = self.store.list_column_families() {
            for family in families {
                let family_id = family.id() as u64;
                if let Ok(records) = self.stream_store.list_resource_metadata(family_id) {
                    for StreamAdminRecord {
                        realm,
                        area,
                        resource,
                        next_offset,
                        committed_size_bytes,
                    } in records
                    {
                        committed_events_total =
                            committed_events_total.saturating_add(next_offset as usize);
                        let last_offset = next_offset.saturating_sub(1);
                        streams.insert(
                            (family_id, realm.clone(), area.clone(), resource.clone()),
                            crate::api::admin::StreamInfo::snapshot(
                                &realm,
                                &area,
                                &resource,
                                last_offset,
                                last_offset,
                                committed_size_bytes,
                                0,
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

        let stream_realm_watermarks = realm_snapshots
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
                                crate::api::admin::StreamRealmWatermark::snapshot(
                                    family_id, watermark,
                                )
                            })
                    })
                    .collect();

                crate::api::admin::StreamRealmWatermarkDetail::snapshot(
                    &realm,
                    snapshot.areas.len(),
                    snapshot.resource_count,
                    family_watermarks,
                )
            })
            .collect();

        let stream_area_watermarks = area_snapshots
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
                                crate::api::admin::StreamAreaWatermark::snapshot(
                                    family_id, watermark,
                                )
                            })
                    })
                    .collect();

                crate::api::admin::StreamAreaWatermarkDetail::snapshot(
                    &realm,
                    &area,
                    snapshot.resource_count,
                    family_watermarks,
                )
            })
            .collect();

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
            let committed_size_bytes = committed_snapshot.map(|item| item.size_bytes).unwrap_or(0);
            let committed_offset = committed_snapshot.map(|item| item.offset);
            let visible_offset = last_offset.or(committed_offset).unwrap_or(0);

            streams.insert(
                stream_key,
                crate::api::admin::StreamInfo::snapshot(
                    &key.realm,
                    &key.area,
                    &key.resource,
                    visible_offset,
                    visible_offset,
                    committed_size_bytes,
                    sessions_active,
                ),
            );
        }

        self.admin_read_model
            .replace_streams(streams.into_values().collect());
        self.admin_read_model
            .replace_stream_realm_watermarks(stream_realm_watermarks);
        self.admin_read_model
            .replace_stream_area_watermarks(stream_area_watermarks);
        self.admin_read_model
            .replace_stream_events_total(committed_events_total);
    }

    fn encode_optional_bytes(encoder: &mut PayloadEncoder, value: Option<&bytes::Bytes>) {
        match value {
            Some(bytes) => {
                encoder.put_u8(1);
                encoder.put_bytes(bytes.as_ref());
            }
            None => encoder.put_u8(0),
        }
    }

    fn encode_stream_record(encoder: &mut PayloadEncoder, record: &StreamRecord) {
        encoder.put_u64(record.resource_offset);
        encoder.put_optional_u64(record.area_offset);
        encoder.put_optional_u64(record.realm_offset);
        encoder.put_bytes(record.body.as_ref());
        Self::encode_optional_bytes(encoder, record.metadata.as_ref());
        encoder.put_u64(record.created_at);
    }

    fn encode_stream_cursor(
        encoder: &mut PayloadEncoder,
        cursor: &crate::domains::stream::protocol::ReadCursor,
    ) {
        encoder.put_u64(cursor.last_resource_offset);
        encoder.put_optional_u64(cursor.last_area_offset);
        encoder.put_optional_u64(cursor.last_realm_offset);
        encoder.put_u8(u8::from(cursor.has_more));
    }

    fn encode_stream_read_data(
        records: &[StreamRecord],
        cursor: &crate::domains::stream::protocol::ReadCursor,
    ) -> Vec<u8> {
        let mut encoder = PayloadEncoder::new();
        encoder.put_u32(records.len() as u32);
        for record in records {
            Self::encode_stream_record(&mut encoder, record);
        }
        Self::encode_stream_cursor(&mut encoder, cursor);
        encoder.finish()
    }

    fn encode_stream_last_data(record: &StreamRecord) -> Vec<u8> {
        let mut encoder = PayloadEncoder::new();
        Self::encode_stream_record(&mut encoder, record);
        encoder.finish()
    }

    fn encode_stream_metadata_data(metadata: &StreamMetadata) -> Vec<u8> {
        let mut encoder = PayloadEncoder::new();
        encoder.put_optional_u64(metadata.first_resource_offset);
        encoder.put_optional_u64(metadata.last_resource_offset);
        encoder.put_u64(metadata.resource_count);
        encoder.put_u64(metadata.max_batch_events as u64);
        encoder.put_u64(metadata.max_batch_bytes as u64);
        encoder.put_optional_u64(metadata.ttl_seconds);
        encoder.put_u64(metadata.area_watermark);
        encoder.put_u64(metadata.realm_watermark);
        encoder.finish()
    }

    fn encode_stream_commit_notify_payload(
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

    fn stream_error_response(
        error: impl Into<String>,
    ) -> crate::protocol::stream_codec::StreamResponse {
        crate::protocol::stream_codec::StreamResponse::Error(error.into())
    }

    fn encode_read_response_data(
        &self,
        family_id: RouteFamily,
        route: &Route,
        from_offset: u64,
        limit: u64,
        max_bytes: Option<usize>,
    ) -> Result<Vec<u8>, String> {
        if limit == 0 {
            return Ok(Self::encode_stream_read_data(
                &[],
                &crate::domains::stream::protocol::ReadCursor {
                    last_resource_offset: from_offset,
                    last_area_offset: None,
                    last_realm_offset: None,
                    has_more: false,
                },
            ));
        }

        let parts =
            route_triplet(route.as_str()).ok_or_else(|| "invalid stream route".to_string())?;
        let read_response = if parts.area == "*" && parts.resource == "*" {
            let (records, cursor) = self.stream_store.read_realm(
                family_id.as_u64(),
                parts.realm,
                from_offset,
                limit,
                max_bytes,
            )?;
            ReadResponse { records, cursor }
        } else if parts.resource == "*" {
            let (records, cursor) = self.stream_store.read_area(
                family_id.as_u64(),
                parts.realm,
                parts.area,
                from_offset,
                limit,
                max_bytes,
            )?;
            ReadResponse { records, cursor }
        } else {
            let key = Self::actor_key_for_route(family_id, route)?;
            let actor = self.get_or_create_actor(&key)?;
            let read_response = actor.lock().read(from_offset, limit, max_bytes)?;
            read_response
        };

        Ok(Self::encode_stream_read_data(
            &read_response.records,
            &read_response.cursor,
        ))
    }

    fn encode_last_response_data(
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

    fn encode_metadata_response_data(
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

    fn handle_domain_publish(
        &self,
        event: &crate::runtime::DomainPublishEvent,
    ) -> Result<(), DeliveryError> {
        let family_id = event.family_id.as_u64();
        let families = self.families.lock();
        if let Some(state) = families.get(&family_id) {
            let mut payload_encoder = PayloadEncoder::with_capacity(256);
            state.for_each_matching(event, |subscription| {
                self.route_commit_notify(subscription, event, &mut payload_encoder);
            });
        }
        Ok(())
    }

    fn route_commit_notify(
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
            crate::boot::observability::counter_inc("fitz_stream_notify_drops_total");
        }
    }

    pub fn unsubscribe_all(&self, session_id: u64) {
        let mut families = self.families.lock();
        for (family_id, state) in families.iter_mut() {
            state.remove_session(RouteFamily::new(*family_id), session_id);
        }
        drop(families);
        self.refresh_metrics_gauges();
    }

    fn cleanup_session(&self, session_id: u64) {
        self.unsubscribe_all(session_id);

        let actors: Vec<Arc<Mutex<StreamActor>>> = self.actors.lock().values().cloned().collect();
        let mut removed_sessions = Vec::new();
        for actor in actors {
            if let Some(stream_session_id) = actor.lock().cleanup_session(session_id) {
                removed_sessions.push(stream_session_id);
            }
        }

        if !removed_sessions.is_empty() {
            let mut session_owners = self.session_owners.lock();
            for stream_session_id in removed_sessions {
                session_owners.remove(&stream_session_id);
            }
            self.mark_admin_snapshot_dirty();
        }
    }

    pub fn subscription_count(&self) -> usize {
        let families = self.families.lock();
        families
            .values()
            .map(|state| state.subscription_count())
            .sum()
    }

    pub fn stream_count(&self) -> usize {
        self.actors.lock().len()
    }
}

impl MailboxSink for StreamDomainSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        if !self.active.load(Ordering::Relaxed) {
            return Err(DeliveryError::ActorStopped);
        }

        if let Some(event) = envelope.payload::<crate::runtime::DomainPublishEvent>() {
            return self.handle_domain_publish(event);
        }

        if let Some(cleanup) = envelope.payload::<crate::runtime::SessionCleanup>() {
            self.cleanup_session(cleanup.session_id);
            return Ok(());
        }

        let frame_ctx = match envelope.payload::<FrameContext>() {
            Some(ctx) => ctx.clone(),
            None => return Err(DeliveryError::ActorStopped),
        };
        let request_started = self
            .metrics
            .as_ref()
            .map(|metrics| metrics.record_request_start());
        let mut payload_encoder = PayloadEncoder::with_capacity(256);

        let parsed_frame = match crate::protocol::stream_codec::parse_request(
            &frame_ctx,
            &frame_ctx.payload,
            *envelope.destination().family(),
            crate::session::SessionId(frame_ctx.session_id),
            envelope.source().cloned().unwrap_or_else(|| {
                RouteAddress::new(
                    *envelope.destination().family(),
                    Route::new(format!("inbox://session/{}", frame_ctx.session_id)),
                )
            }),
        ) {
            Ok(msg) => msg,
            Err(_) => {
                if let (Some(metrics), Some(started_at)) = (self.metrics.as_ref(), request_started)
                {
                    metrics.record_failure(started_at);
                }
                return Err(DeliveryError::ActorStopped);
            }
        };

        use crate::domains::stream::protocol::{StreamMessage, StreamSubscriptionMessage};
        use crate::protocol::stream_codec::{ParsedStreamFrame, StreamResponse};

        if let Some(metrics) = &self.metrics {
            metrics.counter_inc(STREAM_OPERATIONS_TOTAL);
        } else {
            crate::boot::observability::counter_inc(STREAM_OPERATIONS_TOTAL);
        }

        // Subscription messages are handled entirely by the sink without touching StreamActor.
        if let ParsedStreamFrame::Sub(sub_msg) = parsed_frame {
            let (response, _commit_notify, _should_refresh_admin_snapshot): (
                StreamResponse,
                Option<(Route, bytes::Bytes)>,
                bool,
            ) = match sub_msg {
                StreamSubscriptionMessage::Subscribe {
                    family_id,
                    pattern,
                    session_id,
                    subscriber,
                } => {
                    let mut families = self.families.lock();
                    let state = families
                        .entry(family_id.as_u64())
                        .or_insert_with(RoutedSubscriptionSet::new);

                    let subscription_id = if let Some(id) =
                        state.find_existing_id(session_id, pattern.as_str())
                    {
                        id
                    } else {
                        let new_id = self.next_sub_id.fetch_add(1, Ordering::Relaxed);
                        state.insert(
                            family_id,
                            StreamSubscription {
                                pattern: crate::runtime::matcher::Pattern::new(pattern.as_str()),
                                session_id,
                                subscription_id: new_id,
                                subscriber,
                            },
                        );
                        new_id
                    };

                    (
                        StreamResponse::Ok {
                            session_id: Some(subscription_id),
                            data: vec![],
                        },
                        None,
                        false,
                    )
                }
                StreamSubscriptionMessage::Unsubscribe {
                    family_id,
                    pattern,
                    session_id,
                    ..
                } => {
                    let mut families = self.families.lock();
                    if let Some(state) = families.get_mut(&family_id.as_u64()) {
                        state.remove_session_pattern(family_id, session_id, pattern.as_str());
                    }
                    (
                        StreamResponse::Ok {
                            session_id: None,
                            data: vec![],
                        },
                        None,
                        false,
                    )
                }
            };

            let response_bytes = crate::protocol::stream_codec::encode_response_into(
                &mut payload_encoder,
                &response,
            );
            let response_ctx = FrameContext::new(
                frame_ctx.session_id,
                frame_ctx.channel_id,
                crate::protocol::tlv::MessageType::new(frame_ctx.msg_type.as_u16()),
                bytes::Bytes::from(response_bytes),
                frame_ctx.route_family,
            );
            if let Some(response_envelope) = envelope.try_reply_to(response_ctx) {
                let _ = self.router.route(response_envelope);
            }

            self.refresh_metrics_gauges();
            if let (Some(metrics), Some(started_at)) = (self.metrics.as_ref(), request_started) {
                if Self::stream_response_is_failure(&response) {
                    metrics.record_failure(started_at);
                } else {
                    metrics.record_success(started_at);
                }
            }
            return Ok(());
        }

        let stream_msg = match parsed_frame {
            ParsedStreamFrame::Op(msg) => msg,
            ParsedStreamFrame::Sub(_) => unreachable!(),
        };

        let (response, commit_notify, should_refresh_admin_snapshot) = match stream_msg {
            StreamMessage::Begin {
                family_id,
                route,
                ingest_metadata,
            } => match Self::actor_key_for_route(family_id, &route) {
                Ok(key) => match self.get_or_create_actor(&key) {
                    Ok(actor) => {
                        let stream_session_id =
                            self.next_session_id.fetch_add(1, Ordering::Relaxed);
                        let outcome = actor.lock().begin_append_session(
                            frame_ctx.session_id,
                            stream_session_id,
                            ingest_metadata,
                        );
                        match outcome {
                            Ok(session_id) => {
                                self.session_owners.lock().insert(session_id, key);
                                (
                                    StreamResponse::Ok {
                                        session_id: Some(session_id),
                                        data: vec![],
                                    },
                                    None,
                                    true,
                                )
                            }
                            Err(error) => {
                                crate::boot::observability::counter_inc(
                                    "fitz_stream_append_conflicts_total",
                                );
                                (Self::stream_error_response(error), None, false)
                            }
                        }
                    }
                    Err(error) => (Self::stream_error_response(error), None, false),
                },
                Err(error) => (Self::stream_error_response(error), None, false),
            },
            StreamMessage::Append {
                session_id,
                expected_offset,
                body,
                metadata,
            } => {
                let key = self.session_owners.lock().get(&session_id).cloned();
                match key {
                    Some(key) => match self.get_or_create_actor(&key) {
                        Ok(actor) => {
                            let outcome =
                                actor.lock().append_to_session(session_id, expected_offset, body, metadata);
                            match outcome {
                                Ok(assigned_offset) => {
                                    let mut encoder = PayloadEncoder::new();
                                    encoder.put_u64(assigned_offset);
                                    (
                                        StreamResponse::Ok {
                                            session_id: None,
                                            data: encoder.finish(),
                                        },
                                        None,
                                        false,
                                    )
                                }
                                Err(error) => (Self::stream_error_response(error), None, false),
                            }
                        }
                        Err(error) => (Self::stream_error_response(error), None, false),
                    },
                    None => (
                        Self::stream_error_response("session not found"),
                        None,
                        false,
                    ),
                }
            }
            StreamMessage::Commit { session_id, mode } => {
                let key = self.session_owners.lock().get(&session_id).cloned();
                match key {
                    Some(key) => match self.get_or_create_actor(&key) {
                        Ok(actor) => {
                            let outcome = actor.lock().commit_session(session_id, mode);
                            match outcome {
                                Ok(commit) => {
                                    self.session_owners.lock().remove(&session_id);
                                    let payload = Self::encode_stream_commit_notify_payload(
                                        commit.first_resource_offset,
                                        commit.last_resource_offset,
                                        commit.first_area_offset,
                                        commit.last_area_offset,
                                        commit.first_realm_offset,
                                        commit.last_realm_offset,
                                        commit.batch_size,
                                    );
                                    (
                                        StreamResponse::Ok {
                                            session_id: None,
                                            data: vec![],
                                        },
                                        Some((key.resource_route(), payload)),
                                        true,
                                    )
                                }
                                Err(error) => (Self::stream_error_response(error), None, false),
                            }
                        }
                        Err(error) => (Self::stream_error_response(error), None, false),
                    },
                    None => (
                        Self::stream_error_response("session not found"),
                        None,
                        false,
                    ),
                }
            }
            StreamMessage::Rollback { session_id } => {
                let key = self.session_owners.lock().get(&session_id).cloned();
                match key {
                    Some(key) => match self.get_or_create_actor(&key) {
                        Ok(actor) => {
                            let outcome = actor.lock().rollback_session(session_id);
                            match outcome {
                                Ok(()) => {
                                    self.session_owners.lock().remove(&session_id);
                                    (
                                        StreamResponse::Ok {
                                            session_id: None,
                                            data: vec![],
                                        },
                                        None,
                                        true,
                                    )
                                }
                                Err(error) => (Self::stream_error_response(error), None, false),
                            }
                        }
                        Err(error) => (Self::stream_error_response(error), None, false),
                    },
                    None => (
                        Self::stream_error_response("session not found"),
                        None,
                        false,
                    ),
                }
            }
            StreamMessage::Read {
                family_id,
                route,
                from_offset,
                limit,
                max_bytes,
            } => match self.encode_read_response_data(
                family_id,
                &route,
                from_offset,
                limit,
                max_bytes,
            ) {
                Ok(data) => (
                    StreamResponse::Ok {
                        session_id: None,
                        data,
                    },
                    None,
                    false,
                ),
                Err(error) => (Self::stream_error_response(error), None, false),
            },
            StreamMessage::Last { family_id, route } => {
                match self.encode_last_response_data(family_id, &route) {
                    Ok(data) => (
                        StreamResponse::Ok {
                            session_id: None,
                            data,
                        },
                        None,
                        false,
                    ),
                    Err(error) => (Self::stream_error_response(error), None, false),
                }
            }
            StreamMessage::GetMetadata { family_id, route } => {
                match self.encode_metadata_response_data(family_id, &route) {
                    Ok(data) => (
                        StreamResponse::Ok {
                            session_id: None,
                            data,
                        },
                        None,
                        false,
                    ),
                    Err(error) => (Self::stream_error_response(error), None, false),
                }
            }
        };

        if should_refresh_admin_snapshot {
            self.mark_admin_snapshot_dirty();
        }

        if let Some((route, payload)) = commit_notify {
            let event =
                crate::runtime::DomainPublishEvent::new(frame_ctx.route_family, route, payload);
            let _ = self.handle_domain_publish(&event);
        }

        let response_bytes =
            crate::protocol::stream_codec::encode_response_into(&mut payload_encoder, &response);
        let response_ctx = FrameContext::new(
            frame_ctx.session_id,
            frame_ctx.channel_id,
            crate::protocol::tlv::MessageType::new(frame_ctx.msg_type.as_u16()),
            bytes::Bytes::from(response_bytes),
            frame_ctx.route_family,
        );
        if let Some(response_envelope) = envelope.try_reply_to(response_ctx) {
            let _ = self.router.route(response_envelope);
        }

        if let (Some(metrics), Some(started_at)) = (self.metrics.as_ref(), request_started) {
            if Self::stream_response_is_failure(&response) {
                metrics.record_failure(started_at);
            } else {
                metrics.record_success(started_at);
            }
        }

        Ok(())
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::benchkit::{
        build_stream_append, build_stream_append_with_metadata, build_stream_begin,
        build_stream_commit, build_stream_read, count_stream_read_records_from_payload,
        extract_single_tlv_field, register_session_queue_sink, route_frame, FrameQueueSink,
    };
    use crate::protocol::frame::ChannelId;
    use bytes::Bytes;

    const TEST_CLIENT_SESSION_ID: u64 = 1;

    struct TestContext {
        router: Arc<Router>,
        family: RouteFamily,
        source: RouteAddress,
        inbox: Arc<FrameQueueSink>,
        sink: Arc<StreamDomainSink>,
    }

    fn setup_test_context() -> TestContext {
        let family = RouteFamily::new(1);
        let router = Arc::new(Router::new());
        let sink = Arc::new(StreamDomainSink::new(
            crate::benchkit::create_bench_store(),
            router.clone(),
            crate::api::admin::read_model::AdminReadModel::new(),
        ));
        router.register_domain_pattern("stream", sink.clone() as Arc<dyn MailboxSink>);
        let (source, inbox) = register_session_queue_sink(&router, family, TEST_CLIENT_SESSION_ID);

        TestContext {
            router,
            family,
            source,
            inbox,
            sink,
        }
    }

    fn request(context: &TestContext, destination: &str, msg_type: u16, payload: Bytes) -> Bytes {
        route_frame(
            context.router.as_ref(),
            &context.source,
            destination,
            TEST_CLIENT_SESSION_ID,
            ChannelId::Pub,
            msg_type,
            payload,
            context.family,
        )
        .expect("stream route");

        let responses = context.inbox.drain();
        responses
            .last()
            .map(|frame| frame.payload.clone())
            .expect("stream response")
    }

    #[derive(Debug)]
    struct DecodedStreamWireRecord {
        resource_offset: u64,
        area_offset: Option<u64>,
        realm_offset: Option<u64>,
        body: Bytes,
        metadata: Option<Bytes>,
        created_at: u64,
    }

    #[derive(Debug)]
    struct DecodedStreamReadPayload {
        records: Vec<DecodedStreamWireRecord>,
        last_resource_offset: u64,
        last_area_offset: Option<u64>,
        last_realm_offset: Option<u64>,
        has_more: bool,
    }

    #[derive(Debug)]
    struct DecodedStreamMetadataPayload {
        first_resource_offset: Option<u64>,
        last_resource_offset: Option<u64>,
        resource_count: u64,
        max_batch_events: u64,
        max_batch_bytes: u64,
        ttl_seconds: Option<u64>,
        area_watermark: u64,
        realm_watermark: u64,
    }

    fn decode_stream_wire_record(
        decoder: &mut crate::protocol::payload_codec::PayloadDecoder<'_>,
    ) -> DecodedStreamWireRecord {
        let resource_offset = decoder.get_u64().expect("stream resource offset");
        let area_offset = decoder.get_optional_u64().expect("stream area offset");
        let realm_offset = decoder.get_optional_u64().expect("stream realm offset");
        let body = decoder.get_bytes().expect("stream body");
        let metadata = decoder.get_optional_bytes().expect("stream metadata");
        let created_at = decoder.get_u64().expect("stream created_at");

        DecodedStreamWireRecord {
            resource_offset,
            area_offset,
            realm_offset,
            body,
            metadata,
            created_at,
        }
    }

    fn decode_stream_read_payload(data: &[u8]) -> DecodedStreamReadPayload {
        let mut decoder = crate::protocol::payload_codec::PayloadDecoder::new(data);
        let count = decoder.get_u32().expect("stream read record count") as usize;
        let mut records = Vec::with_capacity(count);
        for _ in 0..count {
            records.push(decode_stream_wire_record(&mut decoder));
        }

        let last_resource_offset = decoder.get_u64().expect("stream cursor resource offset");
        let last_area_offset = decoder
            .get_optional_u64()
            .expect("stream cursor area offset");
        let last_realm_offset = decoder
            .get_optional_u64()
            .expect("stream cursor realm offset");
        let has_more = decoder.get_u8().expect("stream cursor has_more") == 1;
        assert!(
            decoder.is_complete(),
            "expected complete stream read payload"
        );

        DecodedStreamReadPayload {
            records,
            last_resource_offset,
            last_area_offset,
            last_realm_offset,
            has_more,
        }
    }

    fn decode_stream_metadata_payload(data: &[u8]) -> DecodedStreamMetadataPayload {
        let mut decoder = crate::protocol::payload_codec::PayloadDecoder::new(data);
        let first_resource_offset = decoder
            .get_optional_u64()
            .expect("first stream metadata offset");
        let last_resource_offset = decoder
            .get_optional_u64()
            .expect("last stream metadata offset");
        let resource_count = decoder.get_u64().expect("stream metadata count");
        let max_batch_events = decoder.get_u64().expect("stream max_batch_events");
        let max_batch_bytes = decoder.get_u64().expect("stream max_batch_bytes");
        let ttl_seconds = decoder.get_optional_u64().expect("stream ttl seconds");
        let area_watermark = decoder.get_u64().expect("stream area watermark");
        let realm_watermark = decoder.get_u64().expect("stream realm watermark");
        assert!(
            decoder.is_complete(),
            "expected complete stream metadata payload"
        );

        DecodedStreamMetadataPayload {
            first_resource_offset,
            last_resource_offset,
            resource_count,
            max_batch_events,
            max_batch_bytes,
            ttl_seconds,
            area_watermark,
            realm_watermark,
        }
    }

    fn begin_stream(context: &TestContext, route: &str) -> u64 {
        let begin_frame = build_stream_begin(route);
        let (msg_type, payload) = extract_single_tlv_field(&begin_frame);
        let response = request(context, route, msg_type, payload);
        crate::benchkit::parse_stream_session_id(response.as_ref()).expect("stream session id")
    }

    fn seed_committed_stream_route(
        context: &TestContext,
        route: &str,
        event_count: usize,
        body: &'static [u8],
    ) {
        let session_id = begin_stream(context, route);

        for expected_offset in 0..event_count as u64 {
            let append_frame = build_stream_append(session_id, expected_offset, body);
            let (append_msg_type, append_payload) = extract_single_tlv_field(&append_frame);
            let _ = request(context, route, append_msg_type, append_payload);
        }

        let commit_frame = build_stream_commit(session_id, 1);
        let (commit_msg_type, commit_payload) = extract_single_tlv_field(&commit_frame);
        let _ = request(context, route, commit_msg_type, commit_payload);
    }

    #[test]
    fn should_return_all_area_records_given_two_resource_batches_on_direct_sink_path() {
        // Arrange
        let context = setup_test_context();
        seed_committed_stream_route(
            &context,
            "stream://bench/area/orders",
            50,
            b"area read event",
        );
        seed_committed_stream_route(
            &context,
            "stream://bench/area/audits",
            50,
            b"area read event",
        );

        // Act
        let area_records = context
            .sink
            .stream_store
            .read_area(1, "bench", "area", 0, 1000, None)
            .expect("read area from store")
            .0;

        let read_frame = build_stream_read("stream://bench/area/*", 0);
        let (read_msg_type, read_payload) = extract_single_tlv_field(&read_frame);
        let response = request(
            &context,
            "stream://bench/area/*",
            read_msg_type,
            read_payload,
        );
        let response_count = count_stream_read_records_from_payload(response.as_ref())
            .expect("count wildcard response records");

        // Assert
        assert_eq!(area_records.len(), 100);
        assert_eq!(response_count, 100);
    }

    #[test]
    fn should_create_stream_sink_given_promotion_frontier_layout() {
        // Arrange
        let router = Arc::new(Router::new());

        // Act
        let sink = StreamDomainSink::new_with_layout(
            crate::benchkit::create_bench_store(),
            router,
            crate::api::admin::read_model::AdminReadModel::new(),
            StreamStorageLayout::PromotionFrontier,
        )
        .expect("create stream sink");

        // Assert
        assert_eq!(
            sink.storage_layout(),
            StreamStorageLayout::PromotionFrontier
        );
    }

    #[test]
    fn should_exclude_uncommitted_stream_from_admin_snapshot_given_active_session() {
        // Arrange
        let context = setup_test_context();
        let _session_id = begin_stream(&context, "stream://bench/events/pending");

        // Act
        context.sink.sync_admin_snapshot();
        let streams = context.sink.admin_read_model.streams(None);
        let events_total = context.sink.admin_read_model.stream_events_total();

        // Assert
        assert!(streams.is_empty());
        assert_eq!(events_total, 0);
    }

    #[test]
    fn should_preserve_committed_snapshot_given_active_session_overlay() {
        // Arrange
        let context = setup_test_context();
        seed_committed_stream_route(&context, "stream://bench/events/orders", 1, b"persisted");
        let _session_id = begin_stream(&context, "stream://bench/events/orders");

        // Act
        context.sink.sync_admin_snapshot();
        let streams = context.sink.admin_read_model.streams(None);
        let stream = streams
            .iter()
            .find(|item| {
                item.realm == "bench" && item.area == "events" && item.resource == "orders"
            })
            .expect("committed stream should remain visible");
        let events_total = context.sink.admin_read_model.stream_events_total();

        // Assert
        assert_eq!(stream.offset, 0);
        assert_eq!(stream.watermark, 0);
        assert_eq!(stream.size_bytes, b"persisted".len() as u64);
        assert_eq!(stream.sessions_active, 1);
        assert_eq!(events_total, 1);
    }

    #[test]
    fn should_preserve_committed_watermarks_given_uncommitted_append_overlay() {
        // Arrange
        let context = setup_test_context();
        let route = "stream://bench/events/orders";
        seed_committed_stream_route(&context, route, 1, b"persisted");
        let session_id = begin_stream(&context, route);
        let append_frame = build_stream_append(session_id, 0, b"staged");
        let (append_msg_type, append_payload) = extract_single_tlv_field(&append_frame);
        let _ = request(&context, route, append_msg_type, append_payload);

        // Act
        context.sink.sync_admin_snapshot();
        let stream = context
            .sink
            .admin_read_model
            .streams(None)
            .into_iter()
            .find(|item| {
                item.realm == "bench" && item.area == "events" && item.resource == "orders"
            })
            .expect("committed stream should remain visible");
        let area_watermark = context
            .sink
            .admin_read_model
            .stream_area_watermark("bench", "events")
            .expect("area watermark detail");
        let realm_watermark = context
            .sink
            .admin_read_model
            .stream_realm_watermark("bench")
            .expect("realm watermark detail");
        let events_total = context.sink.admin_read_model.stream_events_total();

        // Assert
        assert_eq!(stream.offset, 0);
        assert_eq!(stream.watermark, 0);
        assert_eq!(stream.size_bytes, b"persisted".len() as u64);
        assert_eq!(stream.sessions_active, 1);
        assert_eq!(area_watermark.resource_count, 1);
        assert_eq!(area_watermark.family_watermarks.len(), 1);
        assert_eq!(area_watermark.family_watermarks[0].family, 1);
        assert_eq!(area_watermark.family_watermarks[0].watermark, 0);
        assert_eq!(realm_watermark.area_count, 1);
        assert_eq!(realm_watermark.resource_count, 1);
        assert_eq!(realm_watermark.family_watermarks.len(), 1);
        assert_eq!(realm_watermark.family_watermarks[0].family, 1);
        assert_eq!(realm_watermark.family_watermarks[0].watermark, 0);
        assert_eq!(events_total, 1);
    }

    #[test]
    fn should_not_inflate_admin_watermark_counts_given_uncommitted_new_resource_overlay() {
        // Arrange
        let context = setup_test_context();
        seed_committed_stream_route(&context, "stream://bench/events/orders", 1, b"persisted");
        let session_id = begin_stream(&context, "stream://bench/events/audits");
        let append_frame = build_stream_append(session_id, 0, b"staged");
        let (append_msg_type, append_payload) = extract_single_tlv_field(&append_frame);
        let _ = request(
            &context,
            "stream://bench/events/audits",
            append_msg_type,
            append_payload,
        );

        // Act
        context.sink.sync_admin_snapshot();
        let streams = context.sink.admin_read_model.streams(None);
        let area_watermark = context
            .sink
            .admin_read_model
            .stream_area_watermark("bench", "events")
            .expect("area watermark detail");
        let realm_watermark = context
            .sink
            .admin_read_model
            .stream_realm_watermark("bench")
            .expect("realm watermark detail");
        let events_total = context.sink.admin_read_model.stream_events_total();

        // Assert
        assert_eq!(streams.len(), 1);
        assert_eq!(streams[0].resource, "orders");
        assert!(streams.iter().all(|item| item.resource != "audits"));
        assert_eq!(area_watermark.resource_count, 1);
        assert_eq!(area_watermark.family_watermarks.len(), 1);
        assert_eq!(area_watermark.family_watermarks[0].family, 1);
        assert_eq!(area_watermark.family_watermarks[0].watermark, 0);
        assert_eq!(realm_watermark.area_count, 1);
        assert_eq!(realm_watermark.resource_count, 1);
        assert_eq!(realm_watermark.family_watermarks.len(), 1);
        assert_eq!(realm_watermark.family_watermarks[0].family, 1);
        assert_eq!(realm_watermark.family_watermarks[0].watermark, 0);
        assert_eq!(events_total, 1);
    }

    #[test]
    fn should_return_trimmed_head_metadata_summary_given_missing_first_resource_page() {
        // Arrange
        let context = setup_test_context();
        seed_committed_stream_route(
            &context,
            "stream://bench/events/orders",
            crate::domains::stream::storage::REALM_PAGE_RECORD_LIMIT + 1,
            b"persisted",
        );
        let mut txn = context
            .sink
            .store
            .begin_tx(1, cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin stream metadata write tx");
        txn.delete(
            crate::domains::stream::storage::encode_compact_resource_page_key(
                "bench", "events", "orders", 0,
            ),
        )
        .expect("delete first resource page");
        txn.commit(cntryl_midge::WriteOptions::sync())
            .expect("commit trimmed stream head");

        // Act
        let payload = context
            .sink
            .encode_metadata_response_data(
                context.family,
                &Route::new("stream://bench/events/orders"),
            )
            .expect("encode stream metadata summary");
        let metadata = decode_stream_metadata_payload(&payload);

        // Assert
        assert_eq!(
            metadata.first_resource_offset,
            Some(crate::domains::stream::storage::REALM_PAGE_RECORD_LIMIT as u64)
        );
        assert_eq!(
            metadata.last_resource_offset,
            Some(crate::domains::stream::storage::REALM_PAGE_RECORD_LIMIT as u64)
        );
        assert_eq!(metadata.resource_count, 1);
    }

    #[test]
    fn should_encode_exact_resource_read_payload_given_committed_record_with_metadata() {
        // Arrange
        let context = setup_test_context();
        let session_id = begin_stream(&context, "stream://bench/events/orders");
        let append_frame =
            build_stream_append_with_metadata(session_id, 0, b"payload", Some(b"meta"));
        let (append_msg_type, append_payload) = extract_single_tlv_field(&append_frame);
        let _ = request(
            &context,
            "stream://bench/events/orders",
            append_msg_type,
            append_payload,
        );
        let commit_frame = build_stream_commit(session_id, 1);
        let (commit_msg_type, commit_payload) = extract_single_tlv_field(&commit_frame);
        let _ = request(
            &context,
            "stream://bench/events/orders",
            commit_msg_type,
            commit_payload,
        );

        // Act
        let payload = context
            .sink
            .encode_read_response_data(
                context.family,
                &Route::new("stream://bench/events/orders"),
                0,
                10,
                None,
            )
            .expect("encode exact stream read payload");
        let read_payload = decode_stream_read_payload(&payload);

        // Assert
        assert_eq!(read_payload.records.len(), 1);
        assert_eq!(read_payload.records[0].resource_offset, 0);
        assert_eq!(read_payload.records[0].area_offset, Some(0));
        assert_eq!(read_payload.records[0].realm_offset, Some(0));
        assert_eq!(read_payload.records[0].body, Bytes::from_static(b"payload"));
        assert_eq!(
            read_payload.records[0].metadata,
            Some(Bytes::from_static(b"meta"))
        );
        assert!(read_payload.records[0].created_at > 0);
        assert_eq!(read_payload.last_resource_offset, 0);
        assert_eq!(read_payload.last_area_offset, Some(0));
        assert_eq!(read_payload.last_realm_offset, Some(0));
        assert!(!read_payload.has_more);
    }

    #[test]
    fn should_encode_exact_resource_metadata_payload_given_empty_stream() {
        // Arrange
        let context = setup_test_context();

        // Act
        let payload = context
            .sink
            .encode_metadata_response_data(
                context.family,
                &Route::new("stream://bench/events/empty"),
            )
            .expect("encode empty stream metadata payload");
        let metadata = decode_stream_metadata_payload(&payload);

        // Assert
        assert_eq!(metadata.first_resource_offset, None);
        assert_eq!(metadata.last_resource_offset, None);
        assert_eq!(metadata.resource_count, 0);
        assert_eq!(metadata.max_batch_events, 10_000);
        assert_eq!(metadata.max_batch_bytes, 10 * 1024 * 1024);
        assert_eq!(metadata.ttl_seconds, None);
        assert_eq!(metadata.area_watermark, 0);
        assert_eq!(metadata.realm_watermark, 0);
    }

    #[test]
    fn should_return_all_realm_records_given_two_resource_batches_on_direct_sink_path() {
        // Arrange
        let context = setup_test_context();
        seed_committed_stream_route(
            &context,
            "stream://bench/events/orders",
            50,
            b"realm read event",
        );
        seed_committed_stream_route(
            &context,
            "stream://bench/audit/ledger",
            50,
            b"realm read event",
        );

        // Act
        let realm_records = context
            .sink
            .stream_store
            .read_realm(1, "bench", 0, 1000, None)
            .expect("read realm from store")
            .0;

        let read_frame = build_stream_read("stream://bench/*/*", 0);
        let (read_msg_type, read_payload) = extract_single_tlv_field(&read_frame);
        let response = request(&context, "stream://bench/*/*", read_msg_type, read_payload);
        let response_count = count_stream_read_records_from_payload(response.as_ref())
            .expect("count wildcard response records");

        // Assert
        assert_eq!(realm_records.len(), 100);
        assert_eq!(response_count, 100);
    }
}
