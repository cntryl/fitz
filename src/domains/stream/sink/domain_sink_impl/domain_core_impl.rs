#[cfg(test)]
use super::FrameContext;
use super::{
    route_triplet, u64_to_usize_saturating, usize_to_u32_saturating, usize_to_u64_saturating,
    AdminStreamReadRequest, Arc, BTreeMap, Envelope, Mutex, Ordering, PayloadEncoder,
    PendingStreamNotification, ReadResponse, ReadyStreamNotification, Route, RouteFamily,
    StreamActor, StreamActorKey, StreamAdminRecord, StreamAdminSnapshotMap, StreamAreaSnapshotMap,
    StreamClientResponseBody, StreamDomainCore, StreamDomainRuntime, StreamFilteredReason,
    StreamLiveCounts, StreamMetadata, StreamNotificationTarget, StreamReadExecution,
    StreamReadItem, StreamRealmSnapshotMap, StreamRecord, StreamStorageLayout,
    StreamVisibilityFrontier,
};

mod global_read_support;
mod notification_gating;
mod read_finalization;
mod wire_encoding;

use read_finalization::apply_global_snapshot_boundary;

#[derive(Clone, Copy, PartialEq, Eq)]
enum ReadScope {
    Resource,
    Area,
    Realm,
    Global,
}

impl StreamDomainCore {
    fn cursor_integrity_token(
        &self,
        selector_fingerprint: u64,
        captured_watermark: u64,
        next_offset: u64,
    ) -> u64 {
        use hmac::{Hmac, KeyInit, Mac};

        let mut mac = Hmac::<sha2::Sha256>::new_from_slice(self.cursor_integrity_key.as_ref())
            .expect("Stream cursor HMAC key has a valid fixed length");
        mac.update(&[1]);
        mac.update(&selector_fingerprint.to_le_bytes());
        mac.update(&captured_watermark.to_le_bytes());
        mac.update(&next_offset.to_le_bytes());
        let bytes = mac.finalize().into_bytes();
        u64::from_le_bytes(
            bytes[..8]
                .try_into()
                .expect("HMAC-SHA256 output is 32 bytes"),
        )
    }

    pub(in crate::domains::stream::sink) fn storage_layout(&self) -> StreamStorageLayout {
        self.stream_store.storage_layout()
    }

    pub(in crate::domains::stream::sink) fn actor_key_for_route(
        family_id: RouteFamily,
        route: &Route,
    ) -> Result<StreamActorKey, String> {
        let parts =
            route_triplet(route.as_str()).ok_or_else(|| "invalid stream route".to_string())?;
        if parts.realm.is_empty()
            || parts.area.is_empty()
            || parts.resource.is_empty()
            || parts.realm.contains('*')
            || parts.area.contains('*')
            || parts.resource.contains('*')
        {
            return Err("stream append routes require concrete realm/area/resource".to_string());
        }
        if parts.area == crate::domains::stream::INTERNAL_REALM_SEGMENT {
            return Err(format!(
                "area '{}' is reserved for internal broker use",
                crate::domains::stream::INTERNAL_REALM_SEGMENT
            ));
        }
        if parts.resource == crate::domains::stream::INTERNAL_AREA_SEGMENT {
            return Err(format!(
                "resource '{}' is reserved for internal broker use",
                crate::domains::stream::INTERNAL_AREA_SEGMENT
            ));
        }
        Ok(StreamActorKey {
            family_id: family_id.as_u64(),
            realm: parts.realm.to_string(),
            area: parts.area.to_string(),
            resource: parts.resource.to_string(),
        })
    }

    pub(in crate::domains::stream::sink) fn get_or_create_actor(
        &self,
        key: &StreamActorKey,
    ) -> Result<Arc<Mutex<StreamActor>>, String> {
        use std::collections::hash_map::Entry;

        let mut actors = self.actors.lock();
        match actors.entry(key.clone()) {
            Entry::Occupied(entry) => Ok(entry.get().clone()),
            Entry::Vacant(entry) => {
                let actor = Arc::new(Mutex::new(StreamActor::new(
                    RouteFamily::try_from(key.family_id)
                        .expect("stream family IDs originate from RouteFamily"),
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

    /// Notify the bounded area and realm coordinator pools after a durable
    /// batch commit. Visibility itself comes from the atomically committed
    /// scope counters; these actors persist advisory watermark rows and emit
    /// ephemeral watermark notices.
    pub(in crate::domains::stream::sink) fn notify_area_batch_committed(
        &self,
        family_id: RouteFamily,
        realm: &str,
        area: &str,
        commit: &crate::domains::stream::protocol::BatchCommitted,
    ) {
        let realm_address = crate::runtime::routing::RouteAddress::new(
            family_id,
            Route::new(format!(
                "stream://{realm}/{}",
                crate::domains::stream::INTERNAL_REALM_SEGMENT
            )),
        );
        let realm_spawned = {
            let store = self.stream_store.clone();
            let realm_owned = realm.to_string();
            self.realm_watermark_actors.ensure_spawned(
                (family_id.as_u64(), realm.to_string()),
                realm_address.clone(),
                move || {
                    crate::domains::stream::realm_actor::RealmActor::new(
                        family_id,
                        realm_owned.clone(),
                        store.clone(),
                    )
                },
            )
        };

        let area_address = crate::runtime::routing::RouteAddress::new(
            family_id,
            Route::new(format!(
                "stream://{realm}/{area}/{}",
                crate::domains::stream::INTERNAL_AREA_SEGMENT
            )),
        );
        let area_spawned = {
            let store = self.stream_store.clone();
            let realm_owned = realm.to_string();
            let area_owned = area.to_string();
            self.area_watermark_actors.ensure_spawned(
                (family_id.as_u64(), realm.to_string(), area.to_string()),
                area_address.clone(),
                move || {
                    crate::domains::stream::area_actor::AreaActor::new(
                        family_id,
                        realm_owned.clone(),
                        area_owned.clone(),
                        store.clone(),
                    )
                },
            )
        };

        let area_envelope = Envelope::new(
            area_address,
            crate::domains::stream::protocol::StreamCoordinationMessage::BatchCommitted(
                commit.clone(),
            ),
        );
        if !area_spawned {
            self.counter_inc("fitz_stream_watermark_coordination_drops_total");
            tracing::warn!(
                domain = "stream",
                route_family = family_id.id(),
                realm,
                area,
                "Stream area watermark coordinator capacity was exhausted"
            );
        } else if let Err(error) = self.router.route_high_priority(area_envelope) {
            self.counter_inc("fitz_stream_watermark_coordination_drops_total");
            tracing::warn!(
                domain = "stream",
                route_family = family_id.id(),
                realm,
                area,
                error = ?error,
                "Stream area watermark coordination notice was not accepted"
            );
        }

        let realm_envelope = Envelope::new(
            realm_address,
            crate::domains::stream::protocol::StreamCoordinationMessage::BatchCommitted(
                commit.clone(),
            ),
        );
        if !realm_spawned {
            self.counter_inc("fitz_stream_watermark_coordination_drops_total");
            tracing::warn!(
                domain = "stream",
                route_family = family_id.id(),
                realm,
                "Stream realm watermark coordinator capacity was exhausted"
            );
        } else if let Err(error) = self.router.route_high_priority(realm_envelope) {
            self.counter_inc("fitz_stream_watermark_coordination_drops_total");
            tracing::warn!(
                domain = "stream",
                route_family = family_id.id(),
                realm,
                error = ?error,
                "Stream realm watermark coordination notice was not accepted"
            );
        }
    }

    pub(in crate::domains::stream::sink) fn mark_admin_snapshot_dirty(&self) {
        self.admin_snapshot_dirty.store(true, Ordering::Relaxed);
        self.refresh_metrics_gauges();
    }

    pub(in crate::domains::stream::sink) fn refresh_metrics_gauges(&self) {
        let counts = self.aggregate_live_counts();

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

    pub(in crate::domains::stream::sink) fn counter_inc(&self, name: &str) {
        if let Some(metrics) = &self.metrics {
            metrics.counter_inc(name);
        } else {
            crate::observability::counter_inc(name);
        }
    }

    pub(in crate::domains::stream::sink) fn counter_add(&self, name: &str, amount: u64) {
        if let Some(metrics) = &self.metrics {
            metrics.counter_add(name, amount);
        } else {
            crate::observability::counter_add(name, amount);
        }
    }

    pub(in crate::domains::stream::sink) fn stream_response_is_failure(
        response: &StreamClientResponseBody,
    ) -> bool {
        matches!(
            response,
            StreamClientResponseBody::Error(_) | StreamClientResponseBody::SubscriptionError(_)
        )
    }

    pub(in crate::domains::stream::sink) fn refresh_admin_snapshot_if_dirty(&self) {
        if self.admin_snapshot_dirty.swap(false, Ordering::AcqRel) {
            self.sync_admin_snapshot();
        }
    }

    /// # Errors
    ///
    /// Returns an error if the requested route cannot be read or if the stream
    /// store rejects the read parameters.
    pub(in crate::domains::stream::sink) fn admin_read_resource_records(
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

    pub(in crate::domains::stream::sink) fn sync_admin_snapshot(&self) {
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
        let family_cores = self.registered_family_cores();
        if family_cores.is_empty() {
            self.overlay_live_actor_snapshots_from(streams);
            return;
        }

        for family_core in family_cores {
            family_core.overlay_live_actor_snapshots_from(streams);
        }
    }

    fn overlay_live_actor_snapshots_from(&self, streams: &mut StreamAdminSnapshotMap) {
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

    fn empty_global_read_cursor(
        &self,
        request: &StreamReadExecution<'_>,
        selector_fingerprint: u64,
        captured_watermark: u64,
    ) -> crate::domains::stream::protocol::ReadCursor {
        crate::domains::stream::protocol::ReadCursor {
            last_resource_offset: 0,
            last_area_offset: None,
            last_realm_offset: None,
            last_global_offset: None,
            has_more: request.from_offset < captured_watermark,
            cursor_fingerprint: Some(self.cursor_integrity_token(
                selector_fingerprint,
                captured_watermark,
                request.from_offset,
            )),
            captured_watermark: Some(captured_watermark),
        }
    }

    fn execute_read_plan(
        &self,
        scope: ReadScope,
        route_filter_area: Option<&str>,
        route_filter_resource: Option<&str>,
        request: &StreamReadExecution<'_>,
    ) -> Result<ReadResponse, String> {
        let parts = route_triplet(request.route.as_str());
        let (items, cursor) = match scope {
            ReadScope::Realm => {
                let parts = parts.ok_or_else(|| "invalid stream route".to_string())?;
                if let Some(resource) = route_filter_resource {
                    self.stream_store.read_realm_resource_posting(
                        &crate::domains::stream::store::ReadRealmPostingParams {
                            family: request.family_id.as_u64(),
                            realm: parts.realm,
                            resource,
                            from_offset: request.from_offset,
                            limit: request.limit,
                            max_bytes: request.max_bytes,
                        },
                        request.filter,
                    )?
                } else {
                    self.stream_store.read_realm_with_filter(
                        request.family_id.as_u64(),
                        parts.realm,
                        request.from_offset,
                        request.limit,
                        request.max_bytes,
                        request.filter,
                    )?
                }
            }
            ReadScope::Area => {
                let parts = parts.ok_or_else(|| "invalid stream route".to_string())?;
                self.stream_store.read_area_with_filter(
                    &crate::domains::stream::store::ReadAreaParams {
                        family: request.family_id.as_u64(),
                        realm: parts.realm,
                        area: parts.area,
                        from_offset: request.from_offset,
                        limit: request.limit,
                        max_bytes: request.max_bytes,
                    },
                    request.filter,
                )?
            }
            ReadScope::Resource => {
                let key = Self::actor_key_for_route(request.family_id, request.route)?;
                let response = self.get_or_create_actor(&key)?.lock().read_with_filter(
                    request.from_offset,
                    request.limit,
                    request.max_bytes,
                    request.filter,
                )?;
                (response.items, response.cursor)
            }
            ReadScope::Global => self.stream_store.read_global_posting(
                &crate::domains::stream::store::ReadGlobalPostingParams {
                    family: request.family_id.as_u64(),
                    from_offset: request.from_offset,
                    limit: request.limit,
                    max_bytes: request.max_bytes,
                    area: route_filter_area,
                    resource: route_filter_resource,
                },
                request.filter,
            )?,
        };
        Ok(ReadResponse { items, cursor })
    }

    fn finalize_read_response(
        &self,
        request: &StreamReadExecution<'_>,
        selector_fingerprint: u64,
        captured_watermark: u64,
        mut response: ReadResponse,
    ) -> ReadResponse {
        apply_global_snapshot_boundary(request.from_offset, captured_watermark, &mut response);
        let next_offset = response
            .cursor
            .last_global_offset
            .map_or(request.from_offset, |offset| offset.saturating_add(1));
        response.cursor.cursor_fingerprint = Some(self.cursor_integrity_token(
            selector_fingerprint,
            captured_watermark,
            next_offset,
        ));
        response.cursor.captured_watermark = Some(captured_watermark);
        response
    }

    pub(in crate::domains::stream::sink) fn encode_read_response_data(
        &self,
        request: StreamReadExecution<'_>,
    ) -> Result<Vec<u8>, String> {
        use crate::domains::stream::route_grammar::StreamRouteShape;

        let shape = crate::domains::stream::route_grammar::classify_stream_route_shape(
            request.route.as_str(),
        )?;
        let (scope, area_filter, resource_filter) = match &shape {
            StreamRouteShape::Resource { .. } => (ReadScope::Resource, None, None),
            StreamRouteShape::Area { .. } => (ReadScope::Area, None, None),
            StreamRouteShape::Realm { .. } => (ReadScope::Realm, None, None),
            StreamRouteShape::RealmFilterResource { resource, .. } => {
                (ReadScope::Realm, None, Some(*resource))
            }
            StreamRouteShape::Global => (ReadScope::Global, None, None),
            StreamRouteShape::GlobalFilterArea { area } => (ReadScope::Global, Some(*area), None),
            StreamRouteShape::GlobalFilterResource { resource } => {
                (ReadScope::Global, None, Some(*resource))
            }
            StreamRouteShape::GlobalFilterAreaResource { area, resource } => {
                (ReadScope::Global, Some(*area), Some(*resource))
            }
        };
        if scope != ReadScope::Global {
            if request.cursor_fingerprint.is_some() || request.captured_watermark.is_some() {
                return Err(
                    "ERR_CURSOR_UNSUPPORTED: snapshot cursors require a global stream selector"
                        .to_string(),
                );
            }
            let response = self.execute_read_plan(scope, area_filter, resource_filter, &request)?;
            return Ok(Self::encode_stream_read_data(
                &response.items,
                &response.cursor,
                false,
            ));
        }

        let selector_fingerprint = crate::domains::stream::route_grammar::cursor_fingerprint(
            request.family_id,
            &shape,
            request.filter,
        );
        let current_frontier = self
            .stream_store
            .get_global_watermark(request.family_id.as_u64())?;
        let captured_watermark = request.captured_watermark.unwrap_or(current_frontier);
        if captured_watermark > current_frontier {
            return Err(
                "ERR_CURSOR_WATERMARK_INVALID: captured watermark is ahead of the visible frontier"
                    .to_string(),
            );
        }
        let is_continuation =
            request.cursor_fingerprint.is_some() || request.captured_watermark.is_some();
        let expected_token = self.cursor_integrity_token(
            selector_fingerprint,
            captured_watermark,
            request.from_offset,
        );
        if is_continuation && request.cursor_fingerprint != Some(expected_token) {
            return Err(
                "ERR_CURSOR_SELECTOR_MISMATCH: cursor was issued for a different stream route \
                 family, selector, filter, snapshot, or position"
                    .to_string(),
            );
        }
        if request.limit == 0 {
            let cursor =
                self.empty_global_read_cursor(&request, selector_fingerprint, captured_watermark);
            return Ok(Self::encode_stream_read_data(&[], &cursor, true));
        }
        let response = self.execute_read_plan(scope, area_filter, resource_filter, &request)?;
        let response = self.finalize_read_response(
            &request,
            selector_fingerprint,
            captured_watermark,
            response,
        );
        Ok(Self::encode_stream_read_data(
            &response.items,
            &response.cursor,
            true,
        ))
    }

    pub(in crate::domains::stream::sink) fn handle_domain_publish(
        &self,
        event: &crate::runtime::DomainPublishEvent,
    ) {
        self.route_ready_notifications(self.collect_ready_notifications(event));
    }

    pub(in crate::domains::stream::sink) fn handle_visibility_advance(&self, family: RouteFamily) {
        self.route_ready_notifications(self.collect_visible_pending_notifications(family.as_u64()));
    }

    fn route_ready_notifications(&self, ready: Vec<ReadyStreamNotification>) {
        #[cfg(test)]
        let mut payload_encoder = PayloadEncoder::with_capacity(256);
        for notification in ready {
            let target = notification.target;
            let event = notification.event;
            if *target.subscriber.family() != event.family_id {
                crate::observability::counter_inc("fitz_stream_notify_drops_total");
                continue;
            }
            #[cfg(test)]
            self.route_commit_notify(
                target.session_id,
                target.subscription_id,
                &target.subscriber,
                &event,
                &mut payload_encoder,
            );
            #[cfg(not(test))]
            self.route_commit_notify(
                target.session_id,
                target.subscription_id,
                &target.subscriber,
                &event,
            );
        }
    }

    #[cfg(test)]
    pub(in crate::domains::stream::sink) fn route_commit_notify(
        &self,
        session_id: u64,
        subscription_id: u64,
        subscriber: &crate::runtime::routing::RouteAddress,
        event: &crate::runtime::DomainPublishEvent,
        payload_encoder: &mut PayloadEncoder,
    ) {
        let notify_payload = crate::dispatch::protocol::stream_codec::encode_notify_into(
            payload_encoder,
            subscription_id,
            &event.route,
            &event.payload,
        );
        let notify_ctx = FrameContext::new(
            session_id,
            crate::dispatch::protocol::frame::ChannelId::Sub,
            crate::dispatch::protocol::tlv::MessageType::new(609),
            bytes::Bytes::from(notify_payload),
            event.family_id,
        );
        let notify_envelope = Envelope::new(subscriber.clone(), notify_ctx);
        if self.router.route(notify_envelope).is_err() {
            crate::observability::counter_inc("fitz_stream_notify_drops_total");
        }
    }

    #[cfg(not(test))]
    pub(in crate::domains::stream::sink) fn route_commit_notify(
        &self,
        session_id: u64,
        subscription_id: u64,
        subscriber: &crate::runtime::routing::RouteAddress,
        event: &crate::runtime::DomainPublishEvent,
    ) {
        let notify = crate::domains::stream::StreamClientNotification::new(
            session_id,
            event.family_id,
            subscription_id,
            event.route.clone(),
            event.payload.clone(),
        );
        let notify_envelope = Envelope::new(subscriber.clone(), notify);
        if self.router.route(notify_envelope).is_err() {
            crate::observability::counter_inc("fitz_stream_notify_drops_total");
        }
    }

    pub(in crate::domains::stream::sink) fn unsubscribe_all(&self, session_id: u64) {
        let mut families = self.families.lock();
        for (family_id, state) in families.iter_mut() {
            state.remove_session(
                RouteFamily::try_from(*family_id)
                    .expect("stream family IDs originate from RouteFamily"),
                session_id,
            );
        }
        families.retain(|_, state| !state.is_empty());
        drop(families);
        self.remove_pending_notifications_for_session(session_id);
        self.refresh_metrics_gauges();
    }

    pub(in crate::domains::stream::sink) fn cleanup_session(&self, session_id: u64) {
        self.unsubscribe_all(session_id);

        let actors = self
            .actors
            .lock()
            .iter()
            .map(|(key, actor)| (key.family_id, actor.clone()))
            .collect::<Vec<_>>();
        let mut removed_sessions = Vec::new();
        let mut advanced_families = std::collections::BTreeSet::new();
        for (family_id, actor) in actors {
            if let Some(stream_session_id) = actor.lock().cleanup_session(session_id) {
                removed_sessions.push(stream_session_id);
                advanced_families.insert(family_id);
            }
        }

        for family_id in advanced_families {
            self.handle_visibility_advance(
                RouteFamily::try_from(family_id)
                    .expect("stream family IDs originate from RouteFamily"),
            );
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

    pub(in crate::domains::stream::sink) fn live_counts(&self) -> StreamLiveCounts {
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

    fn registered_family_cores(&self) -> Vec<Arc<StreamDomainCore>> {
        let mut family_cores = self.family_cores.lock();
        let mut live = Vec::with_capacity(family_cores.len());
        family_cores.retain(|_, weak| {
            if let Some(core) = weak.upgrade() {
                live.push(core);
                true
            } else {
                false
            }
        });
        live
    }

    fn aggregate_live_counts(&self) -> StreamLiveCounts {
        let family_cores = self.registered_family_cores();
        if family_cores.is_empty() {
            return self.live_counts();
        }

        family_cores
            .into_iter()
            .fold(StreamLiveCounts::default(), |mut total, family_core| {
                let counts = family_core.live_counts();
                total.streams = total.streams.saturating_add(counts.streams);
                total.append_sessions =
                    total.append_sessions.saturating_add(counts.append_sessions);
                total.subscriptions = total.subscriptions.saturating_add(counts.subscriptions);
                total
            })
    }
}

impl StreamDomainRuntime<'_> {
    pub(in crate::domains::stream::sink) fn refresh_admin_snapshot_if_dirty(&self) {
        self.core.refresh_admin_snapshot_if_dirty();
    }

    #[cfg(test)]
    pub(in crate::domains::stream::sink) fn sync_admin_snapshot(&self) {
        self.core.sync_admin_snapshot();
    }

    pub(in crate::domains::stream::sink) fn live_counts(&self) -> StreamLiveCounts {
        self.core.live_counts()
    }

    pub(in crate::domains::stream::sink) fn admin_read_resource_records(
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
}
