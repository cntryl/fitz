use super::parse_route_triplet;
use crate::protocol::frame_context::FrameContext;
use crate::runtime::{DeliveryError, Envelope, MailboxSink, Router};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

/// Subscription entry for stream change notifications
struct StreamSubscription {
    pattern: crate::runtime::matcher::Pattern,
    session_id: u64,
    subscription_id: u64,
    subscriber: crate::runtime::routing::RouteAddress,
}

/// Per-family stream subscription state
struct StreamFamilyState {
    subscriptions: HashMap<u64, StreamSubscription>,
    index: crate::runtime::SubscriptionIndex,
    exact_routes: HashMap<String, Vec<u64>>,
    wildcard_subscription_count: usize,
}

/// Per-route next offset and session tracking for expected_offset enforcement
#[derive(Clone)]
struct PendingStreamRecord {
    body: bytes::Bytes,
}

struct PendingStreamSession {
    route: String,
    initial_next_offset: u64,
    records: Vec<PendingStreamRecord>,
}

#[derive(Clone)]
struct CommittedStreamRecord {
    offset: u64,
    body: bytes::Bytes,
}

struct StreamRouteState {
    next_offset: u64,
    records: Vec<CommittedStreamRecord>,
    last_data: Vec<u8>,
    metadata_data: Vec<u8>,
}

struct StreamWriteState {
    routes: HashMap<String, StreamRouteState>,
    sessions: HashMap<u64, PendingStreamSession>,
    next_area_offsets: HashMap<(String, String), u64>,
    next_realm_offsets: HashMap<String, u64>,
}

/// Stream domain sink: append-only streaming operations with subscription tracking
///
/// Supports dual-path delivery:
/// - PATH 1: `DomainPublishEvent` from stream actors (subscription matching + fanout)
/// - PATH 2: `FrameContext` from client wire frames (BEGIN/APPEND/COMMIT/READ/SUBSCRIBE/UNSUBSCRIBE)
///
/// Enforces expected_offset at Begin: rejects with concurrency error when client's
/// expected_offset does not match the stream's next offset for that route.
pub struct StreamDomainSink {
    #[allow(dead_code)]
    store: Arc<cntryl_midge::Engine>,
    next_session_id: AtomicU64,
    write_state: Mutex<StreamWriteState>,
    families: Mutex<HashMap<u64, StreamFamilyState>>,
    next_sub_id: AtomicU64,
    router: Arc<Router>,
    admin_read_model: Arc<crate::api::admin::read_model::AdminReadModel>,
    active: AtomicBool,
}

impl StreamDomainSink {
    pub fn new(
        store: Arc<cntryl_midge::Engine>,
        router: Arc<Router>,
        admin_read_model: Arc<crate::api::admin::read_model::AdminReadModel>,
    ) -> Self {
        Self {
            store,
            next_session_id: AtomicU64::new(1),
            write_state: Mutex::new(StreamWriteState {
                routes: HashMap::new(),
                sessions: HashMap::new(),
                next_area_offsets: HashMap::new(),
                next_realm_offsets: HashMap::new(),
            }),
            families: Mutex::new(HashMap::new()),
            next_sub_id: AtomicU64::new(1),
            router,
            admin_read_model,
            active: AtomicBool::new(true),
        }
    }

    pub fn stop(&self) {
        self.active.store(false, Ordering::Relaxed);
    }

    fn sync_admin_snapshot(&self) {
        let state = self.write_state.lock();
        let mut sessions_by_route: HashMap<String, usize> = HashMap::new();
        for session in state.sessions.values() {
            *sessions_by_route.entry(session.route.clone()).or_insert(0) += 1;
        }
        let streams = state
            .routes
            .iter()
            .filter_map(|(route, route_state)| {
                parse_route_triplet(route).map(|(realm, area, resource)| {
                    crate::api::admin::StreamInfo {
                        realm,
                        area,
                        resource,
                        offset: route_state.next_offset.saturating_sub(1),
                        watermark: route_state.next_offset,
                        size_bytes: 0,
                        sessions_active: sessions_by_route.get(route).copied().unwrap_or(0),
                    }
                })
            })
            .collect();
        drop(state);
        self.admin_read_model.replace_streams(streams);
    }

    fn encode_stream_read_data(
        records: &[CommittedStreamRecord],
        from_offset: u64,
        limit: u64,
        max_bytes: Option<usize>,
    ) -> Vec<u8> {
        let mut selected = Vec::new();
        let mut total_bytes = 0usize;

        for record in records.iter().filter(|record| record.offset >= from_offset) {
            if selected.len() >= limit as usize {
                break;
            }

            if let Some(max_bytes) = max_bytes {
                let projected = total_bytes + record.body.len();
                if !selected.is_empty() && projected > max_bytes {
                    break;
                }
                total_bytes = projected;
            }

            selected.push(record);
        }

        let mut encoder = crate::protocol::payload_codec::PayloadEncoder::new();
        encoder.put_u32(selected.len() as u32);
        for record in selected {
            encoder.put_u64(record.offset);
            encoder.put_bytes(record.body.as_ref());
        }
        encoder.finish()
    }

    fn encode_stream_last_data(record: &CommittedStreamRecord) -> Vec<u8> {
        let mut encoder = crate::protocol::payload_codec::PayloadEncoder::new();
        encoder.put_u64(record.offset);
        encoder.put_bytes(record.body.as_ref());
        encoder.finish()
    }

    fn allocate_commit_offsets(
        state: &mut StreamWriteState,
        route: &str,
        first_resource_offset: u64,
        batch_size: usize,
    ) -> (u64, u64, u64, u64) {
        let batch_size = batch_size as u64;

        if let Some((realm, area, _resource)) = parse_route_triplet(route) {
            let area_key = (realm.clone(), area);
            let first_area_offset = *state.next_area_offsets.entry(area_key.clone()).or_insert(0);
            let first_realm_offset = *state.next_realm_offsets.entry(realm.clone()).or_insert(0);

            let next_area_offset = first_area_offset.saturating_add(batch_size);
            let next_realm_offset = first_realm_offset.saturating_add(batch_size);
            state.next_area_offsets.insert(area_key, next_area_offset);
            state.next_realm_offsets.insert(realm, next_realm_offset);

            return (
                first_area_offset,
                next_area_offset.saturating_sub(1),
                first_realm_offset,
                next_realm_offset.saturating_sub(1),
            );
        }

        let last_resource_offset = first_resource_offset
            .saturating_add(batch_size)
            .saturating_sub(1);
        (
            first_resource_offset,
            last_resource_offset,
            first_resource_offset,
            last_resource_offset,
        )
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

    fn encode_stream_metadata_summary(first_offset: u64, last_offset: u64, count: u64) -> Vec<u8> {
        let mut encoder = crate::protocol::payload_codec::PayloadEncoder::new();
        encoder.put_u64(first_offset);
        encoder.put_u64(last_offset);
        encoder.put_u64(count);
        encoder.finish()
    }

    fn handle_domain_publish(
        &self,
        event: &crate::runtime::DomainPublishEvent,
    ) -> Result<(), DeliveryError> {
        let family_id = event.family_id.as_u64();
        tracing::info!(
            domain = "stream",
            family_id = family_id,
            route = %event.route,
            "Stream: handle_domain_publish called (ENTRY)"
        );
        let families = self.families.lock();
        if let Some(state) = families.get(&family_id) {
            let mut payload_encoder =
                crate::protocol::payload_codec::PayloadEncoder::with_capacity(256);
            tracing::info!(
                domain = "stream",
                family_id = family_id,
                subscription_count = state.subscriptions.len(),
                "Stream: found family state with subscriptions"
            );
            let mut matched = 0;
            if let Some(sub_ids) = state.exact_routes.get(event.route.as_str()) {
                for sub_id in sub_ids {
                    if let Some(sub) = state.subscriptions.get(sub_id) {
                        matched += 1;
                        let notify_payload = crate::protocol::stream_codec::encode_notify_into(
                            &mut payload_encoder,
                            sub.subscription_id,
                            &event.route,
                            &event.payload,
                        );
                        let notify_ctx = FrameContext::new(
                            sub.session_id,
                            crate::protocol::frame::ChannelId::Sub,
                            crate::protocol::tlv::MessageType::new(609),
                            bytes::Bytes::from(notify_payload),
                            crate::runtime::routing::RouteFamily::from_u32(
                                sub.subscriber.family().id(),
                            ),
                        );
                        let notify_envelope = Envelope::new(sub.subscriber.clone(), notify_ctx);
                        if let Err(e) = self.router.route(notify_envelope) {
                            tracing::warn!(
                                domain = "stream",
                                subscription_id = sub.subscription_id,
                                destination = %sub.subscriber,
                                error = ?e,
                                "Stream: failed to route 609 to subscriber inbox"
                            );
                        } else {
                            tracing::info!(
                                domain = "stream",
                                subscription_id = sub.subscription_id,
                                destination = %sub.subscriber,
                                "Stream: routed 609 to subscriber"
                            );
                        }
                    }
                }
            }
            if state.wildcard_subscription_count > 0 {
                let wildcard_matches = state.index.match_all_with_capacity(
                    event.family_id,
                    &event.route,
                    state.wildcard_subscription_count,
                );
                for sub_id in wildcard_matches {
                    if let Some(sub) = state.subscriptions.get(&sub_id.0) {
                        matched += 1;
                        let notify_payload = crate::protocol::stream_codec::encode_notify_into(
                            &mut payload_encoder,
                            sub.subscription_id,
                            &event.route,
                            &event.payload,
                        );
                        let notify_ctx = FrameContext::new(
                            sub.session_id,
                            crate::protocol::frame::ChannelId::Sub,
                            crate::protocol::tlv::MessageType::new(609),
                            bytes::Bytes::from(notify_payload),
                            crate::runtime::routing::RouteFamily::from_u32(
                                sub.subscriber.family().id(),
                            ),
                        );
                        let notify_envelope = Envelope::new(sub.subscriber.clone(), notify_ctx);
                        if let Err(e) = self.router.route(notify_envelope) {
                            tracing::warn!(
                                domain = "stream",
                                subscription_id = sub.subscription_id,
                                destination = %sub.subscriber,
                                error = ?e,
                                "Stream: failed to route 609 to subscriber inbox"
                            );
                        } else {
                            tracing::info!(
                                domain = "stream",
                                subscription_id = sub.subscription_id,
                                destination = %sub.subscriber,
                                "Stream: routed 609 to subscriber"
                            );
                        }
                    }
                }
            }
            if matched == 0 {
                tracing::info!(
                    domain = "stream",
                    family_id = family_id,
                    route = %event.route,
                    subscription_count = state.subscriptions.len(),
                    "Stream: NO SUBSCRIPTIONS MATCHED event route"
                );
            } else {
                tracing::info!(
                    domain = "stream",
                    family_id = family_id,
                    matched = matched,
                    "Stream: matched {} subscriptions for route",
                    matched
                );
            }
        } else {
            tracing::info!(
                domain = "stream",
                family_id = family_id,
                route = %event.route,
                "Stream: NO FAMILY STATE for event (no subscriptions in this family)"
            );
        }
        Ok(())
    }

    pub fn unsubscribe_all(&self, session_id: u64) {
        let mut families = self.families.lock();
        for (family_id, state) in families.iter_mut() {
            let removed_ids: Vec<u64> = state
                .subscriptions
                .iter()
                .filter_map(|(sub_id, sub)| (sub.session_id == session_id).then_some(*sub_id))
                .collect();
            for sub_id in removed_ids {
                if let Some(sub) = state.subscriptions.remove(&sub_id) {
                    if sub.pattern.route().contains('*') {
                        let pattern = crate::runtime::routing::Route::new(sub.pattern.route());
                        state.index.remove(
                            crate::runtime::routing::RouteFamily::new(*family_id),
                            &pattern,
                            crate::runtime::SubscriptionId(sub_id),
                        );
                        state.wildcard_subscription_count =
                            state.wildcard_subscription_count.saturating_sub(1);
                    } else {
                        let route_key = sub.pattern.route().to_string();
                        let is_empty =
                            if let Some(route_ids) = state.exact_routes.get_mut(&route_key) {
                                route_ids.retain(|id| *id != sub_id);
                                route_ids.is_empty()
                            } else {
                                false
                            };
                        if is_empty {
                            state.exact_routes.remove(&route_key);
                        }
                    }
                }
            }
        }
        tracing::debug!(
            domain = "stream",
            session = session_id,
            "All stream subscriptions removed for session"
        );
    }

    pub fn subscription_count(&self) -> usize {
        let families = self.families.lock();
        families.values().map(|s| s.subscriptions.len()).sum()
    }

    pub fn stream_count(&self) -> usize {
        self.write_state.lock().routes.len()
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
            self.unsubscribe_all(cleanup.session_id);
            return Ok(());
        }

        tracing::debug!(
            domain = "stream",
            destination = %envelope.destination(),
            source = ?envelope.source(),
            "Stream domain sink: received envelope"
        );

        let frame_ctx = match envelope.payload::<FrameContext>() {
            Some(ctx) => ctx.clone(),
            None => {
                tracing::warn!(domain = "stream", "Envelope payload was not FrameContext");
                return Err(DeliveryError::ActorStopped);
            }
        };
        let mut payload_encoder =
            crate::protocol::payload_codec::PayloadEncoder::with_capacity(256);

        let stream_msg = match crate::protocol::stream_codec::parse_request(
            &frame_ctx,
            &frame_ctx.payload,
            *envelope.destination().family(),
            crate::session::SessionId(frame_ctx.session_id),
            if let Some(src) = envelope.source() {
                src.clone()
            } else {
                crate::runtime::routing::RouteAddress::new(
                    *envelope.destination().family(),
                    crate::runtime::routing::Route::new(format!(
                        "inbox://session/{}",
                        frame_ctx.session_id
                    )),
                )
            },
        ) {
            Ok(msg) => {
                tracing::debug!(
                    domain = "stream",
                    session = frame_ctx.session_id,
                    msg_type = frame_ctx.msg_type.as_u16(),
                    "Stream: parsed message successfully"
                );
                msg
            }
            Err(e) => {
                tracing::warn!(domain = "stream", error = %e, "Failed to parse stream message");
                return Err(DeliveryError::ActorStopped);
            }
        };

        use crate::domains::stream::protocol::StreamMessage;
        use crate::protocol::stream_codec::StreamResponse;

        let (response, commit_notify, should_sync_admin_snapshot) = match stream_msg {
            StreamMessage::Begin {
                route,
                expected_offset,
                ingest_metadata: _,
                ..
            } => {
                let route_key = route.as_str().to_string();
                let mut state = self.write_state.lock();
                let current_next_offset = state
                    .routes
                    .get(&route_key)
                    .map_or(0, |route_state| route_state.next_offset);
                if expected_offset != current_next_offset {
                    return {
                        drop(state);
                        let response_bytes = crate::protocol::stream_codec::encode_response_into(
                            &mut payload_encoder,
                            &StreamResponse::Error("concurrency conflict".to_string()),
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
                        Ok(())
                    };
                }
                let session_id = self.next_session_id.fetch_add(1, Ordering::Relaxed);
                state
                    .routes
                    .entry(route_key.clone())
                    .or_insert_with(|| StreamRouteState {
                        next_offset: 0,
                        records: Vec::new(),
                        last_data: Vec::new(),
                        metadata_data: Vec::new(),
                    });
                state.sessions.insert(
                    session_id,
                    PendingStreamSession {
                        route: route_key,
                        initial_next_offset: current_next_offset,
                        records: Vec::new(),
                    },
                );
                (
                    StreamResponse::Ok {
                        session_id: Some(session_id),
                        data: vec![],
                    },
                    None,
                    true,
                )
            }
            StreamMessage::Append {
                session_id,
                body,
                metadata: _,
            } => {
                let mut state = self.write_state.lock();
                let maybe_offset = state.sessions.get_mut(&session_id).map(|session| {
                    let assigned_offset =
                        session.initial_next_offset + session.records.len() as u64;
                    session.records.push(PendingStreamRecord { body });
                    assigned_offset
                });

                let data = if let Some(offset) = maybe_offset {
                    let mut encoder = crate::protocol::payload_codec::PayloadEncoder::new();
                    encoder.put_u64(offset);
                    encoder.finish()
                } else {
                    Vec::new()
                };

                (
                    StreamResponse::Ok {
                        session_id: None,
                        data,
                    },
                    None,
                    false,
                )
            }
            StreamMessage::Commit { session_id, .. } => {
                let mut state = self.write_state.lock();
                let commit_notify = state.sessions.remove(&session_id).map(|session| {
                    let batch_size = session.records.len();
                    let route_key = session.route.clone();
                    let route_state =
                        state
                            .routes
                            .entry(route_key.clone())
                            .or_insert_with(|| StreamRouteState {
                                next_offset: 0,
                                records: Vec::new(),
                                last_data: Vec::new(),
                                metadata_data: Vec::new(),
                            });
                    let first_offset = route_state.next_offset;
                    let mut committed = Vec::with_capacity(batch_size);
                    let mut current_offset = route_state.next_offset;
                    for record in session.records {
                        committed.push(CommittedStreamRecord {
                            offset: current_offset,
                            body: record.body,
                        });
                        current_offset += 1;
                    }

                    route_state.next_offset = current_offset;
                    if !committed.is_empty() {
                        let last_record = committed.last().cloned();
                        route_state.records.extend(committed);
                        let first_record_offset = route_state
                            .records
                            .first()
                            .map(|record| record.offset)
                            .unwrap_or(0);
                        let total_records = route_state.records.len() as u64;
                        if let Some(last_record) = last_record {
                            route_state.last_data = Self::encode_stream_last_data(&last_record);
                            route_state.metadata_data = Self::encode_stream_metadata_summary(
                                first_record_offset,
                                last_record.offset,
                                total_records,
                            );
                        }
                    }

                    let last_offset = current_offset.saturating_sub(1);
                    let (
                        first_area_offset,
                        last_area_offset,
                        first_realm_offset,
                        last_realm_offset,
                    ) = Self::allocate_commit_offsets(
                        &mut state,
                        &route_key,
                        first_offset,
                        batch_size,
                    );
                    let payload = Self::encode_stream_commit_notify_payload(
                        first_offset,
                        last_offset,
                        first_area_offset,
                        last_area_offset,
                        first_realm_offset,
                        last_realm_offset,
                        batch_size,
                    );
                    (crate::runtime::routing::Route::new(route_key), payload)
                });
                (
                    StreamResponse::Ok {
                        session_id: None,
                        data: vec![],
                    },
                    commit_notify,
                    true,
                )
            }
            StreamMessage::Rollback { session_id, .. } => {
                let mut state = self.write_state.lock();
                state.sessions.remove(&session_id);
                (
                    StreamResponse::Ok {
                        session_id: None,
                        data: vec![],
                    },
                    None,
                    true,
                )
            }
            StreamMessage::Read {
                route,
                from_offset,
                limit,
                max_bytes,
                ..
            } => {
                let state = self.write_state.lock();
                let data = state
                    .routes
                    .get(route.as_str())
                    .map(|route_state| {
                        Self::encode_stream_read_data(
                            &route_state.records,
                            from_offset,
                            limit,
                            max_bytes,
                        )
                    })
                    .unwrap_or_default();
                (
                    StreamResponse::Ok {
                        session_id: None,
                        data,
                    },
                    None,
                    false,
                )
            }
            StreamMessage::Last { route, .. } => {
                let state = self.write_state.lock();
                let data = state
                    .routes
                    .get(route.as_str())
                    .map(|route_state| route_state.last_data.clone())
                    .unwrap_or_default();
                (
                    StreamResponse::Ok {
                        session_id: None,
                        data,
                    },
                    None,
                    false,
                )
            }
            StreamMessage::GetMetadata { route, .. } => {
                let state = self.write_state.lock();
                let data = state
                    .routes
                    .get(route.as_str())
                    .map(|route_state| route_state.metadata_data.clone())
                    .unwrap_or_default();
                (
                    StreamResponse::Ok {
                        session_id: None,
                        data,
                    },
                    None,
                    false,
                )
            }
            StreamMessage::Subscribe {
                family_id,
                pattern,
                session_id,
                subscriber,
            } => {
                let fam_id = family_id.as_u64();

                let mut families = self.families.lock();
                let state = families.entry(fam_id).or_insert_with(|| StreamFamilyState {
                    subscriptions: HashMap::new(),
                    index: crate::runtime::SubscriptionIndex::new(),
                    exact_routes: HashMap::new(),
                    wildcard_subscription_count: 0,
                });

                let existing_sub_id = state
                    .subscriptions
                    .values()
                    .find(|s| s.session_id == session_id && s.pattern.route() == pattern.as_str())
                    .map(|s| s.subscription_id);

                let sub_id = if let Some(id) = existing_sub_id {
                    tracing::debug!(
                        domain = "stream",
                        session = session_id,
                        subscription_id = id,
                        pattern = pattern.as_str(),
                        "Stream subscription already exists (idempotent)"
                    );
                    id
                } else {
                    let new_id = self.next_sub_id.fetch_add(1, Ordering::Relaxed);
                    let pat = crate::runtime::matcher::Pattern::new(pattern.as_str());

                    if pattern.as_str().contains('*') {
                        state.index.insert(
                            family_id,
                            &pattern,
                            crate::runtime::SubscriptionId(new_id),
                        );
                        state.wildcard_subscription_count += 1;
                    } else {
                        state
                            .exact_routes
                            .entry(pattern.as_str().to_string())
                            .or_default()
                            .push(new_id);
                    }

                    state.subscriptions.insert(
                        new_id,
                        StreamSubscription {
                            pattern: pat,
                            session_id,
                            subscription_id: new_id,
                            subscriber,
                        },
                    );

                    tracing::debug!(
                        domain = "stream",
                        session = session_id,
                        subscription_id = new_id,
                        pattern = pattern.as_str(),
                        "Stream subscription added"
                    );
                    new_id
                };

                (
                    StreamResponse::Ok {
                        session_id: Some(sub_id),
                        data: vec![],
                    },
                    None,
                    false,
                )
            }
            StreamMessage::Unsubscribe {
                family_id,
                pattern,
                session_id,
                ..
            } => {
                let fam_id = family_id.as_u64();
                let mut families = self.families.lock();
                if let Some(state) = families.get_mut(&fam_id) {
                    let removed_ids: Vec<u64> = state
                        .subscriptions
                        .iter()
                        .filter_map(|(sub_id, sub)| {
                            (sub.session_id == session_id
                                && sub.pattern.route() == pattern.as_str())
                            .then_some(*sub_id)
                        })
                        .collect();
                    for sub_id in removed_ids {
                        if let Some(sub) = state.subscriptions.remove(&sub_id) {
                            if sub.pattern.route().contains('*') {
                                state.index.remove(
                                    family_id,
                                    &pattern,
                                    crate::runtime::SubscriptionId(sub_id),
                                );
                                state.wildcard_subscription_count =
                                    state.wildcard_subscription_count.saturating_sub(1);
                            } else {
                                let is_empty = if let Some(route_ids) =
                                    state.exact_routes.get_mut(pattern.as_str())
                                {
                                    route_ids.retain(|id| *id != sub_id);
                                    route_ids.is_empty()
                                } else {
                                    false
                                };
                                if is_empty {
                                    state.exact_routes.remove(pattern.as_str());
                                }
                            }
                        }
                    }
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
            StreamMessage::UnsubscribeAll { session_id, .. } => {
                self.unsubscribe_all(session_id);
                (
                    StreamResponse::Ok {
                        session_id: None,
                        data: vec![],
                    },
                    None,
                    false,
                )
            }
            _ => (
                StreamResponse::Ok {
                    session_id: None,
                    data: vec![],
                },
                None,
                false,
            ),
        };
        if should_sync_admin_snapshot {
            self.sync_admin_snapshot();
        }

        if let Some((route, payload)) = commit_notify {
            tracing::info!(
                domain = "stream",
                route = %route,
                route_family = frame_ctx.route_family.id(),
                "Stream: commit triggered availability notification - CALLING handle_domain_publish"
            );
            let event =
                crate::runtime::DomainPublishEvent::new(frame_ctx.route_family, route, payload);
            if let Err(e) = self.handle_domain_publish(&event) {
                tracing::warn!(domain = "stream", error = ?e, "Stream: handle_domain_publish FAILED");
            } else {
                tracing::info!(domain = "stream", "Stream: handle_domain_publish SUCCEEDED");
            }
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

        Ok(())
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_stream_domain_sink() {
        // Arrange
        let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
        let router = Arc::new(Router::new());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();

        // Act
        let sink = StreamDomainSink::new(store, router, admin_read_model);

        // Assert
        assert!(sink.active.load(Ordering::Relaxed));
    }

    #[test]
    fn should_encode_stream_commit_notify_payload_with_full_offset_metadata() {
        // Arrange
        let payload =
            StreamDomainSink::encode_stream_commit_notify_payload(10, 11, 20, 21, 30, 31, 2);

        // Act
        let decoded: serde_json::Value =
            serde_json::from_slice(&payload).expect("stream commit payload should be valid JSON");

        // Assert
        assert_eq!(decoded["event"], "committed");
        assert_eq!(decoded["first_resource_offset"], 10);
        assert_eq!(decoded["last_resource_offset"], 11);
        assert_eq!(decoded["first_area_offset"], 20);
        assert_eq!(decoded["last_area_offset"], 21);
        assert_eq!(decoded["first_realm_offset"], 30);
        assert_eq!(decoded["last_realm_offset"], 31);
        assert_eq!(decoded["batch_size"], 2);
    }

    #[test]
    fn should_allocate_area_offsets_per_area_given_multiple_stream_routes() {
        // Arrange
        let mut state = StreamWriteState {
            routes: HashMap::new(),
            sessions: HashMap::new(),
            next_area_offsets: HashMap::new(),
            next_realm_offsets: HashMap::new(),
        };

        // Act
        let first =
            StreamDomainSink::allocate_commit_offsets(&mut state, "stream://prod/app/users", 0, 2);
        let second =
            StreamDomainSink::allocate_commit_offsets(&mut state, "stream://prod/app/orders", 0, 1);
        let third =
            StreamDomainSink::allocate_commit_offsets(&mut state, "stream://prod/ops/logs", 0, 1);

        // Assert
        assert_eq!(first.0, 0);
        assert_eq!(first.1, 1);
        assert_eq!(second.0, 2);
        assert_eq!(second.1, 2);
        assert_eq!(third.0, 0);
        assert_eq!(third.1, 0);
    }

    #[test]
    fn should_allocate_realm_offsets_per_realm_given_multiple_stream_routes() {
        // Arrange
        let mut state = StreamWriteState {
            routes: HashMap::new(),
            sessions: HashMap::new(),
            next_area_offsets: HashMap::new(),
            next_realm_offsets: HashMap::new(),
        };

        // Act
        let first =
            StreamDomainSink::allocate_commit_offsets(&mut state, "stream://prod/app/users", 0, 2);
        let second =
            StreamDomainSink::allocate_commit_offsets(&mut state, "stream://prod/app/orders", 0, 1);
        let third =
            StreamDomainSink::allocate_commit_offsets(&mut state, "stream://prod/ops/logs", 0, 1);

        // Assert
        assert_eq!(first.2, 0);
        assert_eq!(first.3, 1);
        assert_eq!(second.2, 2);
        assert_eq!(second.3, 2);
        assert_eq!(third.2, 3);
        assert_eq!(third.3, 3);
    }
}
