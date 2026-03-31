use super::parse_route_triplet;
use super::subscription_state::{RoutedSubscription, RoutedSubscriptionSet};
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
    families: Mutex<HashMap<u64, RoutedSubscriptionSet<StreamSubscription>>>,
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
                subscription_count = state.subscription_count(),
                "Stream: found family state with subscriptions"
            );
            let matched = state.for_each_matching(event, |subscription| {
                self.route_commit_notify(subscription, event, &mut payload_encoder);
            });
            if matched == 0 {
                tracing::info!(
                    domain = "stream",
                    family_id = family_id,
                    route = %event.route,
                    subscription_count = state.subscription_count(),
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

    fn route_commit_notify(
        &self,
        subscription: &StreamSubscription,
        event: &crate::runtime::DomainPublishEvent,
        payload_encoder: &mut crate::protocol::payload_codec::PayloadEncoder,
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
            crate::runtime::routing::RouteFamily::from_u32(subscription.subscriber.family().id()),
        );
        let notify_envelope = Envelope::new(subscription.subscriber.clone(), notify_ctx);
        if let Err(error) = self.router.route(notify_envelope) {
            tracing::warn!(
                domain = "stream",
                subscription_id = subscription.subscription_id,
                destination = %subscription.subscriber,
                error = ?error,
                "Stream: failed to route 609 to subscriber inbox"
            );
        } else {
            tracing::info!(
                domain = "stream",
                subscription_id = subscription.subscription_id,
                destination = %subscription.subscriber,
                "Stream: routed 609 to subscriber"
            );
        }
    }

    pub fn unsubscribe_all(&self, session_id: u64) {
        let mut families = self.families.lock();
        for (family_id, state) in families.iter_mut() {
            state.remove_session(
                crate::runtime::routing::RouteFamily::new(*family_id),
                session_id,
            );
        }
        tracing::debug!(
            domain = "stream",
            session = session_id,
            "All stream subscriptions removed for session"
        );
    }

    pub fn subscription_count(&self) -> usize {
        let families = self.families.lock();
        families
            .values()
            .map(|state| state.subscription_count())
            .sum()
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
                let state = families
                    .entry(fam_id)
                    .or_insert_with(RoutedSubscriptionSet::new);

                let existing_sub_id = state.find_existing_id(session_id, pattern.as_str());

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
                    state.insert(
                        family_id,
                        StreamSubscription {
                            pattern: crate::runtime::matcher::Pattern::new(pattern.as_str()),
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
    use crate::protocol::frame::ChannelId;
    use crate::protocol::frame_context::FrameContext;
    use crate::protocol::payload_codec::{PayloadDecoder, PayloadEncoder};
    use crate::protocol::tlv::MessageType;
    use crate::runtime::mailbox::Mailbox;
    use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
    use bytes::Bytes;
    use std::sync::Arc;

    fn encode_stream_begin(route: &str, expected_offset: u64) -> Bytes {
        let mut encoder = PayloadEncoder::new();
        encoder.put_string(route);
        encoder.put_u64(expected_offset);
        encoder.put_u8(0);
        Bytes::from(encoder.finish())
    }

    fn encode_stream_append(session_id: u64, body: &[u8]) -> Bytes {
        let mut encoder = PayloadEncoder::new();
        encoder.put_u64(session_id);
        encoder.put_bytes(body);
        encoder.put_u8(0);
        Bytes::from(encoder.finish())
    }

    fn encode_stream_commit(session_id: u64) -> Bytes {
        let mut encoder = PayloadEncoder::new();
        encoder.put_u64(session_id);
        encoder.put_u8(0);
        Bytes::from(encoder.finish())
    }

    fn encode_stream_subscribe(pattern: &str) -> Bytes {
        let mut encoder = PayloadEncoder::new();
        encoder.put_string(pattern);
        Bytes::from(encoder.finish())
    }

    fn decode_stream_ok_response(payload: &[u8]) -> (Option<u64>, Bytes) {
        let mut decoder = PayloadDecoder::new(payload);
        let status = decoder.get_u8().expect("stream response status");
        let maybe_id = decoder.get_optional_u64().expect("stream response id");
        let data = decoder.get_bytes().expect("stream response data");

        assert_eq!(status, 0);
        assert!(decoder.is_complete());

        (maybe_id, Bytes::copy_from_slice(data.as_ref()))
    }

    fn drain_mailbox(mailbox: &Mailbox) {
        while mailbox.receiver().try_recv().is_ok() {}
    }

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
    fn should_publish_stream_notify_to_subscribers_when_commit_succeeds() {
        // Arrange
        let family = RouteFamily::new(1);
        let requester_session_id = 7;
        let subscriber_session_id = 9;
        let stream_route = "stream://acme/app/events";
        let stream_address = RouteAddress::new(family, Route::new(stream_route));
        let requester_address = RouteAddress::new(family, Route::new("inbox://session/7"));
        let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/9"));
        let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
        let router = Arc::new(Router::new());
        let requester_mailbox = Arc::new(Mailbox::new(8));
        let subscriber_mailbox = Arc::new(Mailbox::new(8));
        router.register(requester_address.clone(), requester_mailbox.clone());
        router.register(subscriber_address.clone(), subscriber_mailbox.clone());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let sink = StreamDomainSink::new(store, router, admin_read_model);

        let subscribe_ctx = FrameContext::new(
            subscriber_session_id,
            ChannelId::Sub,
            MessageType::new(607),
            encode_stream_subscribe(stream_route),
            family,
        );
        let begin_ctx = FrameContext::new(
            requester_session_id,
            ChannelId::Sub,
            MessageType::new(600),
            encode_stream_begin(stream_route, 0),
            family,
        );

        sink.deliver(Envelope::from_route(
            subscriber_address.clone(),
            stream_address.clone(),
            subscribe_ctx,
        ))
        .expect("subscribe stream");
        let subscribe_envelope = subscriber_mailbox
            .receiver()
            .try_recv()
            .expect("subscribe ack envelope");
        let subscribe_frame = subscribe_envelope
            .into_payload::<FrameContext>()
            .expect("subscribe ack frame");
        let (subscription_id, subscribe_data) = decode_stream_ok_response(&subscribe_frame.payload);
        let subscription_id = subscription_id.expect("subscription id present");
        assert!(subscribe_data.is_empty());

        sink.deliver(Envelope::from_route(
            requester_address.clone(),
            stream_address.clone(),
            begin_ctx,
        ))
        .expect("begin stream session");
        let begin_envelope = requester_mailbox
            .receiver()
            .try_recv()
            .expect("begin ack envelope");
        let begin_frame = begin_envelope
            .into_payload::<FrameContext>()
            .expect("begin ack frame");
        let (stream_session_id, begin_data) = decode_stream_ok_response(&begin_frame.payload);
        let stream_session_id = stream_session_id.expect("stream session id present");
        assert!(begin_data.is_empty());

        let append_ctx = FrameContext::new(
            requester_session_id,
            ChannelId::Sub,
            MessageType::new(601),
            encode_stream_append(stream_session_id, b"alpha"),
            family,
        );
        let commit_ctx = FrameContext::new(
            requester_session_id,
            ChannelId::Sub,
            MessageType::new(602),
            encode_stream_commit(stream_session_id),
            family,
        );

        // Act
        sink.deliver(Envelope::from_route(
            requester_address.clone(),
            stream_address.clone(),
            append_ctx,
        ))
        .expect("append stream record");
        let append_envelope = requester_mailbox
            .receiver()
            .try_recv()
            .expect("append ack envelope");
        let append_frame = append_envelope
            .into_payload::<FrameContext>()
            .expect("append ack frame");
        let (append_session_id, append_data) = decode_stream_ok_response(&append_frame.payload);
        assert!(append_session_id.is_none());
        assert_eq!(append_data.len(), 8);

        drain_mailbox(&subscriber_mailbox);
        sink.deliver(Envelope::from_route(
            requester_address,
            stream_address,
            commit_ctx,
        ))
        .expect("commit stream session");

        // Assert
        let notify_envelope = subscriber_mailbox
            .receiver()
            .try_recv()
            .expect("stream notify envelope");
        let notify_frame = notify_envelope
            .into_payload::<FrameContext>()
            .expect("stream notify frame");
        assert_eq!(notify_frame.msg_type.as_u16(), 609);

        let mut notify_decoder = PayloadDecoder::new(&notify_frame.payload);
        let notified_subscription_id = notify_decoder.get_u64().expect("notify subscription id");
        let notified_route = notify_decoder.get_string().expect("notify route");
        let notified_payload = notify_decoder.get_bytes().expect("notify payload");
        assert_eq!(notified_subscription_id, subscription_id);
        assert_eq!(notified_route, stream_route);

        let decoded: serde_json::Value =
            serde_json::from_slice(notified_payload.as_ref()).expect("notify payload JSON");
        assert_eq!(decoded["event"], "committed");
        assert_eq!(decoded["first_resource_offset"], 0);
        assert_eq!(decoded["last_resource_offset"], 0);
        assert_eq!(decoded["first_area_offset"], 0);
        assert_eq!(decoded["last_area_offset"], 0);
        assert_eq!(decoded["first_realm_offset"], 0);
        assert_eq!(decoded["last_realm_offset"], 0);
        assert_eq!(decoded["batch_size"], 1);
        assert!(notify_decoder.is_complete());

        let commit_envelope = requester_mailbox
            .receiver()
            .try_recv()
            .expect("commit ack envelope");
        let commit_frame = commit_envelope
            .into_payload::<FrameContext>()
            .expect("commit ack frame");
        let (commit_session_id, commit_data) = decode_stream_ok_response(&commit_frame.payload);
        assert!(commit_session_id.is_none());
        assert!(commit_data.is_empty());
    }

    #[test]
    fn should_remove_stream_subscriptions_given_session_cleanup() {
        // Arrange
        let family = RouteFamily::new(1);
        let requester_session_id = 7;
        let subscriber_session_id = 9;
        let stream_route = "stream://acme/app/events";
        let stream_address = RouteAddress::new(family, Route::new(stream_route));
        let requester_address = RouteAddress::new(family, Route::new("inbox://session/7"));
        let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/9"));
        let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
        let router = Arc::new(Router::new());
        let requester_mailbox = Arc::new(Mailbox::new(8));
        let subscriber_mailbox = Arc::new(Mailbox::new(8));
        router.register(requester_address.clone(), requester_mailbox.clone());
        router.register(subscriber_address.clone(), subscriber_mailbox.clone());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let sink = StreamDomainSink::new(store, router, admin_read_model);

        sink.deliver(Envelope::from_route(
            subscriber_address.clone(),
            stream_address.clone(),
            FrameContext::new(
                subscriber_session_id,
                ChannelId::Sub,
                MessageType::new(607),
                encode_stream_subscribe(stream_route),
                family,
            ),
        ))
        .expect("subscribe stream");
        drain_mailbox(&subscriber_mailbox);
        sink.deliver(Envelope::new(
            RouteAddress::new(family, Route::new("stream://cleanup")),
            crate::runtime::SessionCleanup {
                session_id: subscriber_session_id,
            },
        ))
        .expect("cleanup session");
        sink.deliver(Envelope::from_route(
            requester_address.clone(),
            stream_address.clone(),
            FrameContext::new(
                requester_session_id,
                ChannelId::Sub,
                MessageType::new(600),
                encode_stream_begin(stream_route, 0),
                family,
            ),
        ))
        .expect("begin stream session");
        let begin_envelope = requester_mailbox
            .receiver()
            .try_recv()
            .expect("begin ack envelope");
        let begin_frame = begin_envelope
            .into_payload::<FrameContext>()
            .expect("begin ack frame");
        let (stream_session_id, begin_data) = decode_stream_ok_response(&begin_frame.payload);
        let stream_session_id = stream_session_id.expect("stream session id present");
        assert!(begin_data.is_empty());

        // Act
        sink.deliver(Envelope::from_route(
            requester_address.clone(),
            stream_address.clone(),
            FrameContext::new(
                requester_session_id,
                ChannelId::Sub,
                MessageType::new(601),
                encode_stream_append(stream_session_id, b"alpha"),
                family,
            ),
        ))
        .expect("append stream record");
        let _append_envelope = requester_mailbox
            .receiver()
            .try_recv()
            .expect("append ack envelope");
        sink.deliver(Envelope::from_route(
            requester_address,
            stream_address,
            FrameContext::new(
                requester_session_id,
                ChannelId::Sub,
                MessageType::new(602),
                encode_stream_commit(stream_session_id),
                family,
            ),
        ))
        .expect("commit stream session");

        // Assert
        assert_eq!(sink.subscription_count(), 0);
        assert!(subscriber_mailbox.receiver().try_recv().is_err());
    }

    #[test]
    fn should_retain_other_stream_subscription_given_unsubscribe_on_same_session() {
        // Arrange
        let family = RouteFamily::new(1);
        let subscriber_session_id = 9;
        let removed_route = "stream://acme/app/events";
        let retained_route = "stream://acme/app/audits";
        let removed_address = RouteAddress::new(family, Route::new(removed_route));
        let retained_address = RouteAddress::new(family, Route::new(retained_route));
        let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/9"));
        let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
        let router = Arc::new(Router::new());
        let subscriber_mailbox = Arc::new(Mailbox::new(16));
        router.register(subscriber_address.clone(), subscriber_mailbox.clone());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let sink = StreamDomainSink::new(store, router, admin_read_model);

        sink.deliver(Envelope::from_route(
            subscriber_address.clone(),
            removed_address.clone(),
            FrameContext::new(
                subscriber_session_id,
                ChannelId::Sub,
                MessageType::new(607),
                encode_stream_subscribe(removed_route),
                family,
            ),
        ))
        .expect("subscribe removed stream route");
        let _removed_subscribe_ack = subscriber_mailbox
            .receiver()
            .try_recv()
            .expect("removed subscribe ack envelope");

        sink.deliver(Envelope::from_route(
            subscriber_address.clone(),
            retained_address.clone(),
            FrameContext::new(
                subscriber_session_id,
                ChannelId::Sub,
                MessageType::new(607),
                encode_stream_subscribe(retained_route),
                family,
            ),
        ))
        .expect("subscribe retained stream route");
        let _retained_subscribe_ack = subscriber_mailbox
            .receiver()
            .try_recv()
            .expect("retained subscribe ack envelope");
        assert_eq!(sink.subscription_count(), 2);
        drain_mailbox(&subscriber_mailbox);

        // Act
        sink.deliver(Envelope::from_route(
            subscriber_address,
            removed_address,
            FrameContext::new(
                subscriber_session_id,
                ChannelId::Sub,
                MessageType::new(608),
                encode_stream_subscribe(removed_route),
                family,
            ),
        ))
        .expect("unsubscribe removed stream route");
        let unsubscribe_envelope = subscriber_mailbox
            .receiver()
            .try_recv()
            .expect("unsubscribe ack envelope");
        let unsubscribe_frame = unsubscribe_envelope
            .into_payload::<FrameContext>()
            .expect("unsubscribe ack frame");
        let (unsubscribe_session_id, unsubscribe_data) =
            decode_stream_ok_response(&unsubscribe_frame.payload);
        assert!(unsubscribe_session_id.is_none());
        assert!(unsubscribe_data.is_empty());
        assert_eq!(sink.subscription_count(), 1);

        sink.deliver(Envelope::new(
            RouteAddress::new(family, Route::new("stream://events/removed")),
            crate::runtime::DomainPublishEvent::new(
                family,
                Route::new(removed_route),
                Bytes::from_static(b"removed"),
            ),
        ))
        .expect("deliver removed stream event");
        assert!(subscriber_mailbox.receiver().try_recv().is_err());

        sink.deliver(Envelope::new(
            RouteAddress::new(family, Route::new("stream://events/retained")),
            crate::runtime::DomainPublishEvent::new(
                family,
                Route::new(retained_route),
                Bytes::from_static(b"retained"),
            ),
        ))
        .expect("deliver retained stream event");

        // Assert
        let notify_envelope = subscriber_mailbox
            .receiver()
            .try_recv()
            .expect("retained stream notify envelope");
        let notify_frame = notify_envelope
            .into_payload::<FrameContext>()
            .expect("retained stream notify frame");
        assert_eq!(notify_frame.msg_type.as_u16(), 609);

        let mut notify_decoder = PayloadDecoder::new(&notify_frame.payload);
        let _subscription_id = notify_decoder.get_u64().expect("notify subscription id");
        let notified_route = notify_decoder.get_string().expect("notify route");
        let notified_payload = notify_decoder.get_bytes().expect("notify payload");
        assert_eq!(notified_route, retained_route);
        assert_eq!(notified_payload.as_ref(), b"retained");
        assert!(notify_decoder.is_complete());
        assert!(subscriber_mailbox.receiver().try_recv().is_err());
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
