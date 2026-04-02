use super::subscription_state::{RoutedSubscription, RoutedSubscriptionSet};
use crate::domains::stream::store::StreamAdminRecord;
use crate::domains::stream::{StreamActor, StreamRecord, StreamStore};
use crate::protocol::frame_context::FrameContext;
use crate::protocol::payload_codec::PayloadEncoder;
use crate::runtime::routing::{route_triplet, Route, RouteAddress, RouteFamily};
use crate::runtime::{DeliveryError, Envelope, MailboxSink, Router};
use parking_lot::Mutex;
use std::collections::{BTreeMap, HashMap};
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
    active: AtomicBool,
}

impl StreamDomainSink {
    pub fn new(
        store: Arc<cntryl_midge::Engine>,
        router: Arc<Router>,
        admin_read_model: Arc<crate::api::admin::read_model::AdminReadModel>,
    ) -> Self {
        Self {
            stream_store: Arc::new(StreamStore::new(store.clone())),
            store,
            actors: Mutex::new(HashMap::new()),
            session_owners: Mutex::new(HashMap::new()),
            families: Mutex::new(HashMap::new()),
            next_sub_id: AtomicU64::new(1),
            next_session_id: AtomicU64::new(1),
            router,
            admin_read_model,
            admin_snapshot_dirty: AtomicBool::new(true),
            active: AtomicBool::new(true),
        }
    }

    pub fn stop(&self) {
        self.active.store(false, Ordering::Relaxed);
    }

    fn actor_key_for_route(
        family_id: RouteFamily,
        route: &Route,
    ) -> Result<StreamActorKey, String> {
        let parts = route_triplet(route.as_str()).ok_or_else(|| "invalid stream route".to_string())?;
        Ok(StreamActorKey {
            family_id: family_id.as_u64(),
            realm: parts.realm.to_string(),
            area: parts.area.to_string(),
            resource: parts.resource.to_string(),
        })
    }

    fn get_or_create_actor(&self, key: &StreamActorKey) -> Arc<Mutex<StreamActor>> {
        use std::collections::hash_map::Entry;

        let mut actors = self.actors.lock();
        match actors.entry(key.clone()) {
            Entry::Occupied(entry) => entry.get().clone(),
            Entry::Vacant(entry) => {
                let actor = Arc::new(Mutex::new(StreamActor::new(
                    RouteFamily::new(key.family_id),
                    key.realm.clone(),
                    key.area.clone(),
                    key.resource.clone(),
                    self.stream_store.clone(),
                )));
                entry.insert(actor.clone());
                actor
            }
        }
    }

    fn mark_admin_snapshot_dirty(&self) {
        self.admin_snapshot_dirty.store(true, Ordering::Relaxed);
    }

    pub fn refresh_admin_snapshot_if_dirty(&self) {
        if self.admin_snapshot_dirty.swap(false, Ordering::AcqRel) {
            self.sync_admin_snapshot();
        }
    }

    fn sync_admin_snapshot(&self) {
        let mut streams: BTreeMap<(u64, String, String, String), crate::api::admin::StreamInfo> =
            BTreeMap::new();

        if let Ok(families) = self.store.list_column_families() {
            for family in families {
                if let Ok(records) = self.stream_store.list_resource_metadata(family.id() as u64) {
                    for StreamAdminRecord {
                        realm,
                        area,
                        resource,
                        next_offset,
                        committed_size_bytes,
                    } in records
                    {
                        let last_offset = next_offset.saturating_sub(1);
                        streams.insert(
                            (
                                family.id() as u64,
                                realm.clone(),
                                area.clone(),
                                resource.clone(),
                            ),
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
                    }
                }
            }
        }

        let actors = self.actors.lock();
        for (key, actor) in actors.iter() {
            let actor = actor.lock();
            let last_offset = actor
                .metadata()
                .ok()
                .and_then(|response| response.metadata.last_resource_offset)
                .unwrap_or(0);
            let sessions_active = usize::from(actor.has_active_session());
            let committed_size_bytes = streams
                .get(&(
                    key.family_id,
                    key.realm.clone(),
                    key.area.clone(),
                    key.resource.clone(),
                ))
                .map(|item| item.size_bytes)
                .unwrap_or(0);

            streams.insert(
                (
                    key.family_id,
                    key.realm.clone(),
                    key.area.clone(),
                    key.resource.clone(),
                ),
                crate::api::admin::StreamInfo::snapshot(
                    &key.realm,
                    &key.area,
                    &key.resource,
                    last_offset,
                    last_offset,
                    committed_size_bytes,
                    sessions_active,
                ),
            );
        }

        self.admin_read_model
            .replace_streams(streams.into_values().collect());
    }

    fn encode_stream_read_data(
        records: &[StreamRecord],
        from_offset: u64,
        limit: u64,
        max_bytes: Option<usize>,
    ) -> Vec<u8> {
        let mut selected = Vec::new();
        let mut total_bytes = 0usize;

        for record in records
            .iter()
            .filter(|record| record.resource_offset >= from_offset)
        {
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

        let mut encoder = PayloadEncoder::new();
        encoder.put_u32(selected.len() as u32);
        for record in selected {
            encoder.put_u64(record.resource_offset);
            encoder.put_bytes(record.body.as_ref());
        }
        encoder.finish()
    }

    fn encode_stream_last_data(record: &StreamRecord) -> Vec<u8> {
        let mut encoder = PayloadEncoder::new();
        encoder.put_u64(record.resource_offset);
        encoder.put_bytes(record.body.as_ref());
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

    fn encode_stream_metadata_summary(first_offset: u64, last_offset: u64, count: u64) -> Vec<u8> {
        let mut encoder = PayloadEncoder::new();
        encoder.put_u64(first_offset);
        encoder.put_u64(last_offset);
        encoder.put_u64(count);
        encoder.finish()
    }

    fn stream_error_response(error: impl Into<String>) -> crate::protocol::stream_codec::StreamResponse {
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
            let mut encoder = PayloadEncoder::new();
            encoder.put_u32(0);
            return Ok(encoder.finish());
        }

        let parts = route_triplet(route.as_str()).ok_or_else(|| "invalid stream route".to_string())?;
        let records = if parts.area == "*" && parts.resource == "*" {
            self.stream_store
                .read_realm(family_id.as_u64(), parts.realm, from_offset, limit, max_bytes)?
                .0
        } else if parts.resource == "*" {
            self.stream_store
                .read_area(
                    family_id.as_u64(),
                    parts.realm,
                    parts.area,
                    from_offset,
                    limit,
                    max_bytes,
                )?
                .0
        } else {
            let key = Self::actor_key_for_route(family_id, route)?;
            let actor = self.get_or_create_actor(&key);
            let records = actor.lock().read(from_offset, limit, max_bytes)?.records;
            records
        };

        Ok(Self::encode_stream_read_data(
            &records,
            from_offset,
            limit,
            max_bytes,
        ))
    }

    fn encode_last_response_data(
        &self,
        family_id: RouteFamily,
        route: &Route,
    ) -> Result<Vec<u8>, String> {
        let parts = route_triplet(route.as_str()).ok_or_else(|| "invalid stream route".to_string())?;
        if parts.area == "*" || parts.resource == "*" {
            return Ok(Vec::new());
        }

        let key = Self::actor_key_for_route(family_id, route)?;
        let actor = self.get_or_create_actor(&key);
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
        let parts = route_triplet(route.as_str()).ok_or_else(|| "invalid stream route".to_string())?;
        if parts.area == "*" || parts.resource == "*" {
            return Ok(Vec::new());
        }

        let key = Self::actor_key_for_route(family_id, route)?;
        let actor = self.get_or_create_actor(&key);
        let metadata = actor.lock().metadata()?.metadata;
        let Some(last_offset) = metadata.last_resource_offset else {
            return Ok(Vec::new());
        };

        Ok(Self::encode_stream_metadata_summary(
            0,
            last_offset,
            last_offset.saturating_add(1),
        ))
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
        let _ = self.router.route(notify_envelope);
    }

    pub fn unsubscribe_all(&self, session_id: u64) {
        let mut families = self.families.lock();
        for (family_id, state) in families.iter_mut() {
            state.remove_session(RouteFamily::new(*family_id), session_id);
        }
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
        let mut payload_encoder = PayloadEncoder::with_capacity(256);

        let stream_msg = match crate::protocol::stream_codec::parse_request(
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
            Err(_) => return Err(DeliveryError::ActorStopped),
        };

        use crate::domains::stream::protocol::StreamMessage;
        use crate::protocol::stream_codec::StreamResponse;

        let (response, commit_notify, should_refresh_admin_snapshot) = match stream_msg {
            StreamMessage::Begin {
                family_id,
                route,
                expected_offset,
                ingest_metadata,
            } => match Self::actor_key_for_route(family_id, &route) {
                Ok(key) => {
                    let actor = self.get_or_create_actor(&key);
                    let stream_session_id = self.next_session_id.fetch_add(1, Ordering::Relaxed);
                    let outcome = actor.lock().begin_append_session(
                        frame_ctx.session_id,
                        stream_session_id,
                        expected_offset,
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
                        Err(error) => (Self::stream_error_response(error), None, false),
                    }
                }
                Err(error) => (Self::stream_error_response(error), None, false),
            },
            StreamMessage::Append {
                session_id,
                body,
                metadata,
            } => {
                let key = self.session_owners.lock().get(&session_id).cloned();
                match key {
                    Some(key) => {
                        let actor = self.get_or_create_actor(&key);
                        let outcome = actor.lock().append_to_session(session_id, body, metadata);
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
                    Some(key) => {
                        let actor = self.get_or_create_actor(&key);
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
                    Some(key) => {
                        let actor = self.get_or_create_actor(&key);
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
            StreamMessage::Subscribe {
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
            StreamMessage::Unsubscribe {
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
            StreamMessage::RequestLease { .. }
            | StreamMessage::LeaseGranted { .. }
            | StreamMessage::RequestRealmLease { .. }
            | StreamMessage::BatchCommitted(_)
            | StreamMessage::AreaWatermarkAdvanced(_) => (
                StreamResponse::Ok {
                    session_id: None,
                    data: vec![],
                },
                None,
                false,
            ),
        };

        if should_refresh_admin_snapshot {
            self.mark_admin_snapshot_dirty();
        }

        if let Some((route, payload)) = commit_notify {
            let event = crate::runtime::DomainPublishEvent::new(frame_ctx.route_family, route, payload);
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

        Ok(())
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}
