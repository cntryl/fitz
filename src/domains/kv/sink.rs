//! KV domain sink for session-scoped transaction dispatch.
//!
//! Committed KV writes flow straight to Midge and persist according to the
//! `WriteOptions` selected when the transaction commits. Active `tx_id`
//! handles, resource locks, and admin snapshot entries are separate live
//! in-memory state owned by the current broker process. `cleanup_session`
//! intentionally discards that state on disconnect, and broker restart clears
//! it wholesale instead of attempting transaction recovery.

use crate::domains::subscription_state::{RoutedSubscription, RoutedSubscriptionSet};
use crate::protocol::frame_context::FrameContext;
use crate::runtime::{DeliveryError, Envelope, MailboxSink, Router};
use chrono::Utc;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct KvResourceLockKey {
    family_id: u64,
    realm: String,
    area: String,
    resource: String,
}

impl KvResourceLockKey {
    fn new(family_id: u64, realm: &str, area: &str, resource: &str) -> Self {
        Self {
            family_id,
            realm: realm.to_string(),
            area: area.to_string(),
            resource: resource.to_string(),
        }
    }
}

struct KvSubscription {
    pattern: crate::runtime::matcher::Pattern,
    session_id: u64,
    subscription_id: u64,
    subscriber: crate::runtime::routing::RouteAddress,
}

impl RoutedSubscription for KvSubscription {
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

pub struct KvDomainSink {
    store: Arc<cntryl_midge::Engine>,
    actors: Arc<Mutex<HashMap<u64, crate::domains::kv::KvActor>>>,
    resource_locks: Mutex<HashMap<KvResourceLockKey, u64>>,
    tx_to_resource: Mutex<HashMap<(u64, u64), KvResourceLockKey>>,
    families: Mutex<HashMap<u64, RoutedSubscriptionSet<KvSubscription>>>,
    next_sub_id: AtomicU64,
    router: Arc<Router>,
    admin_read_model: Arc<crate::api::admin::read_model::AdminReadModel>,
    metrics: Option<crate::domains::kv::KvMetrics>,
    active: AtomicBool,
}

impl KvDomainSink {
    pub fn new(
        store: Arc<cntryl_midge::Engine>,
        router: Arc<Router>,
        admin_read_model: Arc<crate::api::admin::read_model::AdminReadModel>,
    ) -> Self {
        Self {
            store,
            actors: Arc::new(Mutex::new(HashMap::new())),
            resource_locks: Mutex::new(HashMap::new()),
            tx_to_resource: Mutex::new(HashMap::new()),
            families: Mutex::new(HashMap::new()),
            next_sub_id: AtomicU64::new(1),
            router,
            admin_read_model,
            metrics: None,
            active: AtomicBool::new(true),
        }
    }

    pub fn with_metrics(
        mut self,
        collector: crate::observability::metrics::MetricsCollector,
    ) -> Self {
        self.metrics = Some(crate::domains::kv::KvMetrics::new(collector));
        self.refresh_metrics_gauges();
        self
    }

    pub fn stop(&self) {
        self.active.store(false, Ordering::Relaxed);
    }

    fn sync_admin_snapshot(&self) {
        let started_at = Utc::now().to_rfc3339();
        let transactions = self
            .tx_to_resource
            .lock()
            .iter()
            .map(|((session_id, tx_id), resource_key)| {
                crate::api::admin::KvTransaction::snapshot(
                    *tx_id,
                    *session_id,
                    &resource_key.realm,
                    &resource_key.area,
                    &resource_key.resource,
                    &started_at,
                )
            })
            .collect();
        self.admin_read_model.replace_kv_transactions(transactions);
        self.refresh_metrics_gauges();
    }

    fn refresh_metrics_gauges(&self) {
        if let Some(metrics) = &self.metrics {
            metrics.set_active_transactions(self.active_transaction_count());
            metrics.set_subscription_count(self.subscription_count());
        }
    }

    fn subscription_count(&self) -> usize {
        self.families
            .lock()
            .values()
            .map(RoutedSubscriptionSet::subscription_count)
            .sum()
    }

    fn session_inbox_address(
        family_id: crate::runtime::routing::RouteFamily,
        session_id: u64,
    ) -> crate::runtime::routing::RouteAddress {
        crate::runtime::routing::RouteAddress::new(
            family_id,
            crate::runtime::routing::Route::new(format!("inbox://session/{session_id}")),
        )
    }

    fn kv_route_for_lock(resource_key: &KvResourceLockKey) -> crate::runtime::routing::Route {
        crate::runtime::routing::Route::new(format!(
            "kv://{}/{}/{}",
            resource_key.realm, resource_key.area, resource_key.resource
        ))
    }

    fn route_kv_notify_to_subscription(
        &self,
        session_id: u64,
        subscription_id: u64,
        subscriber: &crate::runtime::routing::RouteAddress,
        route: &crate::runtime::routing::Route,
        mutation_count: u64,
    ) {
        let payload = crate::protocol::kv::encode_notify(
            subscription_id,
            route,
            crate::domains::kv::KvNotification { mutation_count },
        );
        let notify_ctx = FrameContext::new(
            session_id,
            crate::protocol::frame::ChannelId::Sub,
            crate::protocol::tlv::MessageType::new(crate::protocol::kv::msg_type::NOTIFY),
            bytes::Bytes::from(payload),
            *subscriber.family(),
        );
        let notify_envelope = Envelope::new(subscriber.clone(), notify_ctx);
        if self.router.route(notify_envelope).is_err() {
            crate::observability::counter_inc("fitz_kv_notify_drops_total");
        }
    }

    fn route_kv_notification(&self, resource_key: &KvResourceLockKey, mutation_count: u64) {
        let family_id = crate::runtime::routing::RouteFamily::new(resource_key.family_id);
        let route = Self::kv_route_for_lock(resource_key);
        let families = self.families.lock();
        if let Some(state) = families.get(&resource_key.family_id) {
            state.for_each_matching_route(family_id, route.as_str(), |subscription| {
                self.route_kv_notify_to_subscription(
                    subscription.session_id,
                    subscription.subscription_id,
                    &subscription.subscriber,
                    &route,
                    mutation_count,
                );
            });
        }
    }

    fn route_kv_response(
        &self,
        envelope: &Envelope,
        frame_ctx: &FrameContext,
        response: &crate::domains::kv::KvResponse,
        request_started: Option<std::time::Instant>,
    ) -> Result<(), DeliveryError> {
        let response_bytes = crate::protocol::kv::encode_response(response);
        tracing::trace!(
            domain = "kv",
            session = frame_ctx.session_id,
            response_len = response_bytes.len(),
            "KV response encoded"
        );

        let response_ctx = FrameContext::new(
            frame_ctx.session_id,
            frame_ctx.channel_id,
            crate::protocol::tlv::MessageType::new(frame_ctx.msg_type.as_u16()),
            bytes::Bytes::from(response_bytes),
            frame_ctx.route_family,
        );
        let response_envelope = match envelope.try_reply_to(response_ctx) {
            Some(env) => env,
            None => {
                if let (Some(metrics), Some(started_at)) = (self.metrics.as_ref(), request_started)
                {
                    if matches!(response, crate::domains::kv::KvResponse::Error { .. }) {
                        metrics.record_failure(started_at);
                    } else {
                        metrics.record_success(started_at);
                    }
                }
                tracing::warn!(
                    domain = "kv",
                    session = frame_ctx.session_id,
                    "Cannot route response: envelope has no source address"
                );
                return Ok(());
            }
        };

        match self.router.route(response_envelope) {
            Ok(_) => {
                if let (Some(metrics), Some(started_at)) = (self.metrics.as_ref(), request_started)
                {
                    if matches!(response, crate::domains::kv::KvResponse::Error { .. }) {
                        metrics.record_failure(started_at);
                    } else {
                        metrics.record_success(started_at);
                    }
                }
                tracing::debug!(
                    domain = "kv",
                    session = frame_ctx.session_id,
                    "KV message handled and response routed"
                );
                Ok(())
            }
            Err(error) => {
                if let (Some(metrics), Some(started_at)) = (self.metrics.as_ref(), request_started)
                {
                    metrics.record_failure(started_at);
                }
                tracing::warn!(
                    domain = "kv",
                    session = frame_ctx.session_id,
                    error = ?error,
                    "Failed to route response"
                );
                Err(DeliveryError::ActorStopped)
            }
        }
    }

    /// Remove all live KV transaction state owned by a disconnected session.
    ///
    /// This is the authoritative boundary for session-scoped cleanup: open
    /// transactions are dropped, resource locks are released, and the admin read
    /// model is refreshed so no durable recovery is implied.
    pub fn cleanup_session(&self, session_id: u64) {
        self.actors.lock().remove(&session_id);

        {
            let mut locks = self.resource_locks.lock();
            locks.retain(|_key, holder_id| *holder_id != session_id);
        }

        {
            let mut tx_map = self.tx_to_resource.lock();
            tx_map.retain(|(sid, _tx_id), _key| *sid != session_id);
        }

        {
            let mut families = self.families.lock();
            for (family_id, state) in families.iter_mut() {
                state.remove_session(
                    crate::runtime::routing::RouteFamily::new(*family_id),
                    session_id,
                );
            }
            families.retain(|_, state| !state.is_empty());
        }

        tracing::debug!(
            domain = "kv",
            session = session_id,
            "All KV transactions and resource locks released for session (disconnect cleanup)"
        );
        self.sync_admin_snapshot();
    }

    pub fn active_transaction_count(&self) -> usize {
        self.tx_to_resource.lock().len()
    }
}

impl MailboxSink for KvDomainSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        if let Some(cleanup) = envelope.payload::<crate::runtime::SessionCleanup>() {
            self.cleanup_session(cleanup.session_id);
            return Ok(());
        }
        if !self.active.load(Ordering::Relaxed) {
            return Err(DeliveryError::ActorStopped);
        }

        tracing::debug!(
            domain = "kv",
            destination = %envelope.destination(),
            source = ?envelope.source(),
            "KV domain sink: received envelope"
        );

        let frame_ctx = match envelope.payload::<FrameContext>() {
            Some(ctx) => ctx.clone(),
            None => match envelope.payload::<bytes::Bytes>() {
                Some(_bytes) => {
                    tracing::warn!(
                        domain = "kv",
                        destination = ?envelope.destination(),
                        "Envelope payload was Bytes, expected FrameContext - raw TLV not supported yet"
                    );
                    return Err(DeliveryError::ActorStopped);
                }
                None => {
                    tracing::warn!(
                        domain = "kv",
                        destination = ?envelope.destination(),
                        "Envelope payload was neither FrameContext nor Bytes"
                    );
                    return Err(DeliveryError::ActorStopped);
                }
            },
        };

        let request_started = self
            .metrics
            .as_ref()
            .map(|metrics| metrics.record_request_start());
        let subscriber = envelope.source().cloned().unwrap_or_else(|| {
            Self::session_inbox_address(frame_ctx.route_family, frame_ctx.session_id)
        });

        let parsed_frame = match crate::protocol::kv::parse_frame(
            &frame_ctx,
            &frame_ctx.payload,
            frame_ctx.route_family,
            frame_ctx.session_id,
            subscriber.clone(),
        ) {
            Ok(msg) => msg,
            Err(e) => {
                if let (Some(metrics), Some(started_at)) = (self.metrics.as_ref(), request_started)
                {
                    metrics.record_failure(started_at);
                }
                tracing::warn!(
                    domain = "kv",
                    session = frame_ctx.session_id,
                    msg_type = frame_ctx.msg_type.as_u16(),
                    error = %e,
                    "Failed to parse KV message"
                );
                return Err(DeliveryError::ActorStopped);
            }
        };

        tracing::debug!(
            domain = "kv",
            session = frame_ctx.session_id,
            channel = ?frame_ctx.channel_id,
            msg_type = frame_ctx.msg_type.as_u16(),
            "Parsed KV message successfully"
        );

        if let crate::protocol::kv::ParsedKvFrame::Sub(sub_msg) = parsed_frame {
            let response = match sub_msg {
                crate::domains::kv::KvSubscriptionMessage::Subscribe {
                    family_id,
                    pattern,
                    session_id,
                    subscriber,
                } => {
                    let pattern_str = pattern.as_str().to_string();
                    let subscription_id = {
                        let mut families = self.families.lock();
                        let state = families
                            .entry(family_id.as_u64())
                            .or_insert_with(RoutedSubscriptionSet::new);

                        if let Some(existing_id) =
                            state.find_existing_id(session_id, pattern_str.as_str())
                        {
                            existing_id
                        } else {
                            let new_id = self.next_sub_id.fetch_add(1, Ordering::Relaxed);
                            state.insert(
                                family_id,
                                KvSubscription {
                                    pattern: crate::runtime::matcher::Pattern::new(
                                        pattern_str.as_str(),
                                    ),
                                    session_id,
                                    subscription_id: new_id,
                                    subscriber,
                                },
                            );
                            new_id
                        }
                    };
                    crate::domains::kv::KvResponse::SubscribeOk { subscription_id }
                }
                crate::domains::kv::KvSubscriptionMessage::Unsubscribe {
                    family_id,
                    pattern,
                    session_id,
                    ..
                } => {
                    let mut families = self.families.lock();
                    let remove_family = if let Some(state) = families.get_mut(&family_id.as_u64()) {
                        state.remove_session_pattern(family_id, session_id, pattern.as_str());
                        state.is_empty()
                    } else {
                        false
                    };
                    if remove_family {
                        families.remove(&family_id.as_u64());
                    }
                    crate::domains::kv::KvResponse::UnsubscribeOk
                }
            };

            self.refresh_metrics_gauges();
            return self.route_kv_response(&envelope, &frame_ctx, &response, request_started);
        }

        use crate::domains::kv::{KvError, KvMessage, KvResponse, TxMode};
        let kv_message = match parsed_frame {
            crate::protocol::kv::ParsedKvFrame::Op(msg) => msg,
            crate::protocol::kv::ParsedKvFrame::Sub(_) => unreachable!(),
        };
        let session_id = frame_ctx.session_id;

        tracing::trace!(
            domain = "kv",
            session_id = session_id,
            msg_type = frame_ctx.msg_type.as_u16(),
            "KV deliver: getting or creating actor for session"
        );

        let (response, should_sync_admin_snapshot, commit_notification) = match &kv_message {
            KvMessage::Begin {
                route_family,
                realm,
                area,
                resource,
                mode,
                ..
            } if *mode == TxMode::ReadWrite => {
                let lock_key = KvResourceLockKey::new(route_family.as_u64(), realm, area, resource);
                {
                    let locks = self.resource_locks.lock();
                    if let Some(&holder) = locks.get(&lock_key) {
                        if holder != session_id {
                            drop(locks);
                            (
                                KvResponse::Error {
                                    error: KvError::Conflict(
                                        "resource locked by another session".to_string(),
                                    ),
                                },
                                false,
                                None,
                            )
                        } else {
                            drop(locks);
                            let mut actors = self.actors.lock();
                            let actor = actors.entry(session_id).or_insert_with(|| {
                                tracing::trace!(
                                    domain = "kv",
                                    session_id = session_id,
                                    "Creating new KvActor instance"
                                );
                                crate::domains::kv::KvActor::new(self.store.clone())
                            });
                            tracing::trace!(
                                domain = "kv",
                                session_id = session_id,
                                "Calling actor.handle() for BEGIN (ReadWrite)"
                            );
                            let resp = actor.handle(kv_message.clone());
                            if let KvResponse::BeginOk { tx_id } = resp {
                                tracing::trace!(
                                    domain = "kv",
                                    session_id = session_id,
                                    tx_id = tx_id,
                                    "BEGIN succeeded, storing resource lock"
                                );
                                self.resource_locks
                                    .lock()
                                    .insert(lock_key.clone(), session_id);
                                self.tx_to_resource
                                    .lock()
                                    .insert((session_id, tx_id), lock_key);
                                (resp, true, None)
                            } else {
                                (resp, false, None)
                            }
                        }
                    } else {
                        drop(locks);
                        let mut actors = self.actors.lock();
                        let actor = actors.entry(session_id).or_insert_with(|| {
                            tracing::trace!(
                                domain = "kv",
                                session_id = session_id,
                                "Creating new KvActor instance"
                            );
                            crate::domains::kv::KvActor::new(self.store.clone())
                        });
                        tracing::trace!(
                            domain = "kv",
                            session_id = session_id,
                            "Calling actor.handle() for BEGIN (ReadWrite, acquiring lock)"
                        );
                        let resp = actor.handle(kv_message.clone());
                        if let KvResponse::BeginOk { tx_id } = resp {
                            tracing::trace!(
                                domain = "kv",
                                session_id = session_id,
                                tx_id = tx_id,
                                "BEGIN succeeded, acquiring resource lock"
                            );
                            self.resource_locks
                                .lock()
                                .insert(lock_key.clone(), session_id);
                            self.tx_to_resource
                                .lock()
                                .insert((session_id, tx_id), lock_key);
                            (resp, true, None)
                        } else {
                            (resp, false, None)
                        }
                    }
                }
            }
            KvMessage::Commit { tx_id } => {
                let mut actors = self.actors.lock();
                let actor = actors.entry(session_id).or_insert_with(|| {
                    tracing::trace!(
                        domain = "kv",
                        session_id = session_id,
                        "Creating new KvActor instance (COMMIT)"
                    );
                    crate::domains::kv::KvActor::new(self.store.clone())
                });
                tracing::trace!(
                    domain = "kv",
                    session_id = session_id,
                    tx_id = tx_id,
                    "Calling actor.handle() for COMMIT"
                );
                let mutation_count = actor.mutation_count_for_tx(*tx_id).unwrap_or(0);
                let resp = actor.handle(kv_message.clone());
                if let KvResponse::CommitOk = resp {
                    let lock_key = self.tx_to_resource.lock().remove(&(session_id, *tx_id));
                    if let Some(k) = lock_key {
                        self.resource_locks.lock().remove(&k);
                        let notify = (mutation_count > 0).then_some((k, mutation_count));
                        (resp, true, notify)
                    } else {
                        (resp, true, None)
                    }
                } else {
                    crate::observability::counter_inc("fitz_kv_commits_failed_total");
                    (resp, false, None)
                }
            }
            KvMessage::Rollback { tx_id } => {
                let mut actors = self.actors.lock();
                let actor = actors.entry(session_id).or_insert_with(|| {
                    tracing::trace!(
                        domain = "kv",
                        session_id = session_id,
                        "Creating new KvActor instance (ROLLBACK)"
                    );
                    crate::domains::kv::KvActor::new(self.store.clone())
                });
                tracing::trace!(
                    domain = "kv",
                    session_id = session_id,
                    tx_id = tx_id,
                    "Calling actor.handle() for ROLLBACK"
                );
                let resp = actor.handle(kv_message.clone());
                if let KvResponse::RollbackOk = resp {
                    let lock_key = self.tx_to_resource.lock().remove(&(session_id, *tx_id));
                    if let Some(k) = lock_key {
                        self.resource_locks.lock().remove(&k);
                    }
                    crate::observability::counter_inc("fitz_kv_rollbacks_total");
                    (resp, true, None)
                } else {
                    (resp, false, None)
                }
            }
            _ => {
                let mut actors = self.actors.lock();
                let actor = actors.entry(session_id).or_insert_with(|| {
                    tracing::trace!(
                        domain = "kv",
                        session_id = session_id,
                        "Creating new KvActor instance (other operation)"
                    );
                    crate::domains::kv::KvActor::new(self.store.clone())
                });
                tracing::trace!(
                    domain = "kv",
                    session_id = session_id,
                    msg_type = frame_ctx.msg_type.as_u16(),
                    "Calling actor.handle() for operation"
                );
                (actor.handle(kv_message.clone()), false, None)
            }
        };
        if matches!(
            &response,
            crate::domains::kv::KvResponse::Error {
                error: crate::domains::kv::KvError::InvalidTxId,
                ..
            }
        ) {
            crate::observability::counter_inc("fitz_kv_invalid_transaction_rejects_total");
        }
        if should_sync_admin_snapshot {
            self.sync_admin_snapshot();
        }
        if let Some((resource_key, mutation_count)) = commit_notification {
            self.route_kv_notification(&resource_key, mutation_count);
        }

        tracing::debug!(
            domain = "kv",
            session = frame_ctx.session_id,
            response = ?std::mem::discriminant(&response),
            "KV actor returned response"
        );

        self.route_kv_response(&envelope, &frame_ctx, &response, request_started)
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::frame::ChannelId;
    use crate::protocol::tlv::MessageType;
    use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
    use crate::runtime::Mailbox;
    use bytes::{BufMut, Bytes};
    use std::sync::Arc;

    fn encode_kv_begin(route: &str, mode: u8, durability: u8) -> Bytes {
        let mut payload = Vec::new();
        payload.put_u32(route.len() as u32);
        payload.put_slice(route.as_bytes());
        payload.put_u8(mode);
        payload.put_u8(durability);
        Bytes::from(payload)
    }

    fn encode_kv_put(tx_id: u64, route: &str, key: &[u8], value: &[u8]) -> Bytes {
        let mut payload = Vec::new();
        payload.put_u64(tx_id);
        payload.put_u32(route.len() as u32);
        payload.put_slice(route.as_bytes());
        payload.put_u32(key.len() as u32);
        payload.put_slice(key);
        payload.put_u32(value.len() as u32);
        payload.put_slice(value);
        Bytes::from(payload)
    }

    fn encode_kv_commit(tx_id: u64, route: &str) -> Bytes {
        let mut payload = Vec::new();
        payload.put_u64(tx_id);
        payload.put_u32(route.len() as u32);
        payload.put_slice(route.as_bytes());
        Bytes::from(payload)
    }

    fn encode_kv_subscribe(pattern: &str) -> Bytes {
        let mut payload = Vec::new();
        payload.put_u32(pattern.len() as u32);
        payload.put_slice(pattern.as_bytes());
        Bytes::from(payload)
    }

    fn encode_kv_unsubscribe(pattern: &str) -> Bytes {
        let mut payload = Vec::new();
        payload.put_u32(pattern.len() as u32);
        payload.put_slice(pattern.as_bytes());
        Bytes::from(payload)
    }

    fn decode_kv_begin_tx_id(payload: &[u8]) -> u64 {
        let tx_id_bytes: [u8; 8] = payload[1..9]
            .try_into()
            .expect("begin response tx_id bytes");
        u64::from_be_bytes(tx_id_bytes)
    }

    fn decode_kv_subscription_id(payload: &[u8]) -> u64 {
        let subscription_id_bytes: [u8; 8] = payload[1..9]
            .try_into()
            .expect("subscribe response subscription_id bytes");
        u64::from_be_bytes(subscription_id_bytes)
    }

    fn decode_kv_watch_delivery(frame: &FrameContext) -> (u64, String, u64) {
        let subscription_id = u64::from_be_bytes(frame.payload[0..8].try_into().unwrap());
        let route_len = u32::from_be_bytes(frame.payload[8..12].try_into().unwrap()) as usize;
        let route = String::from_utf8(frame.payload[12..12 + route_len].to_vec())
            .expect("KV watch route should be utf-8");
        let mutation_offset = 12 + route_len;
        let mutation_count = u64::from_be_bytes(
            frame.payload[mutation_offset..mutation_offset + 8]
                .try_into()
                .unwrap(),
        );
        (subscription_id, route, mutation_count)
    }

    fn drain_mailbox(mailbox: &Mailbox) {
        while mailbox.receiver().try_recv().is_ok() {}
    }

    #[test]
    fn should_create_kv_domain_sink() {
        // Arrange
        let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
        let router = Arc::new(Router::new());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();

        // Act
        let sink = KvDomainSink::new(store, router, admin_read_model);

        // Assert
        assert!(sink.active.load(Ordering::Relaxed));
    }

    #[test]
    fn should_release_resource_lock_given_session_cleanup() {
        // Arrange
        let family = RouteFamily::new(1);
        let first_session_id = 7;
        let second_session_id = 8;
        let kv_route = "kv://acme/app/users";
        let kv_address = RouteAddress::new(family, Route::new(kv_route));
        let first_address = RouteAddress::new(family, Route::new("inbox://session/7"));
        let second_address = RouteAddress::new(family, Route::new("inbox://session/8"));
        let first_mailbox = Arc::new(Mailbox::new(8));
        let second_mailbox = Arc::new(Mailbox::new(8));
        let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
        let router = Arc::new(Router::new());
        router.register(first_address.clone(), first_mailbox.clone());
        router.register(second_address.clone(), second_mailbox.clone());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let sink = KvDomainSink::new(store, router, admin_read_model);

        sink.deliver(Envelope::from_route(
            first_address,
            kv_address.clone(),
            FrameContext::new(
                first_session_id,
                ChannelId::Sub,
                MessageType::new(100),
                encode_kv_begin(kv_route, 1, 0),
                family,
            ),
        ))
        .expect("begin first KV transaction");
        let first_begin_envelope = first_mailbox
            .receiver()
            .try_recv()
            .expect("first begin ack envelope");
        let first_begin_frame = first_begin_envelope
            .into_payload::<FrameContext>()
            .expect("first begin ack frame");
        let first_tx_id = decode_kv_begin_tx_id(&first_begin_frame.payload);
        assert_eq!(first_begin_frame.payload[0], 0);
        assert!(first_tx_id > 0);
        assert_eq!(sink.active_transaction_count(), 1);
        assert_eq!(sink.resource_locks.lock().len(), 1);
        drain_mailbox(&first_mailbox);

        // Act
        sink.deliver(Envelope::new(
            RouteAddress::new(family, Route::new("kv://cleanup")),
            crate::runtime::SessionCleanup {
                session_id: first_session_id,
            },
        ))
        .expect("cleanup first KV session");
        assert_eq!(sink.active_transaction_count(), 0);
        assert!(sink.resource_locks.lock().is_empty());

        sink.deliver(Envelope::from_route(
            second_address,
            kv_address,
            FrameContext::new(
                second_session_id,
                ChannelId::Sub,
                MessageType::new(100),
                encode_kv_begin(kv_route, 1, 0),
                family,
            ),
        ))
        .expect("begin second KV transaction");

        // Assert
        let second_begin_envelope = second_mailbox
            .receiver()
            .try_recv()
            .expect("second begin ack envelope");
        let second_begin_frame = second_begin_envelope
            .into_payload::<FrameContext>()
            .expect("second begin ack frame");
        let second_tx_id = decode_kv_begin_tx_id(&second_begin_frame.payload);
        assert_eq!(second_begin_frame.payload[0], 0);
        assert!(second_tx_id > 0);
        assert_eq!(sink.active_transaction_count(), 1);
        assert_eq!(sink.resource_locks.lock().len(), 1);
        assert!(first_mailbox.receiver().try_recv().is_err());
    }

    #[test]
    fn should_notify_kv_subscriber_given_committed_put() {
        // Arrange
        let family = RouteFamily::new(1);
        let watch_session_id = 7;
        let writer_session_id = 8;
        let kv_route = "kv://acme/app/users";
        let kv_address = RouteAddress::new(family, Route::new(kv_route));
        let watcher_address = RouteAddress::new(family, Route::new("inbox://session/7"));
        let writer_address = RouteAddress::new(family, Route::new("inbox://session/8"));
        let watcher_mailbox = Arc::new(Mailbox::new(16));
        let writer_mailbox = Arc::new(Mailbox::new(16));
        let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
        let router = Arc::new(Router::new());
        router.register(watcher_address.clone(), watcher_mailbox.clone());
        router.register(writer_address.clone(), writer_mailbox.clone());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let sink = KvDomainSink::new(store, router, admin_read_model);

        // Act
        sink.deliver(Envelope::from_route(
            watcher_address,
            kv_address.clone(),
            FrameContext::new(
                watch_session_id,
                ChannelId::Pub,
                MessageType::new(crate::protocol::kv::msg_type::SUBSCRIBE),
                encode_kv_subscribe(kv_route),
                family,
            ),
        ))
        .expect("subscribe to KV route");
        let subscribe_frame = watcher_mailbox
            .receiver()
            .try_recv()
            .expect("subscribe ack envelope")
            .into_payload::<FrameContext>()
            .expect("subscribe ack frame");
        let subscription_id = decode_kv_subscription_id(&subscribe_frame.payload);

        sink.deliver(Envelope::from_route(
            writer_address.clone(),
            kv_address.clone(),
            FrameContext::new(
                writer_session_id,
                ChannelId::Pub,
                MessageType::new(crate::protocol::kv::msg_type::BEGIN),
                encode_kv_begin(kv_route, 1, 0),
                family,
            ),
        ))
        .expect("begin KV transaction");
        let begin_frame = writer_mailbox
            .receiver()
            .try_recv()
            .expect("begin ack envelope")
            .into_payload::<FrameContext>()
            .expect("begin ack frame");
        let tx_id = decode_kv_begin_tx_id(&begin_frame.payload);

        sink.deliver(Envelope::from_route(
            writer_address.clone(),
            kv_address.clone(),
            FrameContext::new(
                writer_session_id,
                ChannelId::Pub,
                MessageType::new(crate::protocol::kv::msg_type::PUT),
                encode_kv_put(tx_id, kv_route, b"user:1", b"alice"),
                family,
            ),
        ))
        .expect("put KV value");
        let _ = writer_mailbox
            .receiver()
            .try_recv()
            .expect("put ack envelope");

        sink.deliver(Envelope::from_route(
            writer_address,
            kv_address,
            FrameContext::new(
                writer_session_id,
                ChannelId::Pub,
                MessageType::new(crate::protocol::kv::msg_type::COMMIT),
                encode_kv_commit(tx_id, kv_route),
                family,
            ),
        ))
        .expect("commit KV transaction");
        let _ = writer_mailbox
            .receiver()
            .try_recv()
            .expect("commit ack envelope");

        // Assert
        let notify_frame = watcher_mailbox
            .receiver()
            .try_recv()
            .expect("KV notify envelope")
            .into_payload::<FrameContext>()
            .expect("KV notify frame");
        assert_eq!(
            notify_frame.msg_type.as_u16(),
            crate::protocol::kv::msg_type::NOTIFY
        );
        let (delivered_subscription_id, delivered_route, mutation_count) =
            decode_kv_watch_delivery(&notify_frame);
        assert_eq!(delivered_subscription_id, subscription_id);
        assert_eq!(delivered_route, kv_route);
        assert_eq!(mutation_count, 1);
    }

    #[test]
    fn should_not_notify_kv_subscriber_given_empty_commit() {
        // Arrange
        let family = RouteFamily::new(1);
        let watch_session_id = 7;
        let writer_session_id = 8;
        let kv_route = "kv://acme/app/users";
        let kv_address = RouteAddress::new(family, Route::new(kv_route));
        let watcher_address = RouteAddress::new(family, Route::new("inbox://session/7"));
        let writer_address = RouteAddress::new(family, Route::new("inbox://session/8"));
        let watcher_mailbox = Arc::new(Mailbox::new(16));
        let writer_mailbox = Arc::new(Mailbox::new(16));
        let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
        let router = Arc::new(Router::new());
        router.register(watcher_address.clone(), watcher_mailbox.clone());
        router.register(writer_address.clone(), writer_mailbox.clone());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let sink = KvDomainSink::new(store, router, admin_read_model);

        sink.deliver(Envelope::from_route(
            watcher_address,
            kv_address.clone(),
            FrameContext::new(
                watch_session_id,
                ChannelId::Pub,
                MessageType::new(crate::protocol::kv::msg_type::SUBSCRIBE),
                encode_kv_subscribe(kv_route),
                family,
            ),
        ))
        .expect("subscribe to KV route");
        let _ = watcher_mailbox
            .receiver()
            .try_recv()
            .expect("subscribe ack envelope");

        // Act
        sink.deliver(Envelope::from_route(
            writer_address.clone(),
            kv_address.clone(),
            FrameContext::new(
                writer_session_id,
                ChannelId::Pub,
                MessageType::new(crate::protocol::kv::msg_type::BEGIN),
                encode_kv_begin(kv_route, 1, 0),
                family,
            ),
        ))
        .expect("begin KV transaction");
        let begin_frame = writer_mailbox
            .receiver()
            .try_recv()
            .expect("begin ack envelope")
            .into_payload::<FrameContext>()
            .expect("begin ack frame");
        let tx_id = decode_kv_begin_tx_id(&begin_frame.payload);

        sink.deliver(Envelope::from_route(
            writer_address,
            kv_address,
            FrameContext::new(
                writer_session_id,
                ChannelId::Pub,
                MessageType::new(crate::protocol::kv::msg_type::COMMIT),
                encode_kv_commit(tx_id, kv_route),
                family,
            ),
        ))
        .expect("commit empty KV transaction");
        let _ = writer_mailbox
            .receiver()
            .try_recv()
            .expect("commit ack envelope");

        // Assert
        assert!(watcher_mailbox.receiver().try_recv().is_err());
    }

    #[test]
    fn should_remove_kv_subscription_given_unsubscribe() {
        // Arrange
        let family = RouteFamily::new(1);
        let watch_session_id = 7;
        let writer_session_id = 8;
        let kv_route = "kv://acme/app/users";
        let kv_address = RouteAddress::new(family, Route::new(kv_route));
        let watcher_address = RouteAddress::new(family, Route::new("inbox://session/7"));
        let writer_address = RouteAddress::new(family, Route::new("inbox://session/8"));
        let watcher_mailbox = Arc::new(Mailbox::new(16));
        let writer_mailbox = Arc::new(Mailbox::new(16));
        let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
        let router = Arc::new(Router::new());
        router.register(watcher_address.clone(), watcher_mailbox.clone());
        router.register(writer_address.clone(), writer_mailbox.clone());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let sink = KvDomainSink::new(store, router, admin_read_model);

        sink.deliver(Envelope::from_route(
            watcher_address.clone(),
            kv_address.clone(),
            FrameContext::new(
                watch_session_id,
                ChannelId::Pub,
                MessageType::new(crate::protocol::kv::msg_type::SUBSCRIBE),
                encode_kv_subscribe(kv_route),
                family,
            ),
        ))
        .expect("subscribe to KV route");
        let _ = watcher_mailbox
            .receiver()
            .try_recv()
            .expect("subscribe ack envelope");

        // Act
        sink.deliver(Envelope::from_route(
            watcher_address,
            kv_address.clone(),
            FrameContext::new(
                watch_session_id,
                ChannelId::Pub,
                MessageType::new(crate::protocol::kv::msg_type::UNSUBSCRIBE),
                encode_kv_unsubscribe(kv_route),
                family,
            ),
        ))
        .expect("unsubscribe from KV route");
        let _ = watcher_mailbox
            .receiver()
            .try_recv()
            .expect("unsubscribe ack envelope");

        sink.deliver(Envelope::from_route(
            writer_address.clone(),
            kv_address.clone(),
            FrameContext::new(
                writer_session_id,
                ChannelId::Pub,
                MessageType::new(crate::protocol::kv::msg_type::BEGIN),
                encode_kv_begin(kv_route, 1, 0),
                family,
            ),
        ))
        .expect("begin KV transaction");
        let begin_frame = writer_mailbox
            .receiver()
            .try_recv()
            .expect("begin ack envelope")
            .into_payload::<FrameContext>()
            .expect("begin ack frame");
        let tx_id = decode_kv_begin_tx_id(&begin_frame.payload);

        sink.deliver(Envelope::from_route(
            writer_address.clone(),
            kv_address.clone(),
            FrameContext::new(
                writer_session_id,
                ChannelId::Pub,
                MessageType::new(crate::protocol::kv::msg_type::PUT),
                encode_kv_put(tx_id, kv_route, b"user:1", b"alice"),
                family,
            ),
        ))
        .expect("put KV value");
        let _ = writer_mailbox
            .receiver()
            .try_recv()
            .expect("put ack envelope");

        sink.deliver(Envelope::from_route(
            writer_address,
            kv_address,
            FrameContext::new(
                writer_session_id,
                ChannelId::Pub,
                MessageType::new(crate::protocol::kv::msg_type::COMMIT),
                encode_kv_commit(tx_id, kv_route),
                family,
            ),
        ))
        .expect("commit KV transaction");
        let _ = writer_mailbox
            .receiver()
            .try_recv()
            .expect("commit ack envelope");

        // Assert
        assert!(watcher_mailbox.receiver().try_recv().is_err());
        assert!(sink.families.lock().is_empty());
    }
}
