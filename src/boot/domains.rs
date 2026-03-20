//! Domain actor setup and registration

use crate::boot::runtime::BootResult;
use crate::protocol::frame_context::FrameContext;
use crate::runtime::{DeliveryError, Envelope, MailboxSink, Router};
use chrono::Utc;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::Arc as StdArc;

#[cfg(test)]
use crate::runtime::routing::{Route, RouteAddress, RouteFamily};

fn parse_route_triplet(route: &str) -> Option<(String, String, String)> {
    let path = route.split("://").nth(1).unwrap_or(route);
    let parts: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    if parts.len() < 3 {
        return None;
    }
    Some((
        parts[0].to_string(),
        parts[1].to_string(),
        parts[2].to_string(),
    ))
}

fn parse_route_quad(route: &str) -> Option<(String, String, String, String)> {
    let path = route.split("://").nth(1).unwrap_or(route);
    let parts: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    if parts.len() < 4 {
        return None;
    }
    Some((
        parts[0].to_string(),
        parts[1].to_string(),
        parts[2].to_string(),
        parts[3].to_string(),
    ))
}

// ═══════════════════════════════════════════════════════════════════════════
// GENERIC DOMAIN SINK (FALLBACK)
// ═══════════════════════════════════════════════════════════════════════════

/// Generic domain sink: Forwards envelopes to domain actors
///
/// This is a thread-safe wrapper that:
/// - Holds mutable actor state in a Mutex (100% sync, no async locks)
/// - Parses incoming TLV frames
/// - Dispatches to domain handler
/// - Builds response envelopes
///
/// Each domain (KV, Queue, Notice, etc) instantiates this with their own actor type.
pub struct DomainSink {
    name: &'static str,
    active: AtomicBool,
}

impl DomainSink {
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            active: AtomicBool::new(true),
        }
    }

    pub fn stop(&self) {
        self.active.store(false, Ordering::Relaxed);
    }
}

impl MailboxSink for DomainSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        if !self.active.load(Ordering::Relaxed) {
            return Err(DeliveryError::ActorStopped);
        }

        tracing::debug!(
            domain = self.name,
            destination = ?envelope.destination(),
            "Frame received by domain sink"
        );

        Ok(())
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// KV DOMAIN SINK
// ═══════════════════════════════════════════════════════════════════════════

/// Real KV domain sink with actual KvActor
///
/// This sink:
/// - Maintains per-session KvActor instances
/// - Parses TLV frames to KvMessage
/// - Dispatches to actor
/// - Returns responses
///
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

pub struct KvDomainSink {
    /// Midge storage engine
    store: Arc<cntryl_midge::Engine>,
    /// Per-session actors (keyed by session_id)
    actors: Arc<Mutex<HashMap<u64, crate::domains::kv::KvActor>>>,
    /// Resource lock for ReadWrite transactions: (family_id, resource_key) -> owning session_id
    resource_locks: Mutex<HashMap<KvResourceLockKey, u64>>,
    /// Map (session_id, tx_id) -> (family_id, resource_key) for releasing lock on Commit/Rollback
    tx_to_resource: Mutex<HashMap<(u64, u64), KvResourceLockKey>>,
    /// Router for routing response envelopes back
    router: Arc<Router>,
    admin_read_model: Arc<crate::api::admin::read_model::AdminReadModel>,
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
            router,
            admin_read_model,
            active: AtomicBool::new(true),
        }
    }

    pub fn stop(&self) {
        self.active.store(false, Ordering::Relaxed);
    }

    fn sync_admin_snapshot(&self) {
        let transactions = self
            .tx_to_resource
            .lock()
            .iter()
            .map(
                |((session_id, tx_id), resource_key)| crate::api::admin::KvTransaction {
                    tx_id: *tx_id,
                    realm: resource_key.realm.clone(),
                    area: resource_key.area.clone(),
                    resource: resource_key.resource.clone(),
                    mode: format!("session:{session_id}:readwrite"),
                    started_at: Utc::now().to_rfc3339(),
                    operations_count: 0,
                    idle_seconds: 0,
                },
            )
            .collect();
        self.admin_read_model.replace_kv_transactions(transactions);
    }

    /// Remove actor and release all resource locks for a session (called on disconnect cleanup).
    pub fn cleanup_session(&self, session_id: u64) {
        // Remove the actor for this session
        self.actors.lock().remove(&session_id);

        {
            // Release all resource locks held by this session.
            let mut locks = self.resource_locks.lock();
            locks.retain(|_key, holder_id| *holder_id != session_id);
        }

        {
            // Clean up tx_to_resource mappings for this session.
            let mut tx_map = self.tx_to_resource.lock();
            tx_map.retain(|(sid, _tx_id), _key| *sid != session_id);
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
        if !self.active.load(Ordering::Relaxed) {
            return Err(DeliveryError::ActorStopped);
        }

        // Handle SessionCleanup event (disconnect cleanup)
        if let Some(cleanup) = envelope.payload::<crate::runtime::SessionCleanup>() {
            self.cleanup_session(cleanup.session_id);
            return Ok(());
        }

        tracing::debug!(
            domain = "kv",
            destination = %envelope.destination(),
            source = ?envelope.source(),
            "KV domain sink: received envelope"
        );

        // Extract frame context from envelope payload
        // The transport layer stores FrameContext as the envelope payload
        let frame_ctx = match envelope.payload::<FrameContext>() {
            Some(ctx) => ctx.clone(),
            None => {
                // Fallback: try to extract raw Bytes
                match envelope.payload::<bytes::Bytes>() {
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
                }
            }
        };

        let _route_addr = envelope.destination();

        // Parse TLV frame using codec
        // Per CLIENT_SPEC: All KV operations now include full route on wire
        // RouteFamily is derived server-side from the route string
        let kv_message = match crate::protocol::kv::parse_request(
            frame_ctx.msg_type.as_u16(),
            frame_ctx.route_family,
            &frame_ctx.payload,
        ) {
            Ok(msg) => msg,
            Err(e) => {
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

        // Log successful parsing
        tracing::debug!(
            domain = "kv",
            session = frame_ctx.session_id,
            channel = ?frame_ctx.channel_id,
            msg_type = frame_ctx.msg_type.as_u16(),
            "Parsed KV message successfully"
        );

        // Get or create actor for this session; enforce resource lock for ReadWrite transactions
        use crate::domains::kv::{KvError, KvMessage, KvResponse, TxMode};
        let session_id = frame_ctx.session_id;

        tracing::trace!(
            domain = "kv",
            session_id = session_id,
            msg_type = frame_ctx.msg_type.as_u16(),
            "KV deliver: getting or creating actor for session"
        );

        let (response, should_sync_admin_snapshot) = match &kv_message {
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
                                (resp, true)
                            } else {
                                (resp, false)
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
                            (resp, true)
                        } else {
                            (resp, false)
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
                let resp = actor.handle(kv_message.clone());
                if let KvResponse::CommitOk = resp {
                    let lock_key = self.tx_to_resource.lock().remove(&(session_id, *tx_id));
                    if let Some(k) = lock_key {
                        self.resource_locks.lock().remove(&k);
                    }
                    (resp, true)
                } else {
                    (resp, false)
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
                    (resp, true)
                } else {
                    (resp, false)
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
                (actor.handle(kv_message), false)
            }
        };
        if should_sync_admin_snapshot {
            self.sync_admin_snapshot();
        }

        tracing::debug!(
            domain = "kv",
            session = frame_ctx.session_id,
            response = ?std::mem::discriminant(&response),
            "KV actor returned response"
        );

        // Encode the response
        let response_bytes = crate::protocol::kv::encode_response(&response);
        tracing::trace!(
            domain = "kv",
            session = frame_ctx.session_id,
            response_len = response_bytes.len(),
            "KV response encoded"
        );

        // Build response envelope using try_reply_to (non-panicking)
        // This swaps source/destination and sets causation
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
                tracing::warn!(
                    domain = "kv",
                    session = frame_ctx.session_id,
                    "Cannot route response: envelope has no source address"
                );
                return Ok(());
            }
        };

        // Route response back through the router
        // This will deliver to the ingress/session layer which handles sending to transport
        match self.router.route(response_envelope) {
            Ok(_) => {
                tracing::debug!(
                    domain = "kv",
                    session = frame_ctx.session_id,
                    "KV message handled and response routed"
                );
                Ok(())
            }
            Err(e) => {
                tracing::warn!(
                    domain = "kv",
                    session = frame_ctx.session_id,
                    error = ?e,
                    "Failed to route response"
                );
                Err(DeliveryError::ActorStopped)
            }
        }
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// NOTICE DOMAIN SINK
// ═══════════════════════════════════════════════════════════════════════════

/// Subscription entry for notice pub/sub
struct NoticeSubscription {
    /// Pattern to match against published routes
    pattern: crate::runtime::matcher::Pattern,
    /// Session ID of the subscriber
    session_id: u64,
    /// Unique subscription ID
    subscription_id: u64,
    /// Inbox route address to send notifications to
    subscriber: crate::runtime::routing::RouteAddress,
}

/// Per-family subscription state
struct NoticeFamilyState {
    subscriptions: Vec<NoticeSubscription>,
}

/// Notice domain sink: pub/sub notification routing
///
/// Manages subscriptions per route family and matches published
/// routes against subscriber patterns for fan-out delivery.
pub struct NoticeDomainSink {
    /// Per-family subscription state
    families: Mutex<HashMap<u64, NoticeFamilyState>>,
    /// Monotonic subscription ID counter
    next_sub_id: AtomicU64,
    /// Router for routing notification envelopes
    router: Arc<Router>,
    admin_read_model: Arc<crate::api::admin::read_model::AdminReadModel>,
    active: AtomicBool,
}

impl NoticeDomainSink {
    pub fn new(
        router: Arc<Router>,
        admin_read_model: Arc<crate::api::admin::read_model::AdminReadModel>,
    ) -> Self {
        Self {
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
}

impl NoticeDomainSink {
    fn sync_admin_snapshot(&self) {
        let families = self.families.lock();
        let mut subscriptions = Vec::new();
        let mut routes: HashMap<String, usize> = HashMap::new();
        for state in families.values() {
            for subscription in &state.subscriptions {
                let pattern = subscription.pattern.route().to_string();
                if let Some((realm, _area, _resource)) = parse_route_triplet(&pattern) {
                    subscriptions.push(crate::api::admin::NoticeSubscription {
                        subscription_id: subscription.subscription_id,
                        session_id: subscription.session_id.to_string(),
                        realm,
                        pattern: pattern.clone(),
                        created_at: Utc::now().to_rfc3339(),
                        notifications_received: 0,
                    });
                    *routes.entry(pattern).or_insert(0) += 1;
                }
            }
        }
        drop(families);
        self.admin_read_model
            .replace_notice_subscriptions(subscriptions);
        self.admin_read_model.replace_notice_routes(
            routes
                .into_iter()
                .map(|(route, subscribers)| crate::api::admin::NoticeRouteInfo {
                    route,
                    subscribers,
                    publishes_total: 0,
                    publishes_per_minute: 0.0,
                })
                .collect(),
        );
    }

    /// Handle a DomainPublishEvent from another domain (e.g. Schedule target_resource execution).
    /// Matches the event route against notice subscription patterns and fans out
    /// NOTICE NOTIFY (504) frames to matching subscribers.
    fn handle_domain_publish(
        &self,
        event: &crate::runtime::DomainPublishEvent,
    ) -> Result<(), DeliveryError> {
        let family_id = event.family_id.as_u64();
        let mut payload_encoder =
            crate::protocol::payload_codec::PayloadEncoder::with_capacity(256);
        let families = self.families.lock();
        if let Some(state) = families.get(&family_id) {
            for sub in &state.subscriptions {
                if sub.pattern.matches(&event.route) {
                    let notify_payload = crate::protocol::notice_codec::encode_notify_into(
                        sub.subscription_id,
                        &event.route,
                        &event.payload,
                        &mut payload_encoder,
                    );
                    let notify_ctx = FrameContext::new(
                        sub.session_id,
                        crate::protocol::frame::ChannelId::Sub, // notification channel
                        crate::protocol::tlv::MessageType::new(504), // NOTICE NOTIFY
                        bytes::Bytes::from(notify_payload),
                        crate::runtime::routing::RouteFamily::from_u32(
                            sub.subscriber.family().id(),
                        ),
                    );
                    let notify_envelope = Envelope::new(sub.subscriber.clone(), notify_ctx);
                    let _ = self.router.route(notify_envelope);
                }
            }
        }
        Ok(())
    }

    /// Remove all subscriptions for a given session (called on disconnect cleanup).
    pub fn unsubscribe_all_for_session(&self, session_id: u64) {
        let mut families = self.families.lock();
        for state in families.values_mut() {
            state.subscriptions.retain(|s| s.session_id != session_id);
        }
        tracing::debug!(
            domain = "notice",
            session = session_id,
            "All notice subscriptions removed for session (disconnect cleanup)"
        );
    }

    pub fn subscription_count(&self) -> usize {
        let families = self.families.lock();
        families
            .values()
            .map(|state| state.subscriptions.len())
            .sum()
    }
}

impl MailboxSink for NoticeDomainSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        if !self.active.load(Ordering::Relaxed) {
            return Err(DeliveryError::ActorStopped);
        }

        // PATH 1: DomainPublishEvent from another domain (e.g. Schedule target_resource)
        if let Some(event) = envelope.payload::<crate::runtime::DomainPublishEvent>() {
            return self.handle_domain_publish(event);
        }

        // PATH 1b: SessionCleanup from disconnect handler
        if let Some(cleanup) = envelope.payload::<crate::runtime::SessionCleanup>() {
            self.unsubscribe_all_for_session(cleanup.session_id);
            return Ok(());
        }

        tracing::debug!(
            domain = "notice",
            destination = %envelope.destination(),
            source = ?envelope.source(),
            "Notice domain sink: received envelope"
        );

        // PATH 2: FrameContext from client wire frames
        let frame_ctx = match envelope.payload::<FrameContext>() {
            Some(ctx) => ctx.clone(),
            None => {
                tracing::warn!(domain = "notice", "Envelope payload was not FrameContext");
                return Err(DeliveryError::ActorStopped);
            }
        };

        tracing::debug!(
            domain = "notice",
            session = frame_ctx.session_id,
            msg_type = frame_ctx.msg_type.as_u16(),
            payload_len = frame_ctx.payload.len(),
            "Notice: parsing request"
        );

        let notice_msg = match crate::protocol::notice_codec::parse_request(
            &frame_ctx,
            &frame_ctx.payload,
            *envelope.destination().family(),
            crate::session::SessionId(frame_ctx.session_id),
            // Determine subscriber address: use envelope source if available,
            // otherwise use session inbox for routing notifications back to client
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
            Ok(msg) => msg,
            Err(e) => {
                tracing::warn!(domain = "notice", error = %e, "Failed to parse notice message");
                return Err(DeliveryError::ActorStopped);
            }
        };

        use crate::domains::notice::protocol::NotificationMessage;
        use crate::protocol::notice_codec::NoticeResponse;
        let mut payload_encoder =
            crate::protocol::payload_codec::PayloadEncoder::with_capacity(256);

        let (response_opt, should_sync_admin_snapshot) = match notice_msg {
            NotificationMessage::Publish(pub_msg) => {
                // Publish responds with OK after fanout to keep request/response symmetry in tests.
                let family_id = pub_msg.family_id.as_u64();
                let families = self.families.lock();
                if let Some(state) = families.get(&family_id) {
                    let route = pub_msg.route.clone();

                    for sub in &state.subscriptions {
                        if sub.pattern.matches(&route) {
                            let notify_payload = crate::protocol::notice_codec::encode_notify_into(
                                sub.subscription_id,
                                &route,
                                &pub_msg.payload,
                                &mut payload_encoder,
                            );
                            let notify_ctx = FrameContext::new(
                                sub.session_id,
                                crate::protocol::frame::ChannelId::Sub,
                                crate::protocol::tlv::MessageType::new(504), // NOTICE NOTIFY
                                bytes::Bytes::from(notify_payload),
                                crate::runtime::routing::RouteFamily::from_u32(
                                    sub.subscriber.family().id(),
                                ),
                            );
                            let notify_envelope = Envelope::new(sub.subscriber.clone(), notify_ctx);
                            let _ = self.router.route(notify_envelope);
                        }
                    }
                }
                (
                    Some(NoticeResponse::Ok {
                        subscription_id: None,
                    }),
                    false,
                )
            }
            NotificationMessage::Subscribe(sub_msg) => {
                let family_id = sub_msg.family_id.as_u64();

                let mut families = self.families.lock();
                let state = families
                    .entry(family_id)
                    .or_insert_with(|| NoticeFamilyState {
                        subscriptions: Vec::new(),
                    });

                // Idempotent: if (session_id, pattern) already exists, return existing subscription_id
                let existing_sub_id = state
                    .subscriptions
                    .iter()
                    .find(|s| {
                        s.session_id == sub_msg.session_id.0
                            && s.pattern.route() == sub_msg.pattern.as_str()
                    })
                    .map(|s| s.subscription_id);

                // Validate pattern is not empty
                if sub_msg.pattern.as_str().is_empty() {
                    tracing::warn!(
                        domain = "notice",
                        session = sub_msg.session_id.0,
                        "Rejected empty subscription pattern"
                    );
                    (
                        Some(NoticeResponse::Error("empty pattern".to_string())),
                        false,
                    )
                } else {
                    let sub_id = if let Some(id) = existing_sub_id {
                        tracing::debug!(
                            domain = "notice",
                            session = sub_msg.session_id.0,
                            subscription_id = id,
                            pattern = sub_msg.pattern.as_str(),
                            "Notice subscription already exists (idempotent)"
                        );
                        id
                    } else {
                        let new_id = self.next_sub_id.fetch_add(1, Ordering::Relaxed);
                        let pattern =
                            crate::runtime::matcher::Pattern::new(sub_msg.pattern.as_str());

                        state.subscriptions.push(NoticeSubscription {
                            pattern,
                            session_id: sub_msg.session_id.0,
                            subscription_id: new_id,
                            subscriber: sub_msg.subscriber.clone(),
                        });

                        tracing::debug!(
                            domain = "notice",
                            session = sub_msg.session_id.0,
                            subscription_id = new_id,
                            pattern = sub_msg.pattern.as_str(),
                            "Notice subscription added"
                        );
                        new_id
                    };

                    (
                        Some(NoticeResponse::Ok {
                            subscription_id: Some(sub_id),
                        }),
                        true,
                    )
                }
            }
            NotificationMessage::Unsubscribe(unsub_msg) => {
                let family_id = unsub_msg.family_id.as_u64();
                let mut families = self.families.lock();
                if let Some(state) = families.get_mut(&family_id) {
                    state.subscriptions.retain(|s| {
                        !(s.session_id == unsub_msg.session_id.0
                            && s.pattern.route() == unsub_msg.pattern.as_str())
                    });
                }
                (
                    Some(NoticeResponse::Ok {
                        subscription_id: None,
                    }),
                    true,
                )
            }
            NotificationMessage::UnsubscribeAll(unsub_all) => {
                let session_id = unsub_all.session_id.0;
                let mut families = self.families.lock();
                for state in families.values_mut() {
                    state.subscriptions.retain(|s| s.session_id != session_id);
                }
                tracing::debug!(
                    domain = "notice",
                    session = session_id,
                    "All subscriptions removed for session"
                );
                (
                    Some(NoticeResponse::Ok {
                        subscription_id: None,
                    }),
                    true,
                )
            }
            NotificationMessage::Deliver(_) => {
                // Deliver is internal delivery, no response needed
                (
                    Some(NoticeResponse::Ok {
                        subscription_id: None,
                    }),
                    false,
                )
            }
        };
        if should_sync_admin_snapshot {
            self.sync_admin_snapshot();
        }

        // Only send response if one was generated (PUBLISH returns None for fire-and-forget)
        if let Some(response) = response_opt {
            let response_bytes = crate::protocol::notice_codec::encode_response_into(
                &response,
                &mut payload_encoder,
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
        }

        Ok(())
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// RPC DOMAIN SINK
// ═══════════════════════════════════════════════════════════════════════════

/// Worker entry for RPC routing
struct RpcWorker {
    /// Route the worker registered for (e.g. rpc://realm/area/service)
    addr: crate::runtime::routing::RouteAddress,
    /// Session ID of the worker (for routing forwarded requests to session inbox)
    session_id: u64,
    /// Route family of the worker's connection
    route_family: crate::runtime::routing::RouteFamily,
}

/// RPC domain state
struct RpcState {
    /// Registered workers keyed by route pattern string
    workers: HashMap<String, Vec<RpcWorker>>,
    /// Round-robin index per route pattern
    rr_index: HashMap<String, usize>,
    /// Pending requests: correlation_id -> (caller session_id, caller route_family) for response routing to inbox
    pending: HashMap<uuid::Uuid, (u64, crate::runtime::routing::RouteFamily)>,
}

/// RPC domain sink: request/response worker pool routing
///
/// Manages worker registration, request dispatching (round-robin),
/// and response forwarding via correlation IDs.
pub struct RpcDomainSink {
    state: Mutex<RpcState>,
    router: Arc<Router>,
    admin_read_model: Arc<crate::api::admin::read_model::AdminReadModel>,
    active: AtomicBool,
}

impl RpcDomainSink {
    pub fn new(
        router: Arc<Router>,
        admin_read_model: Arc<crate::api::admin::read_model::AdminReadModel>,
    ) -> Self {
        Self {
            state: Mutex::new(RpcState {
                workers: HashMap::new(),
                rr_index: HashMap::new(),
                pending: HashMap::new(),
            }),
            router,
            admin_read_model,
            active: AtomicBool::new(true),
        }
    }

    pub fn stop(&self) {
        self.active.store(false, Ordering::Relaxed);
    }

    fn sync_admin_snapshot(&self) {
        let state = self.state.lock();
        let workers = state
            .workers
            .iter()
            .flat_map(|(route, workers)| {
                workers.iter().filter_map(|worker| {
                    parse_route_quad(route).map(|(realm, _area, _resource, _operation)| {
                        crate::api::admin::RpcWorker {
                            session_id: worker.session_id.to_string(),
                            realm,
                            route: route.clone(),
                            registered_at: Utc::now().to_rfc3339(),
                            requests_handled: 0,
                            average_latency_ms: 0.0,
                        }
                    })
                })
            })
            .collect();
        let pending = state
            .pending
            .iter()
            .map(
                |(correlation_id, (session_id, _family))| crate::api::admin::RpcPendingRequest {
                    correlation_id: correlation_id.to_string(),
                    route: format!("rpc://pending/session/{session_id}"),
                    submitted_at: Utc::now().to_rfc3339(),
                    age_seconds: 0,
                    worker_session_id: None,
                },
            )
            .collect();
        drop(state);
        self.admin_read_model.replace_rpc_workers(workers);
        self.admin_read_model.replace_rpc_pending(pending);
    }

    pub fn worker_count(&self) -> usize {
        let state = self.state.lock();
        state.workers.values().map(Vec::len).sum()
    }

    pub fn pending_request_count(&self) -> usize {
        self.state.lock().pending.len()
    }
}

impl MailboxSink for RpcDomainSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        if !self.active.load(Ordering::Relaxed) {
            return Err(DeliveryError::ActorStopped);
        }

        tracing::debug!(
            domain = "rpc",
            destination = %envelope.destination(),
            source = ?envelope.source(),
            "RPC domain sink: received envelope"
        );

        let frame_ctx = match envelope.payload::<FrameContext>() {
            Some(ctx) => ctx.clone(),
            None => {
                tracing::warn!(domain = "rpc", "Envelope payload was not FrameContext");
                return Err(DeliveryError::ActorStopped);
            }
        };

        tracing::debug!(
            domain = "rpc",
            session = frame_ctx.session_id,
            msg_type = frame_ctx.msg_type.as_u16(),
            payload_len = frame_ctx.payload.len(),
            "RPC: parsing request"
        );

        let rpc_msg = match crate::protocol::rpc_codec::parse_request(
            &frame_ctx,
            &frame_ctx.payload,
            *envelope.destination().family(),
        ) {
            Ok(msg) => msg,
            Err(e) => {
                tracing::warn!(domain = "rpc", error = %e, "Failed to parse RPC message");
                return Err(DeliveryError::ActorStopped);
            }
        };

        use crate::domains::rpc::protocol::RpcMessage;
        use crate::protocol::rpc_codec::RpcResponseMsg;
        let mut payload_encoder =
            crate::protocol::payload_codec::PayloadEncoder::with_capacity(256);

        // Only emit operation responses for subscribe/unsubscribe or explicit errors.
        let (response, should_sync_admin_snapshot) = match rpc_msg {
            RpcMessage::Subscribe { worker_addr } => {
                let route_key = worker_addr.route().as_str().to_string();
                let mut state = self.state.lock();
                let workers = state.workers.entry(route_key).or_default();
                workers.push(RpcWorker {
                    addr: worker_addr.clone(),
                    session_id: frame_ctx.session_id,
                    route_family: *envelope.destination().family(),
                });
                tracing::debug!(
                    domain = "rpc",
                    worker = worker_addr.route().as_str(),
                    session = frame_ctx.session_id,
                    "Worker registered"
                );
                (Some(RpcResponseMsg::Ok { data: vec![] }), true)
            }
            RpcMessage::Unsubscribe { worker_addr } => {
                let route_key = worker_addr.route().as_str().to_string();
                let mut state = self.state.lock();
                if let Some(workers) = state.workers.get_mut(&route_key) {
                    workers
                        .retain(|w| w.addr != worker_addr || w.session_id != frame_ctx.session_id);
                }
                tracing::debug!(
                    domain = "rpc",
                    worker = worker_addr.route().as_str(),
                    "Worker unregistered"
                );
                (Some(RpcResponseMsg::Ok { data: vec![] }), true)
            }
            RpcMessage::Request(req) => {
                let route_key = req.route.as_str().to_string();
                let mut state = self.state.lock();

                // Find a worker via round-robin — clone the addr to avoid borrow conflict
                let worker_addr = state.workers.get(&route_key).and_then(|workers| {
                    if workers.is_empty() {
                        None
                    } else {
                        let idx = workers.len(); // used below after we get rr_index
                        Some((workers.len(), idx))
                    }
                });

                if let Some((worker_count, _)) = worker_addr {
                    let idx = state.rr_index.entry(route_key.clone()).or_insert(0);
                    let pick = *idx % worker_count;
                    *idx = idx.wrapping_add(1);

                    // Store pending: caller session_id and family for response routing to caller inbox
                    state.pending.insert(
                        req.correlation_id,
                        (frame_ctx.session_id, *envelope.destination().family()),
                    );

                    let (worker_route_family, worker_session_id) = {
                        let worker = &state.workers[&route_key][pick];
                        (worker.route_family, worker.session_id)
                    };
                    let worker_inbox_addr = crate::runtime::routing::RouteAddress::new(
                        worker_route_family,
                        crate::runtime::routing::Route::new(format!(
                            "inbox://session/{}",
                            worker_session_id
                        )),
                    );

                    // Drop state lock before routing
                    drop(state);

                    // Encode REQUEST delivery for worker (similar to Notice NOTIFY encoding)
                    let work_item = crate::domains::rpc::protocol::RpcWorkItem::from_request(&req);
                    let request_payload = crate::protocol::rpc_codec::encode_request_delivery_into(
                        &work_item,
                        &mut payload_encoder,
                    );

                    // Forward request to worker's session inbox (avoids RPC domain re-entry / stack overflow)
                    let forward_ctx = FrameContext::new(
                        worker_session_id,
                        frame_ctx.channel_id,
                        crate::protocol::tlv::MessageType::new(302), // Request msg_type
                        bytes::Bytes::from(request_payload),
                        worker_route_family,
                    );
                    let forward_envelope = Envelope::new(worker_inbox_addr, forward_ctx);
                    let _ = self.router.route(forward_envelope);

                    tracing::debug!(
                        domain = "rpc",
                        correlation_id = %req.correlation_id,
                        route = route_key,
                        "Request forwarded to worker"
                    );
                    // Ack dispatch so caller's SendRequest(302) unblocks; actual response comes via 303.
                    (Some(RpcResponseMsg::Ok { data: vec![] }), true)
                } else {
                    (
                        Some(RpcResponseMsg::Error(
                            "No workers registered for route".to_string(),
                        )),
                        false,
                    )
                }
            }
            RpcMessage::Response(resp) => {
                let mut state = self.state.lock();
                // Keep pending until stream_end so streaming responses (multiple 303 chunks) all get forwarded.
                let caller_info = state.pending.get(&resp.correlation_id).copied();
                let mut state_changed = false;
                if let Some((caller_session_id, caller_family_id)) = caller_info {
                    if resp.stream_end {
                        state.pending.remove(&resp.correlation_id);
                        state_changed = true;
                    }
                    drop(state);

                    // Forward raw RPC RESPONSE payload so clients receive [correlation_id][seq][body][stream_end] per CLIENT_SPEC.
                    let encoded_response = crate::protocol::rpc_codec::encode_response_message_into(
                        &resp,
                        &mut payload_encoder,
                    );

                    // Forward response to caller's session inbox (avoids RPC domain re-entry)
                    let caller_inbox_addr = crate::runtime::routing::RouteAddress::new(
                        caller_family_id,
                        crate::runtime::routing::Route::new(format!(
                            "inbox://session/{}",
                            caller_session_id
                        )),
                    );
                    let forward_ctx = FrameContext::new(
                        caller_session_id,
                        frame_ctx.channel_id,
                        crate::protocol::tlv::MessageType::new(303), // Response msg_type
                        bytes::Bytes::from(encoded_response),
                        caller_family_id,
                    );
                    let forward_envelope = Envelope::new(caller_inbox_addr, forward_ctx);
                    let _ = self.router.route(forward_envelope);

                    // Send ACK back to worker to unblock their SendRequest
                    let ack_payload = crate::protocol::rpc_codec::encode_ack_into(
                        &resp.correlation_id,
                        &mut payload_encoder,
                    );
                    let ack_ctx = FrameContext::new(
                        frame_ctx.session_id,
                        frame_ctx.channel_id,
                        crate::protocol::tlv::MessageType::new(304), // ACK msg_type
                        bytes::Bytes::from(ack_payload),
                        crate::runtime::routing::RouteFamily::from_u32(
                            envelope.destination().family().id(),
                        ),
                    );
                    let worker_inbox_addr = crate::runtime::routing::RouteAddress::new(
                        *envelope.destination().family(),
                        crate::runtime::routing::Route::new(format!(
                            "inbox://session/{}",
                            frame_ctx.session_id
                        )),
                    );
                    let ack_envelope = Envelope::new(worker_inbox_addr, ack_ctx);
                    if let Err(e) = self.router.route(ack_envelope) {
                        tracing::warn!(
                            domain = "rpc",
                            correlation_id = %resp.correlation_id,
                            error = ?e,
                            "Failed to send ACK to worker"
                        );
                    }

                    tracing::debug!(
                        domain = "rpc",
                        correlation_id = %resp.correlation_id,
                        stream_end = resp.stream_end,
                        "Response forwarded to requester and ACK sent to worker"
                    );
                } else {
                    tracing::warn!(
                        domain = "rpc",
                        correlation_id = %resp.correlation_id,
                        "No pending request for response"
                    );
                }
                (None, state_changed)
            }
            RpcMessage::Ack { correlation_id } => {
                let mut state = self.state.lock();
                let removed = state.pending.remove(&correlation_id).is_some();
                tracing::debug!(
                    domain = "rpc",
                    correlation_id = %correlation_id,
                    "Request acknowledged and cleaned up"
                );
                (None, removed)
            }
            RpcMessage::Deliver(_) => {
                // Deliver should only be sent TO workers by route actors,
                // not received from clients. Ignore or error.
                (
                    Some(RpcResponseMsg::Error(
                        "Deliver not valid client message".to_string(),
                    )),
                    false,
                )
            }
        };
        if should_sync_admin_snapshot {
            self.sync_admin_snapshot();
        }

        if let Some(response) = response {
            // Encode and route response back
            let response_bytes =
                crate::protocol::rpc_codec::encode_response_into(&response, &mut payload_encoder);
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
        }

        Ok(())
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// QUEUE DOMAIN SINK
// ═══════════════════════════════════════════════════════════════════════════

/// Queue subscription (for availability notifications)
struct QueueSubscription {
    /// Pattern to match (e.g., "queue://realm/area/resource")
    pattern: crate::runtime::matcher::Pattern,
    /// Session ID
    session_id: u64,
    /// Subscriber route address
    subscriber: crate::runtime::routing::RouteAddress,
    /// Subscription ID
    subscription_id: u64,
}

/// Queue family subscription state
struct QueueFamilyState {
    /// Active subscriptions for this family
    subscriptions: Vec<QueueSubscription>,
}

/// Queue domain sink with per-queue QueueActor instances
///
/// This sink:
/// - Maintains per-queue QueueActor instances keyed by QueueKey
/// - Parses TLV frames to QueueMessage
/// - Dispatches to the correct actor based on route
/// - Returns responses
/// - Tracks subscriptions for availability notifications (empty→non-empty transitions)
pub struct QueueDomainSink {
    /// Midge storage engine
    store: Arc<cntryl_midge::Engine>,
    /// Per-queue actors keyed by QueueKey
    actors: Mutex<HashMap<crate::domains::queue::QueueKey, crate::domains::queue::QueueActor>>,
    /// Per-family subscription state for queue availability notifications
    families: Mutex<HashMap<u64, QueueFamilyState>>,
    /// Monotonic subscription ID counter
    next_sub_id: AtomicU64,
    /// Router for routing response envelopes back
    router: Arc<Router>,
    admin_read_model: Arc<crate::api::admin::read_model::AdminReadModel>,
    active: AtomicBool,
}

impl QueueDomainSink {
    pub fn new(
        store: Arc<cntryl_midge::Engine>,
        router: Arc<Router>,
        admin_read_model: Arc<crate::api::admin::read_model::AdminReadModel>,
    ) -> Self {
        Self {
            store,
            actors: Mutex::new(HashMap::new()),
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
        let queues = self
            .actors
            .lock()
            .keys()
            .map(|key| crate::api::admin::QueueInfo {
                realm: key.realm.clone(),
                area: key.area.clone(),
                resource: key.resource.clone(),
                messages_ready: 0,
                messages_leased: 0,
                messages_total: 0,
                oldest_message_age_seconds: 0,
            })
            .collect();
        self.admin_read_model.replace_queues(queues);
    }

    /// Handle a DomainPublishEvent from queue actors.
    /// Matches the event route against subscription patterns and fans out
    /// QUEUE_NOTIFY (209) frames to matching subscribers.
    fn handle_domain_publish(
        &self,
        event: &crate::runtime::DomainPublishEvent,
    ) -> Result<(), DeliveryError> {
        let family_id = event.family_id.as_u64();
        tracing::info!(
            domain = "queue",
            family_id = family_id,
            route = %event.route,
            "Queue: handle_domain_publish called (ENTRY)"
        );
        let families = self.families.lock();
        if let Some(state) = families.get(&family_id) {
            tracing::info!(
                domain = "queue",
                family_id = family_id,
                subscription_count = state.subscriptions.len(),
                "Queue: found family state with subscriptions"
            );
            let mut matched = 0;
            for sub in &state.subscriptions {
                if sub.pattern.matches(&event.route) {
                    matched += 1;
                    let notify_payload = crate::protocol::queue_codec::encode_notify(
                        sub.subscription_id,
                        &event.route,
                        &event.payload,
                    );
                    let notify_ctx = FrameContext::new(
                        sub.session_id,
                        crate::protocol::frame::ChannelId::Sub, // notification channel
                        crate::protocol::tlv::MessageType::new(209), // QUEUE_NOTIFY
                        bytes::Bytes::from(notify_payload),
                        crate::runtime::routing::RouteFamily::from_u32(
                            sub.subscriber.family().id(),
                        ),
                    );
                    let notify_envelope = Envelope::new(sub.subscriber.clone(), notify_ctx);
                    if let Err(e) = self.router.route(notify_envelope) {
                        tracing::warn!(
                            domain = "queue",
                            destination = %sub.subscriber,
                            error = ?e,
                            "Queue: failed to route 209 to subscriber inbox"
                        );
                    } else {
                        tracing::debug!(
                            domain = "queue",
                            session_id = sub.session_id,
                            destination = %sub.subscriber,
                            "Queue: routed 209 to subscriber"
                        );
                    }
                }
            }
            if matched == 0 {
                tracing::debug!(
                    domain = "queue",
                    family_id = family_id,
                    route = %event.route,
                    subscription_count = state.subscriptions.len(),
                    "Queue: no subscription matched event route"
                );
            }
        } else {
            tracing::debug!(
                domain = "queue",
                family_id = family_id,
                route = %event.route,
                "Queue: no family state for event (no subscriptions in this family)"
            );
        }
        Ok(())
    }

    /// Remove all subscriptions for a given session (called on disconnect cleanup).
    pub fn unsubscribe_all(&self, session_id: u64) {
        let mut families = self.families.lock();
        for state in families.values_mut() {
            state.subscriptions.retain(|s| s.session_id != session_id);
        }
        tracing::debug!(
            domain = "queue",
            session = session_id,
            "All queue subscriptions removed for session"
        );
    }

    /// Get the total number of active queue subscriptions (for stats).
    pub fn subscription_count(&self) -> usize {
        let families = self.families.lock();
        families.values().map(|s| s.subscriptions.len()).sum()
    }

    pub fn pending_message_count(&self) -> usize {
        let actors = self.actors.lock();
        actors.values().map(|actor| actor.ready_len()).sum()
    }

    pub fn active_lease_count(&self) -> usize {
        let actors = self.actors.lock();
        actors.values().map(|actor| actor.inflight.len()).sum()
    }
}

impl MailboxSink for QueueDomainSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        if !self.active.load(Ordering::Relaxed) {
            return Err(DeliveryError::ActorStopped);
        }

        // PATH 1: DomainPublishEvent from internal queue actors
        if let Some(event) = envelope.payload::<crate::runtime::DomainPublishEvent>() {
            return self.handle_domain_publish(event);
        }

        // PATH 1b: SessionCleanup from disconnect handler
        if let Some(cleanup) = envelope.payload::<crate::runtime::SessionCleanup>() {
            self.unsubscribe_all(cleanup.session_id);
            return Ok(());
        }

        // PATH 2: FrameContext from client wire frames (Subscribe/Unsubscribe/etc)
        // or internal timer events to be routed to actors

        tracing::debug!(
            domain = "queue",
            destination = %envelope.destination(),
            source = ?envelope.source(),
            "Queue domain sink: received envelope"
        );

        let frame_ctx = match envelope.payload::<FrameContext>() {
            Some(ctx) => ctx.clone(),
            None => {
                tracing::warn!(domain = "queue", "Envelope payload was not FrameContext");
                return Err(DeliveryError::ActorStopped);
            }
        };

        let route_addr = envelope.destination();
        let route_family = *route_addr.family();

        // Parse queue message using codec.
        // Subscribe (207) and Unsubscribe (208) need session_id and subscriber from envelope.
        let queue_msg = {
            let mt = frame_ctx.msg_type.as_u16();
            if mt == crate::protocol::queue_codec::msg_type::SUBSCRIBE {
                // Determine subscriber address: use envelope source if available,
                // otherwise use session inbox for routing notifications back to client
                let subscriber = if let Some(src) = envelope.source() {
                    tracing::debug!(
                        domain = "queue",
                        session = frame_ctx.session_id,
                        source = %src,
                        "Queue SUBSCRIBE: using envelope source as subscriber"
                    );
                    src.clone()
                } else {
                    let session_inbox = crate::runtime::routing::session_inbox_address(
                        route_family,
                        frame_ctx.session_id,
                    );
                    tracing::debug!(
                        domain = "queue",
                        session = frame_ctx.session_id,
                        subscriber = %session_inbox,
                        "Queue SUBSCRIBE: using session inbox as subscriber (no envelope source)"
                    );
                    session_inbox
                };
                match crate::protocol::queue_codec::parse_subscribe(
                    route_family,
                    &frame_ctx.payload,
                    frame_ctx.session_id,
                    subscriber,
                ) {
                    Ok(msg) => msg,
                    Err(e) => {
                        tracing::warn!(
                            domain = "queue",
                            session = frame_ctx.session_id,
                            msg_type = mt,
                            error = %e,
                            "Failed to parse Queue SUBSCRIBE"
                        );
                        return Err(DeliveryError::ActorStopped);
                    }
                }
            } else if mt == crate::protocol::queue_codec::msg_type::UNSUBSCRIBE {
                // Determine subscriber address: use envelope source if available,
                // otherwise use session inbox for routing notifications back to client
                let subscriber = if let Some(src) = envelope.source() {
                    src.clone()
                } else {
                    crate::runtime::routing::session_inbox_address(
                        route_family,
                        frame_ctx.session_id,
                    )
                };
                match crate::protocol::queue_codec::parse_unsubscribe(
                    route_family,
                    &frame_ctx.payload,
                    frame_ctx.session_id,
                    subscriber,
                ) {
                    Ok(msg) => msg,
                    Err(e) => {
                        tracing::warn!(
                            domain = "queue",
                            session = frame_ctx.session_id,
                            msg_type = mt,
                            error = %e,
                            "Failed to parse Queue UNSUBSCRIBE"
                        );
                        return Err(DeliveryError::ActorStopped);
                    }
                }
            } else {
                match crate::protocol::queue_codec::parse_request(
                    frame_ctx.msg_type.as_u16(),
                    route_family,
                    &frame_ctx.payload,
                ) {
                    Ok(msg) => msg,
                    Err(e) => {
                        tracing::warn!(
                            domain = "queue",
                            session = frame_ctx.session_id,
                            msg_type = frame_ctx.msg_type.as_u16(),
                            error = %e,
                            "Failed to parse Queue message"
                        );
                        return Err(DeliveryError::ActorStopped);
                    }
                }
            }
        };

        tracing::debug!(
            domain = "queue",
            session = frame_ctx.session_id,
            msg_type = frame_ctx.msg_type.as_u16(),
            "Parsed Queue message successfully"
        );

        // Dispatch to per-queue actor or handle subscription
        use crate::domains::queue::protocol::{QueueKey, QueueMessage};

        let (response, availability_notify_route, should_sync_admin_snapshot) = {
            use std::collections::hash_map::Entry;

            let mut actors = self.actors.lock();

            match queue_msg {
                QueueMessage::Send {
                    family_id,
                    route,
                    body,
                    delay_seconds,
                } => {
                    let key = QueueKey::from_route(family_id, &route).unwrap_or(QueueKey {
                        family: family_id,
                        realm: String::new(),
                        area: String::new(),
                        resource: "default".to_string(),
                    });
                    let store = self.store.clone();
                    let (actor, created_actor) = match actors.entry(key.clone()) {
                        Entry::Occupied(entry) => (entry.into_mut(), false),
                        Entry::Vacant(entry) => (
                            entry.insert(crate::domains::queue::QueueActor::new(
                                family_id,
                                key.clone(),
                                store,
                                None,
                                crate::utils::idempotency::global_dedup_store(),
                            )),
                            true,
                        ),
                    };
                    actor.process_expired_timers();
                    actor.process_delayed_messages();
                    let resp = actor.handle_send(body, delay_seconds);
                    let notify_route = if actor.take_needs_notify_availability() {
                        tracing::info!(
                            domain = "queue",
                            session = frame_ctx.session_id,
                            route = %route,
                            family_id = %family_id,
                            "Queue: SEND triggered availability notification"
                        );
                        Some(route)
                    } else {
                        None
                    };
                    (resp, notify_route, created_actor)
                }
                QueueMessage::Receive {
                    family_id,
                    route,
                    lease_seconds,
                    batch_size,
                    ..
                } => {
                    let key = QueueKey::from_route(family_id, &route).unwrap_or(QueueKey {
                        family: family_id,
                        realm: String::new(),
                        area: String::new(),
                        resource: "default".to_string(),
                    });
                    let store = self.store.clone();
                    let (actor, created_actor) = match actors.entry(key.clone()) {
                        Entry::Occupied(entry) => (entry.into_mut(), false),
                        Entry::Vacant(entry) => (
                            entry.insert(crate::domains::queue::QueueActor::new(
                                family_id,
                                key.clone(),
                                store,
                                None,
                                crate::utils::idempotency::global_dedup_store(),
                            )),
                            true,
                        ),
                    };
                    actor.process_expired_timers();
                    actor.process_delayed_messages();
                    (
                        actor.handle_receive(lease_seconds, batch_size),
                        None,
                        created_actor,
                    )
                }
                QueueMessage::Extend {
                    family_id,
                    route,
                    id,
                    token,
                    lease_seconds,
                } => {
                    let key = QueueKey::from_route(family_id, &route).unwrap_or(QueueKey {
                        family: family_id,
                        realm: String::new(),
                        area: String::new(),
                        resource: "default".to_string(),
                    });
                    let store = self.store.clone();
                    let (actor, created_actor) = match actors.entry(key.clone()) {
                        Entry::Occupied(entry) => (entry.into_mut(), false),
                        Entry::Vacant(entry) => (
                            entry.insert(crate::domains::queue::QueueActor::new(
                                family_id,
                                key.clone(),
                                store,
                                None,
                                crate::utils::idempotency::global_dedup_store(),
                            )),
                            true,
                        ),
                    };
                    actor.process_expired_timers();
                    actor.process_delayed_messages();
                    (
                        actor.handle_extend(id, token, lease_seconds),
                        None,
                        created_actor,
                    )
                }
                QueueMessage::Ack {
                    family_id,
                    route,
                    id,
                    token,
                } => {
                    let key = QueueKey::from_route(family_id, &route).unwrap_or(QueueKey {
                        family: family_id,
                        realm: String::new(),
                        area: String::new(),
                        resource: "default".to_string(),
                    });
                    let store = self.store.clone();
                    let (actor, created_actor) = match actors.entry(key.clone()) {
                        Entry::Occupied(entry) => (entry.into_mut(), false),
                        Entry::Vacant(entry) => (
                            entry.insert(crate::domains::queue::QueueActor::new(
                                family_id,
                                key.clone(),
                                store,
                                None,
                                crate::utils::idempotency::global_dedup_store(),
                            )),
                            true,
                        ),
                    };
                    actor.process_expired_timers();
                    actor.process_delayed_messages();
                    (actor.handle_ack(id, token), None, created_actor)
                }
                QueueMessage::LeaseExpired { .. } => {
                    // Internal message, not dispatched via sink
                    (
                        crate::domains::queue::QueueResponse::Error {
                            message: "LeaseExpired is an internal message".to_string(),
                        },
                        None,
                        false,
                    )
                }

                QueueMessage::Subscribe {
                    family_id,
                    pattern,
                    session_id,
                    subscriber,
                } => {
                    let fam_id = family_id.as_u64();

                    let mut families = self.families.lock();
                    let state = families.entry(fam_id).or_insert_with(|| QueueFamilyState {
                        subscriptions: Vec::new(),
                    });

                    // Idempotent: if (session_id, pattern) already exists, return existing subscription_id
                    let existing_sub_id = state
                        .subscriptions
                        .iter()
                        .find(|s| {
                            s.session_id == session_id && s.pattern.route() == pattern.as_str()
                        })
                        .map(|s| s.subscription_id);

                    let sub_id = if let Some(id) = existing_sub_id {
                        tracing::debug!(
                            domain = "queue",
                            session = session_id,
                            subscription_id = id,
                            pattern = pattern.as_str(),
                            "Queue subscription already exists (idempotent)"
                        );
                        id
                    } else {
                        let new_id = self.next_sub_id.fetch_add(1, Ordering::Relaxed);
                        let pat = crate::runtime::matcher::Pattern::new(pattern.as_str());

                        state.subscriptions.push(QueueSubscription {
                            pattern: pat,
                            session_id,
                            subscription_id: new_id,
                            subscriber,
                        });

                        tracing::debug!(
                            domain = "queue",
                            session = session_id,
                            subscription_id = new_id,
                            pattern = pattern.as_str(),
                            "Queue subscription added"
                        );
                        new_id
                    };

                    (
                        crate::domains::queue::QueueResponse::SubscribeOk {
                            subscription_id: sub_id,
                        },
                        None,
                        false,
                    )
                }
                QueueMessage::Unsubscribe {
                    family_id,
                    pattern,
                    session_id,
                    ..
                } => {
                    let fam_id = family_id.as_u64();
                    let mut families = self.families.lock();
                    if let Some(state) = families.get_mut(&fam_id) {
                        state.subscriptions.retain(|s| {
                            !(s.session_id == session_id && s.pattern.route() == pattern.as_str())
                        });
                    }

                    tracing::debug!(
                        domain = "queue",
                        session = session_id,
                        pattern = pattern.as_str(),
                        "Queue subscription removed"
                    );

                    (
                        crate::domains::queue::QueueResponse::UnsubscribeOk,
                        None,
                        false,
                    )
                }
                QueueMessage::UnsubscribeAll { session_id, .. } => {
                    self.unsubscribe_all(session_id);
                    (
                        crate::domains::queue::QueueResponse::UnsubscribeOk,
                        None,
                        false,
                    )
                }
            }
        };
        if should_sync_admin_snapshot {
            self.sync_admin_snapshot();
        }

        // If we just did a Send that transitioned empty->non-empty, fan out QUEUE_NOTIFY (209) to subscribers
        if let Some(notify_route) = availability_notify_route {
            tracing::info!(
                domain = "queue",
                route = %notify_route,
                route_family = route_family.id(),
                "Queue: fanning out availability notification (209) - CALLING handle_domain_publish"
            );
            let event = crate::runtime::DomainPublishEvent::new(
                route_family,
                notify_route,
                bytes::Bytes::from("{}"),
            );
            if let Err(e) = self.handle_domain_publish(&event) {
                tracing::warn!(domain = "queue", error = ?e, "Queue: handle_domain_publish FAILED");
            } else {
                tracing::info!(domain = "queue", "Queue: handle_domain_publish SUCCEEDED");
            }
        }

        // Encode and route response back
        let response_bytes = crate::protocol::queue_codec::encode_response(&response);
        let response_ctx = FrameContext::new(
            frame_ctx.session_id,
            frame_ctx.channel_id,
            crate::protocol::tlv::MessageType::new(frame_ctx.msg_type.as_u16()),
            bytes::Bytes::from(response_bytes),
            frame_ctx.route_family,
        );
        if let Some(response_envelope) = envelope.try_reply_to(response_ctx) {
            match self.router.route(response_envelope) {
                Ok(_) => {
                    tracing::debug!(
                        domain = "queue",
                        session = frame_ctx.session_id,
                        "Queue message handled and response routed"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        domain = "queue",
                        session = frame_ctx.session_id,
                        error = ?e,
                        "Failed to route queue response"
                    );
                }
            }
        }

        Ok(())
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// STREAM DOMAIN SINK
// ═══════════════════════════════════════════════════════════════════════════

/// Subscription entry for stream change notifications
struct StreamSubscription {
    pattern: crate::runtime::matcher::Pattern,
    session_id: u64,
    subscription_id: u64,
    subscriber: crate::runtime::routing::RouteAddress,
}

/// Per-family stream subscription state
struct StreamFamilyState {
    subscriptions: Vec<StreamSubscription>,
}

/// Per-route next offset and session tracking for expected_offset enforcement
#[derive(Clone)]
struct PendingStreamRecord {
    body: bytes::Bytes,
}

struct PendingStreamSession {
    route: String,
    records: Vec<PendingStreamRecord>,
}

#[derive(Clone)]
struct CommittedStreamRecord {
    offset: u64,
    body: bytes::Bytes,
}

struct StreamWriteState {
    next_offset_by_route: HashMap<String, u64>,
    session_to_route: HashMap<u64, String>,
    sessions: HashMap<u64, PendingStreamSession>,
    records_by_route: HashMap<String, Vec<CommittedStreamRecord>>,
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
    /// Midge storage engine (for future StreamStore usage)
    #[allow(dead_code)]
    store: Arc<cntryl_midge::Engine>,
    /// Session ID counter for Begin operations
    next_session_id: AtomicU64,
    /// Per-route next offset and session->route for expected_offset enforcement
    write_state: Mutex<StreamWriteState>,
    /// Per-family subscription state for stream change notifications
    families: Mutex<HashMap<u64, StreamFamilyState>>,
    /// Monotonic subscription ID counter
    next_sub_id: AtomicU64,
    /// Router for routing response envelopes back
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
                next_offset_by_route: HashMap::new(),
                session_to_route: HashMap::new(),
                sessions: HashMap::new(),
                records_by_route: HashMap::new(),
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
        for route in state.session_to_route.values() {
            *sessions_by_route.entry(route.clone()).or_insert(0) += 1;
        }
        let streams = state
            .next_offset_by_route
            .iter()
            .filter_map(|(route, next_offset)| {
                parse_route_triplet(route).map(|(realm, area, resource)| {
                    crate::api::admin::StreamInfo {
                        realm,
                        area,
                        resource,
                        offset: next_offset.saturating_sub(1),
                        watermark: *next_offset,
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

    fn encode_stream_metadata_data(records: &[CommittedStreamRecord]) -> Vec<u8> {
        if records.is_empty() {
            return Vec::new();
        }

        let mut encoder = crate::protocol::payload_codec::PayloadEncoder::new();
        encoder.put_u64(records.first().map(|record| record.offset).unwrap_or(0));
        encoder.put_u64(records.last().map(|record| record.offset).unwrap_or(0));
        encoder.put_u64(records.len() as u64);
        encoder.finish()
    }

    /// Handle a DomainPublishEvent from stream actors.
    /// Matches the event route against subscription patterns and fans out
    /// STREAM_NOTIFY (609) frames to matching subscribers.
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
            for sub in &state.subscriptions {
                if sub.pattern.matches(&event.route) {
                    matched += 1;
                    let notify_payload = crate::protocol::stream_codec::encode_notify_into(
                        &mut payload_encoder,
                        sub.subscription_id,
                        &event.route,
                        &event.payload,
                    );
                    let notify_ctx = FrameContext::new(
                        sub.session_id,
                        crate::protocol::frame::ChannelId::Sub, // notification channel
                        crate::protocol::tlv::MessageType::new(609), // STREAM_NOTIFY
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

    /// Remove all subscriptions for a given session (called on disconnect cleanup).
    pub fn unsubscribe_all(&self, session_id: u64) {
        let mut families = self.families.lock();
        for state in families.values_mut() {
            state.subscriptions.retain(|s| s.session_id != session_id);
        }
        tracing::debug!(
            domain = "stream",
            session = session_id,
            "All stream subscriptions removed for session"
        );
    }

    /// Get the total number of active stream subscriptions (for stats).
    pub fn subscription_count(&self) -> usize {
        let families = self.families.lock();
        families.values().map(|s| s.subscriptions.len()).sum()
    }

    pub fn stream_count(&self) -> usize {
        self.write_state.lock().next_offset_by_route.len()
    }
}

impl MailboxSink for StreamDomainSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        if !self.active.load(Ordering::Relaxed) {
            return Err(DeliveryError::ActorStopped);
        }

        // PATH 1: DomainPublishEvent from internal domain actors
        if let Some(event) = envelope.payload::<crate::runtime::DomainPublishEvent>() {
            return self.handle_domain_publish(event);
        }

        // PATH 1b: SessionCleanup from disconnect handler
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

        // PATH 2: FrameContext from client wire frames
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
            // Determine subscriber address: use envelope source if available,
            // otherwise use session inbox for routing notifications back to client
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

        // Enforce expected_offset at Begin; persist committed records in-memory for READ/LAST/METADATA.
        // If Commit advances a route, emit STREAM_NOTIFY (609) with batch offset metadata.
        let (response, commit_notify, should_sync_admin_snapshot) = match stream_msg {
            StreamMessage::Begin {
                route,
                expected_offset,
                ingest_metadata: _,
                ..
            } => {
                let route_key = route.as_str().to_string();
                let mut state = self.write_state.lock();
                let next = state
                    .next_offset_by_route
                    .entry(route_key.clone())
                    .or_insert(0);
                if expected_offset != *next {
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
                state.session_to_route.insert(session_id, route_key.clone());
                state.sessions.insert(
                    session_id,
                    PendingStreamSession {
                        route: route_key,
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
                let route_key = state.session_to_route.get(&session_id).cloned();
                let maybe_offset = route_key.and_then(|route_key| {
                    let next_offset = state
                        .next_offset_by_route
                        .get(&route_key)
                        .copied()
                        .unwrap_or(0);
                    state.sessions.get_mut(&session_id).map(|session| {
                        let assigned_offset = next_offset + session.records.len() as u64;
                        session.records.push(PendingStreamRecord { body });
                        assigned_offset
                    })
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
                state.session_to_route.remove(&session_id);
                let commit_notify = state.sessions.remove(&session_id).map(|session| {
                    let batch_size = session.records.len();
                    let next_offset = state
                        .next_offset_by_route
                        .entry(session.route.clone())
                        .or_insert(0);
                    let first_offset = *next_offset;
                    let mut committed = Vec::with_capacity(batch_size);
                    let mut current_offset = *next_offset;
                    for record in session.records {
                        committed.push(CommittedStreamRecord {
                            offset: current_offset,
                            body: record.body,
                        });
                        current_offset += 1;
                    }

                    *next_offset = current_offset;
                    if !committed.is_empty() {
                        state
                            .records_by_route
                            .entry(session.route.clone())
                            .or_default()
                            .extend(committed);
                    }

                    let last_offset = current_offset.saturating_sub(1);
                    let payload = bytes::Bytes::from(format!(
                        "{{\"first_resource_offset\":{},\"last_resource_offset\":{},\"batch_size\":{}}}",
                        first_offset, last_offset, batch_size
                    ));
                    (crate::runtime::routing::Route::new(session.route), payload)
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
                state.session_to_route.remove(&session_id);
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
                    .records_by_route
                    .get(route.as_str())
                    .map(|records| {
                        Self::encode_stream_read_data(records, from_offset, limit, max_bytes)
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
                    .records_by_route
                    .get(route.as_str())
                    .and_then(|records| records.last())
                    .map(Self::encode_stream_last_data)
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
                    .records_by_route
                    .get(route.as_str())
                    .map(|records| Self::encode_stream_metadata_data(records))
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
                    subscriptions: Vec::new(),
                });

                // Idempotent: if (session_id, pattern) already exists, return existing subscription_id
                let existing_sub_id = state
                    .subscriptions
                    .iter()
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

                    state.subscriptions.push(StreamSubscription {
                        pattern: pat,
                        session_id,
                        subscription_id: new_id,
                        subscriber,
                    });

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
                    state.subscriptions.retain(|s| {
                        !(s.session_id == session_id && s.pattern.route() == pattern.as_str())
                    });
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
                // Internal messages (RequestLease, LeaseGranted, etc.)
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

        // If we just committed a stream session, fan out STREAM_NOTIFY (609) to matching subscribers
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

        // Encode and route response back
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

// ═══════════════════════════════════════════════════════════════════════════
// LEASE DOMAIN SINK
// ═══════════════════════════════════════════════════════════════════════════

/// Internal lease state for LeaseDomainSink
/// Replicates LeaseActor logic since handle methods are private
struct SinkLeaseState {
    owner_id: String,
    fencing_token: u64,
    expiry: std::time::Instant,
}

/// Lease domain sink: distributed lock operations
///
/// Manages lease state directly (replicating LeaseActor logic) since
/// LeaseActor handle methods are crate-private. Uses per-key state
/// with parking_lot::Mutex for synchronization.
pub struct LeaseDomainSink {
    /// Lease state keyed by LeaseKey
    leases: Mutex<HashMap<crate::domains::lease::protocol::LeaseKey, SinkLeaseState>>,
    /// Next fencing token counter (monotonic)
    next_token: AtomicU64,
    /// Router for routing response envelopes back
    router: Arc<Router>,
    active: AtomicBool,
    /// Subscription tracking per family: RouteFamily → FamilyState with subscriptions
    families: Mutex<HashMap<u64, LeaseFamilyState>>,
    /// Next subscription ID
    next_sub_id: AtomicU64,
    admin_read_model: Arc<crate::api::admin::read_model::AdminReadModel>,
}

/// Lease subscription to availability notifications
#[derive(Debug, Clone)]
struct LeaseSubscription {
    /// Subscription pattern string (e.g., "lease://*/*/resource/changed")
    pattern_str: String,
    /// Parsed subscription pattern
    pattern: crate::runtime::matcher::Pattern,
    /// Session that requested this subscription
    session_id: u64,
    /// Route address for sending NOTIFY frames
    route_address: crate::runtime::routing::RouteAddress,
    /// Unique subscription ID
    subscription_id: u64,
}

/// Per-family lease subscription state
#[derive(Debug, Default)]
struct LeaseFamilyState {
    /// Active subscriptions for this family
    subscriptions: Vec<LeaseSubscription>,
}

impl LeaseDomainSink {
    pub fn new(
        router: Arc<Router>,
        admin_read_model: Arc<crate::api::admin::read_model::AdminReadModel>,
    ) -> Self {
        Self {
            leases: Mutex::new(HashMap::new()),
            next_token: AtomicU64::new(1),
            router,
            active: AtomicBool::new(true),
            families: Mutex::new(HashMap::new()),
            next_sub_id: AtomicU64::new(1),
            admin_read_model,
        }
    }

    pub fn stop(&self) {
        self.active.store(false, Ordering::Relaxed);
    }

    fn sync_admin_snapshot(&self) {
        let now = std::time::Instant::now();
        let leases = self
            .leases
            .lock()
            .iter()
            .map(|(key, state)| crate::api::admin::LeaseInfo {
                realm: key.realm.clone(),
                area: key.area.clone(),
                resource: key.resource.clone(),
                owner_session_id: state.owner_id.clone(),
                acquired_at: Utc::now().to_rfc3339(),
                expires_at: Utc::now()
                    .checked_add_signed(chrono::TimeDelta::seconds(
                        state.expiry.saturating_duration_since(now).as_secs() as i64,
                    ))
                    .unwrap_or_else(Utc::now)
                    .to_rfc3339(),
                renewals: 0,
                fencing_token: state.fencing_token,
            })
            .collect();
        self.admin_read_model.replace_leases(leases);
    }

    /// Release all leases held by owner matching the given prefix (called on disconnect cleanup).
    pub fn cleanup_session(&self, session_id: u64) {
        let owner_prefix = format!("session:{}", session_id);
        let mut leases = self.leases.lock();

        // Remove all leases where owner starts with the prefix
        let count_before = leases.len();
        leases.retain(|_key, state| !state.owner_id.starts_with(&owner_prefix));
        let count_removed = count_before - leases.len();

        tracing::debug!(
            domain = "lease",
            session = session_id,
            count_removed = count_removed,
            "Lease: released all leases for disconnected session"
        );

        // Also remove all subscriptions for this session
        self.unsubscribe_all(session_id);
        drop(leases);
        self.sync_admin_snapshot();
    }

    pub fn lease_count(&self) -> usize {
        self.leases.lock().len()
    }

    pub fn subscription_count(&self) -> usize {
        let families = self.families.lock();
        families
            .values()
            .map(|state| state.subscriptions.len())
            .sum()
    }

    /// Handle domain publish event (availability notifications)
    fn handle_domain_publish(
        &self,
        event: &crate::runtime::DomainPublishEvent,
    ) -> Result<(), DeliveryError> {
        let family_id = event.family_id.as_u64();
        let families = self.families.lock();

        if let Some(family_state) = families.get(&family_id) {
            let mut payload_encoder =
                crate::protocol::payload_codec::PayloadEncoder::with_capacity(256);
            for sub in &family_state.subscriptions {
                if sub.pattern.matches(&event.route) {
                    // Encode notification frame using lease codec (409=NOTIFY)
                    let notify_payload = crate::protocol::lease_codec::encode_notify_into(
                        &mut payload_encoder,
                        sub.subscription_id,
                        event.route.as_str(),
                        &event.payload,
                    );

                    // build context using the actual subscriber session ID (previously hardcoded 0)
                    let notify_ctx = FrameContext::new(
                        sub.session_id,
                        crate::protocol::frame::ChannelId::Sub,
                        crate::protocol::tlv::MessageType::new(409), // LEASE_NOTIFY
                        bytes::Bytes::from(notify_payload),
                        event.family_id,
                    );

                    let notify_envelope = Envelope::new(sub.route_address.clone(), notify_ctx);
                    let _ = self.router.route(notify_envelope);
                }
            }
        }
        Ok(())
    }

    /// Remove all subscriptions for a session
    fn unsubscribe_all(&self, session_id: u64) {
        let mut families = self.families.lock();
        for state in families.values_mut() {
            state
                .subscriptions
                .retain(|sub| sub.session_id != session_id);
        }
    }

    fn next_fencing_token(&self) -> u64 {
        self.next_token.fetch_add(1, Ordering::Relaxed)
    }

    fn handle_acquire(
        &self,
        key: crate::domains::lease::protocol::LeaseKey,
        owner_id: String,
        ttl_secs: u64,
    ) -> crate::domains::lease::protocol::LeaseResponse {
        use crate::domains::lease::protocol::LeaseResponse;
        use std::time::{Duration, Instant};

        let now = Instant::now();
        let ttl = Duration::from_secs(ttl_secs);
        let mut leases = self.leases.lock();

        match leases.get(&key) {
            None => {
                let token = self.next_fencing_token();
                leases.insert(
                    key,
                    SinkLeaseState {
                        owner_id,
                        fencing_token: token,
                        expiry: now + ttl,
                    },
                );
                LeaseResponse::Acquired {
                    fencing_token: token,
                }
            }
            Some(state) => {
                if state.expiry <= now {
                    let token = self.next_fencing_token();
                    leases.insert(
                        key,
                        SinkLeaseState {
                            owner_id,
                            fencing_token: token,
                            expiry: now + ttl,
                        },
                    );
                    LeaseResponse::Acquired {
                        fencing_token: token,
                    }
                } else if state.owner_id == owner_id {
                    LeaseResponse::AlreadyHeld {
                        fencing_token: state.fencing_token,
                    }
                } else {
                    LeaseResponse::HeldByOther {
                        current_owner: state.owner_id.clone(),
                    }
                }
            }
        }
    }

    fn handle_extend(
        &self,
        key: crate::domains::lease::protocol::LeaseKey,
        owner_id: String,
        fencing_token: u64,
        ttl_secs: u64,
    ) -> crate::domains::lease::protocol::LeaseResponse {
        use crate::domains::lease::protocol::LeaseResponse;
        use std::time::{Duration, Instant};

        let now = Instant::now();
        let ttl = Duration::from_secs(ttl_secs);
        let mut leases = self.leases.lock();

        match leases.get_mut(&key) {
            None => LeaseResponse::NotHeld,
            Some(state) => {
                if state.expiry <= now {
                    LeaseResponse::Expired
                } else if state.owner_id != owner_id {
                    LeaseResponse::NotHeld
                } else if state.fencing_token != fencing_token {
                    LeaseResponse::Fenced {
                        current_token: state.fencing_token,
                    }
                } else {
                    let new_token = self.next_fencing_token();
                    state.expiry = now + ttl;
                    state.fencing_token = new_token;
                    LeaseResponse::Extended {
                        fencing_token: new_token,
                    }
                }
            }
        }
    }

    fn handle_release(
        &self,
        key: crate::domains::lease::protocol::LeaseKey,
        owner_id: String,
        fencing_token: u64,
    ) -> crate::domains::lease::protocol::LeaseResponse {
        use crate::domains::lease::protocol::LeaseResponse;
        use std::time::Instant;

        let now = Instant::now();
        let mut leases = self.leases.lock();

        match leases.get(&key) {
            None => LeaseResponse::NotHeld,
            Some(state) => {
                if state.expiry <= now {
                    LeaseResponse::Expired
                } else if state.owner_id != owner_id {
                    LeaseResponse::NotHeld
                } else if state.fencing_token != fencing_token {
                    LeaseResponse::Fenced {
                        current_token: state.fencing_token,
                    }
                } else {
                    leases.remove(&key);
                    LeaseResponse::Released
                }
            }
        }
    }

    fn handle_query(
        &self,
        key: crate::domains::lease::protocol::LeaseKey,
    ) -> crate::domains::lease::protocol::LeaseResponse {
        use crate::domains::lease::protocol::LeaseResponse;
        use std::time::Instant;

        let now = Instant::now();
        let leases = self.leases.lock();

        match leases.get(&key) {
            None => LeaseResponse::NotFound,
            Some(state) => {
                if state.expiry <= now {
                    LeaseResponse::Expired
                } else {
                    let expires_in = state.expiry.duration_since(now);
                    LeaseResponse::Status {
                        owner_id: state.owner_id.clone(),
                        fencing_token: state.fencing_token,
                        expires_in_secs: expires_in.as_secs(),
                        pending_waiters: 0, // boot domain doesn't track waiters
                    }
                }
            }
        }
    }
}

impl MailboxSink for LeaseDomainSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        if !self.active.load(Ordering::Relaxed) {
            return Err(DeliveryError::ActorStopped);
        }

        // Handle SessionCleanup event (disconnect cleanup)
        if let Some(cleanup) = envelope.payload::<crate::runtime::SessionCleanup>() {
            self.cleanup_session(cleanup.session_id);
            return Ok(());
        }

        // PATH 1: DomainPublishEvent from internal lease actors
        if let Some(event) = envelope.payload::<crate::runtime::DomainPublishEvent>() {
            return self.handle_domain_publish(event);
        }

        // PATH 2: FrameContext from client wire frames (Subscribe/Unsubscribe/etc)
        // or internal timer events to be routed to actors

        tracing::debug!(
            domain = "lease",
            destination = %envelope.destination(),
            source = ?envelope.source(),
            "Lease domain sink: received envelope"
        );

        let frame_ctx = match envelope.payload::<FrameContext>() {
            Some(ctx) => ctx.clone(),
            None => {
                tracing::warn!(domain = "lease", "Envelope payload was not FrameContext");
                return Err(DeliveryError::ActorStopped);
            }
        };
        let mut payload_encoder =
            crate::protocol::payload_codec::PayloadEncoder::with_capacity(256);

        let lease_msg = match crate::protocol::lease_codec::parse_request(
            &frame_ctx,
            &frame_ctx.payload,
            *envelope.destination().family(),
        ) {
            Ok(msg) => {
                tracing::debug!(
                    domain = "lease",
                    session = frame_ctx.session_id,
                    msg_type = frame_ctx.msg_type.as_u16(),
                    "Lease: parsed message successfully"
                );
                msg
            }
            Err(e) => {
                tracing::warn!(domain = "lease", error = %e, "Failed to parse lease message");
                return Err(DeliveryError::ActorStopped);
            }
        };

        use crate::domains::lease::protocol::{LeaseKey, LeaseMessage, LeaseResponse};

        // Always prefix owner_id with session scope to ensure cleanup works on disconnect
        // Format: "session:{session_id}:{custom_owner}" or "session:{session_id}" if empty
        let session_prefix = frame_ctx.session_id.to_string();
        let effective_owner = |owner_id: String| {
            if owner_id.is_empty() {
                let mut scoped = String::with_capacity("session:".len() + session_prefix.len());
                scoped.push_str("session:");
                scoped.push_str(&session_prefix);
                scoped
            } else {
                let mut scoped = String::with_capacity(
                    "session::".len() + session_prefix.len() + owner_id.len(),
                );
                scoped.push_str("session:");
                scoped.push_str(&session_prefix);
                scoped.push(':');
                scoped.push_str(&owner_id);
                scoped
            }
        };

        // Dispatch to lease handlers and get domain response
        let domain_response = match lease_msg {
            LeaseMessage::Acquire {
                family_id,
                route,
                owner_id,
                ttl_secs,
                wait_seconds: _,
            } => match LeaseKey::from_route(family_id, &route) {
                Some(key) => self.handle_acquire(key, effective_owner(owner_id), ttl_secs),
                None => LeaseResponse::NotFound,
            },
            LeaseMessage::Extend {
                family_id,
                route,
                owner_id,
                fencing_token,
                ttl_secs,
            } => match LeaseKey::from_route(family_id, &route) {
                Some(key) => {
                    self.handle_extend(key, effective_owner(owner_id), fencing_token, ttl_secs)
                }
                None => LeaseResponse::NotFound,
            },
            LeaseMessage::Release {
                family_id,
                route,
                owner_id,
                fencing_token,
            } => match LeaseKey::from_route(family_id, &route) {
                Some(key) => {
                    let resp =
                        self.handle_release(key.clone(), effective_owner(owner_id), fencing_token);
                    if let crate::domains::lease::protocol::LeaseResponse::Released = resp {
                        // Notify subscribers that this lease route is now available
                        let route = key.to_route();
                        let event = crate::runtime::DomainPublishEvent::new(
                            key.family,
                            route,
                            bytes::Bytes::new(),
                        );
                        let _ = self.handle_domain_publish(&event);
                    }
                    resp
                }
                None => LeaseResponse::NotFound,
            },
            LeaseMessage::Query { family_id, route } => {
                match LeaseKey::from_route(family_id, &route) {
                    Some(key) => self.handle_query(key),
                    None => LeaseResponse::NotFound,
                }
            }
            LeaseMessage::Tick => {
                // Proactively expire old leases and notify for each expired route
                let now = std::time::Instant::now();
                let mut leases = self.leases.lock();
                let expired_keys: Vec<crate::domains::lease::protocol::LeaseKey> = leases
                    .iter()
                    .filter(|(_, state)| state.expiry <= now)
                    .map(|(k, _)| k.clone())
                    .collect();

                for key in &expired_keys {
                    leases.remove(key);
                    let route = key.to_route();
                    let event = crate::runtime::DomainPublishEvent::new(
                        key.family,
                        route,
                        bytes::Bytes::new(),
                    );
                    let _ = self.handle_domain_publish(&event);
                }
                return Ok(());
            }
            LeaseMessage::Subscribe { family_id, pattern } => {
                // Handle subscription to availability notifications
                let parsed_pattern = crate::runtime::matcher::Pattern::new(&pattern);

                // Get route address for sending notifications
                let route_address = match envelope.source() {
                    Some(src) => src,
                    None => {
                        // No source provided - can't send notifications
                        let error_bytes = vec![1u8]; // Error marker
                        let response_ctx = FrameContext::new(
                            frame_ctx.session_id,
                            frame_ctx.channel_id,
                            crate::protocol::tlv::MessageType::new(frame_ctx.msg_type.as_u16()),
                            bytes::Bytes::from(error_bytes),
                            frame_ctx.route_family,
                        );
                        if let Some(response_envelope) = envelope.try_reply_to(response_ctx) {
                            let _ = self.router.route(response_envelope);
                        }
                        return Ok(());
                    }
                };

                let subscription_id = self.next_sub_id.fetch_add(1, Ordering::Relaxed);
                let family_id_u64 = family_id.as_u64();

                let mut families = self.families.lock();
                let family_state = families.entry(family_id_u64).or_default();

                // Check for existing subscription with same pattern and session
                if let Some(existing) = family_state
                    .subscriptions
                    .iter()
                    .find(|s| s.session_id == frame_ctx.session_id && s.pattern_str == pattern)
                {
                    // Idempotent: return existing subscription_id
                    let response_bytes = crate::protocol::lease_codec::encode_domain_response_into(
                        &mut payload_encoder,
                        &LeaseResponse::SubscribeOk {
                            subscription_id: existing.subscription_id,
                        },
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
                    return Ok(());
                }

                // Add new subscription
                family_state.subscriptions.push(LeaseSubscription {
                    pattern_str: pattern,
                    pattern: parsed_pattern,
                    session_id: frame_ctx.session_id,
                    route_address: route_address.clone(),
                    subscription_id,
                });

                let response_bytes = crate::protocol::lease_codec::encode_domain_response_into(
                    &mut payload_encoder,
                    &LeaseResponse::SubscribeOk { subscription_id },
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
                return Ok(());
            }
            LeaseMessage::Unsubscribe { family_id, pattern } => {
                // Handle unsubscribe from availability notifications
                let family_id_u64 = family_id.as_u64();
                let mut families = self.families.lock();

                if let Some(family_state) = families.get_mut(&family_id_u64) {
                    family_state.subscriptions.retain(|s| {
                        !(s.session_id == frame_ctx.session_id && s.pattern_str == pattern)
                    });
                }

                let response_bytes = crate::protocol::lease_codec::encode_domain_response_into(
                    &mut payload_encoder,
                    &LeaseResponse::UnsubscribeOk,
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
                return Ok(());
            }
            LeaseMessage::UnsubscribeAll => {
                // Handled by cleanup_session, just return UnsubscribeOk
                let response_bytes = crate::protocol::lease_codec::encode_domain_response_into(
                    &mut payload_encoder,
                    &LeaseResponse::UnsubscribeOk,
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
                return Ok(());
            }
        };

        let response_bytes = crate::protocol::lease_codec::encode_domain_response_into(
            &mut payload_encoder,
            &domain_response,
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
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// SCHEDULE DOMAIN SINK
// ═══════════════════════════════════════════════════════════════════════════

/// Subscription entry for schedule fire notifications
struct ScheduleSubscription {
    pattern: crate::runtime::matcher::Pattern,
    session_id: u64,
    subscription_id: u64,
    subscriber: crate::runtime::routing::RouteAddress,
}

/// Per-family schedule subscription state
struct ScheduleFamilyState {
    subscriptions: Vec<ScheduleSubscription>,
}

/// Schedule domain sink: delayed/recurring task management with subscription tracking
///
/// Supports dual-path delivery:
/// - PATH 1: `DomainPublishEvent` from schedule actors (subscription matching + fanout)
/// - PATH 2: `FrameContext` from client wire frames (CREATE/CANCEL/LIST/SUBSCRIBE/UNSUBSCRIBE)
pub struct ScheduleDomainSink {
    /// Midge storage engine (for ScheduleStore)
    store: Arc<cntryl_midge::Engine>,
    /// Per-family schedule actors
    actors: Mutex<
        HashMap<crate::runtime::routing::RouteFamily, crate::domains::schedule::ScheduleActor>,
    >,
    /// Per-family subscription state for schedule fire notifications
    sub_families: Mutex<HashMap<u64, ScheduleFamilyState>>,
    /// Monotonic subscription ID counter
    next_sub_id: AtomicU64,
    /// Router for routing response envelopes back
    router: Arc<Router>,
    admin_read_model: Arc<crate::api::admin::read_model::AdminReadModel>,
    active: AtomicBool,
}

impl ScheduleDomainSink {
    pub fn new(
        store: Arc<cntryl_midge::Engine>,
        router: Arc<Router>,
        admin_read_model: Arc<crate::api::admin::read_model::AdminReadModel>,
    ) -> Self {
        Self {
            store,
            actors: Mutex::new(HashMap::new()),
            sub_families: Mutex::new(HashMap::new()),
            next_sub_id: AtomicU64::new(1),
            router,
            admin_read_model,
            active: AtomicBool::new(true),
        }
    }

    pub fn stop(&self) {
        self.active.store(false, Ordering::Relaxed);
    }

    /// Handle a DomainPublishEvent from schedule actors.
    /// Matches the event route against subscription patterns and fans out
    /// SCHEDULE_NOTIFY (705) frames to matching subscribers.
    fn handle_domain_publish(
        &self,
        event: &crate::runtime::DomainPublishEvent,
    ) -> Result<(), DeliveryError> {
        let family_id = event.family_id.as_u64();
        let families = self.sub_families.lock();
        if let Some(state) = families.get(&family_id) {
            let mut payload_encoder =
                crate::protocol::payload_codec::PayloadEncoder::with_capacity(256);
            for sub in &state.subscriptions {
                if sub.pattern.matches(&event.route) {
                    let notify_payload = crate::protocol::schedule_codec::encode_notify_into(
                        &mut payload_encoder,
                        &event.payload,
                    );
                    let notify_ctx = FrameContext::new(
                        sub.session_id,
                        crate::protocol::frame::ChannelId::Sub, // notification channel
                        crate::protocol::tlv::MessageType::new(705), // SCHEDULE_NOTIFY
                        bytes::Bytes::from(notify_payload),
                        crate::runtime::routing::RouteFamily::from_u32(
                            sub.subscriber.family().id(),
                        ),
                    );
                    let notify_envelope = Envelope::new(sub.subscriber.clone(), notify_ctx);
                    let _ = self.router.route(notify_envelope);
                }
            }
        }
        Ok(())
    }

    /// Remove all subscriptions for a given session (called on disconnect cleanup).
    pub fn unsubscribe_all(&self, session_id: u64) {
        let mut families = self.sub_families.lock();
        for state in families.values_mut() {
            state.subscriptions.retain(|s| s.session_id != session_id);
        }
        tracing::debug!(
            domain = "schedule",
            session = session_id,
            "All schedule subscriptions removed for session"
        );
    }

    /// Get the total number of active schedule subscriptions (for stats).
    pub fn subscription_count(&self) -> usize {
        let families = self.sub_families.lock();
        families.values().map(|s| s.subscriptions.len()).sum()
    }

    pub fn schedule_count(&self) -> usize {
        let actors = self.actors.lock();
        actors.values().map(|actor| actor.schedule_count()).sum()
    }
}

impl MailboxSink for ScheduleDomainSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        if !self.active.load(Ordering::Relaxed) {
            return Err(DeliveryError::ActorStopped);
        }

        // PATH 1: DomainPublishEvent from internal domain actors
        if let Some(event) = envelope.payload::<crate::runtime::DomainPublishEvent>() {
            return self.handle_domain_publish(event);
        }

        // PATH 1b: SessionCleanup from disconnect handler
        if let Some(cleanup) = envelope.payload::<crate::runtime::SessionCleanup>() {
            self.unsubscribe_all(cleanup.session_id);
            return Ok(());
        }

        tracing::debug!(
            domain = "schedule",
            destination = %envelope.destination(),
            source = ?envelope.source(),
            "Schedule domain sink: received envelope"
        );

        // PATH 2: FrameContext from client wire frames
        let frame_ctx = match envelope.payload::<FrameContext>() {
            Some(ctx) => ctx.clone(),
            None => {
                tracing::warn!(domain = "schedule", "Envelope payload was not FrameContext");
                return Err(DeliveryError::ActorStopped);
            }
        };
        let mut payload_encoder =
            crate::protocol::payload_codec::PayloadEncoder::with_capacity(256);

        let schedule_msg = match crate::protocol::schedule_codec::parse_request(
            &frame_ctx,
            &frame_ctx.payload,
            *envelope.destination().family(),
            crate::session::SessionId(frame_ctx.session_id),
            // Determine subscriber address: use envelope source if available,
            // otherwise use session inbox for routing notifications back to client
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
            Ok(msg) => msg,
            Err(e) => {
                tracing::warn!(
                    domain = "schedule",
                    error = %e,
                    "Failed to parse schedule message"
                );
                return Err(DeliveryError::ActorStopped);
            }
        };

        let route_addr = envelope.destination();
        let route_family = *route_addr.family();

        use crate::domains::schedule::{ScheduleMessage, ScheduleResponse};
        enum ScheduleAdminUpdate {
            Upsert {
                realm: String,
                area: String,
                resource: String,
                cron: String,
            },
            Remove {
                realm: String,
                area: String,
                resource: String,
            },
        }

        let mut admin_update: Option<ScheduleAdminUpdate> = None;

        let response = {
            let store = self.store.clone();
            let mut actors = self.actors.lock();
            let actor = actors.entry(route_family).or_insert_with(|| {
                crate::domains::schedule::ScheduleActor::new(
                    route_family,
                    store,
                    cntryl_midge::WriteOptions::buffered(),
                )
            });

            match schedule_msg {
                ScheduleMessage::Create {
                    route,
                    cron,
                    payload,
                } => {
                    let route_for_admin = route.clone();
                    let cron_for_admin = cron.clone();
                    match actor.create_schedule(route, cron, payload) {
                        Ok(changed) => {
                            if changed {
                                if let Some((realm, area, resource)) =
                                    parse_route_triplet(&route_for_admin)
                                {
                                    admin_update = Some(ScheduleAdminUpdate::Upsert {
                                        realm,
                                        area,
                                        resource,
                                        cron: cron_for_admin,
                                    });
                                }
                            }
                            ScheduleResponse::Ok
                        }
                        Err(e) => ScheduleResponse::Error(e),
                    }
                }
                ScheduleMessage::Cancel { route } => {
                    let route_for_admin = route.clone();
                    match actor.delete_schedule(route) {
                        Ok(removed) => {
                            if removed {
                                if let Some((realm, area, resource)) =
                                    parse_route_triplet(&route_for_admin)
                                {
                                    admin_update = Some(ScheduleAdminUpdate::Remove {
                                        realm,
                                        area,
                                        resource,
                                    });
                                }
                            }
                            ScheduleResponse::Ok
                        }
                        Err(e) => ScheduleResponse::Error(e),
                    }
                }
                ScheduleMessage::List { offset, limit } => {
                    let (entries, total_count) = actor.list_entries(offset, limit);

                    ScheduleResponse::ListDefs {
                        entries,
                        total_count,
                    }
                }
                ScheduleMessage::Subscribe {
                    family_id,
                    pattern,
                    session_id,
                    subscriber,
                } => {
                    let fam_id = family_id.as_u64();

                    let mut families = self.sub_families.lock();
                    let state = families
                        .entry(fam_id)
                        .or_insert_with(|| ScheduleFamilyState {
                            subscriptions: Vec::new(),
                        });

                    // Idempotent: if (session_id, pattern) already exists, return existing subscription_id
                    let existing_sub_id = state
                        .subscriptions
                        .iter()
                        .find(|s| {
                            s.session_id == session_id && s.pattern.route() == pattern.as_str()
                        })
                        .map(|s| s.subscription_id);

                    let _sub_id = if let Some(id) = existing_sub_id {
                        tracing::debug!(
                            domain = "schedule",
                            session = session_id,
                            subscription_id = id,
                            pattern = pattern.as_str(),
                            "Schedule subscription already exists (idempotent)"
                        );
                        id
                    } else {
                        let new_id = self.next_sub_id.fetch_add(1, Ordering::Relaxed);
                        let pat = crate::runtime::matcher::Pattern::new(pattern.as_str());

                        state.subscriptions.push(ScheduleSubscription {
                            pattern: pat,
                            session_id,
                            subscription_id: new_id,
                            subscriber,
                        });

                        tracing::debug!(
                            domain = "schedule",
                            session = session_id,
                            subscription_id = new_id,
                            pattern = pattern.as_str(),
                            "Schedule subscription added"
                        );
                        new_id
                    };

                    ScheduleResponse::Ok
                }
                ScheduleMessage::Unsubscribe {
                    family_id,
                    pattern,
                    session_id,
                    ..
                } => {
                    let fam_id = family_id.as_u64();
                    let mut families = self.sub_families.lock();
                    if let Some(state) = families.get_mut(&fam_id) {
                        state.subscriptions.retain(|s| {
                            !(s.session_id == session_id && s.pattern.route() == pattern.as_str())
                        });
                    }
                    ScheduleResponse::Ok
                }
                ScheduleMessage::UnsubscribeAll { session_id, .. } => {
                    self.unsubscribe_all(session_id);
                    ScheduleResponse::Ok
                }
            }
        };

        if let Some(update) = admin_update {
            match update {
                ScheduleAdminUpdate::Upsert {
                    realm,
                    area,
                    resource,
                    cron,
                } => {
                    self.admin_read_model
                        .upsert_schedule_fields(realm, area, resource, cron);
                }
                ScheduleAdminUpdate::Remove {
                    realm,
                    area,
                    resource,
                } => {
                    self.admin_read_model
                        .remove_schedule(&realm, &area, &resource);
                }
            }
        }

        // Encode and route response back
        let response_bytes =
            crate::protocol::schedule_codec::encode_response_into(&mut payload_encoder, &response);
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

// ═══════════════════════════════════════════════════════════════════════════
// DOMAIN SETUP
// ═══════════════════════════════════════════════════════════════════════════

#[derive(Clone)]
pub struct DomainHandles {
    pub kv: Arc<KvDomainSink>,
    pub queue: Arc<QueueDomainSink>,
    pub notice: Arc<NoticeDomainSink>,
    pub stream: Arc<StreamDomainSink>,
    pub rpc: Arc<RpcDomainSink>,
    pub lease: Arc<LeaseDomainSink>,
    pub schedule: Arc<ScheduleDomainSink>,
}

/// Set up all 7 domain actors and register them with the router
///
/// Register all domain handlers with the router
///
/// # Architecture
///
/// Domains are registered **globally** with route pattern matching, NOT per route family.
///
/// - **Route Family** = Realm/isolation boundary (e.g., realm_acme = family 100)
/// - **Domain** = Handler identified by route scheme (e.g., "kv://", "queue://", "notice://")
/// - **Realm** = Logical boundary within the route string (part of the path)
///
/// # Example
///
/// ```text
/// // Realm ACME (family 100) sends KV request
/// RouteAddress::new(RouteFamily::new(100), Route::new("kv://acme/app/users/get"))
/// → Routes to KV domain, isolated to family 100
///
/// // Realm XYZ (family 200) sends KV request
/// RouteAddress::new(RouteFamily::new(200), Route::new("kv://xyz/app/users/get"))
/// → Routes to same KV domain, isolated to family 200
/// ```
///
/// The router matches on the route pattern (domain scheme) and enforces isolation
/// via the route family. Domains operate on all families but see isolated state.
///
/// # Route Family Assignment
///
/// Route families are **NOT assigned by this function**. They are:
/// - Dynamically allocated by the control plane per realm
/// - Passed in client requests (part of the RouteAddress)
/// - Enforced by the storage layer (aligned with Midge ColumnFamilyId)
pub fn setup(
    router: &StdArc<Router>,
    store: &StdArc<cntryl_midge::Engine>,
    admin_read_model: &Arc<crate::api::admin::read_model::AdminReadModel>,
) -> BootResult<DomainHandles> {
    // Register domain handlers globally using wildcard route family (matches all)
    // Each domain matches its route scheme pattern across ALL route families

    // KV domain: Handles all "kv://*" routes across all families
    let kv_sink = Arc::new(KvDomainSink::new(
        store.clone(),
        router.clone(),
        admin_read_model.clone(),
    ));
    router.register_domain_pattern("kv", kv_sink.clone() as Arc<dyn MailboxSink>);
    tracing::info!("Registered KV domain (handles kv://* across all route families)");

    // Queue domain: Handles all "queue://*" routes across all families
    let queue_sink = Arc::new(QueueDomainSink::new(
        store.clone(),
        router.clone(),
        admin_read_model.clone(),
    ));
    router.register_domain_pattern("queue", queue_sink.clone() as Arc<dyn MailboxSink>);
    tracing::info!("Registered Queue domain (handles queue://* across all route families)");

    // Notice domain: Handles all "notice://*" routes across all families
    let notice_sink = Arc::new(NoticeDomainSink::new(
        router.clone(),
        admin_read_model.clone(),
    ));
    router.register_domain_pattern("notice", notice_sink.clone() as Arc<dyn MailboxSink>);
    tracing::info!("Registered Notice domain (handles notice://* across all route families)");

    // Stream domain: Handles all "stream://*" routes across all families
    let stream_sink = Arc::new(StreamDomainSink::new(
        store.clone(),
        router.clone(),
        admin_read_model.clone(),
    ));
    router.register_domain_pattern("stream", stream_sink.clone() as Arc<dyn MailboxSink>);
    tracing::info!("Registered Stream domain (handles stream://* across all route families)");

    // RPC domain: Handles all "rpc://*" routes across all families
    let rpc_sink = Arc::new(RpcDomainSink::new(router.clone(), admin_read_model.clone()));
    router.register_domain_pattern("rpc", rpc_sink.clone() as Arc<dyn MailboxSink>);
    tracing::info!("Registered RPC domain (handles rpc://* across all route families)");

    // Lease domain: Handles all "lease://*" routes across all families
    let lease_sink = Arc::new(LeaseDomainSink::new(
        router.clone(),
        admin_read_model.clone(),
    ));
    router.register_domain_pattern("lease", lease_sink.clone() as Arc<dyn MailboxSink>);
    tracing::info!("Registered Lease domain (handles lease://* across all route families)");

    // Schedule domain: Handles all "schedule://*" routes across all families
    let schedule_sink = Arc::new(ScheduleDomainSink::new(
        store.clone(),
        router.clone(),
        admin_read_model.clone(),
    ));
    router.register_domain_pattern("schedule", schedule_sink.clone() as Arc<dyn MailboxSink>);
    tracing::info!("Registered Schedule domain (handles schedule://* across all route families)");

    tracing::info!("All 7 domain sinks registered with router");

    Ok(DomainHandles {
        kv: kv_sink,
        queue: queue_sink,
        notice: notice_sink,
        stream: stream_sink,
        rpc: rpc_sink,
        lease: lease_sink,
        schedule: schedule_sink,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_define_domain_setup() {
        // Placeholder: Domain setup structure is well-defined
    }

    #[test]
    fn should_create_domain_sinks() {
        let kv_sink = DomainSink::new("kv");
        let notice_sink = DomainSink::new("notice");

        // Both should be active initially
        assert!(kv_sink.active.load(Ordering::Relaxed));
        assert!(notice_sink.active.load(Ordering::Relaxed));

        // Stopping should work
        kv_sink.stop();
        assert!(!kv_sink.active.load(Ordering::Relaxed));
    }

    #[test]
    fn should_create_kv_domain_sink() {
        // Arrange
        let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
        let router = Arc::new(Router::new());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();

        // Act
        let kv_sink = KvDomainSink::new(store, router, admin_read_model);

        // Assert
        assert!(kv_sink.active.load(Ordering::Relaxed));
    }

    #[test]
    fn should_handle_delivery_when_active() {
        // Arrange
        let sink = DomainSink::new("kv");
        let address = RouteAddress::new(RouteFamily::new(1), Route::new("kv"));
        let envelope = Envelope::new(address, vec![0u8; 10]);

        // Act
        let result = sink.deliver(envelope);

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_reject_delivery_when_stopped() {
        // Arrange
        let sink = DomainSink::new("kv");
        sink.stop();

        let address = RouteAddress::new(RouteFamily::new(1), Route::new("kv"));
        let envelope = Envelope::new(address, vec![0u8; 10]);

        // Act
        let result = sink.deliver(envelope);

        // Assert
        assert!(matches!(result, Err(DeliveryError::ActorStopped)));
    }

    #[test]
    fn should_handle_high_priority_delivery() {
        // Arrange
        let sink = DomainSink::new("kv");
        let address = RouteAddress::new(RouteFamily::new(1), Route::new("kv"));
        let envelope = Envelope::new(address, vec![0u8; 10]);

        // Act
        let result = sink.deliver_high_priority(envelope);

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_setup_all_seven_domains() {
        // Arrange - Create test engine with all 7 domain column families
        let store = crate::testkit::midge::create_test_engine_with_cfs(vec![1, 2, 3, 4, 5, 6, 7]);
        let router = Arc::new(Router::new());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();

        // Act
        let result = setup(&router, &store, &admin_read_model);

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_create_notice_domain_sink() {
        // Arrange
        let router = Arc::new(Router::new());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();

        // Act
        let sink = NoticeDomainSink::new(router, admin_read_model);

        // Assert
        assert!(sink.active.load(Ordering::Relaxed));
    }

    #[test]
    fn should_create_rpc_domain_sink() {
        // Arrange
        let router = Arc::new(Router::new());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();

        // Act
        let sink = RpcDomainSink::new(router, admin_read_model);

        // Assert
        assert!(sink.active.load(Ordering::Relaxed));
    }

    #[test]
    fn should_create_lease_domain_sink() {
        // Arrange
        let router = Arc::new(Router::new());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();

        // Act
        let sink = LeaseDomainSink::new(router, admin_read_model);

        // Assert
        assert!(sink.active.load(Ordering::Relaxed));
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
    fn should_create_schedule_domain_sink() {
        // Arrange
        let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
        let router = Arc::new(Router::new());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();

        // Act
        let sink = ScheduleDomainSink::new(store, router, admin_read_model);

        // Assert
        assert!(sink.active.load(Ordering::Relaxed));
    }

    #[test]
    fn should_create_queue_domain_sink() {
        // Arrange
        let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
        let router = Arc::new(Router::new());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();

        // Act
        let sink = QueueDomainSink::new(store, router, admin_read_model);

        // Assert
        assert!(sink.active.load(Ordering::Relaxed));
    }

    /// Integration test: Subscribe then Send must result in QUEUE_NOTIFY (209) delivered to inbox.
    #[test]
    fn should_fan_out_queue_notify_209_after_send_when_subscribed() {
        use crate::protocol::frame::ChannelId;
        use crate::protocol::tlv::MessageType;
        use bytes::Bytes;
        use std::sync::Mutex;

        use super::queue_notify_test_helpers::CaptureFrameContextSink;

        let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
        let router = Arc::new(Router::new());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let queue_sink = Arc::new(QueueDomainSink::new(
            store.clone(),
            router.clone(),
            admin_read_model,
        ));

        // Capture sink: records msg_type of each FrameContext delivered to inbox
        let received: Arc<Mutex<Vec<u16>>> = Arc::new(Mutex::new(Vec::new()));
        let received_clone = received.clone();
        let capture_sink = Arc::new(CaptureFrameContextSink {
            msg_types: received_clone,
        });

        let family = RouteFamily::new(1);
        let inbox_addr = RouteAddress::new(family, Route::new("inbox://session/1"));
        let queue_inbound_addr = RouteAddress::new(family, Route::new("queue://inbound"));

        router.register(
            inbox_addr.clone(),
            capture_sink as Arc<dyn crate::runtime::router::MailboxSink>,
        );
        router.register_domain_pattern(
            "queue",
            queue_sink as Arc<dyn crate::runtime::router::MailboxSink>,
        );

        // 1) Subscribe (207): pattern "queue://realm/area/resource"
        let pattern = "queue://realm/area/resource";
        let mut sub_payload = Vec::new();
        sub_payload.extend_from_slice(&(pattern.len() as u32).to_be_bytes());
        sub_payload.extend_from_slice(pattern.as_bytes());

        let sub_ctx = FrameContext::new(
            1,
            ChannelId::Pub,
            MessageType::new(207),
            Bytes::from(sub_payload),
            family,
        );
        let sub_env = Envelope::from_route(inbox_addr.clone(), queue_inbound_addr.clone(), sub_ctx);
        router.route(sub_env).expect("route subscribe");

        // 2) Send (200): same route, body "x"
        let route = "queue://realm/area/resource";
        let body: &[u8] = b"x";
        let mut send_payload = Vec::new();
        send_payload.extend_from_slice(&(route.len() as u32).to_be_bytes());
        send_payload.extend_from_slice(route.as_bytes());
        send_payload.extend_from_slice(&(body.len() as u32).to_be_bytes());
        send_payload.extend_from_slice(body);

        let send_ctx = FrameContext::new(
            1,
            ChannelId::Pub,
            MessageType::new(200),
            Bytes::from(send_payload),
            family,
        );
        let send_env =
            Envelope::from_route(inbox_addr.clone(), queue_inbound_addr.clone(), send_ctx);
        router.route(send_env).expect("route send");

        // 3) Assert inbox received 209 (QUEUE_NOTIFY)
        let msg_types = received.lock().unwrap();
        assert!(
            msg_types.contains(&209),
            "expected inbox to receive msg_type 209 (QUEUE_NOTIFY), got {:?}",
            *msg_types
        );
    }
}

#[cfg(test)]
mod queue_notify_test_helpers {
    use super::*;
    use std::sync::Mutex;

    /// Sink that records msg_type of each FrameContext (used by queue notify test).
    pub(super) struct CaptureFrameContextSink {
        pub msg_types: Arc<Mutex<Vec<u16>>>,
    }

    impl MailboxSink for CaptureFrameContextSink {
        fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
            if let Some(ctx) = envelope.payload::<FrameContext>() {
                self.msg_types.lock().unwrap().push(ctx.msg_type.as_u16());
            }
            Ok(())
        }

        fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
            self.deliver(envelope)
        }
    }
}
