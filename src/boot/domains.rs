//! Domain actor setup and registration

use crate::boot::runtime::BootResult;
use crate::protocol::frame_context::FrameContext;
use crate::runtime::{DeliveryError, Envelope, MailboxSink, Router};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::Arc as StdArc;

#[cfg(test)]
use crate::runtime::routing::{Route, RouteAddress, RouteFamily};

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
/// Key for pessimistic resource lock: (route_family_id, resource_key)
type KvResourceLockKey = (u64, String);

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
    active: AtomicBool,
}

impl KvDomainSink {
    pub fn new(store: Arc<cntryl_midge::Engine>, router: Arc<Router>) -> Self {
        Self {
            store,
            actors: Arc::new(Mutex::new(HashMap::new())),
            resource_locks: Mutex::new(HashMap::new()),
            tx_to_resource: Mutex::new(HashMap::new()),
            router,
            active: AtomicBool::new(true),
        }
    }

    pub fn stop(&self) {
        self.active.store(false, Ordering::Relaxed);
    }
}

impl MailboxSink for KvDomainSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        if !self.active.load(Ordering::Relaxed) {
            return Err(DeliveryError::ActorStopped);
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

        let route_addr = envelope.destination();
        let route_family = route_addr.family();

        // Parse TLV frame using codec
        // Per CLIENT_SPEC: All KV operations now include full route on wire
        let kv_message = match crate::protocol::kv::parse_request(
            frame_ctx.msg_type.as_u16(),
            *route_family,
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

        let response = match &kv_message {
            KvMessage::Begin {
                route_family,
                realm,
                area,
                resource,
                mode,
                ..
            } if *mode == TxMode::ReadWrite => {
                let resource_key = format!("{}/{}/{}", realm, area, resource);
                let lock_key: KvResourceLockKey = (route_family.as_u64(), resource_key.clone());
                {
                    let locks = self.resource_locks.lock();
                    if let Some(&holder) = locks.get(&lock_key) {
                        if holder != session_id {
                            drop(locks);
                            KvResponse::Error {
                                error: KvError::Conflict(
                                    "resource locked by another session".to_string(),
                                ),
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
                            }
                            resp
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
                        }
                        resp
                    }
                }
            }
            KvMessage::Commit { tx_id } => {
                let mut actors = self.actors.lock();
                let actor = actors
                    .entry(session_id)
                    .or_insert_with(|| {
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
                }
                resp
            }
            KvMessage::Rollback { tx_id } => {
                let mut actors = self.actors.lock();
                let actor = actors
                    .entry(session_id)
                    .or_insert_with(|| {
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
                }
                resp
            }
            _ => {
                let mut actors = self.actors.lock();
                let actor = actors
                    .entry(session_id)
                    .or_insert_with(|| {
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
                actor.handle(kv_message)
            }
        };

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
    active: AtomicBool,
}

impl NoticeDomainSink {
    pub fn new(router: Arc<Router>) -> Self {
        Self {
            families: Mutex::new(HashMap::new()),
            next_sub_id: AtomicU64::new(1),
            router,
            active: AtomicBool::new(true),
        }
    }

    pub fn stop(&self) {
        self.active.store(false, Ordering::Relaxed);
    }
}

impl NoticeDomainSink {
    /// Handle a DomainPublishEvent from another domain (e.g. Schedule target_resource execution).
    /// Matches the event route against notice subscription patterns and fans out
    /// NOTICE NOTIFY (504) frames to matching subscribers.
    fn handle_domain_publish(
        &self,
        event: &crate::runtime::DomainPublishEvent,
    ) -> Result<(), DeliveryError> {
        let family_id = event.family_id.as_u64();
        let families = self.families.lock();
        if let Some(state) = families.get(&family_id) {
            for sub in &state.subscriptions {
                if sub.pattern.matches(&event.route) {
                    let notify_payload = crate::protocol::notice_codec::encode_notify(
                        sub.subscription_id,
                        &event.route,
                        &event.payload,
                    );
                    let notify_ctx = FrameContext::new(
                        sub.session_id,
                        crate::protocol::frame::ChannelId::Sub, // notification channel
                        crate::protocol::tlv::MessageType::new(504), // NOTICE NOTIFY
                        bytes::Bytes::from(notify_payload),
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
            envelope
                .source()
                .cloned()
                .unwrap_or_else(|| envelope.destination().clone()),
        ) {
            Ok(msg) => msg,
            Err(e) => {
                tracing::warn!(domain = "notice", error = %e, "Failed to parse notice message");
                return Err(DeliveryError::ActorStopped);
            }
        };

        use crate::domains::notice::protocol::NotificationMessage;
        use crate::protocol::notice_codec::NoticeResponse;

        let response_opt = match notice_msg {
            NotificationMessage::Publish(pub_msg) => {
                // PUBLISH is fire-and-forget per CLIENT_SPEC: no response, just fanout and return
                let family_id = pub_msg.family_id.as_u64();
                let families = self.families.lock();
                if let Some(state) = families.get(&family_id) {
                    let route = pub_msg.route.clone();

                    for sub in &state.subscriptions {
                        if sub.pattern.matches(&route) {
                            let notify_payload = crate::protocol::notice_codec::encode_notify(
                                sub.subscription_id,
                                &route,
                                &pub_msg.payload,
                            );
                            let notify_ctx = FrameContext::new(
                                sub.session_id,
                                crate::protocol::frame::ChannelId::Sub,
                                crate::protocol::tlv::MessageType::new(504), // NOTICE NOTIFY
                                bytes::Bytes::from(notify_payload),
                            );
                            let notify_envelope = Envelope::new(sub.subscriber.clone(), notify_ctx);
                            let _ = self.router.route(notify_envelope);
                        }
                    }
                }
                // Return None to indicate no response should be sent (fire-and-forget)
                None
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
                    let pattern = crate::runtime::matcher::Pattern::new(sub_msg.pattern.as_str());

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

                Some(NoticeResponse::Ok {
                    subscription_id: Some(sub_id),
                })
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
                Some(NoticeResponse::Ok {
                    subscription_id: None,
                })
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
                Some(NoticeResponse::Ok {
                    subscription_id: None,
                })
            }
            NotificationMessage::Notify(_) => {
                // Notify is internal delivery, no response needed
                Some(NoticeResponse::Ok {
                    subscription_id: None,
                })
            }
        };

        // Only send response if one was generated (PUBLISH returns None for fire-and-forget)
        if let Some(response) = response_opt {
            let response_bytes = crate::protocol::notice_codec::encode_response(&response);
            let response_ctx = FrameContext::new(
                frame_ctx.session_id,
                frame_ctx.channel_id,
                crate::protocol::tlv::MessageType::new(frame_ctx.msg_type.as_u16()),
                bytes::Bytes::from(response_bytes),
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
    active: AtomicBool,
}

impl RpcDomainSink {
    pub fn new(router: Arc<Router>) -> Self {
        Self {
            state: Mutex::new(RpcState {
                workers: HashMap::new(),
                rr_index: HashMap::new(),
                pending: HashMap::new(),
            }),
            router,
            active: AtomicBool::new(true),
        }
    }

    pub fn stop(&self) {
        self.active.store(false, Ordering::Relaxed);
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

        let response = match rpc_msg {
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
                RpcResponseMsg::Ok { data: vec![] }
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
                RpcResponseMsg::Ok { data: vec![] }
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

                    let worker = &state.workers[&route_key][pick];
                    let worker_inbox_addr = crate::runtime::routing::RouteAddress::new(
                        worker.route_family,
                        crate::runtime::routing::Route::new(format!(
                            "inbox://session/{}",
                            worker.session_id
                        )),
                    );

                    // Store pending: caller session_id and family for response routing to caller inbox
                    state.pending.insert(
                        req.correlation_id,
                        (frame_ctx.session_id, *envelope.destination().family()),
                    );

                    // Drop state lock before routing
                    drop(state);

                    // Encode REQUEST delivery for worker (similar to Notice NOTIFY encoding)
                    let work_item = crate::domains::rpc::protocol::RpcWorkItem::from_request(&req);
                    let request_payload =
                        crate::protocol::rpc_codec::encode_request_delivery(&work_item);

                    // Forward request to worker's session inbox (avoids RPC domain re-entry / stack overflow)
                    let forward_ctx = FrameContext::new(
                        frame_ctx.session_id,
                        frame_ctx.channel_id,
                        crate::protocol::tlv::MessageType::new(302), // Request msg_type
                        bytes::Bytes::from(request_payload),
                    );
                    let forward_envelope = Envelope::new(worker_inbox_addr, forward_ctx);
                    let _ = self.router.route(forward_envelope);

                    tracing::debug!(
                        domain = "rpc",
                        correlation_id = %req.correlation_id,
                        route = route_key,
                        "Request forwarded to worker"
                    );
                    RpcResponseMsg::Ok { data: vec![] }
                } else {
                    RpcResponseMsg::Error("No workers registered for route".to_string())
                }
            }
            RpcMessage::Response(resp) => {
                let mut state = self.state.lock();
                // Keep pending until stream_end so streaming responses (multiple 303 chunks) all get forwarded.
                let caller_info = state.pending.get(&resp.correlation_id).copied();
                if let Some((caller_session_id, caller_family_id)) = caller_info {
                    if resp.stream_end {
                        state.pending.remove(&resp.correlation_id);
                    }
                    drop(state);

                    // Forward response to caller's session inbox (avoids RPC domain re-entry)
                    let caller_inbox_addr = crate::runtime::routing::RouteAddress::new(
                        caller_family_id,
                        crate::runtime::routing::Route::new(format!(
                            "inbox://session/{}",
                            caller_session_id
                        )),
                    );
                    let forward_ctx = FrameContext::new(
                        frame_ctx.session_id,
                        frame_ctx.channel_id,
                        crate::protocol::tlv::MessageType::new(303), // Response msg_type
                        frame_ctx.payload.clone(),
                    );
                    let forward_envelope = Envelope::new(caller_inbox_addr, forward_ctx);
                    let _ = self.router.route(forward_envelope);

                    tracing::debug!(
                        domain = "rpc",
                        correlation_id = %resp.correlation_id,
                        stream_end = resp.stream_end,
                        "Response forwarded to requester"
                    );
                } else {
                    tracing::warn!(
                        domain = "rpc",
                        correlation_id = %resp.correlation_id,
                        "No pending request for response"
                    );
                }
                RpcResponseMsg::Ok { data: vec![] }
            }
            RpcMessage::Ack { correlation_id } => {
                let mut state = self.state.lock();
                state.pending.remove(&correlation_id);
                tracing::debug!(
                    domain = "rpc",
                    correlation_id = %correlation_id,
                    "Request acknowledged and cleaned up"
                );
                RpcResponseMsg::Ok { data: vec![] }
            }
            RpcMessage::RequestDelivery(_) => {
                // RequestDelivery should only be sent TO workers by route actors,
                // not received from clients. Ignore or error.
                RpcResponseMsg::Error("RequestDelivery not valid client message".to_string())
            }
        };

        // Encode and route response back
        let response_bytes = crate::protocol::rpc_codec::encode_response(&response);
        let response_ctx = FrameContext::new(
            frame_ctx.session_id,
            frame_ctx.channel_id,
            crate::protocol::tlv::MessageType::new(frame_ctx.msg_type.as_u16()),
            bytes::Bytes::from(response_bytes),
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
// QUEUE DOMAIN SINK
// ═══════════════════════════════════════════════════════════════════════════

/// Queue domain sink with per-queue QueueActor instances
///
/// This sink:
/// - Maintains per-queue QueueActor instances keyed by QueueKey
/// - Parses TLV frames to QueueMessage
/// - Dispatches to the correct actor based on route
/// - Returns responses
pub struct QueueDomainSink {
    /// Midge storage engine
    store: Arc<cntryl_midge::Engine>,
    /// Per-queue actors keyed by QueueKey
    actors: Mutex<HashMap<crate::domains::queue::QueueKey, crate::domains::queue::QueueActor>>,
    /// Router for routing response envelopes back
    router: Arc<Router>,
    active: AtomicBool,
}

impl QueueDomainSink {
    pub fn new(store: Arc<cntryl_midge::Engine>, router: Arc<Router>) -> Self {
        Self {
            store,
            actors: Mutex::new(HashMap::new()),
            router,
            active: AtomicBool::new(true),
        }
    }

    pub fn stop(&self) {
        self.active.store(false, Ordering::Relaxed);
    }
}

impl MailboxSink for QueueDomainSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        if !self.active.load(Ordering::Relaxed) {
            return Err(DeliveryError::ActorStopped);
        }

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

        // Parse queue message using codec
        // Per CLIENT_SPEC: All Queue operations now include full route on wire
        let queue_msg = match crate::protocol::queue_codec::parse_request(
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
        };

        tracing::debug!(
            domain = "queue",
            session = frame_ctx.session_id,
            msg_type = frame_ctx.msg_type.as_u16(),
            "Parsed Queue message successfully"
        );

        // Dispatch to per-queue actor
        use crate::domains::queue::protocol::{QueueKey, QueueMessage};

        let response = {
            let mut actors = self.actors.lock();

            match queue_msg {
                QueueMessage::Enqueue {
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
                    let actor = actors.entry(key.clone()).or_insert_with(|| {
                        crate::domains::queue::QueueActor::new(
                            family_id,
                            key.clone(),
                            store,
                            None,
                            crate::utils::idempotency::global_dedup_store(),
                        )
                    });
                    actor.process_expired_timers();
                    actor.process_delayed_messages();
                    actor.handle_enqueue(body, delay_seconds)
                }
                QueueMessage::Reserve {
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
                    let actor = actors.entry(key.clone()).or_insert_with(|| {
                        crate::domains::queue::QueueActor::new(
                            family_id,
                            key.clone(),
                            store,
                            None,
                            crate::utils::idempotency::global_dedup_store(),
                        )
                    });
                    actor.process_expired_timers();
                    actor.process_delayed_messages();
                    actor.handle_reserve(lease_seconds, batch_size)
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
                    let actor = actors.entry(key.clone()).or_insert_with(|| {
                        crate::domains::queue::QueueActor::new(
                            family_id,
                            key.clone(),
                            store,
                            None,
                            crate::utils::idempotency::global_dedup_store(),
                        )
                    });
                    actor.process_expired_timers();
                    actor.process_delayed_messages();
                    actor.handle_extend(id, token, lease_seconds)
                }
                QueueMessage::Complete {
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
                    let actor = actors.entry(key.clone()).or_insert_with(|| {
                        crate::domains::queue::QueueActor::new(
                            family_id,
                            key.clone(),
                            store,
                            None,
                            crate::utils::idempotency::global_dedup_store(),
                        )
                    });
                    actor.process_expired_timers();
                    actor.process_delayed_messages();
                    actor.handle_complete(id, token)
                }
                QueueMessage::LeaseExpired { .. } => {
                    // Internal message, not dispatched via sink
                    crate::domains::queue::QueueResponse::Error {
                        message: "LeaseExpired is an internal message".to_string(),
                    }
                }
            }
        };

        // Encode and route response back
        let response_bytes = crate::protocol::queue_codec::encode_response(&response);
        let response_ctx = FrameContext::new(
            frame_ctx.session_id,
            frame_ctx.channel_id,
            crate::protocol::tlv::MessageType::new(frame_ctx.msg_type.as_u16()),
            bytes::Bytes::from(response_bytes),
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
struct StreamWriteState {
    next_offset_by_route: HashMap<String, u64>,
    session_to_route: HashMap<u64, String>,
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
    active: AtomicBool,
}

impl StreamDomainSink {
    pub fn new(store: Arc<cntryl_midge::Engine>, router: Arc<Router>) -> Self {
        Self {
            store,
            next_session_id: AtomicU64::new(1),
            write_state: Mutex::new(StreamWriteState {
                next_offset_by_route: HashMap::new(),
                session_to_route: HashMap::new(),
            }),
            families: Mutex::new(HashMap::new()),
            next_sub_id: AtomicU64::new(1),
            router,
            active: AtomicBool::new(true),
        }
    }

    pub fn stop(&self) {
        self.active.store(false, Ordering::Relaxed);
    }

    /// Handle a DomainPublishEvent from stream actors.
    /// Matches the event route against subscription patterns and fans out
    /// STREAM_NOTIFY (609) frames to matching subscribers.
    fn handle_domain_publish(
        &self,
        event: &crate::runtime::DomainPublishEvent,
    ) -> Result<(), DeliveryError> {
        let family_id = event.family_id.as_u64();
        let families = self.families.lock();
        if let Some(state) = families.get(&family_id) {
            for sub in &state.subscriptions {
                if sub.pattern.matches(&event.route) {
                    let notify_payload = crate::protocol::stream_codec::encode_notify(
                        sub.subscription_id,
                        &event.route,
                        &event.payload,
                    );
                    let notify_ctx = FrameContext::new(
                        sub.session_id,
                        crate::protocol::frame::ChannelId::Sub, // notification channel
                        crate::protocol::tlv::MessageType::new(609), // STREAM_NOTIFY
                        bytes::Bytes::from(notify_payload),
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

        let stream_msg = match crate::protocol::stream_codec::parse_request(
            &frame_ctx,
            &frame_ctx.payload,
            *envelope.destination().family(),
            crate::session::SessionId(frame_ctx.session_id),
            envelope
                .source()
                .cloned()
                .unwrap_or_else(|| envelope.destination().clone()),
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

        // Enforce expected_offset at Begin; track session->route for Commit (advance next_offset).
        let response = match stream_msg {
            StreamMessage::Begin {
                route,
                expected_offset,
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
                        let response_bytes = crate::protocol::stream_codec::encode_response(
                            &StreamResponse::Error("concurrency conflict".to_string()),
                        );
                        let response_ctx = FrameContext::new(
                            frame_ctx.session_id,
                            frame_ctx.channel_id,
                            crate::protocol::tlv::MessageType::new(frame_ctx.msg_type.as_u16()),
                            bytes::Bytes::from(response_bytes),
                        );
                        if let Some(response_envelope) = envelope.try_reply_to(response_ctx) {
                            let _ = self.router.route(response_envelope);
                        }
                        Ok(())
                    };
                }
                let session_id = self.next_session_id.fetch_add(1, Ordering::Relaxed);
                state.session_to_route.insert(session_id, route_key);
                StreamResponse::Ok {
                    session_id: Some(session_id),
                    data: vec![],
                }
            }
            StreamMessage::Append { .. } => StreamResponse::Ok {
                session_id: None,
                data: vec![],
            },
            StreamMessage::Commit { session_id, .. } => {
                let mut state = self.write_state.lock();
                if let Some(route_key) = state.session_to_route.remove(&session_id) {
                    state
                        .next_offset_by_route
                        .entry(route_key)
                        .and_modify(|n| *n += 1)
                        .or_insert(1);
                }
                StreamResponse::Ok {
                    session_id: None,
                    data: vec![],
                }
            }
            StreamMessage::Rollback { session_id, .. } => {
                self.write_state.lock().session_to_route.remove(&session_id);
                StreamResponse::Ok {
                    session_id: None,
                    data: vec![],
                }
            }
            StreamMessage::Read { .. } => StreamResponse::Ok {
                session_id: None,
                data: vec![],
            },
            StreamMessage::Last { .. } => StreamResponse::Ok {
                session_id: None,
                data: vec![],
            },
            StreamMessage::GetMetadata { .. } => StreamResponse::Ok {
                session_id: None,
                data: vec![],
            },
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

                StreamResponse::Ok {
                    session_id: Some(sub_id),
                    data: vec![],
                }
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
                StreamResponse::Ok {
                    session_id: None,
                    data: vec![],
                }
            }
            StreamMessage::UnsubscribeAll { session_id, .. } => {
                self.unsubscribe_all(session_id);
                StreamResponse::Ok {
                    session_id: None,
                    data: vec![],
                }
            }
            _ => {
                // Internal messages (RequestLease, LeaseGranted, etc.)
                StreamResponse::Ok {
                    session_id: None,
                    data: vec![],
                }
            }
        };

        // Encode and route response back
        let response_bytes = crate::protocol::stream_codec::encode_response(&response);
        let response_ctx = FrameContext::new(
            frame_ctx.session_id,
            frame_ctx.channel_id,
            crate::protocol::tlv::MessageType::new(frame_ctx.msg_type.as_u16()),
            bytes::Bytes::from(response_bytes),
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
}

impl LeaseDomainSink {
    pub fn new(router: Arc<Router>) -> Self {
        Self {
            leases: Mutex::new(HashMap::new()),
            next_token: AtomicU64::new(1),
            router,
            active: AtomicBool::new(true),
        }
    }

    pub fn stop(&self) {
        self.active.store(false, Ordering::Relaxed);
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

    fn handle_renew(
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
                    state.expiry = now + ttl;
                    LeaseResponse::Renewed { fencing_token }
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

        // When owner_id is empty, use session_id so different sessions are distinct (enforces exclusivity)
        let effective_owner = |owner_id: String| {
            if owner_id.is_empty() {
                format!("session:{}", frame_ctx.session_id)
            } else {
                owner_id
            }
        };

        // Dispatch to lease handlers and get domain response
        let domain_response = match lease_msg {
            LeaseMessage::Acquire {
                family_id,
                route,
                owner_id,
                ttl_secs,
            } => match LeaseKey::from_route(family_id, &route) {
                Some(key) => self.handle_acquire(key, effective_owner(owner_id), ttl_secs),
                None => LeaseResponse::NotFound,
            },
            LeaseMessage::Renew {
                family_id,
                route,
                owner_id,
                fencing_token,
                ttl_secs,
            } => match LeaseKey::from_route(family_id, &route) {
                Some(key) => {
                    self.handle_renew(key, effective_owner(owner_id), fencing_token, ttl_secs)
                }
                None => LeaseResponse::NotFound,
            },
            LeaseMessage::Release {
                family_id,
                route,
                owner_id,
                fencing_token,
            } => match LeaseKey::from_route(family_id, &route) {
                Some(key) => self.handle_release(key, effective_owner(owner_id), fencing_token),
                None => LeaseResponse::NotFound,
            },
            LeaseMessage::Query { family_id, route } => {
                match LeaseKey::from_route(family_id, &route) {
                    Some(key) => self.handle_query(key),
                    None => LeaseResponse::NotFound,
                }
            }
            LeaseMessage::Tick => {
                // Proactively expire old leases
                let now = std::time::Instant::now();
                let mut leases = self.leases.lock();
                leases.retain(|_, state| state.expiry > now);
                return Ok(());
            }
        };

        // Convert domain LeaseResponse to codec LeaseResponse for encoding
        let codec_response = match domain_response {
            LeaseResponse::Acquired { fencing_token } => {
                crate::protocol::lease_codec::LeaseResponse::Ok {
                    token: Some(fencing_token),
                }
            }
            LeaseResponse::AlreadyHeld { fencing_token } => {
                crate::protocol::lease_codec::LeaseResponse::Ok {
                    token: Some(fencing_token),
                }
            }
            LeaseResponse::Renewed { fencing_token } => {
                crate::protocol::lease_codec::LeaseResponse::Ok {
                    token: Some(fencing_token),
                }
            }
            LeaseResponse::Released => {
                crate::protocol::lease_codec::LeaseResponse::Ok { token: None }
            }
            LeaseResponse::HeldByOther { current_owner } => {
                crate::protocol::lease_codec::LeaseResponse::Error(format!(
                    "Held by: {}",
                    current_owner
                ))
            }
            LeaseResponse::NotHeld => {
                crate::protocol::lease_codec::LeaseResponse::Error("Not held".to_string())
            }
            LeaseResponse::Fenced { current_token } => {
                crate::protocol::lease_codec::LeaseResponse::Error(format!(
                    "Fenced: current token {}",
                    current_token
                ))
            }
            LeaseResponse::Expired => {
                crate::protocol::lease_codec::LeaseResponse::Error("Expired".to_string())
            }
            LeaseResponse::NotFound => {
                crate::protocol::lease_codec::LeaseResponse::Error("Not found".to_string())
            }
            LeaseResponse::Status {
                owner_id: _,
                fencing_token,
                expires_in_secs: _,
            } => crate::protocol::lease_codec::LeaseResponse::Ok {
                token: Some(fencing_token),
            },
        };

        let response_bytes = crate::protocol::lease_codec::encode_response(&codec_response);
        let response_ctx = FrameContext::new(
            frame_ctx.session_id,
            frame_ctx.channel_id,
            crate::protocol::tlv::MessageType::new(frame_ctx.msg_type.as_u16()),
            bytes::Bytes::from(response_bytes),
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
    active: AtomicBool,
}

impl ScheduleDomainSink {
    pub fn new(store: Arc<cntryl_midge::Engine>, router: Arc<Router>) -> Self {
        Self {
            store,
            actors: Mutex::new(HashMap::new()),
            sub_families: Mutex::new(HashMap::new()),
            next_sub_id: AtomicU64::new(1),
            router,
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
            for sub in &state.subscriptions {
                if sub.pattern.matches(&event.route) {
                    let notify_payload = crate::protocol::schedule_codec::encode_notify(
                        sub.subscription_id,
                        &event.route,
                        &event.payload,
                    );
                    let notify_ctx = FrameContext::new(
                        sub.session_id,
                        crate::protocol::frame::ChannelId::Sub, // notification channel
                        crate::protocol::tlv::MessageType::new(705), // SCHEDULE_NOTIFY
                        bytes::Bytes::from(notify_payload),
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

        let schedule_msg = match crate::protocol::schedule_codec::parse_request(
            &frame_ctx,
            &frame_ctx.payload,
            *envelope.destination().family(),
            crate::session::SessionId(frame_ctx.session_id),
            envelope
                .source()
                .cloned()
                .unwrap_or_else(|| envelope.destination().clone()),
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

        use crate::protocol::schedule_codec::{ScheduleMessage, ScheduleResponse};

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
                ScheduleMessage::Create { payload } => {
                    let route_str = format!(
                        "schedule://default/default/{}/{}",
                        payload.target_resource, payload.target_operation
                    );
                    let route = crate::runtime::routing::Route::new(route_str);
                    let payload_bytes = payload.encode();

                    match actor.create_schedule(route, payload_bytes) {
                        Ok(id) => ScheduleResponse::Ok {
                            schedule_id: Some(id.to_string()),
                        },
                        Err(e) => ScheduleResponse::Error(e),
                    }
                }
                ScheduleMessage::Cancel { schedule_id } => match schedule_id.parse::<u64>() {
                    Ok(id) => match actor.delete_schedule(id) {
                        Ok(()) => ScheduleResponse::Ok { schedule_id: None },
                        Err(e) => ScheduleResponse::Error(e),
                    },
                    Err(_) => {
                        ScheduleResponse::Error(format!("Invalid schedule ID: {}", schedule_id))
                    }
                },
                ScheduleMessage::List => ScheduleResponse::ListIds(actor.list_schedule_ids()),
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

                    let sub_id = if let Some(id) = existing_sub_id {
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

                    ScheduleResponse::Ok {
                        schedule_id: Some(sub_id.to_string()),
                    }
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
                    ScheduleResponse::Ok { schedule_id: None }
                }
                ScheduleMessage::UnsubscribeAll { session_id, .. } => {
                    self.unsubscribe_all(session_id);
                    ScheduleResponse::Ok { schedule_id: None }
                }
            }
        };

        // Encode and route response back
        let response_bytes = crate::protocol::schedule_codec::encode_response(&response);
        let response_ctx = FrameContext::new(
            frame_ctx.session_id,
            frame_ctx.channel_id,
            crate::protocol::tlv::MessageType::new(frame_ctx.msg_type.as_u16()),
            bytes::Bytes::from(response_bytes),
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
pub fn setup(router: &StdArc<Router>, store: &StdArc<cntryl_midge::Engine>) -> BootResult<()> {
    // Register domain handlers globally using wildcard route family (matches all)
    // Each domain matches its route scheme pattern across ALL route families

    // KV domain: Handles all "kv://*" routes across all families
    let kv_sink = Arc::new(KvDomainSink::new(store.clone(), router.clone()));
    router.register_domain_pattern("kv", kv_sink.clone() as Arc<dyn MailboxSink>);
    tracing::info!("Registered KV domain (handles kv://* across all route families)");

    // Queue domain: Handles all "queue://*" routes across all families
    let queue_sink = Arc::new(QueueDomainSink::new(store.clone(), router.clone()));
    router.register_domain_pattern("queue", queue_sink as Arc<dyn MailboxSink>);
    tracing::info!("Registered Queue domain (handles queue://* across all route families)");

    // Notice domain: Handles all "notice://*" routes across all families
    let notice_sink = Arc::new(NoticeDomainSink::new(router.clone()));
    router.register_domain_pattern("notice", notice_sink as Arc<dyn MailboxSink>);
    tracing::info!("Registered Notice domain (handles notice://* across all route families)");

    // Stream domain: Handles all "stream://*" routes across all families
    let stream_sink = Arc::new(StreamDomainSink::new(store.clone(), router.clone()));
    router.register_domain_pattern("stream", stream_sink as Arc<dyn MailboxSink>);
    tracing::info!("Registered Stream domain (handles stream://* across all route families)");

    // RPC domain: Handles all "rpc://*" routes across all families
    let rpc_sink = Arc::new(RpcDomainSink::new(router.clone()));
    router.register_domain_pattern("rpc", rpc_sink as Arc<dyn MailboxSink>);
    tracing::info!("Registered RPC domain (handles rpc://* across all route families)");

    // Lease domain: Handles all "lease://*" routes across all families
    let lease_sink = Arc::new(LeaseDomainSink::new(router.clone()));
    router.register_domain_pattern("lease", lease_sink as Arc<dyn MailboxSink>);
    tracing::info!("Registered Lease domain (handles lease://* across all route families)");

    // Schedule domain: Handles all "schedule://*" routes across all families
    let schedule_sink = Arc::new(ScheduleDomainSink::new(store.clone(), router.clone()));
    router.register_domain_pattern("schedule", schedule_sink as Arc<dyn MailboxSink>);
    tracing::info!("Registered Schedule domain (handles schedule://* across all route families)");

    tracing::info!("All 7 domain sinks registered with router");

    Ok(())
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

        // Act
        let kv_sink = KvDomainSink::new(store, router);

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

        // Act
        let result = setup(&router, &store);

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_create_notice_domain_sink() {
        // Arrange
        let router = Arc::new(Router::new());

        // Act
        let sink = NoticeDomainSink::new(router);

        // Assert
        assert!(sink.active.load(Ordering::Relaxed));
    }

    #[test]
    fn should_create_rpc_domain_sink() {
        // Arrange
        let router = Arc::new(Router::new());

        // Act
        let sink = RpcDomainSink::new(router);

        // Assert
        assert!(sink.active.load(Ordering::Relaxed));
    }

    #[test]
    fn should_create_lease_domain_sink() {
        // Arrange
        let router = Arc::new(Router::new());

        // Act
        let sink = LeaseDomainSink::new(router);

        // Assert
        assert!(sink.active.load(Ordering::Relaxed));
    }

    #[test]
    fn should_create_stream_domain_sink() {
        // Arrange
        let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
        let router = Arc::new(Router::new());

        // Act
        let sink = StreamDomainSink::new(store, router);

        // Assert
        assert!(sink.active.load(Ordering::Relaxed));
    }

    #[test]
    fn should_create_schedule_domain_sink() {
        // Arrange
        let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
        let router = Arc::new(Router::new());

        // Act
        let sink = ScheduleDomainSink::new(store, router);

        // Assert
        assert!(sink.active.load(Ordering::Relaxed));
    }

    #[test]
    fn should_create_queue_domain_sink() {
        // Arrange
        let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
        let router = Arc::new(Router::new());

        // Act
        let sink = QueueDomainSink::new(store, router);

        // Assert
        assert!(sink.active.load(Ordering::Relaxed));
    }
}
