//! Lease domain sink for ephemeral in-memory coordination
//!
//! The boot path mirrors the lease actor's live state into the admin read model.
//! Lease state is expected to vanish on broker restart, and disconnect cleanup
//! removes any session-owned leases immediately.

use super::subscription_state::{RoutedSubscription, RoutedSubscriptionSet};
use crate::protocol::frame_context::FrameContext;
use crate::runtime::{DeliveryError, Envelope, MailboxSink, Router};
use chrono::Utc;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const LEASE_MAX_WAIT_SECONDS: u32 = 30;
const LEASE_MAX_QUEUE_DEPTH: usize = 100;
const LEASE_SWEEP_INTERVAL: Duration = Duration::from_millis(50);

#[derive(Clone)]
struct SinkLeaseState {
    owner_id: String,
    owner_session_id: u64,
    fencing_token: u64,
    expiry: Instant,
    acquired_at: String,
}

#[derive(Clone)]
struct PendingAcquire {
    session_id: u64,
    owner_id: String,
    reply_destination: crate::runtime::routing::RouteAddress,
    reply_source: crate::runtime::routing::RouteAddress,
    channel_id: crate::protocol::frame::ChannelId,
    route_family: crate::runtime::routing::RouteFamily,
    queued_token: u64,
    ttl_secs: u64,
    expires_at: Instant,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PendingAcquireRef {
    key: crate::domains::lease::protocol::LeaseKey,
    queued_token: u64,
}

struct LeaseAcquireRequest {
    key: crate::domains::lease::protocol::LeaseKey,
    owner_session_id: u64,
    owner_id: String,
    ttl_secs: u64,
    wait_seconds: u32,
    reply_source: crate::runtime::routing::RouteAddress,
    reply_destination: Option<crate::runtime::routing::RouteAddress>,
    channel_id: crate::protocol::frame::ChannelId,
    route_family: crate::runtime::routing::RouteFamily,
}

pub struct LeaseDomainSink {
    leases: Mutex<HashMap<crate::domains::lease::protocol::LeaseKey, SinkLeaseState>>,
    session_leases: Mutex<HashMap<u64, HashSet<crate::domains::lease::protocol::LeaseKey>>>,
    pending_acquires:
        Mutex<HashMap<crate::domains::lease::protocol::LeaseKey, VecDeque<PendingAcquire>>>,
    session_waiters: Mutex<HashMap<u64, HashSet<PendingAcquireRef>>>,
    /// Process-local fencing token counter; resets on broker restart.
    next_token: AtomicU64,
    router: Arc<Router>,
    active: AtomicBool,
    families: Mutex<HashMap<u64, RoutedSubscriptionSet<LeaseSubscription>>>,
    next_sub_id: AtomicU64,
    admin_read_model: Arc<crate::api::admin::read_model::AdminReadModel>,
}

struct LeaseSubscription {
    pattern: crate::runtime::matcher::Pattern,
    session_id: u64,
    route_address: crate::runtime::routing::RouteAddress,
    subscription_id: u64,
}

impl RoutedSubscription for LeaseSubscription {
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

impl LeaseDomainSink {
    pub fn new(
        router: Arc<Router>,
        admin_read_model: Arc<crate::api::admin::read_model::AdminReadModel>,
    ) -> Self {
        Self {
            leases: Mutex::new(HashMap::new()),
            session_leases: Mutex::new(HashMap::new()),
            pending_acquires: Mutex::new(HashMap::new()),
            session_waiters: Mutex::new(HashMap::new()),
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

    pub fn start_timeout_loop(self: &Arc<Self>) {
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            tracing::debug!("Lease timeout loop not started: no Tokio runtime available");
            return;
        };

        let weak = Arc::downgrade(self);
        handle.spawn(async move {
            let mut interval = tokio::time::interval(LEASE_SWEEP_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                interval.tick().await;

                let Some(sink) = weak.upgrade() else {
                    break;
                };
                if !sink.active.load(Ordering::Relaxed) {
                    break;
                }

                sink.sweep_expired_state();
            }
        });
    }

    fn lease_info_from_state(
        &self,
        key: &crate::domains::lease::protocol::LeaseKey,
        state: &SinkLeaseState,
    ) -> crate::api::admin::LeaseInfo {
        let now = std::time::Instant::now();
        let expires_at = Utc::now()
            .checked_add_signed(chrono::TimeDelta::seconds(
                state.expiry.saturating_duration_since(now).as_secs() as i64,
            ))
            .unwrap_or_else(Utc::now)
            .to_rfc3339();
        crate::api::admin::LeaseInfo::snapshot(
            &key.realm,
            &key.area,
            &key.resource,
            &state.owner_id,
            &state.acquired_at,
            expires_at,
            state.fencing_token,
        )
    }

    fn upsert_admin_lease(
        &self,
        key: &crate::domains::lease::protocol::LeaseKey,
        state: &SinkLeaseState,
    ) {
        self.admin_read_model
            .upsert_lease(self.lease_info_from_state(key, state));
    }

    fn remove_admin_lease(&self, key: &crate::domains::lease::protocol::LeaseKey) {
        self.admin_read_model
            .remove_lease(&key.realm, &key.area, &key.resource);
    }

    fn track_session_lease(
        &self,
        session_id: u64,
        key: &crate::domains::lease::protocol::LeaseKey,
    ) {
        self.session_leases
            .lock()
            .entry(session_id)
            .or_default()
            .insert(key.clone());
    }

    fn untrack_session_lease(
        &self,
        session_id: u64,
        key: &crate::domains::lease::protocol::LeaseKey,
    ) {
        let mut session_leases = self.session_leases.lock();
        let should_remove_session = if let Some(keys) = session_leases.get_mut(&session_id) {
            keys.remove(key);
            keys.is_empty()
        } else {
            false
        };

        if should_remove_session {
            session_leases.remove(&session_id);
        }
    }

    fn track_session_waiter(
        &self,
        session_id: u64,
        key: &crate::domains::lease::protocol::LeaseKey,
        queued_token: u64,
    ) {
        self.session_waiters
            .lock()
            .entry(session_id)
            .or_default()
            .insert(PendingAcquireRef {
                key: key.clone(),
                queued_token,
            });
    }

    fn untrack_session_waiter(
        &self,
        session_id: u64,
        key: &crate::domains::lease::protocol::LeaseKey,
        queued_token: u64,
    ) {
        let mut session_waiters = self.session_waiters.lock();
        let should_remove_session = if let Some(waiters) = session_waiters.get_mut(&session_id) {
            waiters.remove(&PendingAcquireRef {
                key: key.clone(),
                queued_token,
            });
            waiters.is_empty()
        } else {
            false
        };

        if should_remove_session {
            session_waiters.remove(&session_id);
        }
    }

    fn send_waiter_response(
        &self,
        waiter: &PendingAcquire,
        response: &crate::domains::lease::protocol::LeaseResponse,
    ) {
        let mut payload_encoder =
            crate::protocol::payload_codec::PayloadEncoder::with_capacity(128);
        let response_bytes = crate::protocol::lease_codec::encode_domain_response_into(
            &mut payload_encoder,
            response,
        );
        let response_ctx = FrameContext::new(
            waiter.session_id,
            waiter.channel_id,
            crate::protocol::tlv::MessageType::new(400),
            bytes::Bytes::from(response_bytes),
            waiter.route_family,
        );
        let response_envelope = Envelope::from_route(
            waiter.reply_source.clone(),
            waiter.reply_destination.clone(),
            response_ctx,
        );
        let _ = self.router.route(response_envelope);
    }

    fn notify_lease_change(&self, key: &crate::domains::lease::protocol::LeaseKey) {
        let event = crate::runtime::DomainPublishEvent::new(
            key.family,
            key.to_route(),
            bytes::Bytes::new(),
        );
        let _ = self.handle_domain_publish(&event);
    }

    fn remove_session_waiters(&self, session_id: u64) -> usize {
        let waiter_refs = self
            .session_waiters
            .lock()
            .remove(&session_id)
            .map(|waiters| waiters.into_iter().collect::<Vec<_>>())
            .unwrap_or_default();

        if waiter_refs.is_empty() {
            return 0;
        }

        let mut removed = 0;
        let mut pending_acquires = self.pending_acquires.lock();
        let mut empty_keys = Vec::new();
        for waiter_ref in waiter_refs {
            if let Some(queue) = pending_acquires.get_mut(&waiter_ref.key) {
                if let Some(index) = queue
                    .iter()
                    .position(|waiter| waiter.queued_token == waiter_ref.queued_token)
                {
                    queue.remove(index);
                    removed += 1;
                }
                if queue.is_empty() {
                    empty_keys.push(waiter_ref.key.clone());
                }
            }
        }

        for key in empty_keys {
            pending_acquires.remove(&key);
        }

        removed
    }

    fn expire_timed_out_waiters_for_key(
        &self,
        key: &crate::domains::lease::protocol::LeaseKey,
        now: Instant,
    ) {
        let expired_waiters = {
            let mut pending_acquires = self.pending_acquires.lock();
            let mut expired = Vec::new();
            let mut remove_queue = false;

            if let Some(queue) = pending_acquires.get_mut(key) {
                let mut index = 0;
                while index < queue.len() {
                    if queue[index].expires_at <= now {
                        expired.push(queue.remove(index).expect("waiter removal"));
                    } else {
                        index += 1;
                    }
                }
                remove_queue = queue.is_empty();
            }

            if remove_queue {
                pending_acquires.remove(key);
            }

            expired
        };

        for waiter in expired_waiters {
            self.untrack_session_waiter(waiter.session_id, key, waiter.queued_token);
            self.send_waiter_response(
                &waiter,
                &crate::domains::lease::protocol::LeaseResponse::Timeout,
            );
        }
    }

    fn pending_waiter_count(&self, key: &crate::domains::lease::protocol::LeaseKey) -> usize {
        self.pending_acquires
            .lock()
            .get(key)
            .map(VecDeque::len)
            .unwrap_or(0)
    }

    fn grant_next_waiter_if_available(
        &self,
        key: &crate::domains::lease::protocol::LeaseKey,
        now: Instant,
    ) {
        self.expire_timed_out_waiters_for_key(key, now);

        let granted_waiter = {
            let mut pending_acquires = self.pending_acquires.lock();
            let mut leases = self.leases.lock();

            if leases.contains_key(key) {
                None
            } else {
                let mut remove_queue = false;
                let waiter = if let Some(queue) = pending_acquires.get_mut(key) {
                    let waiter = queue.pop_front();
                    remove_queue = queue.is_empty();
                    waiter
                } else {
                    None
                };

                if remove_queue {
                    pending_acquires.remove(key);
                }

                waiter.map(|waiter| {
                    let state = SinkLeaseState {
                        owner_id: waiter.owner_id.clone(),
                        owner_session_id: waiter.session_id,
                        fencing_token: waiter.queued_token,
                        expiry: now + Duration::from_secs(waiter.ttl_secs),
                        acquired_at: Utc::now().to_rfc3339(),
                    };
                    leases.insert(key.clone(), state.clone());
                    (waiter, state)
                })
            }
        };

        if let Some((waiter, state)) = granted_waiter {
            self.untrack_session_waiter(waiter.session_id, key, waiter.queued_token);
            self.track_session_lease(waiter.session_id, key);
            self.upsert_admin_lease(key, &state);
            self.send_waiter_response(
                &waiter,
                &crate::domains::lease::protocol::LeaseResponse::Acquired {
                    fencing_token: waiter.queued_token,
                },
            );
        }
    }

    fn sweep_expired_state(&self) {
        let now = Instant::now();

        let expired_waiters = {
            let mut pending_acquires = self.pending_acquires.lock();
            let mut expired = Vec::new();
            let mut empty_keys = Vec::new();

            for (key, queue) in pending_acquires.iter_mut() {
                let mut index = 0;
                while index < queue.len() {
                    if queue[index].expires_at <= now {
                        expired.push((key.clone(), queue.remove(index).expect("waiter removal")));
                    } else {
                        index += 1;
                    }
                }

                if queue.is_empty() {
                    empty_keys.push(key.clone());
                }
            }

            for key in empty_keys {
                pending_acquires.remove(&key);
            }

            expired
        };

        for (key, waiter) in expired_waiters {
            self.untrack_session_waiter(waiter.session_id, &key, waiter.queued_token);
            self.send_waiter_response(
                &waiter,
                &crate::domains::lease::protocol::LeaseResponse::Timeout,
            );
        }

        let expired_leases = {
            let mut leases = self.leases.lock();
            let expired_keys: Vec<_> = leases
                .iter()
                .filter(|(_, state)| state.expiry <= now)
                .map(|(key, _)| key.clone())
                .collect();

            let mut expired = Vec::with_capacity(expired_keys.len());
            for key in expired_keys {
                if let Some(state) = leases.remove(&key) {
                    expired.push((key, state));
                }
            }
            expired
        };

        for (key, state) in expired_leases {
            self.untrack_session_lease(state.owner_session_id, &key);
            self.remove_admin_lease(&key);
            self.notify_lease_change(&key);
            self.grant_next_waiter_if_available(&key, now);
        }

        let queued_keys = self
            .pending_acquires
            .lock()
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for key in queued_keys {
            self.grant_next_waiter_if_available(&key, now);
        }
    }

    pub fn cleanup_session(&self, session_id: u64) {
        let now = Instant::now();
        let tracked_keys = self
            .session_leases
            .lock()
            .remove(&session_id)
            .map(|keys| keys.into_iter().collect::<Vec<_>>())
            .unwrap_or_default();
        let removed_waiters = self.remove_session_waiters(session_id);

        let mut removed_keys = Vec::with_capacity(tracked_keys.len());
        if !tracked_keys.is_empty() {
            let mut leases = self.leases.lock();
            for key in tracked_keys {
                if leases.remove(&key).is_some() {
                    removed_keys.push(key);
                }
            }
        }

        let removed_subscriptions = self.unsubscribe_all(session_id);
        for key in &removed_keys {
            self.remove_admin_lease(key);
            self.notify_lease_change(key);
        }
        for key in &removed_keys {
            self.grant_next_waiter_if_available(key, now);
        }

        tracing::debug!(
            domain = "lease",
            session = session_id,
            count_removed = removed_keys.len(),
            waiters_removed = removed_waiters,
            subscriptions_removed = removed_subscriptions,
            "Lease: released all leases for disconnected session"
        );
    }

    pub fn lease_count(&self) -> usize {
        self.leases.lock().len()
    }

    pub fn subscription_count(&self) -> usize {
        let families = self.families.lock();
        families
            .values()
            .map(|state| state.subscription_count())
            .sum()
    }

    fn handle_domain_publish(
        &self,
        event: &crate::runtime::DomainPublishEvent,
    ) -> Result<(), DeliveryError> {
        let family_id = event.family_id.as_u64();
        let families = self.families.lock();

        if let Some(family_state) = families.get(&family_id) {
            let mut payload_encoder =
                crate::protocol::payload_codec::PayloadEncoder::with_capacity(256);
            family_state.for_each_matching(event, |sub| {
                let notify_payload = crate::protocol::lease_codec::encode_notify_into(
                    &mut payload_encoder,
                    sub.subscription_id,
                    event.route.as_str(),
                    &event.payload,
                );
                let notify_ctx = FrameContext::new(
                    sub.session_id,
                    crate::protocol::frame::ChannelId::Sub,
                    crate::protocol::tlv::MessageType::new(409),
                    bytes::Bytes::from(notify_payload),
                    event.family_id,
                );

                let notify_envelope = Envelope::new(sub.route_address.clone(), notify_ctx);
                let _ = self.router.route(notify_envelope);
            });
        }
        Ok(())
    }

    fn unsubscribe_all(&self, session_id: u64) -> usize {
        let mut families = self.families.lock();
        let mut removed = 0;
        for (family_id, state) in families.iter_mut() {
            removed += state.remove_session(
                crate::runtime::routing::RouteFamily::new(*family_id),
                session_id,
            );
        }
        removed
    }

    fn next_fencing_token(&self) -> u64 {
        self.next_token.fetch_add(1, Ordering::Relaxed)
    }

    fn handle_acquire(&self, request: LeaseAcquireRequest) -> crate::domains::lease::protocol::LeaseResponse {
        use crate::domains::lease::protocol::LeaseResponse;

        let LeaseAcquireRequest {
            key,
            owner_session_id,
            owner_id,
            ttl_secs,
            wait_seconds,
            reply_source,
            reply_destination,
            channel_id,
            route_family,
        } = request;

        let now = Instant::now();
        let ttl = Duration::from_secs(ttl_secs);

        if wait_seconds > LEASE_MAX_WAIT_SECONDS {
            return LeaseResponse::Timeout;
        }

        let expired_state = {
            let mut leases = self.leases.lock();
            match leases.get(&key) {
                Some(state) if state.expiry <= now => leases.remove(&key),
                _ => None,
            }
        };

        if let Some(state) = expired_state {
            self.untrack_session_lease(state.owner_session_id, &key);
            self.remove_admin_lease(&key);
            self.notify_lease_change(&key);
            self.grant_next_waiter_if_available(&key, now);
        }

        if !self.leases.lock().contains_key(&key) {
            self.grant_next_waiter_if_available(&key, now);
        }

        let mut acquired_state = None;
        let response = {
            let mut leases = self.leases.lock();

            match leases.get(&key).cloned() {
                None => {
                    let token = self.next_fencing_token();
                    let state = SinkLeaseState {
                        owner_id,
                        owner_session_id,
                        fencing_token: token,
                        expiry: now + ttl,
                        acquired_at: Utc::now().to_rfc3339(),
                    };
                    leases.insert(key.clone(), state.clone());
                    acquired_state = Some(state);
                    LeaseResponse::Acquired {
                        fencing_token: token,
                    }
                }
                Some(state) if state.owner_id == owner_id => LeaseResponse::AlreadyHeld {
                    fencing_token: state.fencing_token,
                },
                Some(state) if wait_seconds == 0 => LeaseResponse::HeldByOther {
                    current_owner: state.owner_id,
                },
                Some(state) => {
                    let Some(reply_destination) = reply_destination else {
                        return LeaseResponse::HeldByOther {
                            current_owner: state.owner_id,
                        };
                    };

                    let mut pending_acquires = self.pending_acquires.lock();
                    if let Some(queue) = pending_acquires.get(&key) {
                        if let Some(existing) =
                            queue.iter().find(|waiter| waiter.owner_id == owner_id)
                        {
                            return LeaseResponse::AlreadyQueued {
                                fencing_token: existing.queued_token,
                            };
                        }

                        if queue.len() >= LEASE_MAX_QUEUE_DEPTH {
                            return LeaseResponse::QueueFull {
                                pending_count: queue.len(),
                            };
                        }
                    }

                    let queued_token = self.next_fencing_token();
                    pending_acquires
                        .entry(key.clone())
                        .or_default()
                        .push_back(PendingAcquire {
                            session_id: owner_session_id,
                            owner_id,
                            reply_destination,
                            reply_source,
                            channel_id,
                            route_family,
                            queued_token,
                            ttl_secs,
                            expires_at: now + Duration::from_secs(wait_seconds as u64),
                        });
                    drop(pending_acquires);

                    self.track_session_waiter(owner_session_id, &key, queued_token);

                    LeaseResponse::Queued {
                        fencing_token: queued_token,
                    }
                }
            }
        };

        if let Some(state) = acquired_state.as_ref() {
            self.track_session_lease(owner_session_id, &key);
            self.upsert_admin_lease(&key, state);
        }

        response
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
        let mut updated_state = None;

        let response = match leases.get_mut(&key) {
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
                    updated_state = Some(state.clone());
                    LeaseResponse::Extended {
                        fencing_token: new_token,
                    }
                }
            }
        };
        drop(leases);

        if let Some(state) = updated_state.as_ref() {
            self.upsert_admin_lease(&key, state);
        }

        response
    }

    fn handle_release(
        &self,
        key: crate::domains::lease::protocol::LeaseKey,
        owner_id: String,
        fencing_token: u64,
    ) -> crate::domains::lease::protocol::LeaseResponse {
        use crate::domains::lease::protocol::LeaseResponse;

        let now = Instant::now();
        let mut removed_state = None;

        let response = {
            let mut leases = self.leases.lock();

            match leases.get(&key).cloned() {
                None => LeaseResponse::Released,
                Some(state) if state.expiry <= now => {
                    leases.remove(&key);
                    removed_state = Some(state);
                    LeaseResponse::Released
                }
                Some(state) if state.owner_id != owner_id => LeaseResponse::NotHeld,
                Some(state) if state.fencing_token != fencing_token => LeaseResponse::Fenced {
                    current_token: state.fencing_token,
                },
                Some(state) => {
                    leases.remove(&key);
                    removed_state = Some(state);
                    LeaseResponse::Released
                }
            }
        };

        if let Some(state) = removed_state {
            self.untrack_session_lease(state.owner_session_id, &key);
            self.remove_admin_lease(&key);
            self.notify_lease_change(&key);
            self.grant_next_waiter_if_available(&key, now);
        }

        response
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
                        pending_waiters: self.pending_waiter_count(&key),
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

        if let Some(cleanup) = envelope.payload::<crate::runtime::SessionCleanup>() {
            self.cleanup_session(cleanup.session_id);
            return Ok(());
        }

        if let Some(event) = envelope.payload::<crate::runtime::DomainPublishEvent>() {
            return self.handle_domain_publish(event);
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

        let domain_response = match lease_msg {
            LeaseMessage::Acquire {
                family_id,
                route,
                owner_id,
                ttl_secs,
                wait_seconds,
            } => match LeaseKey::from_route(family_id, &route) {
                Some(key) => self.handle_acquire(LeaseAcquireRequest {
                    key,
                    owner_session_id: frame_ctx.session_id,
                    owner_id: effective_owner(owner_id),
                    ttl_secs,
                    wait_seconds,
                    reply_source: envelope.destination().clone(),
                    reply_destination: envelope.source().cloned(),
                    channel_id: frame_ctx.channel_id,
                    route_family: frame_ctx.route_family,
                }),
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
                self.sweep_expired_state();
                return Ok(());
            }
            LeaseMessage::Subscribe { family_id, pattern } => {
                let route_address = match envelope.source() {
                    Some(src) => src,
                    None => {
                        let error_bytes = vec![1u8];
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
                let family_state = families
                    .entry(family_id_u64)
                    .or_insert_with(RoutedSubscriptionSet::new);

                if let Some(existing) =
                    family_state.find_existing_id(frame_ctx.session_id, pattern.as_str())
                {
                    let response_bytes = crate::protocol::lease_codec::encode_domain_response_into(
                        &mut payload_encoder,
                        &LeaseResponse::SubscribeOk {
                            subscription_id: existing,
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

                family_state.insert(
                    family_id,
                    LeaseSubscription {
                        pattern: crate::runtime::matcher::Pattern::new(pattern.as_str()),
                        session_id: frame_ctx.session_id,
                        route_address: route_address.clone(),
                        subscription_id,
                    },
                );

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
                let family_id_u64 = family_id.as_u64();
                let mut families = self.families.lock();

                if let Some(family_state) = families.get_mut(&family_id_u64) {
                    family_state.remove_session_pattern(
                        family_id,
                        frame_ctx.session_id,
                        pattern.as_str(),
                    );
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
                self.unsubscribe_all(frame_ctx.session_id);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::frame::ChannelId;
    use crate::protocol::frame_context::FrameContext;
    use crate::protocol::payload_codec::PayloadEncoder;
    use crate::protocol::tlv::MessageType;
    use crate::runtime::mailbox::Mailbox;
    use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
    use bytes::Bytes;
    use std::sync::Arc;

    fn encode_lease_acquire(route: &str, owner_id: &str, ttl_secs: u64) -> Bytes {
        let mut encoder = PayloadEncoder::new();
        encoder.put_string(route);
        encoder.put_string(owner_id);
        encoder.put_u64(ttl_secs);
        Bytes::from(encoder.finish())
    }

    fn encode_lease_release(route: &str, owner_id: &str, fencing_token: u64) -> Bytes {
        let mut encoder = PayloadEncoder::new();
        encoder.put_string(route);
        encoder.put_string(owner_id);
        encoder.put_u64(fencing_token);
        Bytes::from(encoder.finish())
    }

    fn encode_lease_subscribe(pattern: &str) -> Bytes {
        let mut encoder = PayloadEncoder::new();
        encoder.put_string(pattern);
        Bytes::from(encoder.finish())
    }

    fn drain_mailbox(mailbox: &Mailbox) {
        while mailbox.receiver().try_recv().is_ok() {}
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
    fn should_clear_session_state_given_session_cleanup() {
        // Arrange
        let family = RouteFamily::new(1);
        let session_id = 7;
        let lease_route = "lease://acme/locks/resource";
        let lease_address = RouteAddress::new(family, Route::new(lease_route));
        let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/7"));
        let router = Arc::new(Router::new());
        let subscriber_mailbox = Arc::new(Mailbox::new(8));
        router.register(subscriber_address.clone(), subscriber_mailbox.clone());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let sink = LeaseDomainSink::new(router, admin_read_model.clone());

        sink.deliver(Envelope::from_route(
            subscriber_address.clone(),
            lease_address.clone(),
            FrameContext::new(
                session_id,
                ChannelId::Sub,
                MessageType::new(400),
                encode_lease_acquire(lease_route, "", 30),
                family,
            ),
        ))
        .expect("acquire lease");
        let _acquire_ack = subscriber_mailbox
            .receiver()
            .try_recv()
            .expect("acquire ack envelope");
        sink.deliver(Envelope::from_route(
            subscriber_address.clone(),
            lease_address,
            FrameContext::new(
                session_id,
                ChannelId::Sub,
                MessageType::new(407),
                encode_lease_subscribe(lease_route),
                family,
            ),
        ))
        .expect("subscribe lease route");
        let _subscribe_ack = subscriber_mailbox
            .receiver()
            .try_recv()
            .expect("subscribe ack envelope");
        assert_eq!(sink.lease_count(), 1);
        assert_eq!(sink.subscription_count(), 1);
        assert_eq!(admin_read_model.leases(None).len(), 1);
        assert_eq!(
            admin_read_model.leases(None)[0].owner_session_id,
            "session:7"
        );
        drain_mailbox(&subscriber_mailbox);

        // Act
        sink.deliver(Envelope::new(
            RouteAddress::new(family, Route::new("lease://cleanup")),
            crate::runtime::SessionCleanup { session_id },
        ))
        .expect("cleanup lease session");
        sink.deliver(Envelope::new(
            RouteAddress::new(family, Route::new("lease://events")),
            crate::runtime::DomainPublishEvent::new(
                family,
                Route::new(lease_route),
                Bytes::from_static(b"expired"),
            ),
        ))
        .expect("deliver lease publish event");

        // Assert
        assert_eq!(sink.lease_count(), 0);
        assert_eq!(sink.subscription_count(), 0);
        assert!(admin_read_model.leases(None).is_empty());
        assert!(subscriber_mailbox.receiver().try_recv().is_err());
    }

    #[test]
    fn should_preserve_other_session_leases_given_session_cleanup() {
        // Arrange
        let family = RouteFamily::new(1);
        let route_a = "lease://acme/locks/resource-a";
        let route_b = "lease://acme/locks/resource-b";
        let router = Arc::new(Router::new());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let sink = LeaseDomainSink::new(router, admin_read_model.clone());

        sink.deliver(Envelope::from_route(
            RouteAddress::new(family, Route::new("inbox://session/7")),
            RouteAddress::new(family, Route::new(route_a)),
            FrameContext::new(
                7,
                ChannelId::Sub,
                MessageType::new(400),
                encode_lease_acquire(route_a, "", 30),
                family,
            ),
        ))
        .expect("session 7 acquire lease");
        sink.deliver(Envelope::from_route(
            RouteAddress::new(family, Route::new("inbox://session/8")),
            RouteAddress::new(family, Route::new(route_b)),
            FrameContext::new(
                8,
                ChannelId::Sub,
                MessageType::new(400),
                encode_lease_acquire(route_b, "", 30),
                family,
            ),
        ))
        .expect("session 8 acquire lease");

        // Act
        sink.cleanup_session(7);
        let leases = admin_read_model.leases(None);

        // Assert
        assert_eq!(sink.lease_count(), 1);
        assert_eq!(leases.len(), 1);
        assert_eq!(leases[0].resource, "resource-b");
        assert_eq!(leases[0].owner_session_id, "session:8");
    }

    #[test]
    fn should_remove_admin_lease_given_release() {
        // Arrange
        let family = RouteFamily::new(1);
        let session_id = 7;
        let lease_route = "lease://acme/locks/resource";
        let lease_address = RouteAddress::new(family, Route::new(lease_route));
        let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/7"));
        let router = Arc::new(Router::new());
        let subscriber_mailbox = Arc::new(Mailbox::new(8));
        router.register(subscriber_address.clone(), subscriber_mailbox.clone());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let sink = LeaseDomainSink::new(router, admin_read_model.clone());

        sink.deliver(Envelope::from_route(
            subscriber_address.clone(),
            lease_address.clone(),
            FrameContext::new(
                session_id,
                ChannelId::Sub,
                MessageType::new(400),
                encode_lease_acquire(lease_route, "", 30),
                family,
            ),
        ))
        .expect("acquire lease");
        let acquire_ack = subscriber_mailbox
            .receiver()
            .try_recv()
            .expect("acquire ack envelope");
        let frame_ctx = acquire_ack
            .payload::<FrameContext>()
            .cloned()
            .expect("frame context");
        let fencing_token = u64::from_be_bytes([
            frame_ctx.payload[2],
            frame_ctx.payload[3],
            frame_ctx.payload[4],
            frame_ctx.payload[5],
            frame_ctx.payload[6],
            frame_ctx.payload[7],
            frame_ctx.payload[8],
            frame_ctx.payload[9],
        ]);

        // Act
        sink.deliver(Envelope::from_route(
            subscriber_address,
            lease_address,
            FrameContext::new(
                session_id,
                ChannelId::Sub,
                MessageType::new(402),
                encode_lease_release(lease_route, "", fencing_token),
                family,
            ),
        ))
        .expect("release lease");

        // Assert
        assert!(admin_read_model.leases(None).is_empty());
        assert_eq!(sink.lease_count(), 0);
    }
}
