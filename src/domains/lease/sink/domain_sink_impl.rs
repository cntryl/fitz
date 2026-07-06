use super::model::{
    Duration, Instant, LeaseAcquireRequest, LeaseDomainRuntime, Ordering, PendingAcquire,
    PendingAcquireRef, QueuedAcquireRequest, SinkLeaseState, Utc, LEASE_MAX_QUEUE_DEPTH,
    LEASE_MAX_WAIT_SECONDS,
};
use crate::domains::subscription_state::RoutedSubscriptionSet;
#[cfg(test)]
use crate::protocol::frame_context::FrameContext;
use crate::runtime::Envelope;
use std::collections::VecDeque;

impl LeaseDomainRuntime<'_> {
    pub(super) fn track_session_lease(
        &self,
        session_id: u64,
        key: &crate::domains::lease::protocol::LeaseKey,
    ) {
        self.core
            .session_leases
            .lock()
            .entry(session_id)
            .or_default()
            .insert(key.clone());
    }

    pub(super) fn untrack_session_lease(
        &self,
        session_id: u64,
        key: &crate::domains::lease::protocol::LeaseKey,
    ) {
        let mut session_leases = self.core.session_leases.lock();
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

    pub(super) fn track_session_waiter(
        &self,
        session_id: u64,
        key: &crate::domains::lease::protocol::LeaseKey,
        queued_token: u64,
    ) {
        self.core
            .session_waiters
            .lock()
            .entry(session_id)
            .or_default()
            .insert(PendingAcquireRef {
                key: key.clone(),
                queued_token,
            });
    }

    pub(super) fn untrack_session_waiter(
        &self,
        session_id: u64,
        key: &crate::domains::lease::protocol::LeaseKey,
        queued_token: u64,
    ) {
        let mut session_waiters = self.core.session_waiters.lock();
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

    pub(super) fn send_waiter_response(
        &self,
        waiter: &PendingAcquire,
        response: &crate::domains::lease::protocol::LeaseResponse,
    ) {
        #[cfg(test)]
        let response_ctx = {
            let mut payload_encoder =
                crate::protocol::payload_codec::PayloadEncoder::with_capacity(128);
            let response_bytes = crate::protocol::lease_codec::encode_domain_response_into(
                &mut payload_encoder,
                response,
            );
            FrameContext::new(
                waiter.session_id,
                test_protocol_channel_from_client(waiter.channel),
                crate::protocol::tlv::MessageType::new(
                    crate::protocol::lease_codec::msg_type::ACQUIRE,
                ),
                bytes::Bytes::from(response_bytes),
                waiter.route_family,
            )
        };

        #[cfg(not(test))]
        let response_ctx = crate::domains::lease::LeaseClientResponse::new(
            crate::runtime::ClientFrameMeta::new(
                waiter.session_id,
                waiter.channel,
                crate::protocol::lease_codec::msg_type::ACQUIRE,
                waiter.route_family,
            ),
            response.clone(),
        );

        let response_envelope = Envelope::from_route(
            waiter.reply_source.clone(),
            waiter.reply_destination.clone(),
            response_ctx,
        );
        let _ = self.core.router.route(response_envelope);
    }

    pub(super) fn route_lease_response(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        response: &crate::domains::lease::protocol::LeaseResponse,
        request_started: Option<std::time::Instant>,
    ) {
        #[cfg(test)]
        let response_ctx = {
            let response_bytes = crate::protocol::lease_codec::encode_domain_response(response);
            FrameContext::new(
                meta.session_id,
                test_protocol_channel_from_client(meta.channel),
                crate::protocol::tlv::MessageType::new(meta.message_type),
                bytes::Bytes::from(response_bytes),
                meta.route_family,
            )
        };

        #[cfg(not(test))]
        let response_ctx = crate::domains::lease::LeaseClientResponse::new(meta, response.clone());

        if let Some(response_envelope) = envelope.try_reply_to(response_ctx) {
            let response_sink = self
                .core
                .router
                .resolve_sink(response_envelope.destination());
            if let Some(sink) = response_sink {
                let _ = sink.deliver(response_envelope);
            } else {
                let _ = self.core.router.route(response_envelope);
            }
        }

        if let (Some(metrics), Some(started_at)) = (self.core.metrics.as_ref(), request_started) {
            if Self::lease_response_is_failure(response) {
                metrics.record_failure(started_at);
            } else {
                metrics.record_success(started_at);
            }
        }
    }

    pub(super) fn notify_lease_change(&self, key: &crate::domains::lease::protocol::LeaseKey) {
        let event = crate::runtime::DomainPublishEvent::new(
            key.family,
            key.to_route(),
            bytes::Bytes::new(),
        );
        self.handle_domain_publish(&event);
    }

    pub(super) fn remove_session_waiters(&self, session_id: u64) -> usize {
        let waiter_refs = self
            .core
            .session_waiters
            .lock()
            .remove(&session_id)
            .map(|waiters| waiters.into_iter().collect::<Vec<_>>())
            .unwrap_or_default();

        if waiter_refs.is_empty() {
            return 0;
        }

        let mut removed = 0;
        let mut pending_acquires = self.core.pending_acquires.lock();
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

    pub(super) fn expire_timed_out_waiters_for_key(
        &self,
        key: &crate::domains::lease::protocol::LeaseKey,
        now: Instant,
    ) {
        let expired_waiters = {
            let mut pending_acquires = self.core.pending_acquires.lock();
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
            self.counter_inc("fitz_lease_acquire_timeouts_total");
            self.send_waiter_response(
                &waiter,
                &crate::domains::lease::protocol::LeaseResponse::Timeout,
            );
        }

        if !key.resource.is_empty() {
            self.refresh_metrics_gauges();
        }
    }

    pub(super) fn pending_waiter_count(
        &self,
        key: &crate::domains::lease::protocol::LeaseKey,
    ) -> usize {
        self.core
            .pending_acquires
            .lock()
            .get(key)
            .map_or(0, VecDeque::len)
    }

    pub(super) fn grant_next_waiter_if_available(
        &self,
        key: &crate::domains::lease::protocol::LeaseKey,
        now: Instant,
    ) {
        self.expire_timed_out_waiters_for_key(key, now);

        let granted_waiter = {
            let mut pending_acquires = self.core.pending_acquires.lock();
            let mut leases = self.core.leases.lock();

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
                        renewals: 0,
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
            self.refresh_metrics_gauges();
        }
    }

    pub(crate) fn sweep_expired_state(&self) {
        let now = Instant::now();

        let expired_waiters = {
            let mut pending_acquires = self.core.pending_acquires.lock();
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

        let had_expired_waiters = !expired_waiters.is_empty();

        for (key, waiter) in expired_waiters {
            self.untrack_session_waiter(waiter.session_id, &key, waiter.queued_token);
            self.send_waiter_response(
                &waiter,
                &crate::domains::lease::protocol::LeaseResponse::Timeout,
            );
        }

        if had_expired_waiters {
            self.refresh_metrics_gauges();
        }

        let expired_leases = {
            let mut leases = self.core.leases.lock();
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
            self.counter_inc("fitz_lease_forced_releases_total");
            self.untrack_session_lease(state.owner_session_id, &key);
            self.remove_admin_lease(&key);
            self.notify_lease_change(&key);
            self.grant_next_waiter_if_available(&key, now);
        }

        let queued_keys = self
            .core
            .pending_acquires
            .lock()
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for key in queued_keys {
            self.grant_next_waiter_if_available(&key, now);
        }

        self.refresh_metrics_gauges();
    }

    pub fn cleanup_session(&self, session_id: u64) {
        let now = Instant::now();
        let tracked_keys = self
            .core
            .session_leases
            .lock()
            .remove(&session_id)
            .map(|keys| keys.into_iter().collect::<Vec<_>>())
            .unwrap_or_default();
        let removed_waiters = self.remove_session_waiters(session_id);

        let mut removed_keys = Vec::with_capacity(tracked_keys.len());
        if !tracked_keys.is_empty() {
            let mut leases = self.core.leases.lock();
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
        self.refresh_metrics_gauges();
    }

    pub fn lease_count(&self) -> usize {
        self.core.leases.lock().len()
    }

    pub fn subscription_count(&self) -> usize {
        let families = self.core.families.lock();
        families
            .values()
            .map(RoutedSubscriptionSet::subscription_count)
            .sum()
    }

    pub(super) fn handle_domain_publish(&self, event: &crate::runtime::DomainPublishEvent) {
        let family_id = event.family_id.as_u64();
        let families = self.core.families.lock();

        if let Some(family_state) = families.get(&family_id) {
            #[cfg(test)]
            let mut payload_encoder =
                crate::protocol::payload_codec::PayloadEncoder::with_capacity(256);
            family_state.for_each_matching(event, |sub| {
                #[cfg(test)]
                {
                    let notify_payload = crate::protocol::lease_codec::encode_notify_into(
                        &mut payload_encoder,
                        sub.subscription_id,
                        event.route.as_str(),
                        &event.payload,
                    );
                    let notify_ctx = FrameContext::new(
                        sub.session_id,
                        crate::protocol::frame::ChannelId::Sub,
                        crate::protocol::tlv::MessageType::new(
                            crate::protocol::lease_codec::msg_type::NOTIFY,
                        ),
                        bytes::Bytes::from(notify_payload),
                        event.family_id,
                    );

                    let notify_envelope = Envelope::new(sub.route_address.clone(), notify_ctx);
                    let _ = self.core.router.route(notify_envelope);
                }

                #[cfg(not(test))]
                {
                    let notification = crate::domains::lease::LeaseClientNotification::new(
                        sub.session_id,
                        event.family_id,
                        sub.subscription_id,
                        event.route.clone(),
                        event.payload.clone(),
                    );
                    let notify_envelope = Envelope::new(sub.route_address.clone(), notification);
                    let _ = self.core.router.route(notify_envelope);
                }
            });
        }
    }

    pub(super) fn unsubscribe_all(&self, session_id: u64) -> usize {
        let mut families = self.core.families.lock();
        let mut removed = 0;
        for (family_id, state) in families.iter_mut() {
            removed += state.remove_session(
                crate::runtime::routing::RouteFamily::new(*family_id),
                session_id,
            );
        }
        families.retain(|_, state| !state.is_empty());
        removed
    }

    pub(super) fn next_fencing_token(&self) -> u64 {
        self.core.next_token.fetch_add(1, Ordering::Relaxed)
    }

    pub(super) fn handle_acquire(
        &self,
        request: LeaseAcquireRequest,
    ) -> crate::domains::lease::protocol::LeaseResponse {
        use crate::domains::lease::protocol::LeaseResponse;

        let LeaseAcquireRequest {
            key,
            owner_session_id,
            owner_id,
            ttl_secs,
            wait_seconds,
            reply_source,
            reply_destination,
            channel,
            route_family,
        } = request;

        let now = Instant::now();
        let ttl = Duration::from_secs(ttl_secs);

        if wait_seconds > LEASE_MAX_WAIT_SECONDS {
            return LeaseResponse::Timeout;
        }

        self.prepare_acquire_key(&key, now);

        let mut acquired_state = None;
        let response = {
            let mut leases = self.core.leases.lock();

            match leases.get(&key).cloned() {
                None => {
                    let token = self.next_fencing_token();
                    let state = SinkLeaseState {
                        owner_id,
                        owner_session_id,
                        fencing_token: token,
                        expiry: now + ttl,
                        acquired_at: Utc::now().to_rfc3339(),
                        renewals: 0,
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
                Some(state) => self.queue_acquire_waiter(
                    &key,
                    QueuedAcquireRequest {
                        current_owner: state.owner_id,
                        owner_session_id,
                        owner_id,
                        ttl_secs,
                        wait_seconds,
                        reply_source,
                        reply_destination,
                        channel,
                        route_family,
                        now,
                    },
                ),
            }
        };

        if let Some(state) = acquired_state.as_ref() {
            self.track_session_lease(owner_session_id, &key);
            self.upsert_admin_lease(&key, state);
        }

        if matches!(response, LeaseResponse::Queued { .. }) {
            self.refresh_metrics_gauges();
        }

        response
    }

    fn prepare_acquire_key(&self, key: &crate::domains::lease::protocol::LeaseKey, now: Instant) {
        let (expired_state, lease_exists) = {
            let mut leases = self.core.leases.lock();
            match leases.get(key) {
                Some(state) if state.expiry <= now => (leases.remove(key), false),
                Some(_) => (None, true),
                None => (None, false),
            }
        };

        if let Some(state) = expired_state {
            self.untrack_session_lease(state.owner_session_id, key);
            self.remove_admin_lease(key);
            self.notify_lease_change(key);
            self.grant_next_waiter_if_available(key, now);
            return;
        }

        if !lease_exists && self.pending_waiter_count(key) > 0 {
            self.grant_next_waiter_if_available(key, now);
        }
    }

    fn queue_acquire_waiter(
        &self,
        key: &crate::domains::lease::protocol::LeaseKey,
        request: QueuedAcquireRequest,
    ) -> crate::domains::lease::protocol::LeaseResponse {
        use crate::domains::lease::protocol::LeaseResponse;

        let QueuedAcquireRequest {
            current_owner,
            owner_session_id,
            owner_id,
            ttl_secs,
            wait_seconds,
            reply_source,
            reply_destination,
            channel,
            route_family,
            now,
        } = request;

        let Some(reply_destination) = reply_destination else {
            return LeaseResponse::HeldByOther { current_owner };
        };

        let mut pending_acquires = self.core.pending_acquires.lock();
        if let Some(queue) = pending_acquires.get(key) {
            if let Some(existing) = queue.iter().find(|waiter| waiter.owner_id == owner_id) {
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
                channel,
                route_family,
                queued_token,
                ttl_secs,
                expires_at: now + Duration::from_secs(u64::from(wait_seconds)),
            });
        drop(pending_acquires);

        self.track_session_waiter(owner_session_id, key, queued_token);

        LeaseResponse::Queued {
            fencing_token: queued_token,
        }
    }

    pub(super) fn handle_extend(
        &self,
        key: &crate::domains::lease::protocol::LeaseKey,
        owner_id: &str,
        fencing_token: u64,
        ttl_secs: u64,
    ) -> crate::domains::lease::protocol::LeaseResponse {
        use crate::domains::lease::protocol::LeaseResponse;
        use std::time::{Duration, Instant};

        let now = Instant::now();
        let ttl = Duration::from_secs(ttl_secs);
        let mut updated_state = None;
        let mut expired_state = None;

        let response = {
            let mut leases = self.core.leases.lock();

            match leases.get(key).cloned() {
                None => LeaseResponse::NotHeld,
                Some(state) if state.expiry <= now => {
                    leases.remove(key);
                    expired_state = Some(state);
                    LeaseResponse::Expired
                }
                Some(state) if state.owner_id != owner_id => LeaseResponse::NotHeld,
                Some(state) if state.fencing_token != fencing_token => {
                    self.counter_inc("fitz_lease_invalid_token_rejects_total");
                    LeaseResponse::Fenced {
                        current_token: state.fencing_token,
                    }
                }
                Some(_) => {
                    let new_token = self.next_fencing_token();
                    if let Some(state) = leases.get_mut(key) {
                        state.expiry = now + ttl;
                        state.fencing_token = new_token;
                        state.renewals = state.renewals.saturating_add(1);
                        updated_state = Some(state.clone());
                    }
                    LeaseResponse::Extended {
                        fencing_token: new_token,
                    }
                }
            }
        };

        if let Some(state) = updated_state.as_ref() {
            self.counter_inc("fitz_lease_ownership_churn_total");
            self.upsert_admin_lease(key, state);
        }
        if let Some(state) = expired_state {
            self.untrack_session_lease(state.owner_session_id, key);
            self.remove_admin_lease(key);
            self.notify_lease_change(key);
            self.grant_next_waiter_if_available(key, now);
        }

        response
    }

    pub(super) fn handle_release(
        &self,
        key: &crate::domains::lease::protocol::LeaseKey,
        owner_id: &str,
        fencing_token: u64,
    ) -> crate::domains::lease::protocol::LeaseResponse {
        use crate::domains::lease::protocol::LeaseResponse;

        let now = Instant::now();
        let mut removed_state = None;

        let response = {
            let mut leases = self.core.leases.lock();

            match leases.get(key).cloned() {
                None => LeaseResponse::Released,
                Some(state) if state.expiry <= now => {
                    leases.remove(key);
                    removed_state = Some(state);
                    LeaseResponse::Released
                }
                Some(state) if state.owner_id != owner_id => LeaseResponse::NotHeld,
                Some(state) if state.fencing_token != fencing_token => {
                    self.counter_inc("fitz_lease_invalid_token_rejects_total");
                    LeaseResponse::Fenced {
                        current_token: state.fencing_token,
                    }
                }
                Some(state) => {
                    leases.remove(key);
                    removed_state = Some(state);
                    LeaseResponse::Released
                }
            }
        };

        if let Some(state) = removed_state {
            self.untrack_session_lease(state.owner_session_id, key);
            self.remove_admin_lease(key);
            self.notify_lease_change(key);
            self.grant_next_waiter_if_available(key, now);
        }

        response
    }

    pub(super) fn handle_query(
        &self,
        key: &crate::domains::lease::protocol::LeaseKey,
    ) -> crate::domains::lease::protocol::LeaseResponse {
        use crate::domains::lease::protocol::LeaseResponse;
        use std::time::Instant;

        let now = Instant::now();
        let leases = self.core.leases.lock();

        match leases.get(key) {
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
                        pending_waiters: self.pending_waiter_count(key),
                    }
                }
            }
        }
    }
}

#[cfg(test)]
fn test_protocol_channel_from_client(
    channel: crate::runtime::ClientChannel,
) -> crate::protocol::frame::ChannelId {
    match channel {
        crate::runtime::ClientChannel::Control => crate::protocol::frame::ChannelId::Control,
        crate::runtime::ClientChannel::Pub => crate::protocol::frame::ChannelId::Pub,
        crate::runtime::ClientChannel::Sub => crate::protocol::frame::ChannelId::Sub,
        crate::runtime::ClientChannel::Rpc => crate::protocol::frame::ChannelId::Rpc,
        crate::runtime::ClientChannel::Lease => crate::protocol::frame::ChannelId::Lease,
        crate::runtime::ClientChannel::Internal => crate::protocol::frame::ChannelId::Internal,
    }
}
