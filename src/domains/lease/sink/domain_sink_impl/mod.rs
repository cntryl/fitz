use super::model::{
    Duration, Instant, LeaseAcquireRequest, LeaseDomainRuntime, Ordering, PendingAcquire,
    QueuedAcquireRequest, SinkLeaseState, Utc, LEASE_MAX_QUEUE_DEPTH, LEASE_MAX_WAIT_SECONDS,
};
use crate::domains::subscription_state::RoutedSubscriptionSet;

mod expiry;
mod routing;
mod waiter_tracking;

enum AcquireDecision {
    Respond(crate::domains::lease::protocol::LeaseResponse),
    Queue(QueuedAcquireRequest),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WaiterProgress {
    Unchanged,
    Expired,
    Consumed,
}

#[derive(Clone, Copy)]
enum DeliveryDropKind {
    Response,
    Notification,
}

impl WaiterProgress {
    const fn changed(self) -> bool {
        !matches!(self, Self::Unchanged)
    }
}

impl DeliveryDropKind {
    const fn label(self) -> &'static str {
        match self {
            Self::Response => "response",
            Self::Notification => "notification",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LeaseAuthorization {
    Missing,
    Expired,
    NotOwner,
    Fenced(u64),
    Authorized,
}

#[derive(Default)]
struct LeaseEffects {
    updated: Option<SinkLeaseState>,
    removed: Option<SinkLeaseState>,
    record_ownership_churn: bool,
}

fn authorize_owned_lease(
    state: Option<&SinkLeaseState>,
    owner_id: &str,
    fencing_token: u64,
    now: Instant,
) -> LeaseAuthorization {
    let Some(state) = state else {
        return LeaseAuthorization::Missing;
    };
    if state.expiry <= now {
        LeaseAuthorization::Expired
    } else if state.owner_id != owner_id {
        LeaseAuthorization::NotOwner
    } else if state.fencing_token != fencing_token {
        LeaseAuthorization::Fenced(state.fencing_token)
    } else {
        LeaseAuthorization::Authorized
    }
}

#[cfg(test)]
mod tests;

impl LeaseDomainRuntime<'_> {
    fn apply_lease_effects(
        &self,
        key: &crate::domains::lease::protocol::LeaseKey,
        now: Instant,
        effects: LeaseEffects,
    ) {
        if let Some(state) = effects.updated.as_ref() {
            if effects.record_ownership_churn {
                self.counter_inc("fitz_lease_ownership_churn_total");
            }
            self.upsert_admin_lease(key, state);
        }
        if let Some(state) = effects.removed {
            self.untrack_session_lease(state.owner_session_id, key);
            self.remove_admin_lease(key);
            self.notify_lease_change(key);
            let _ = self.advance_waiter_queue(key, now);
            self.refresh_metrics_gauges();
        }
    }

    /// Drops session waiters before ownership and grants released keys in FIFO order.
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
            let _ = self.advance_waiter_queue(key, now);
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

    pub(super) fn unsubscribe_all(&self, session_id: u64) -> usize {
        let mut families = self.core.families.lock();
        let mut removed = 0;
        for (family_id, state) in families.iter_mut() {
            removed += state.remove_session(
                crate::runtime::routing::RouteFamily::try_from(*family_id)
                    .expect("lease family IDs originate from RouteFamily"),
                session_id,
            );
        }
        families.retain(|_, state| !state.is_empty());
        removed
    }

    pub(super) fn next_fencing_token(&self) -> Option<u64> {
        self.core
            .next_token
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_add(1)
            })
            .ok()
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
        let expiry = match Self::lease_expiry(now, ttl_secs) {
            Ok(expiry) => expiry,
            Err(response) => return response,
        };

        if wait_seconds > LEASE_MAX_WAIT_SECONDS {
            return LeaseResponse::Error(format!(
                "wait_seconds must not exceed {LEASE_MAX_WAIT_SECONDS}"
            ));
        }

        let prepared_count_changed = self.prepare_acquire_key(&key, now);

        let mut acquired_state = None;
        let decision = {
            let mut leases = self.core.leases.lock();

            match leases.get(&key) {
                None => {
                    let Some(token) = self.next_fencing_token() else {
                        return LeaseResponse::Error("fencing token space exhausted".to_string());
                    };
                    let state = SinkLeaseState {
                        owner_id,
                        owner_session_id,
                        fencing_token: token,
                        expiry,
                        acquired_at: Utc::now().to_rfc3339(),
                        renewals: 0,
                    };
                    leases.insert(key.clone(), state.clone());
                    acquired_state = Some(state);
                    AcquireDecision::Respond(LeaseResponse::Acquired {
                        fencing_token: token,
                    })
                }
                Some(state) if state.owner_id == owner_id => {
                    AcquireDecision::Respond(LeaseResponse::AlreadyHeld {
                        fencing_token: state.fencing_token,
                    })
                }
                Some(state) if wait_seconds == 0 => {
                    AcquireDecision::Respond(LeaseResponse::HeldByOther {
                        current_owner: state.owner_id.clone(),
                    })
                }
                Some(state) => AcquireDecision::Queue(QueuedAcquireRequest {
                    current_owner: state.owner_id.clone(),
                    owner_session_id,
                    owner_id,
                    ttl_secs,
                    wait_seconds,
                    reply_source,
                    reply_destination,
                    channel,
                    route_family,
                    now,
                }),
            }
        };
        let response = match decision {
            AcquireDecision::Respond(response) => response,
            AcquireDecision::Queue(request) => self.queue_acquire_waiter(&key, request),
        };

        if let Some(state) = acquired_state.as_ref() {
            self.track_session_lease(owner_session_id, &key);
            self.upsert_admin_lease(&key, state);
        }

        if prepared_count_changed
            || acquired_state.is_some()
            || matches!(response, LeaseResponse::Queued { .. })
        {
            self.refresh_metrics_gauges();
        }

        response
    }

    fn prepare_acquire_key(
        &self,
        key: &crate::domains::lease::protocol::LeaseKey,
        now: Instant,
    ) -> bool {
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
            let _ = self.advance_waiter_queue(key, now);
            return true;
        }

        if !lease_exists && self.pending_waiter_count(key) > 0 {
            return self.advance_waiter_queue(key, now).changed();
        }

        false
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

        let Some(queued_token) = self.next_fencing_token() else {
            return LeaseResponse::Error("fencing token space exhausted".to_string());
        };
        pending_acquires
            .entry(key.clone())
            .or_default()
            .push_back(PendingAcquire {
                owner_session_id,
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
        use std::time::Instant;

        let now = Instant::now();
        let expiry = match Self::lease_expiry(now, ttl_secs) {
            Ok(expiry) => expiry,
            Err(response) => return response,
        };
        let mut effects = LeaseEffects::default();

        let response = {
            let mut leases = self.core.leases.lock();

            match authorize_owned_lease(leases.get(key), owner_id, fencing_token, now) {
                LeaseAuthorization::Missing | LeaseAuthorization::NotOwner => {
                    LeaseResponse::NotHeld
                }
                LeaseAuthorization::Expired => {
                    effects.removed = leases.remove(key);
                    LeaseResponse::Expired
                }
                LeaseAuthorization::Fenced(current_token) => {
                    self.counter_inc("fitz_lease_invalid_token_rejects_total");
                    LeaseResponse::Fenced { current_token }
                }
                LeaseAuthorization::Authorized => {
                    let Some(new_token) = self.next_fencing_token() else {
                        return LeaseResponse::Error("fencing token space exhausted".to_string());
                    };
                    if let Some(state) = leases.get_mut(key) {
                        state.expiry = expiry;
                        state.fencing_token = new_token;
                        state.renewals = state.renewals.saturating_add(1);
                        effects.updated = Some(state.clone());
                        effects.record_ownership_churn = true;
                    }
                    LeaseResponse::Extended {
                        fencing_token: new_token,
                    }
                }
            }
        };

        self.apply_lease_effects(key, now, effects);

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
        let mut effects = LeaseEffects::default();

        let response = {
            let mut leases = self.core.leases.lock();

            match authorize_owned_lease(leases.get(key), owner_id, fencing_token, now) {
                LeaseAuthorization::Missing => LeaseResponse::Released,
                LeaseAuthorization::Expired | LeaseAuthorization::Authorized => {
                    effects.removed = leases.remove(key);
                    LeaseResponse::Released
                }
                LeaseAuthorization::NotOwner => LeaseResponse::NotHeld,
                LeaseAuthorization::Fenced(current_token) => {
                    self.counter_inc("fitz_lease_invalid_token_rejects_total");
                    LeaseResponse::Fenced { current_token }
                }
            }
        };

        self.apply_lease_effects(key, now, effects);

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
) -> crate::dispatch::protocol::frame::ChannelId {
    match channel {
        crate::runtime::ClientChannel::Control => {
            crate::dispatch::protocol::frame::ChannelId::Control
        }
        crate::runtime::ClientChannel::Pub => crate::dispatch::protocol::frame::ChannelId::Pub,
        crate::runtime::ClientChannel::Sub => crate::dispatch::protocol::frame::ChannelId::Sub,
        crate::runtime::ClientChannel::Rpc => crate::dispatch::protocol::frame::ChannelId::Rpc,
        crate::runtime::ClientChannel::Lease => crate::dispatch::protocol::frame::ChannelId::Lease,
        crate::runtime::ClientChannel::Internal => {
            crate::dispatch::protocol::frame::ChannelId::Internal
        }
    }
}
