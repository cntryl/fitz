//! Lease acquire/extend/release/query business logic: authorization,
//! ownership mutation, and FIFO waiter queuing.

use super::model::{
    LeaseAcquireRequest, LeaseFamilyRuntime, PendingAcquire, QueuedAcquireRequest, SinkLeaseState,
    LEASE_MAX_QUEUE_DEPTH, LEASE_MAX_WAIT_SECONDS,
};
use chrono::Utc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

enum AcquireDecision {
    Respond(crate::domains::lease::protocol::LeaseResponse),
    Queue(QueuedAcquireRequest),
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

impl LeaseFamilyRuntime<'_> {
    pub(super) fn rollback_undeliverable_acquire(
        &mut self,
        key: &crate::domains::lease::protocol::LeaseKey,
        session_id: u64,
        response: &crate::domains::lease::protocol::LeaseResponse,
    ) {
        use crate::domains::lease::protocol::LeaseResponse;

        match response {
            LeaseResponse::Acquired { fencing_token } => {
                let removed = {
                    let leases = &mut self.core.leases;
                    if leases.get(key).is_some_and(|state| {
                        state.owner_session_id == session_id
                            && state.fencing_token == *fencing_token
                    }) {
                        leases.remove(key)
                    } else {
                        None
                    }
                };
                if removed.is_some() {
                    self.untrack_session_lease(session_id, key);
                    self.remove_admin_lease(key);
                    self.notify_lease_change(key);
                    let _ = self.advance_waiter_queue(key, Instant::now());
                    self.refresh_metrics_gauges();
                }
            }
            LeaseResponse::Queued { fencing_token } => {
                let removed = {
                    let pending = &mut self.core.pending_acquires;
                    let mut removed = false;
                    let mut empty = false;
                    if let Some(waiters) = pending.get_mut(key) {
                        if let Some(index) = waiters.iter().position(|waiter| {
                            waiter.owner_session_id == session_id
                                && waiter.queued_token == *fencing_token
                        }) {
                            waiters.remove(index);
                            removed = true;
                        }
                        empty = waiters.is_empty();
                    }
                    if empty {
                        pending.remove(key);
                    }
                    removed
                };
                if removed {
                    self.untrack_session_waiter(session_id, key, *fencing_token);
                    self.refresh_metrics_gauges();
                }
            }
            _ => {}
        }
    }

    fn apply_lease_effects(
        &mut self,
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

    pub(super) fn next_fencing_token(next_token: &AtomicU64) -> Option<u64> {
        next_token
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_add(1)
            })
            .ok()
    }

    pub(super) fn handle_acquire(
        &mut self,
        request: LeaseAcquireRequest,
    ) -> crate::domains::lease::protocol::LeaseResponse {
        use crate::domains::lease::protocol::LeaseResponse;

        let LeaseAcquireRequest {
            correlation,
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
            let leases = &mut self.core.leases;

            match leases.get(&key) {
                None => {
                    let Some(token) = Self::next_fencing_token(&self.core.next_token) else {
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
                    correlation,
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
            // A previously unowned lease just gained a holder: fleet
            // observers watching this route (exact or wildcard) need to see
            // the new membership, per routing-design.md §7.5.
            self.notify_lease_change(&key);
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
        &mut self,
        key: &crate::domains::lease::protocol::LeaseKey,
        now: Instant,
    ) -> bool {
        let (expired_state, lease_exists) = {
            let leases = &mut self.core.leases;
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
        &mut self,
        key: &crate::domains::lease::protocol::LeaseKey,
        request: QueuedAcquireRequest,
    ) -> crate::domains::lease::protocol::LeaseResponse {
        use crate::domains::lease::protocol::LeaseResponse;

        let QueuedAcquireRequest {
            correlation,
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

        let pending_acquires = &mut self.core.pending_acquires;
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

        let Some(queued_token) = Self::next_fencing_token(&self.core.next_token) else {
            return LeaseResponse::Error("fencing token space exhausted".to_string());
        };
        pending_acquires
            .entry(key.clone())
            .or_default()
            .push_back(PendingAcquire {
                correlation,
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
        let _ = pending_acquires;

        self.track_session_waiter(owner_session_id, key, queued_token);

        LeaseResponse::Queued {
            fencing_token: queued_token,
        }
    }

    pub(super) fn handle_extend(
        &mut self,
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
            let leases = &mut self.core.leases;

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
                    let Some(new_token) = Self::next_fencing_token(&self.core.next_token) else {
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
        &mut self,
        key: &crate::domains::lease::protocol::LeaseKey,
        owner_id: &str,
        fencing_token: u64,
    ) -> crate::domains::lease::protocol::LeaseResponse {
        use crate::domains::lease::protocol::LeaseResponse;

        let now = Instant::now();
        let mut effects = LeaseEffects::default();

        let response = {
            let leases = &mut self.core.leases;

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
        &mut self,
        key: &crate::domains::lease::protocol::LeaseKey,
    ) -> crate::domains::lease::protocol::LeaseResponse {
        use crate::domains::lease::protocol::LeaseResponse;
        use std::time::Instant;

        let now = Instant::now();
        let leases = &mut self.core.leases;

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
mod tests;
