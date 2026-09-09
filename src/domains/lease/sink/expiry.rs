//! TTL expiry: reaping timed-out waiters and expired leases, and advancing
//! each key's FIFO wait queue once it becomes free.

use super::model::{LeaseFamilyRuntime, PendingAcquire, SinkLeaseState};
use chrono::Utc;
use std::collections::VecDeque;
use std::time::Instant;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum WaiterProgress {
    Unchanged,
    Expired,
    Consumed,
}

impl WaiterProgress {
    pub(super) const fn changed(self) -> bool {
        !matches!(self, Self::Unchanged)
    }
}

fn drain_expired_waiters(
    queue: &mut VecDeque<PendingAcquire>,
    now: Instant,
) -> Vec<PendingAcquire> {
    let mut expired = Vec::new();
    let mut index = 0;
    while index < queue.len() {
        if queue[index].expires_at <= now {
            expired.push(queue.remove(index).expect("indexed waiter must exist"));
        } else {
            index += 1;
        }
    }
    expired
}

impl LeaseFamilyRuntime<'_> {
    pub(in crate::domains::lease::sink) fn expire_timed_out_waiters_for_key(
        &mut self,
        key: &crate::domains::lease::protocol::LeaseKey,
        now: Instant,
    ) -> usize {
        let expired_waiters = {
            let pending_acquires = &mut self.core.pending_acquires;
            let mut expired = Vec::new();
            let mut remove_queue = false;

            if let Some(queue) = pending_acquires.get_mut(key) {
                expired = drain_expired_waiters(queue, now);
                remove_queue = queue.is_empty();
            }

            if remove_queue {
                pending_acquires.remove(key);
            }

            expired
        };

        let expired_count = expired_waiters.len();
        for waiter in expired_waiters {
            self.untrack_session_waiter(waiter.owner_session_id, key, waiter.queued_token);
            self.counter_inc("fitz_lease_acquire_timeouts_total");
            let _ = self.send_waiter_response(
                &waiter,
                &crate::domains::lease::protocol::LeaseResponse::Timeout,
            );
        }

        expired_count
    }

    /// Reads the queue depth while preserving FIFO order for the key.
    pub(in crate::domains::lease::sink) fn pending_waiter_count(
        &mut self,
        key: &crate::domains::lease::protocol::LeaseKey,
    ) -> usize {
        self.core.pending_acquires.get(key).map_or(0, VecDeque::len)
    }

    /// Removes expired waiters and grants the oldest eligible waiter when the key is free.
    pub(super) fn advance_waiter_queue(
        &mut self,
        key: &crate::domains::lease::protocol::LeaseKey,
        now: Instant,
    ) -> WaiterProgress {
        let expired_waiter_count = self.expire_timed_out_waiters_for_key(key, now);

        let granted_waiter = {
            let pending_acquires = &mut self.core.pending_acquires;
            let leases = &mut self.core.leases;

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
                    let state = Self::lease_expiry(now, waiter.ttl_secs).ok().map(|expiry| {
                        SinkLeaseState {
                            owner_id: waiter.owner_id.clone(),
                            owner_session_id: waiter.owner_session_id,
                            fencing_token: waiter.queued_token,
                            expiry,
                            acquired_at: Utc::now().to_rfc3339(),
                            renewals: 0,
                        }
                    });
                    if let Some(state) = state.as_ref() {
                        leases.insert(key.clone(), state.clone());
                    }
                    (waiter, state)
                })
            }
        };

        match granted_waiter {
            Some((waiter, Some(state))) => {
                self.untrack_session_waiter(waiter.owner_session_id, key, waiter.queued_token);
                self.track_session_lease(waiter.owner_session_id, key);
                self.upsert_admin_lease(key, &state);
                // A queued waiter just became the live holder: notify fleet
                // observers before attempting delivery to the waiter itself,
                // matching the immediate-acquisition notify in acquire.rs.
                self.notify_lease_change(key);
                let delivered = self.send_waiter_response(
                    &waiter,
                    &crate::domains::lease::protocol::LeaseResponse::Acquired {
                        fencing_token: waiter.queued_token,
                    },
                );
                if !delivered {
                    self.core.leases.remove(key);
                    self.untrack_session_lease(waiter.owner_session_id, key);
                    self.remove_admin_lease(key);
                    self.notify_lease_change(key);
                    // A disconnected waiter must not hold up the next live
                    // waiter until the periodic sweep. Queue depth is bounded
                    // by `LEASE_MAX_QUEUE_DEPTH`, so advancing here remains
                    // bounded even when several consecutive grants fail.
                    let _ = self.advance_waiter_queue(key, now);
                }
                return WaiterProgress::Consumed;
            }
            Some((waiter, None)) => {
                self.untrack_session_waiter(waiter.owner_session_id, key, waiter.queued_token);
                let _ = self.send_waiter_response(
                    &waiter,
                    &crate::domains::lease::protocol::LeaseResponse::Error(
                        "queued ttl_secs exceeds the supported lease duration".to_string(),
                    ),
                );
                return WaiterProgress::Consumed;
            }
            None => {}
        }

        if expired_waiter_count > 0 {
            WaiterProgress::Expired
        } else {
            WaiterProgress::Unchanged
        }
    }

    /// Reaps waiters before leases, then advances each newly available FIFO queue.
    pub(crate) fn sweep_expired_state(&mut self) {
        let now = Instant::now();

        let expired_waiters = {
            let pending_acquires = &mut self.core.pending_acquires;
            let mut expired = Vec::new();
            let mut empty_keys = Vec::new();

            for (key, queue) in pending_acquires.iter_mut() {
                expired.extend(
                    drain_expired_waiters(queue, now)
                        .into_iter()
                        .map(|waiter| (key.clone(), waiter)),
                );

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
            self.untrack_session_waiter(waiter.owner_session_id, &key, waiter.queued_token);
            let _ = self.send_waiter_response(
                &waiter,
                &crate::domains::lease::protocol::LeaseResponse::Timeout,
            );
        }

        let expired_leases = {
            let leases = &mut self.core.leases;
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
            let _ = self.advance_waiter_queue(&key, now);
        }

        let queued_keys = self
            .core
            .pending_acquires
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for key in queued_keys {
            let _ = self.advance_waiter_queue(&key, now);
        }

        self.sweep_idle_list_snapshots();
        self.refresh_metrics_gauges();
    }
}
