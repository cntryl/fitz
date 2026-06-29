//! LeaseActor: manages ephemeral lease state inside one broker process
//!
//! Each lease has:
//! - Identity: (realm, area, resource) from route
//! - Current owner
//! - Fencing token (monotonically increasing within the process)
//! - Expiration time (TTL-based)
//!
//! This actor models single-broker coordination only. Restart clears lease
//! ownership and resets fencing-token lineage, so neither state nor tokens are
//! meaningful across processes or nodes.
//!
//! # Invariants
//!
//! 1. **Exclusive ownership**: At most one owner per lease at any time
//! 2. **Monotonic tokens**: Fencing tokens never decrease within a running process, but restart resets the lineage
//! 3. **Expiration semantics**: Lease with expiry <= now_instant() is expired and can be taken
//! 4. **Idempotency**: Same operation by same owner produces same result
//!
//! # State Model
//!
//! ```text
//! [UNOWNED] --acquire--> [OWNED]
//!     ^                      |
//!     |--release/expire------|
//!     |                      |
//!     |--acquire(other)------|  (if expired)
//! ```

use super::protocol::{LeaseKey, LeaseMessage, LeaseResponse};
use crate::runtime::actor::{Actor, Context};
use crate::runtime::clock::{Clock, SystemClock};
use crate::runtime::context::TimerId;
use crate::runtime::routing::RouteAddress;
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

/// Pending lease acquisition waiting for lease to become available
#[derive(Debug, Clone)]
struct PendingAcquire {
    /// Owner requesting the lease
    owner_id: String,

    /// Timer ID for the timeout (for cancellation)
    timer_id: TimerId,

    /// Route address of the requester (for sending reply back)
    source: Option<RouteAddress>,

    /// Provisional fencing token reserved at enqueue time
    queued_token: u64,

    /// TTL duration for the lease when/if acquired
    ttl_secs: u64,
}

/// State of a single lease
#[derive(Debug, Clone)]
struct LeaseState {
    /// Current owner of the lease
    owner_id: String,

    /// Monotonically increasing fencing token
    fencing_token: u64,

    /// Absolute expiration time
    expiry: Instant,
}

impl LeaseState {
    /// Check if this lease is expired at the given time
    fn is_expired(&self, now: Instant) -> bool {
        self.expiry <= now
    }

    /// Check if this lease is held by the given owner
    fn is_held_by(&self, owner_id: &str) -> bool {
        self.owner_id == owner_id
    }
}

/// Lease actor managing a collection of leases
///
/// Each lease actor is responsible for a shard of the lease namespace within
/// the current broker process. In a multi-actor deployment, leases are
/// partitioned across actors by `(family_id, route)` tuple inside that same
/// process; this is not a durable or cross-node lease service.
///
/// # State
///
/// - `family`: RouteFamily this actor serves (for validation)
/// - `leases`: Map of (RouteFamily, Route) → LeaseState
/// - `pending_acquires`: Map of LeaseKey → VecDeque of waiting acquirers (FIFO queue)
/// - `timer_to_waiter`: Map of TimerId → (LeaseKey) for validating stale timers
/// - `next_token`: Process-local token counter (resets on restart)
/// - `clock`: Time source for expiration checks
/// - `max_wait_seconds`: Maximum wait time allowed (capped to prevent DoS)
/// - `max_queue_depth`: Maximum pending acquirers per lease (capped to prevent memory bloat)
pub struct LeaseActor {
    /// Route family this actor serves (for validation)
    family: crate::runtime::routing::RouteFamily,

    /// Map of LeaseKey to current lease state
    leases: HashMap<LeaseKey, LeaseState>,

    /// Map of LeaseKey to pending acquire queue (FIFO)
    pending_acquires: HashMap<LeaseKey, VecDeque<PendingAcquire>>,

    /// Map of TimerId to LeaseKey for timer validation
    timer_to_waiter: HashMap<TimerId, LeaseKey>,

    /// Next fencing token to issue within the current process
    next_token: u64,

    /// Clock for time-based operations
    clock: Box<dyn Clock>,

    /// Maximum wait time in seconds (default 30)
    max_wait_seconds: u32,

    /// Maximum pending acquirers per lease (default 100)
    max_queue_depth: usize,

    /// Pending availability notifications (for debouncing)
    pending_publish: Vec<crate::runtime::DomainPublishEvent>,

    /// Timer ID for debounced availability notifications
    notify_timer: Option<TimerId>,
}

impl LeaseActor {
    /// Create a new lease actor with system clock
    pub fn new(family: crate::runtime::routing::RouteFamily) -> Self {
        Self::with_clock(family, Box::new(SystemClock))
    }

    /// Create a new lease actor with a custom clock (for testing)
    pub fn with_clock(family: crate::runtime::routing::RouteFamily, clock: Box<dyn Clock>) -> Self {
        Self {
            family,
            leases: HashMap::new(),
            pending_acquires: HashMap::new(),
            timer_to_waiter: HashMap::new(),
            next_token: 1,
            clock,
            max_wait_seconds: 30,
            max_queue_depth: 100,
            pending_publish: Vec::new(),
            notify_timer: None,
        }
    }

    /// Create a new lease actor with custom configuration (for testing)
    pub fn with_config(
        family: crate::runtime::routing::RouteFamily,
        clock: Box<dyn Clock>,
        max_wait_seconds: u32,
        max_queue_depth: usize,
    ) -> Self {
        Self {
            family,
            leases: HashMap::new(),
            pending_acquires: HashMap::new(),
            timer_to_waiter: HashMap::new(),
            next_token: 1,
            clock,
            max_wait_seconds,
            max_queue_depth,
            pending_publish: Vec::new(),
            notify_timer: None,
        }
    }

    /// Handle a lease message and return an optional response.
    ///
    /// Returns None for messages that should not emit a reply (e.g., Tick)
    /// or when the message is misrouted.
    pub fn handle_message(
        &mut self,
        msg: LeaseMessage,
        ctx: &mut Context<LeaseActor>,
    ) -> Option<LeaseResponse> {
        let response = match msg {
            LeaseMessage::Acquire {
                family_id,
                route,
                owner_id,
                ttl_secs,
                wait_seconds,
            } => {
                if family_id != self.family {
                    return None;
                }
                match LeaseKey::from_route(family_id, &route) {
                    Some(key) => {
                        let source = ctx
                            .current_metadata()
                            .as_ref()
                            .and_then(|m| m.source.clone());

                        self.handle_acquire(key, owner_id, ttl_secs, wait_seconds, source, ctx)
                    }
                    None => LeaseResponse::NotFound,
                }
            }
            LeaseMessage::Extend {
                family_id,
                route,
                owner_id,
                fencing_token,
                ttl_secs,
            } => {
                if family_id != self.family {
                    return None;
                }
                match LeaseKey::from_route(family_id, &route) {
                    Some(key) => self.handle_extend(key, owner_id, fencing_token, ttl_secs, ctx),
                    None => LeaseResponse::NotFound,
                }
            }
            LeaseMessage::Release {
                family_id,
                route,
                owner_id,
                fencing_token,
            } => {
                if family_id != self.family {
                    return None;
                }
                match LeaseKey::from_route(family_id, &route) {
                    Some(key) => {
                        let response = self.handle_release(key.clone(), owner_id, fencing_token);

                        if matches!(response, LeaseResponse::Released) {
                            // Schedule debounced availability notification on the concrete lease route
                            let route = key.to_route();
                            self.schedule_availability_notification(ctx, route);
                            self.grant_next_waiter(&key, ctx);
                        }

                        response
                    }
                    None => LeaseResponse::NotFound,
                }
            }
            LeaseMessage::Query { family_id, route } => {
                if family_id != self.family {
                    return None;
                }
                match LeaseKey::from_route(family_id, &route) {
                    Some(key) => self.handle_query(key),
                    None => LeaseResponse::NotFound,
                }
            }
            LeaseMessage::Tick => {
                self.expire_old_leases(ctx);
                return None;
            }
        };

        Some(response)
    }

    /// Allocate and return the next fencing token
    fn next_fencing_token(&mut self) -> u64 {
        let token = self.next_token;
        self.next_token += 1;
        token
    }

    /// Handle lease acquisition
    /// Handle lease acquisition with optional waiting
    ///
    /// If the lease is available, returns Acquired immediately.
    /// If the lease is unavailable and wait_seconds > 0, queues the request
    /// and schedules a timeout, returning Queued (deferred response).
    /// If the lease is unavailable and wait_seconds = 0, returns HeldByOther immediately.
    fn handle_acquire(
        &mut self,
        key: LeaseKey,
        owner_id: String,
        ttl_secs: u64,
        wait_seconds: u32,
        source: Option<RouteAddress>,
        ctx: &mut Context<LeaseActor>,
    ) -> LeaseResponse {
        let now = self.clock.now_instant();
        let ttl = Duration::from_secs(ttl_secs);

        // Validate wait_seconds against max limit
        if wait_seconds > self.max_wait_seconds {
            return LeaseResponse::Timeout; // Or return error; spec says reject
        }

        // Expire the lease if needed and promote queued waiters before new acquires.
        self.expire_lease_if_needed(&key, now, ctx);

        // If the lease is currently unowned but a queue exists, promote the next waiter first.
        if !self.leases.contains_key(&key) {
            self.grant_next_waiter(&key, ctx);
        }

        // Check if lease is available
        if !self.leases.contains_key(&key) {
            // Lease doesn't exist or is expired - grant it immediately
            let token = self.next_fencing_token();
            let expiry = now + ttl;

            self.leases.insert(
                key,
                LeaseState {
                    owner_id: owner_id.clone(),
                    fencing_token: token,
                    expiry,
                },
            );

            LeaseResponse::Acquired {
                fencing_token: token,
            }
        } else {
            // Lease exists and is not expired
            let state = &self.leases[&key];
            if state.is_held_by(&owner_id) {
                // Idempotent - already held by this owner
                LeaseResponse::AlreadyHeld {
                    fencing_token: state.fencing_token,
                }
            } else if wait_seconds == 0 {
                // Held by another owner and no wait - reject immediately
                LeaseResponse::HeldByOther {
                    current_owner: state.owner_id.clone(),
                }
            } else {
                // Held by another owner and willing to wait
                self.enqueue_waiter(key, owner_id, ttl_secs, wait_seconds, source, ctx)
            }
        }
    }

    /// Enqueue a waiter for a lease that's currently held by another owner
    ///
    /// Schedules a timeout and returns Queued response (deferred).
    /// For backward compatibility, returns an immediate Queued response as a provisional token.
    fn enqueue_waiter(
        &mut self,
        key: LeaseKey,
        owner_id: String,
        ttl_secs: u64,
        wait_seconds: u32,
        source: Option<RouteAddress>,
        ctx: &mut Context<LeaseActor>,
    ) -> LeaseResponse {
        // Check if this owner is already waiting for this lease (idempotency)
        if let Some(queue) = self.pending_acquires.get(&key) {
            if let Some(existing) = queue.iter().find(|p| p.owner_id == owner_id) {
                // Already queued - return AlreadyQueued with existing token
                return LeaseResponse::AlreadyQueued {
                    fencing_token: existing.queued_token,
                };
            }
        }

        // Check queue depth limit
        if let Some(queue) = self.pending_acquires.get(&key) {
            if queue.len() >= self.max_queue_depth {
                return LeaseResponse::QueueFull {
                    pending_count: queue.len(),
                };
            }
        }

        // Schedule timeout
        let timeout_duration = Duration::from_secs(wait_seconds as u64);
        let timer_id = ctx.timer_manager().schedule_once(timeout_duration);

        // Enqueue waiter
        let queued_token = self.next_fencing_token();
        let pending = PendingAcquire {
            owner_id,
            timer_id,
            source,
            queued_token,
            ttl_secs,
        };

        // Add to pending queue
        self.pending_acquires
            .entry(key.clone())
            .or_default()
            .push_back(pending);

        // Track timer for validation
        self.timer_to_waiter.insert(timer_id, key);

        // Return Queued response with the provisional token
        LeaseResponse::Queued {
            fencing_token: queued_token,
        }
    }

    /// Grant the next waiter in the pending queue for a lease (if any)
    ///
    /// Called after release or natural expiration.
    /// Picks the first (FIFO) waiter, assigns the lease,  cancels the timer,
    /// and sends the response.
    fn grant_next_waiter(&mut self, key: &LeaseKey, ctx: &mut Context<LeaseActor>) {
        // Pop waiter first (to avoid double borrow)
        let waiter = match self.pending_acquires.get_mut(key) {
            None => return,
            Some(q) if q.is_empty() => return,
            Some(q) => q.pop_front(),
        };

        if let Some(waiter) = waiter {
            // Now we can call methods on self without borrow conflicts
            // Cancel the timeout timer
            ctx.timer_manager().cancel(waiter.timer_id);
            self.timer_to_waiter.remove(&waiter.timer_id);

            // Generate new fencing token
            let token = waiter.queued_token;
            let now = self.clock.now_instant();
            let expiry = now + Duration::from_secs(waiter.ttl_secs);

            // Insert into leases
            self.leases.insert(
                key.clone(),
                LeaseState {
                    owner_id: waiter.owner_id.clone(),
                    fencing_token: token,
                    expiry,
                },
            );

            // Send response to waiter's source address
            if let Some(source) = waiter.source {
                let response = LeaseResponse::Acquired {
                    fencing_token: token,
                };

                // NOTE: We use send() directly because current_metadata is not set
                // This is the deferred reply path
                let _ = ctx.send(source, response).ok(); // Ignore send errors (best effort)
            }

            // Clean up empty queue
            if let Some(q) = self.pending_acquires.get(key) {
                if q.is_empty() {
                    self.pending_acquires.remove(key);
                }
            }
        }
    }

    /// Handle lease extension
    fn handle_extend(
        &mut self,
        key: LeaseKey,
        owner_id: String,
        fencing_token: u64,
        ttl_secs: u64,
        ctx: &mut Context<LeaseActor>,
    ) -> LeaseResponse {
        let now = self.clock.now_instant();
        let ttl = Duration::from_secs(ttl_secs);

        enum ExtendDecision {
            NotHeld,
            Expired,
            Fenced(u64),
            Extend,
        }

        let decision = match self.leases.get(&key) {
            None => ExtendDecision::NotHeld,
            Some(state) => {
                if state.is_expired(now) {
                    ExtendDecision::Expired
                } else if !state.is_held_by(&owner_id) {
                    ExtendDecision::NotHeld
                } else if state.fencing_token != fencing_token {
                    ExtendDecision::Fenced(state.fencing_token)
                } else {
                    ExtendDecision::Extend
                }
            }
        };

        match decision {
            ExtendDecision::NotHeld => LeaseResponse::NotHeld,
            ExtendDecision::Expired => {
                self.leases.remove(&key);
                self.schedule_availability_notification(ctx, key.to_route());
                self.grant_next_waiter(&key, ctx);
                LeaseResponse::Expired
            }
            ExtendDecision::Fenced(current_token) => LeaseResponse::Fenced { current_token },
            ExtendDecision::Extend => {
                let new_token = self.next_fencing_token();
                if let Some(state) = self.leases.get_mut(&key) {
                    state.expiry = now + ttl;
                    state.fencing_token = new_token;
                }
                LeaseResponse::Extended {
                    fencing_token: new_token,
                }
            }
        }
    }

    /// Handle lease release
    fn handle_release(
        &mut self,
        key: LeaseKey,
        owner_id: String,
        fencing_token: u64,
    ) -> LeaseResponse {
        let now = self.clock.now_instant();

        let response = match self.leases.get(&key) {
            None => {
                // Idempotent delete: if lease doesn't exist, it's already released
                LeaseResponse::Released
            }
            Some(state) => {
                if state.is_expired(now) {
                    // Already expired - remove it and return success (idempotent)
                    self.leases.remove(&key);
                    LeaseResponse::Released
                } else if !state.is_held_by(&owner_id) {
                    LeaseResponse::NotHeld
                } else if state.fencing_token != fencing_token {
                    // Token mismatch - fencing
                    LeaseResponse::Fenced {
                        current_token: state.fencing_token,
                    }
                } else {
                    // Valid release - remove lease and set notification flag
                    self.leases.remove(&key);
                    LeaseResponse::Released
                }
            }
        };

        response
    }

    /// Handle lease query (for testing/debugging)
    fn handle_query(&self, key: LeaseKey) -> LeaseResponse {
        let now = self.clock.now_instant();

        match self.leases.get(&key) {
            None => LeaseResponse::NotFound,
            Some(state) => {
                if state.is_expired(now) {
                    LeaseResponse::Expired
                } else {
                    let expires_in = state.expiry.duration_since(now);
                    let pending_waiters = self
                        .pending_acquires
                        .get(&key)
                        .map(|q| q.len())
                        .unwrap_or(0);

                    LeaseResponse::Status {
                        owner_id: state.owner_id.clone(),
                        fencing_token: state.fencing_token,
                        expires_in_secs: expires_in.as_secs(),
                        pending_waiters,
                    }
                }
            }
        }
    }

    /// Schedule availability notification (debounced)
    ///
    /// Publishes to `lease://{realm}/{area}/{resource}/changed` when a lease
    /// is released or expires. Uses 25ms debounce to coalesce multiple events.
    fn schedule_availability_notification(
        &mut self,
        ctx: &mut Context<LeaseActor>,
        route: crate::runtime::routing::Route,
    ) {
        // Publish on the specific lease route (clients subscribe to the lease route itself)
        let event =
            crate::runtime::DomainPublishEvent::new(self.family, route, bytes::Bytes::new());
        if !self
            .pending_publish
            .iter()
            .any(|pending| pending.route == event.route)
        {
            self.pending_publish.push(event);
        }

        // Schedule timer for 25ms debounce
        if self.notify_timer.is_none() {
            self.notify_timer = Some(ctx.timer_manager().schedule_once(Duration::from_millis(25)));
        }
    }

    /// Remove an expired lease and promote queued waiters before new acquires.
    fn expire_lease_if_needed(
        &mut self,
        key: &LeaseKey,
        now: Instant,
        ctx: &mut Context<LeaseActor>,
    ) {
        let is_expired = self
            .leases
            .get(key)
            .map(|state| state.is_expired(now))
            .unwrap_or(false);

        if !is_expired {
            return;
        }

        self.leases.remove(key);
        // notify on the specific lease route
        let route = key.to_route();
        self.schedule_availability_notification(ctx, route);
        self.grant_next_waiter(key, ctx);
    }
}

impl Default for LeaseActor {
    fn default() -> Self {
        Self::new(crate::runtime::routing::RouteFamily::new(0))
    }
}

impl Actor for LeaseActor {
    type Message = LeaseMessage;

    fn receive(&mut self, msg: Self::Message, ctx: &mut Context<Self>) {
        if let Some(response) = self.handle_message(msg, ctx) {
            let _ = ctx.reply(response).ok();
        }
    }

    /// Handle timer callback for waiter timeouts and debounced notifications
    ///
    /// Called when a waiting acquire request times out or when the debounce
    /// timer for availability notifications fires.
    fn on_timer(&mut self, timer_id: crate::runtime::context::TimerId, ctx: &mut Context<Self>) {
        // Check if this is the availability notification timer
        if Some(timer_id) == self.notify_timer {
            self.notify_timer = None;
            let events = std::mem::take(&mut self.pending_publish);
            for event in events {
                ctx.publish_event(event).ok();
            }
            return;
        }

        // Otherwise, it's a waiter timeout
        // Look up which lease this timer belongs to
        let lease_key = match self.timer_to_waiter.get(&timer_id) {
            Some(key) => key.clone(),
            None => return, // Stale timer; waiter already removed
        };

        // Find and remove the waiter from the queue
        let queue = match self.pending_acquires.get_mut(&lease_key) {
            None => return,
            Some(q) => q,
        };

        // Find the waiter with this timer_id
        let waiter_idx = queue.iter().position(|w| w.timer_id == timer_id);
        if let Some(idx) = waiter_idx {
            let waiter = queue.remove(idx).unwrap();

            // Clean up timer tracking
            self.timer_to_waiter.remove(&timer_id);

            // Send Timeout response to the waiter
            if let Some(source) = waiter.source {
                let response = LeaseResponse::Timeout;
                let _ = ctx.send(source, response).ok(); // Best effort
            }

            // Clean up empty queue
            if queue.is_empty() {
                self.pending_acquires.remove(&lease_key);
            }
        }
    }
}

impl LeaseActor {
    /// Proactively expire old leases (called on Tick)
    ///
    /// This removes expired leases from state without waiting for
    /// them to be accessed. Enables runtime-driven expiration.
    fn expire_old_leases(&mut self, ctx: &mut Context<LeaseActor>) {
        let now = self.clock.now_instant();
        let expired_keys: Vec<LeaseKey> = self
            .leases
            .iter()
            .filter(|(_, state)| state.is_expired(now))
            .map(|(k, _)| k.clone())
            .collect();

        // Remove expired leases and schedule notification if any expired
        for key in &expired_keys {
            self.leases.remove(key);
        }

        if !expired_keys.is_empty() {
            // Schedule debounced availability notification for each expired lease
            // (debounce logic will coalesce notifications if they happen close together)
            for key in &expired_keys {
                let route = key.to_route();
                self.schedule_availability_notification(ctx, route);
            }
        }

        // Grant next waiter for each expired lease
        for key in expired_keys {
            self.grant_next_waiter(&key, ctx);
        }
    }

    /// Testing helper: get the number of active leases
    pub fn lease_count(&self) -> usize {
        self.leases.len()
    }
}

#[cfg(test)]
mod tests;
