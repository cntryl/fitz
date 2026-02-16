//! LeaseActor: manages lease state and enforces invariants
//!
//! Each lease has:
//! - Identity: (realm, area, resource) from route
//! - Current owner
//! - Fencing token (monotonically increasing)
//! - Expiration time (TTL-based)
//!
//! # Invariants
//!
//! 1. **Exclusive ownership**: At most one owner per lease at any time
//! 2. **Monotonic tokens**: Fencing tokens never decrease
//! 3. **Expiration semantics**: Lease with expiry <= now() is expired and can be taken
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
use crate::runtime::context::TimerId;
use crate::runtime::routing::RouteAddress;
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

/// Clock abstraction for testable time
pub trait Clock: Send + Sync {
    fn now(&self) -> Instant;
}

/// System clock using Instant::now()
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

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
/// Each lease actor is responsible for a shard of the lease namespace.
/// In a multi-actor deployment, leases are partitioned across actors
/// by (family_id, route) tuple (e.g., consistent hashing).
///
/// # State
///
/// - `family`: RouteFamily this actor serves (for validation)
/// - `leases`: Map of (RouteFamily, Route) → LeaseState
/// - `pending_acquires`: Map of LeaseKey → VecDeque of waiting acquirers (FIFO queue)
/// - `timer_to_waiter`: Map of TimerId → (LeaseKey) for validating stale timers
/// - `next_token`: Global token counter (monotonic)
/// - `clock`: Time source for expiration checks
/// - `max_wait_seconds`: Maximum wait time allowed (capped to prevent DoS)
/// - `max_queue_depth`: Maximum pending acquirers per lease (capped to prevent memory bloat)
///
/// # Persistence Hooks (Future)
///
/// When persistence is added, each state mutation will write to a log:
/// - `LeaseAcquired { key, owner_id, token, expiry }`
/// - `LeaseRenewed { key, token, new_expiry }`
/// - `LeaseReleased { key, token }`
///
/// On recovery, replay the log to reconstruct state.
pub struct LeaseActor {
    /// Route family this actor serves (for validation)
    family: crate::runtime::routing::RouteFamily,

    /// Map of LeaseKey to current lease state
    leases: HashMap<LeaseKey, LeaseState>,

    /// Map of LeaseKey to pending acquire queue (FIFO)
    pending_acquires: HashMap<LeaseKey, VecDeque<PendingAcquire>>,

    /// Map of TimerId to LeaseKey for timer validation
    timer_to_waiter: HashMap<TimerId, LeaseKey>,

    /// Next fencing token to issue (monotonic counter)
    next_token: u64,

    /// Clock for time-based operations
    clock: Box<dyn Clock>,

    /// Maximum wait time in seconds (default 30)
    max_wait_seconds: u32,

    /// Maximum pending acquirers per lease (default 100)
    max_queue_depth: usize,
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
            LeaseMessage::Renew {
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
                    Some(key) => self.handle_renew(key, owner_id, fencing_token, ttl_secs),
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
        let now = self.clock.now();
        let ttl = Duration::from_secs(ttl_secs);

        // Validate wait_seconds against max limit
        if wait_seconds > self.max_wait_seconds {
            return LeaseResponse::Timeout; // Or return error; spec says reject
        }

        // Check if lease is available
        let is_expired = self
            .leases
            .get(&key)
            .map(|state| state.is_expired(now))
            .unwrap_or(false);

        if !self.leases.contains_key(&key) || is_expired {
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
            let now = self.clock.now();
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

    /// Handle lease renewal
    fn handle_renew(
        &mut self,
        key: LeaseKey,
        owner_id: String,
        fencing_token: u64,
        ttl_secs: u64,
    ) -> LeaseResponse {
        let now = self.clock.now();
        let ttl = Duration::from_secs(ttl_secs);

        enum RenewDecision {
            NotHeld,
            Expired,
            Fenced(u64),
            Renew,
        }

        let decision = match self.leases.get(&key) {
            None => RenewDecision::NotHeld,
            Some(state) => {
                if state.is_expired(now) {
                    RenewDecision::Expired
                } else if !state.is_held_by(&owner_id) {
                    RenewDecision::NotHeld
                } else if state.fencing_token != fencing_token {
                    RenewDecision::Fenced(state.fencing_token)
                } else {
                    RenewDecision::Renew
                }
            }
        };

        match decision {
            RenewDecision::NotHeld => LeaseResponse::NotHeld,
            RenewDecision::Expired => LeaseResponse::Expired,
            RenewDecision::Fenced(current_token) => LeaseResponse::Fenced { current_token },
            RenewDecision::Renew => {
                let new_token = self.next_fencing_token();
                if let Some(state) = self.leases.get_mut(&key) {
                    state.expiry = now + ttl;
                    state.fencing_token = new_token;
                }
                LeaseResponse::Renewed {
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
        let now = self.clock.now();

        match self.leases.get(&key) {
            None => LeaseResponse::NotHeld,
            Some(state) => {
                if state.is_expired(now) {
                    // Already expired - no need to release
                    LeaseResponse::Expired
                } else if !state.is_held_by(&owner_id) {
                    LeaseResponse::NotHeld
                } else if state.fencing_token != fencing_token {
                    // Token mismatch - fencing
                    LeaseResponse::Fenced {
                        current_token: state.fencing_token,
                    }
                } else {
                    // Valid release - remove lease
                    self.leases.remove(&key);
                    LeaseResponse::Released
                }
            }
        }
    }

    /// Handle lease query (for testing/debugging)
    fn handle_query(&self, key: LeaseKey) -> LeaseResponse {
        let now = self.clock.now();

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

    /// Handle timer callback for waiter timeouts
    ///
    /// Called when a waiting acquire request times out.
    /// Sends Timeout response to the waiting client.
    fn on_timer(&mut self, timer_id: crate::runtime::context::TimerId, ctx: &mut Context<Self>) {
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
        let now = self.clock.now();
        let expired_keys: Vec<LeaseKey> = self
            .leases
            .iter()
            .filter(|(_, state)| state.is_expired(now))
            .map(|(k, _)| k.clone())
            .collect();

        // Remove expired leases
        for key in &expired_keys {
            self.leases.remove(key);
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
mod tests {
    use super::*;
    use crate::runtime::routing::RouteFamily;
    use parking_lot::Mutex;
    use std::sync::Arc;

    /// Mock clock for deterministic testing
    struct MockClock {
        now: Arc<Mutex<Instant>>,
    }

    impl MockClock {
        fn new() -> Self {
            Self {
                now: Arc::new(Mutex::new(Instant::now())),
            }
        }

        fn advance(&self, duration: Duration) {
            let mut now = self.now.lock();
            *now += duration;
        }
    }

    impl Clock for MockClock {
        fn now(&self) -> Instant {
            *self.now.lock()
        }
    }

    fn test_key(realm: &str, area: &str, resource: &str) -> LeaseKey {
        LeaseKey {
            family: crate::runtime::routing::RouteFamily::new(1),
            realm: realm.to_string(),
            area: area.to_string(),
            resource: resource.to_string(),
        }
    }

    /// Test helper: acquire a lease without waiting (for backward compatibility with existing tests)
    fn test_acquire(
        actor: &mut LeaseActor,
        key: LeaseKey,
        owner_id: String,
        ttl_secs: u64,
    ) -> LeaseResponse {
        // Create a minimal context for testing (deferred responses won't be sent)
        let address = crate::runtime::routing::RouteAddress::new(
            RouteFamily::new(1),
            crate::runtime::routing::Route::new("test://lease-actor"),
        );
        let router = std::sync::Arc::new(crate::runtime::router::Router::new());
        let mut ctx = Context::new(address, router);

        // Call handle_acquire without waiting (wait_seconds=0, source=None)
        actor.handle_acquire(key, owner_id, ttl_secs, 0, None, &mut ctx)
    }

    #[test]
    fn should_acquire_unowned_lease() {
        // Arrange
        let mut actor = LeaseActor::new(RouteFamily::new(1));

        // Act
        let response = test_acquire(
            &mut actor,
            test_key("acme", "locks", "test1"),
            "owner1".to_string(),
            60,
        );

        // Assert
        assert!(matches!(
            response,
            LeaseResponse::Acquired { fencing_token: 1 }
        ));
    }

    #[test]
    fn should_return_existing_token_for_idempotent_acquire() {
        // Arrange
        let mut actor = LeaseActor::new(RouteFamily::new(1));
        let first = test_acquire(
            &mut actor,
            test_key("acme", "locks", "test1"),
            "owner1".to_string(),
            60,
        );
        let first_token = match first {
            LeaseResponse::Acquired { fencing_token } => fencing_token,
            _ => panic!("Expected Acquired"),
        };

        // Act
        let response = test_acquire(
            &mut actor,
            test_key("acme", "locks", "test1"),
            "owner1".to_string(),
            60,
        );

        // Assert
        assert_eq!(
            response,
            LeaseResponse::AlreadyHeld {
                fencing_token: first_token
            }
        );
    }

    #[test]
    fn should_reject_acquire_when_held_by_other() {
        // Arrange
        let mut actor = LeaseActor::new(RouteFamily::new(1));
        test_acquire(
            &mut actor,
            test_key("acme", "locks", "test1"),
            "owner1".to_string(),
            60,
        );

        // Act
        let response = test_acquire(
            &mut actor,
            test_key("acme", "locks", "test1"),
            "owner2".to_string(),
            60,
        );

        // Assert
        assert_eq!(
            response,
            LeaseResponse::HeldByOther {
                current_owner: "owner1".to_string()
            }
        );
    }

    #[test]
    fn should_allow_expired_lease_takeover() {
        // Arrange
        let clock = MockClock::new();
        let clock_ref = Arc::new(clock);
        let mut actor = LeaseActor::with_clock(
            RouteFamily::new(1),
            Box::new(MockClock {
                now: clock_ref.now.clone(),
            }),
        );

        test_acquire(
            &mut actor,
            test_key("acme", "locks", "test1"),
            "owner1".to_string(),
            5,
        );

        // Advance time past expiration
        clock_ref.advance(Duration::from_secs(10));

        // Act
        let response = test_acquire(
            &mut actor,
            test_key("acme", "locks", "test1"),
            "owner2".to_string(),
            60,
        );

        // Assert
        assert!(matches!(
            response,
            LeaseResponse::Acquired { fencing_token: 2 }
        ));
    }

    #[test]
    fn should_issue_monotonic_fencing_tokens() {
        // Arrange
        let clock = MockClock::new();
        let clock_ref = Arc::new(clock);
        let mut actor = LeaseActor::with_clock(
            RouteFamily::new(1),
            Box::new(MockClock {
                now: clock_ref.now.clone(),
            }),
        );

        // Act - acquire first lease
        let response1 = test_acquire(
            &mut actor,
            test_key("acme", "locks", "test1"),
            "owner1".to_string(),
            5,
        );
        let token1 = match response1 {
            LeaseResponse::Acquired { fencing_token } => fencing_token,
            _ => panic!("Expected Acquired"),
        };

        // Expire and takeover
        clock_ref.advance(Duration::from_secs(10));
        let response2 = test_acquire(
            &mut actor,
            test_key("acme", "locks", "test1"),
            "owner2".to_string(),
            5,
        );
        let token2 = match response2 {
            LeaseResponse::Acquired { fencing_token } => fencing_token,
            _ => panic!("Expected Acquired"),
        };

        // Assert
        assert!(token2 > token1);
    }

    #[test]
    fn should_renew_lease_with_valid_token() {
        // Arrange
        let mut actor = LeaseActor::new(RouteFamily::new(1));
        let response = test_acquire(
            &mut actor,
            test_key("acme", "locks", "test1"),
            "owner1".to_string(),
            60,
        );
        let token = match response {
            LeaseResponse::Acquired { fencing_token } => fencing_token,
            _ => panic!("Expected Acquired"),
        };

        // Act
        let renew_response = actor.handle_renew(
            test_key("acme", "locks", "test1"),
            "owner1".to_string(),
            token,
            60,
        );

        // Assert
        match renew_response {
            LeaseResponse::Renewed { fencing_token } => {
                assert!(fencing_token > token);
            }
            _ => panic!("Expected Renewed"),
        }
    }

    #[test]
    fn should_reject_renew_with_wrong_token() {
        // Arrange
        let mut actor = LeaseActor::new(RouteFamily::new(1));
        test_acquire(
            &mut actor,
            test_key("acme", "locks", "test1"),
            "owner1".to_string(),
            60,
        );

        // Act
        let response = actor.handle_renew(
            test_key("acme", "locks", "test1"),
            "owner1".to_string(),
            999,
            60,
        );

        // Assert
        assert!(matches!(
            response,
            LeaseResponse::Fenced { current_token: 1 }
        ));
    }

    #[test]
    fn should_reject_renew_of_expired_lease() {
        // Arrange
        let clock = MockClock::new();
        let clock_ref = Arc::new(clock);
        let mut actor = LeaseActor::with_clock(
            RouteFamily::new(1),
            Box::new(MockClock {
                now: clock_ref.now.clone(),
            }),
        );

        let response = test_acquire(
            &mut actor,
            test_key("acme", "locks", "test1"),
            "owner1".to_string(),
            5,
        );
        let token = match response {
            LeaseResponse::Acquired { fencing_token } => fencing_token,
            _ => panic!("Expected Acquired"),
        };

        // Advance time past expiration
        clock_ref.advance(Duration::from_secs(10));

        // Act
        let renew_response = actor.handle_renew(
            test_key("acme", "locks", "test1"),
            "owner1".to_string(),
            token,
            60,
        );

        // Assert
        assert_eq!(renew_response, LeaseResponse::Expired);
    }

    #[test]
    fn should_release_lease_with_valid_token() {
        // Arrange
        let mut actor = LeaseActor::new(RouteFamily::new(1));
        let response = test_acquire(
            &mut actor,
            test_key("acme", "locks", "test1"),
            "owner1".to_string(),
            60,
        );
        let token = match response {
            LeaseResponse::Acquired { fencing_token } => fencing_token,
            _ => panic!("Expected Acquired"),
        };

        // Act
        let release_response = actor.handle_release(
            test_key("acme", "locks", "test1"),
            "owner1".to_string(),
            token,
        );

        // Assert
        assert_eq!(release_response, LeaseResponse::Released);
    }

    #[test]
    fn should_reject_release_with_wrong_token() {
        // Arrange
        let mut actor = LeaseActor::new(RouteFamily::new(1));
        test_acquire(
            &mut actor,
            test_key("acme", "locks", "test1"),
            "owner1".to_string(),
            60,
        );

        // Act
        let response = actor.handle_release(
            test_key("acme", "locks", "test1"),
            "owner1".to_string(),
            999,
        );

        // Assert
        assert!(matches!(
            response,
            LeaseResponse::Fenced { current_token: 1 }
        ));
    }

    #[test]
    fn should_allow_reacquire_after_release() {
        // Arrange
        let mut actor = LeaseActor::new(RouteFamily::new(1));
        let response = test_acquire(
            &mut actor,
            test_key("acme", "locks", "test1"),
            "owner1".to_string(),
            60,
        );
        let token = match response {
            LeaseResponse::Acquired { fencing_token } => fencing_token,
            _ => panic!("Expected Acquired"),
        };
        actor.handle_release(
            test_key("acme", "locks", "test1"),
            "owner1".to_string(),
            token,
        );

        // Act
        let reacquire = test_acquire(
            &mut actor,
            test_key("acme", "locks", "test1"),
            "owner2".to_string(),
            60,
        );

        // Assert
        assert!(matches!(
            reacquire,
            LeaseResponse::Acquired { fencing_token: 2 }
        ));
    }

    #[test]
    fn should_query_lease_status() {
        // Arrange
        let mut actor = LeaseActor::new(RouteFamily::new(1));
        test_acquire(
            &mut actor,
            test_key("acme", "locks", "test1"),
            "owner1".to_string(),
            60,
        );

        // Act
        let response = actor.handle_query(test_key("acme", "locks", "test1"));

        // Assert
        assert!(matches!(
            response,
            LeaseResponse::Status {
                owner_id,
                fencing_token: 1,
                expires_in_secs: _,
                pending_waiters: _
            } if owner_id == "owner1"
        ));
    }

    #[test]
    fn should_enqueue_waiter_and_grant_on_release() {
        // Arrange
        let mut actor = LeaseActor::new(RouteFamily::new(1));
        let mut ctx = crate::testkit::lease::create_test_lease_context(None);
        let key = test_key("acme", "locks", "queue-test");

        // Act
        let r1 = actor.handle_acquire(key.clone(), "owner1".to_string(), 30, 0, None, &mut ctx);
        let queued = actor.handle_acquire(key.clone(), "owner2".to_string(), 30, 5, None, &mut ctx);
        let release_msg = LeaseMessage::Release {
            family_id: RouteFamily::new(1),
            route: crate::runtime::routing::Route::new("lease://acme/locks/queue-test/release"),
            owner_id: "owner1".to_string(),
            fencing_token: 1,
        };
        let released = actor.handle_message(release_msg, &mut ctx);

        // Assert
        assert!(matches!(r1, LeaseResponse::Acquired { .. }));

        let queued_token = match queued {
            LeaseResponse::Queued { fencing_token } => fencing_token,
            _ => panic!("Expected Queued response"),
        };

        assert_eq!(released, Some(LeaseResponse::Released));

        let status = actor.handle_query(key.clone());
        match status {
            LeaseResponse::Status { owner_id, fencing_token, .. } => {
                assert_eq!(owner_id, "owner2");
                assert_eq!(fencing_token, queued_token);
            }
            _ => panic!("Expected Status after promotion"),
        }
    }

    #[test]
    fn should_isolate_leases_by_realm_and_area() {
        // Arrange
        let mut actor = LeaseActor::new(RouteFamily::new(1));

        // Act - acquire identical resource name in two different realms/areas
        let r1 = test_acquire(
            &mut actor,
            test_key("realm_a", "locks", "shared-resource"),
            "owner-a".to_string(),
            60,
        );
        let r2 = test_acquire(
            &mut actor,
            test_key("realm_b", "locks", "shared-resource"),
            "owner-b".to_string(),
            60,
        );

        // Assert - both succeed and are independent
        assert!(matches!(r1, LeaseResponse::Acquired { fencing_token: 1 }));
        assert!(matches!(r2, LeaseResponse::Acquired { fencing_token: 2 }));

        // Verify queries reflect different owners
        let s1 = actor.handle_query(test_key("realm_a", "locks", "shared-resource"));
        let s2 = actor.handle_query(test_key("realm_b", "locks", "shared-resource"));

        match (s1, s2) {
            (
                LeaseResponse::Status { owner_id: o1, .. },
                LeaseResponse::Status { owner_id: o2, .. },
            ) => {
                assert_eq!(o1, "owner-a");
                assert_eq!(o2, "owner-b");
            }
            _ => panic!("Expected Status for both realms"),
        }
    }

    #[test]
    fn should_handle_concurrent_acquire_race() {
        // Arrange
        let mut actor = LeaseActor::new(RouteFamily::new(1));
        let mut ctx = crate::testkit::lease::create_test_lease_context(None);
        let key = test_key("race", "locks", "res");

        // Act - owner1 acquires immediately; owner2 and owner3 queue
        // Arrange
        let a1 = actor.handle_acquire(key.clone(), "owner1".to_string(), 30, 0, None, &mut ctx);

        // Act - two concurrent waiters
        let q2 = actor.handle_acquire(key.clone(), "owner2".to_string(), 30, 10, None, &mut ctx);
        let q3 = actor.handle_acquire(key.clone(), "owner3".to_string(), 30, 10, None, &mut ctx);

        // Assert - initial acquire succeeded, others queued
        assert!(matches!(a1, LeaseResponse::Acquired { .. }));
        let t2 = match q2 { LeaseResponse::Queued { fencing_token } => fencing_token, _ => panic!("expected queued") };
        let t3 = match q3 { LeaseResponse::Queued { fencing_token } => fencing_token, _ => panic!("expected queued") };
        assert!(t3 > t2, "queued tokens should be monotonic");

        // Act - release owner1 via the public message path so the waiter is promoted
        let release_msg = LeaseMessage::Release {
            family_id: RouteFamily::new(1),
            route: crate::runtime::routing::Route::new("lease://race/locks/res/release"),
            owner_id: "owner1".to_string(),
            fencing_token: 1,
        };
        let released = actor.handle_message(release_msg, &mut ctx);
        assert_eq!(released, Some(LeaseResponse::Released));

        // Query to verify owner2 now holds the lease (promotion happened via handle_message)
        let status = actor.handle_query(key.clone());
        match status {
            LeaseResponse::Status { owner_id, fencing_token, pending_waiters, .. } => {
                assert_eq!(owner_id, "owner2");
                assert_eq!(fencing_token, t2);
                assert_eq!(pending_waiters, 1);
            }
            _ => panic!("expected status after promotion"),
        }

        // Act - release owner2 via public message path so waiter promotion runs
        let release_msg2 = LeaseMessage::Release {
            family_id: RouteFamily::new(1),
            route: crate::runtime::routing::Route::new("lease://race/locks/res/release"),
            owner_id: "owner2".to_string(),
            fencing_token: t2,
        };
        let released2 = actor.handle_message(release_msg2, &mut ctx);
        assert_eq!(released2, Some(LeaseResponse::Released));

        let status2 = actor.handle_query(key.clone());
        match status2 {
            LeaseResponse::Status { owner_id, fencing_token, pending_waiters, .. } => {
                assert_eq!(owner_id, "owner3");
                assert_eq!(fencing_token, t3);
                assert_eq!(pending_waiters, 0);
            }
            _ => panic!("expected status after second promotion"),
        }
    }

    #[test]
    fn should_scale_under_high_contention_queueing() {
        // Arrange - create actor with a larger queue depth so the benchmark-style stress can enqueue many waiters
        let mut actor = LeaseActor::with_config(RouteFamily::new(1), Box::new(SystemClock), 30, 1000);
        let mut ctx = crate::testkit::lease::create_test_lease_context(None);
        let key = test_key("bench", "locks", "contend");

        // Act - holder acquires first
        let _ = actor.handle_acquire(key.clone(), "holder".to_string(), 60, 0, None, &mut ctx);

        // Rapidly enqueue many waiters
        for i in 0..200 {
            let owner = format!("w{:03}", i);
            let _ = actor.handle_acquire(key.clone(), owner, 30, 10, None, &mut ctx);
        }

        // Assert - queue depth reflects the enqueued waiters
        let status = actor.handle_query(key.clone());
        match status {
            LeaseResponse::Status { pending_waiters, .. } => {
                assert!(pending_waiters >= 200, "expected at least 200 waiters, got {}", pending_waiters);
            }
            _ => panic!("expected status"),
        }
    }


}


