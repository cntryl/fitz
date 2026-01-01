//! Lease domain implementation
//!
//! The lease domain provides distributed locking with fencing tokens.
//! Each lease has:
//! - Unique ID
//! - Current owner
//! - Fencing token (monotonically increasing)
//! - Expiration time (TTL-based)
//!
//! # Invariants
//!
//! 1. **Exclusive ownership**: At most one owner per lease at any given time
//! 2. **Monotonic tokens**: Fencing tokens never decrease for a lease
//! 3. **Expiration semantics**: A lease with expiry <= now() is expired and can be taken
//! 4. **Idempotency**: Same operation by same owner produces same result
//! 5. **Crash safety**: State transitions are designed to be replayable from persistent log
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
//!
//! # Time Abstraction
//!
//! Uses a `Clock` trait for testing and determinism. In production, uses system time.

use super::protocol::{LeaseMessage, LeaseResponse};
use crate::runtime::actor::{Actor, Context};
use std::collections::HashMap;
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
/// by lease_id (e.g., consistent hashing).
///
/// # State
///
/// - `leases`: Map of lease_id → LeaseState
/// - `next_token`: Global token counter (monotonic)
/// - `clock`: Time source for expiration checks
///
/// # Persistence Hooks (Future)
///
/// When persistence is added, each state mutation will write to a log:
/// - `LeaseAcquired { lease_id, owner_id, token, expiry }`
/// - `LeaseRenewed { lease_id, token, new_expiry }`
/// - `LeaseReleased { lease_id, token }`
///
/// On recovery, replay the log to reconstruct state.
pub struct LeaseActor {
    /// Map of lease_id to current lease state
    leases: HashMap<String, LeaseState>,

    /// Next fencing token to issue (monotonic counter)
    next_token: u64,

    /// Clock for time-based operations
    clock: Box<dyn Clock>,
}

impl LeaseActor {
    /// Create a new lease actor with system clock
    pub fn new() -> Self {
        Self::with_clock(Box::new(SystemClock))
    }

    /// Create a new lease actor with a custom clock (for testing)
    pub fn with_clock(clock: Box<dyn Clock>) -> Self {
        Self {
            leases: HashMap::new(),
            next_token: 1,
            clock,
        }
    }

    /// Allocate and return the next fencing token
    fn next_fencing_token(&mut self) -> u64 {
        let token = self.next_token;
        self.next_token += 1;
        token
    }

    /// Handle lease acquisition
    fn handle_acquire(
        &mut self,
        lease_id: String,
        owner_id: String,
        ttl_secs: u64,
    ) -> LeaseResponse {
        let now = self.clock.now();
        let ttl = Duration::from_secs(ttl_secs);

        match self.leases.get(&lease_id) {
            None => {
                // Lease doesn't exist - grant it
                let token = self.next_fencing_token();
                let expiry = now + ttl;

                self.leases.insert(
                    lease_id.clone(),
                    LeaseState {
                        owner_id: owner_id.clone(),
                        fencing_token: token,
                        expiry,
                    },
                );

                LeaseResponse::Acquired {
                    fencing_token: token,
                }
            }
            Some(state) => {
                if state.is_expired(now) {
                    // Lease expired - grant to new owner
                    let token = self.next_fencing_token();
                    let expiry = now + ttl;

                    self.leases.insert(
                        lease_id.clone(),
                        LeaseState {
                            owner_id: owner_id.clone(),
                            fencing_token: token,
                            expiry,
                        },
                    );

                    LeaseResponse::Acquired {
                        fencing_token: token,
                    }
                } else if state.is_held_by(&owner_id) {
                    // Idempotent - already held by this owner
                    LeaseResponse::AlreadyHeld {
                        fencing_token: state.fencing_token,
                    }
                } else {
                    // Held by another owner
                    LeaseResponse::HeldByOther {
                        current_owner: state.owner_id.clone(),
                    }
                }
            }
        }
    }

    /// Handle lease renewal
    fn handle_renew(
        &mut self,
        lease_id: String,
        owner_id: String,
        fencing_token: u64,
        ttl_secs: u64,
    ) -> LeaseResponse {
        let now = self.clock.now();
        let ttl = Duration::from_secs(ttl_secs);

        match self.leases.get_mut(&lease_id) {
            None => LeaseResponse::NotHeld,
            Some(state) => {
                if state.is_expired(now) {
                    LeaseResponse::Expired
                } else if !state.is_held_by(&owner_id) {
                    LeaseResponse::NotHeld
                } else if state.fencing_token != fencing_token {
                    // Token mismatch - fencing
                    LeaseResponse::Fenced {
                        current_token: state.fencing_token,
                    }
                } else {
                    // Valid renewal - extend expiry
                    state.expiry = now + ttl;
                    LeaseResponse::Renewed {
                        fencing_token: state.fencing_token,
                    }
                }
            }
        }
    }

    /// Handle lease release
    fn handle_release(
        &mut self,
        lease_id: String,
        owner_id: String,
        fencing_token: u64,
    ) -> LeaseResponse {
        let now = self.clock.now();

        match self.leases.get(&lease_id) {
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
                    self.leases.remove(&lease_id);
                    LeaseResponse::Released
                }
            }
        }
    }

    /// Handle lease query (for testing/debugging)
    fn handle_query(&self, lease_id: String) -> LeaseResponse {
        let now = self.clock.now();

        match self.leases.get(&lease_id) {
            None => LeaseResponse::NotFound,
            Some(state) => {
                if state.is_expired(now) {
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

impl Default for LeaseActor {
    fn default() -> Self {
        Self::new()
    }
}

impl Actor for LeaseActor {
    type Message = LeaseMessage;

    fn receive(&mut self, msg: Self::Message, ctx: &mut Context<Self>) {
        let response = match msg {
            LeaseMessage::Acquire {
                lease_id,
                owner_id,
                ttl_secs,
            } => self.handle_acquire(lease_id, owner_id, ttl_secs),
            LeaseMessage::Renew {
                lease_id,
                owner_id,
                fencing_token,
                ttl_secs,
            } => self.handle_renew(lease_id, owner_id, fencing_token, ttl_secs),
            LeaseMessage::Release {
                lease_id,
                owner_id,
                fencing_token,
            } => self.handle_release(lease_id, owner_id, fencing_token),
            LeaseMessage::Query { lease_id } => self.handle_query(lease_id),
        };

        // In a real system, we would reply to the sender
        // For now, just log the response
        println!("[LeaseActor {}] Response: {:?}", ctx.actor_id(), response);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

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
            let mut now = self.now.lock().unwrap();
            *now += duration;
        }
    }

    impl Clock for MockClock {
        fn now(&self) -> Instant {
            *self.now.lock().unwrap()
        }
    }

    #[test]
    fn should_acquire_unowned_lease() {
        // Arrange
        let mut actor = LeaseActor::new();

        // Act
        let response = actor.handle_acquire("lease1".to_string(), "owner1".to_string(), 60);

        // Assert
        assert!(matches!(response, LeaseResponse::Acquired { fencing_token: 1 }));
    }

    #[test]
    fn should_return_existing_token_for_idempotent_acquire() {
        // Arrange
        let mut actor = LeaseActor::new();
        let first = actor.handle_acquire("lease1".to_string(), "owner1".to_string(), 60);
        let first_token = match first {
            LeaseResponse::Acquired { fencing_token } => fencing_token,
            _ => panic!("Expected Acquired"),
        };

        // Act
        let response = actor.handle_acquire("lease1".to_string(), "owner1".to_string(), 60);

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
        let mut actor = LeaseActor::new();
        actor.handle_acquire("lease1".to_string(), "owner1".to_string(), 60);

        // Act
        let response = actor.handle_acquire("lease1".to_string(), "owner2".to_string(), 60);

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
        let mut actor = LeaseActor::with_clock(Box::new(MockClock {
            now: clock_ref.now.clone(),
        }));

        actor.handle_acquire("lease1".to_string(), "owner1".to_string(), 5);

        // Advance time past expiration
        clock_ref.advance(Duration::from_secs(10));

        // Act
        let response = actor.handle_acquire("lease1".to_string(), "owner2".to_string(), 60);

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
        let mut actor = LeaseActor::with_clock(Box::new(MockClock {
            now: clock_ref.now.clone(),
        }));

        // Act - acquire first lease
        let response1 = actor.handle_acquire("lease1".to_string(), "owner1".to_string(), 5);
        let token1 = match response1 {
            LeaseResponse::Acquired { fencing_token } => fencing_token,
            _ => panic!("Expected Acquired"),
        };

        // Expire and takeover
        clock_ref.advance(Duration::from_secs(10));
        let response2 = actor.handle_acquire("lease1".to_string(), "owner2".to_string(), 5);
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
        let mut actor = LeaseActor::new();
        let response = actor.handle_acquire("lease1".to_string(), "owner1".to_string(), 60);
        let token = match response {
            LeaseResponse::Acquired { fencing_token } => fencing_token,
            _ => panic!("Expected Acquired"),
        };

        // Act
        let renew_response =
            actor.handle_renew("lease1".to_string(), "owner1".to_string(), token, 60);

        // Assert
        assert_eq!(
            renew_response,
            LeaseResponse::Renewed {
                fencing_token: token
            }
        );
    }

    #[test]
    fn should_reject_renew_with_wrong_token() {
        // Arrange
        let mut actor = LeaseActor::new();
        actor.handle_acquire("lease1".to_string(), "owner1".to_string(), 60);

        // Act
        let response = actor.handle_renew("lease1".to_string(), "owner1".to_string(), 999, 60);

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
        let mut actor = LeaseActor::with_clock(Box::new(MockClock {
            now: clock_ref.now.clone(),
        }));

        let response = actor.handle_acquire("lease1".to_string(), "owner1".to_string(), 5);
        let token = match response {
            LeaseResponse::Acquired { fencing_token } => fencing_token,
            _ => panic!("Expected Acquired"),
        };

        // Advance time past expiration
        clock_ref.advance(Duration::from_secs(10));

        // Act
        let renew_response =
            actor.handle_renew("lease1".to_string(), "owner1".to_string(), token, 60);

        // Assert
        assert_eq!(renew_response, LeaseResponse::Expired);
    }

    #[test]
    fn should_release_lease_with_valid_token() {
        // Arrange
        let mut actor = LeaseActor::new();
        let response = actor.handle_acquire("lease1".to_string(), "owner1".to_string(), 60);
        let token = match response {
            LeaseResponse::Acquired { fencing_token } => fencing_token,
            _ => panic!("Expected Acquired"),
        };

        // Act
        let release_response =
            actor.handle_release("lease1".to_string(), "owner1".to_string(), token);

        // Assert
        assert_eq!(release_response, LeaseResponse::Released);
    }

    #[test]
    fn should_reject_release_with_wrong_token() {
        // Arrange
        let mut actor = LeaseActor::new();
        actor.handle_acquire("lease1".to_string(), "owner1".to_string(), 60);

        // Act
        let response = actor.handle_release("lease1".to_string(), "owner1".to_string(), 999);

        // Assert
        assert!(matches!(
            response,
            LeaseResponse::Fenced { current_token: 1 }
        ));
    }

    #[test]
    fn should_allow_reacquire_after_release() {
        // Arrange
        let mut actor = LeaseActor::new();
        let response = actor.handle_acquire("lease1".to_string(), "owner1".to_string(), 60);
        let token = match response {
            LeaseResponse::Acquired { fencing_token } => fencing_token,
            _ => panic!("Expected Acquired"),
        };
        actor.handle_release("lease1".to_string(), "owner1".to_string(), token);

        // Act
        let reacquire = actor.handle_acquire("lease1".to_string(), "owner2".to_string(), 60);

        // Assert
        assert!(matches!(
            reacquire,
            LeaseResponse::Acquired { fencing_token: 2 }
        ));
    }

    #[test]
    fn should_query_lease_status() {
        // Arrange
        let mut actor = LeaseActor::new();
        actor.handle_acquire("lease1".to_string(), "owner1".to_string(), 60);

        // Act
        let response = actor.handle_query("lease1".to_string());

        // Assert
        assert!(matches!(
            response,
            LeaseResponse::Status {
                owner_id,
                fencing_token: 1,
                expires_in_secs: _
            } if owner_id == "owner1"
        ));
    }
}
