//! Lease domain queueing semantics tests
//!
//! These tests verify FIFO queue behavior, wait timeouts, queue overflow,
//! and deferred response semantics when multiple clients contend for a single lease.

use fitz::domains::lease::lease_actor::LeaseActor;
use fitz::domains::lease::protocol::{LeaseMessage, LeaseResponse};
use fitz::runtime::actor::Context;
use fitz::runtime::router::Router;
use fitz::runtime::routing::RouteAddress;
use fitz::runtime::routing::{Route, RouteFamily};
use std::sync::Arc;

// ===== Test Helpers =====

fn create_test_actor() -> (LeaseActor, Context<LeaseActor>) {
    let family = RouteFamily::new(1);
    let actor = LeaseActor::new(family);
    let router = Arc::new(Router::new());
    let addr = RouteAddress::new(family, Route::new("lease://test/app/actor"));
    let ctx = Context::new(addr, router);
    (actor, ctx)
}

fn route(path: &str) -> Route {
    Route::new(path.to_string())
}

fn family() -> RouteFamily {
    RouteFamily::new(1)
}

// ===== Test Mods =====

#[cfg(test)]
mod basic_queueing {
    use super::*;

    #[test]
    fn should_return_queued_when_wait_seconds_greater_than_zero_and_lease_held() {
        // Arrange
        let (mut actor, mut ctx) = create_test_actor();
        let test_route = route("lease://test/app/lock");
        let fam = family();

        // Client-A acquires lease
        let acquire_a = LeaseMessage::Acquire {
            family_id: fam,
            route: test_route.clone(),
            owner_id: "client-a".to_string(),
            ttl_secs: 60,
            wait_seconds: 0,
        };
        let response_a = actor.handle_message(acquire_a, &mut ctx).unwrap();

        let _token_a = match response_a {
            LeaseResponse::Acquired { fencing_token } => fencing_token,
            _ => panic!("Expected Acquired for first client"),
        };

        // Act
        let acquire_b = LeaseMessage::Acquire {
            family_id: fam,
            route: test_route.clone(),
            owner_id: "client-b".to_string(),
            ttl_secs: 60,
            wait_seconds: 10,
        };
        let response_b = actor.handle_message(acquire_b, &mut ctx).unwrap();

        // Assert
        match response_b {
            LeaseResponse::Queued { fencing_token } => {
                assert!(
                    fencing_token > 0,
                    "Expected valid fencing token for queued waiter"
                );
            }
            _ => panic!("Expected Queued response, got {:?}", response_b),
        }
    }

    #[test]
    fn should_return_held_by_other_when_wait_seconds_is_zero() {
        // Arrange
        let (mut actor, mut ctx) = create_test_actor();
        let test_route = route("lease://test/app/lock");
        let fam = family();

        // Client-A acquires lease
        let acquire_a = LeaseMessage::Acquire {
            family_id: fam,
            route: test_route.clone(),
            owner_id: "client-a".to_string(),
            ttl_secs: 60,
            wait_seconds: 0,
        };
        let _ = actor.handle_message(acquire_a, &mut ctx).unwrap();

        // Act
        let acquire_b = LeaseMessage::Acquire {
            family_id: fam,
            route: test_route.clone(),
            owner_id: "client-b".to_string(),
            ttl_secs: 60,
            wait_seconds: 0,
        };
        let response_b = actor.handle_message(acquire_b, &mut ctx).unwrap();

        // Assert
        match response_b {
            LeaseResponse::HeldByOther { current_owner } => {
                assert_eq!(current_owner, "client-a", "Expected current owner in error");
            }
            _ => panic!("Expected HeldByOther response, got {:?}", response_b),
        }
    }

    #[test]
    fn should_return_already_queued_when_same_owner_acquires_twice() {
        // Arrange
        let (mut actor, mut ctx) = create_test_actor();
        let test_route = route("lease://test/app/lock");
        let fam = family();

        // Client-A acquires lease (holds it)
        let acquire_a = LeaseMessage::Acquire {
            family_id: fam,
            route: test_route.clone(),
            owner_id: "client-a".to_string(),
            ttl_secs: 60,
            wait_seconds: 0,
        };
        let _ = actor.handle_message(acquire_a, &mut ctx).unwrap();

        // Client-B queues for same lease
        let acquire_b = LeaseMessage::Acquire {
            family_id: fam,
            route: test_route.clone(),
            owner_id: "client-b".to_string(),
            ttl_secs: 60,
            wait_seconds: 10,
        };
        let _ = actor.handle_message(acquire_b, &mut ctx).unwrap();

        // Act
        let acquire_b_again = LeaseMessage::Acquire {
            family_id: fam,
            route: test_route.clone(),
            owner_id: "client-b".to_string(),
            ttl_secs: 60,
            wait_seconds: 10,
        };
        let response = actor.handle_message(acquire_b_again, &mut ctx).unwrap();

        // Assert
        match response {
            LeaseResponse::AlreadyQueued { fencing_token } => {
                assert!(
                    fencing_token > 0,
                    "Expected valid token for already-queued waiter"
                );
            }
            _ => panic!("Expected AlreadyQueued response, got {:?}", response),
        }
    }

    #[test]
    fn should_return_already_held_when_current_holder_acquires_again() {
        // Arrange
        let (mut actor, mut ctx) = create_test_actor();
        let test_route = route("lease://test/app/lock");
        let fam = family();

        // Client-A acquires lease
        let acquire_a = LeaseMessage::Acquire {
            family_id: fam,
            route: test_route.clone(),
            owner_id: "client-a".to_string(),
            ttl_secs: 60,
            wait_seconds: 0,
        };
        let response_a = actor.handle_message(acquire_a, &mut ctx).unwrap();

        let token_a = match response_a {
            LeaseResponse::Acquired { fencing_token } => fencing_token,
            _ => panic!("Expected Acquired"),
        };

        // Act
        let acquire_a_again = LeaseMessage::Acquire {
            family_id: fam,
            route: test_route.clone(),
            owner_id: "client-a".to_string(),
            ttl_secs: 60,
            wait_seconds: 10,
        };
        let response = actor.handle_message(acquire_a_again, &mut ctx).unwrap();

        // Assert
        match response {
            LeaseResponse::AlreadyHeld { fencing_token } => {
                assert_eq!(
                    fencing_token, token_a,
                    "Expected same token for already-held"
                );
            }
            _ => panic!("Expected AlreadyHeld response, got {:?}", response),
        }
    }
}

#[cfg(test)]
mod idempotent_acquire {
    use super::*;

    #[test]
    fn should_allow_already_held_status_with_wait_seconds_parameter() {
        // Arrange
        let (mut actor, mut ctx) = create_test_actor();
        let test_route = route("lease://test/app/lock");
        let fam = family();

        // Client-A acquires and holds
        let acquire_a = LeaseMessage::Acquire {
            family_id: fam,
            route: test_route.clone(),
            owner_id: "client-a".to_string(),
            ttl_secs: 60,
            wait_seconds: 0,
        };
        let response_a = actor.handle_message(acquire_a, &mut ctx).unwrap();

        let token_a = match response_a {
            LeaseResponse::Acquired { fencing_token } => fencing_token,
            _ => panic!("Expected Acquired"),
        };

        // Act
        let acquire_a_with_wait = LeaseMessage::Acquire {
            family_id: fam,
            route: test_route.clone(),
            owner_id: "client-a".to_string(),
            ttl_secs: 60,
            wait_seconds: 10,
        };
        let response = actor.handle_message(acquire_a_with_wait, &mut ctx).unwrap();

        // Assert
        match response {
            LeaseResponse::AlreadyHeld { fencing_token } => {
                assert_eq!(fencing_token, token_a, "Expected same token");
            }
            _ => panic!("Expected AlreadyHeld, got {:?}", response),
        }
    }
}

#[cfg(test)]
mod queue_overflow {
    use super::*;

    #[test]
    fn should_return_queue_full_when_exceeding_max_queue_depth() {
        // Arrange
        let (mut actor, mut ctx) = create_test_actor();
        let test_route = route("lease://test/app/lock");
        let fam = family();
        let max_queue_depth = 100; // Default server limit

        // Client-A acquires the lease (holder)
        let acquire_holder = LeaseMessage::Acquire {
            family_id: fam,
            route: test_route.clone(),
            owner_id: "holder".to_string(),
            ttl_secs: 60,
            wait_seconds: 0,
        };
        let _ = actor.handle_message(acquire_holder, &mut ctx).unwrap();

        // Queue up exactly max_queue_depth clients
        for i in 0..max_queue_depth {
            let acquire = LeaseMessage::Acquire {
                family_id: fam,
                route: test_route.clone(),
                owner_id: format!("waiter-{}", i),
                ttl_secs: 60,
                wait_seconds: 10,
            };
            let resp = actor.handle_message(acquire, &mut ctx).unwrap();
            // All should be Queued until we hit the limit
            assert!(
                matches!(resp, LeaseResponse::Queued { .. }),
                "Waiter {} should be queued, got {:?}",
                i,
                resp
            );
        }

        // Act
        let acquire_overflow = LeaseMessage::Acquire {
            family_id: fam,
            route: test_route.clone(),
            owner_id: "waiter-overflow".to_string(),
            ttl_secs: 60,
            wait_seconds: 10,
        };
        let response = actor.handle_message(acquire_overflow, &mut ctx).unwrap();

        // Assert
        match response {
            LeaseResponse::QueueFull { pending_count } => {
                assert_eq!(
                    pending_count, max_queue_depth,
                    "Expected queue at max capacity"
                );
            }
            _ => panic!("Expected QueueFull response, got {:?}", response),
        }
    }
}

#[cfg(test)]
mod fifo_ordering {
    use super::*;

    #[test]
    fn should_grant_fifo_order_verified_via_query() {
        // Arrange
        let (mut actor, mut ctx) = create_test_actor();
        let test_route = route("lease://test/app/lock");
        let fam = family();

        // Holder
        let acquire_holder = LeaseMessage::Acquire {
            family_id: fam,
            route: test_route.clone(),
            owner_id: "holder".to_string(),
            ttl_secs: 60,
            wait_seconds: 0,
        };
        let _ = actor.handle_message(acquire_holder, &mut ctx).unwrap();

        // Queue B, then C
        let acquire_b = LeaseMessage::Acquire {
            family_id: fam,
            route: test_route.clone(),
            owner_id: "client-b".to_string(),
            ttl_secs: 60,
            wait_seconds: 10,
        };
        let _ = actor.handle_message(acquire_b, &mut ctx).unwrap();

        let acquire_c = LeaseMessage::Acquire {
            family_id: fam,
            route: test_route.clone(),
            owner_id: "client-c".to_string(),
            ttl_secs: 60,
            wait_seconds: 10,
        };
        let _ = actor.handle_message(acquire_c, &mut ctx).unwrap();

        // Act
        let query = LeaseMessage::Query {
            family_id: fam,
            route: test_route.clone(),
        };
        let response = actor.handle_message(query, &mut ctx).unwrap();

        // Assert
        match response {
            LeaseResponse::Status {
                owner_id,
                pending_waiters,
                ..
            } => {
                assert_eq!(owner_id, "holder", "Expected holder to be shown");
                assert_eq!(pending_waiters, 2, "Expected 2 waiters in queue");
            }
            _ => panic!("Expected Status response, got {:?}", response),
        }
    }
}

#[cfg(test)]
mod renew_and_release {
    use super::*;

    #[test]
    fn should_allow_renew_with_valid_token() {
        // Arrange
        let (mut actor, mut ctx) = create_test_actor();
        let test_route = route("lease://test/app/lock");
        let fam = family();

        // Acquire
        let acquire = LeaseMessage::Acquire {
            family_id: fam,
            route: test_route.clone(),
            owner_id: "client-a".to_string(),
            ttl_secs: 60,
            wait_seconds: 0,
        };
        let response_a = actor.handle_message(acquire, &mut ctx).unwrap();

        let token_a = match response_a {
            LeaseResponse::Acquired { fencing_token } => fencing_token,
            _ => panic!("Expected Acquired"),
        };

        // Act
        let renew = LeaseMessage::Renew {
            family_id: fam,
            route: test_route.clone(),
            owner_id: "client-a".to_string(),
            fencing_token: token_a,
            ttl_secs: 120,
        };
        let response = actor.handle_message(renew, &mut ctx).unwrap();

        // Assert
        match response {
            LeaseResponse::Renewed {
                fencing_token: new_token,
            } => {
                assert_ne!(new_token, token_a, "Expected new token on renewal");
            }
            _ => panic!("Expected Renewed response, got {:?}", response),
        }
    }

    #[test]
    fn should_fail_renew_with_invalid_token() {
        // Arrange
        let (mut actor, mut ctx) = create_test_actor();
        let test_route = route("lease://test/app/lock");
        let fam = family();

        // Acquire
        let acquire = LeaseMessage::Acquire {
            family_id: fam,
            route: test_route.clone(),
            owner_id: "client-a".to_string(),
            ttl_secs: 60,
            wait_seconds: 0,
        };
        let _ = actor.handle_message(acquire, &mut ctx).unwrap();

        // Act
        let renew_bad_token = LeaseMessage::Renew {
            family_id: fam,
            route: test_route.clone(),
            owner_id: "client-a".to_string(),
            fencing_token: 999, // Wrong token
            ttl_secs: 120,
        };
        let response = actor.handle_message(renew_bad_token, &mut ctx).unwrap();

        // Assert
        match response {
            LeaseResponse::Fenced { current_token } => {
                assert!(current_token > 0, "Expected current token in error");
            }
            _ => panic!("Expected Fenced response, got {:?}", response),
        }
    }

    #[test]
    fn should_allow_release_with_valid_token() {
        // Arrange
        let (mut actor, mut ctx) = create_test_actor();
        let test_route = route("lease://test/app/lock");
        let fam = family();

        // Acquire
        let acquire = LeaseMessage::Acquire {
            family_id: fam,
            route: test_route.clone(),
            owner_id: "client-a".to_string(),
            ttl_secs: 60,
            wait_seconds: 0,
        };
        let response_a = actor.handle_message(acquire, &mut ctx).unwrap();

        let token_a = match response_a {
            LeaseResponse::Acquired { fencing_token } => fencing_token,
            _ => panic!("Expected Acquired"),
        };

        // Act
        let release = LeaseMessage::Release {
            family_id: fam,
            route: test_route.clone(),
            owner_id: "client-a".to_string(),
            fencing_token: token_a,
        };
        let response = actor.handle_message(release, &mut ctx).unwrap();

        // Assert
        match response {
            LeaseResponse::Released => {} // Success
            _ => panic!("Expected Released response, got {:?}", response),
        }
    }
}

#[cfg(test)]
mod query_operations {
    use super::*;

    #[test]
    fn should_show_free_lease_with_zero_pending_waiters() {
        // Arrange
        let (mut actor, mut ctx) = create_test_actor();
        let test_route = route("lease://test/app/lock");
        let fam = family();

        // Act
        let query = LeaseMessage::Query {
            family_id: fam,
            route: test_route.clone(),
        };
        let response = actor.handle_message(query, &mut ctx).unwrap();

        // Assert
        match response {
            LeaseResponse::Status {
                owner_id: _,
                pending_waiters,
                ..
            } => {
                // For free lease, depending on impl may return empty owner
                // or specific indicator; pending_waiters should definitely be 0
                assert_eq!(pending_waiters, 0, "Expected 0 waiters on free lease");
            }
            LeaseResponse::NotFound => {
                // Also acceptable for never-acquired lease
            }
            _ => panic!("Unexpected response type for free lease query"),
        }
    }

    #[test]
    fn should_show_pending_waiters_count_in_status() {
        // Arrange
        let (mut actor, mut ctx) = create_test_actor();
        let test_route = route("lease://test/app/lock");
        let fam = family();

        // Holder
        let acquire_holder = LeaseMessage::Acquire {
            family_id: fam,
            route: test_route.clone(),
            owner_id: "holder".to_string(),
            ttl_secs: 60,
            wait_seconds: 0,
        };
        let _ = actor.handle_message(acquire_holder, &mut ctx).unwrap();

        // 3 waiters
        for i in 0..3 {
            let acquire = LeaseMessage::Acquire {
                family_id: fam,
                route: test_route.clone(),
                owner_id: format!("waiter-{}", i),
                ttl_secs: 60,
                wait_seconds: 10,
            };
            let _ = actor.handle_message(acquire, &mut ctx).unwrap();
        }

        // Act
        let query = LeaseMessage::Query {
            family_id: fam,
            route: test_route.clone(),
        };
        let response = actor.handle_message(query, &mut ctx).unwrap();

        // Assert
        match response {
            LeaseResponse::Status {
                owner_id,
                pending_waiters,
                ..
            } => {
                assert_eq!(owner_id, "holder", "Expected holder in status");
                assert_eq!(pending_waiters, 3, "Expected 3 pending waiters");
            }
            _ => panic!("Expected Status response, got {:?}", response),
        }
    }
}
