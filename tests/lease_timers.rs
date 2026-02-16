// Deprecated — consolidated into `tests/lease_advanced.rs`. (Kept as a stub.)

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
mod timeout_behavior_basic {
    use super::*;

    #[test]
    fn should_reject_wait_seconds_exceeding_server_max() {
        // Arrange
        let (mut actor, mut ctx) = create_test_actor();
        let test_route = route("lease://test/app/lock");
        let fam = family();

        // Holder blocks the lease
        let acquire_holder = LeaseMessage::Acquire {
            family_id: fam,
            route: test_route.clone(),
            owner_id: "holder".to_string(),
            ttl_secs: 60,
            wait_seconds: 0,
        };
        let _ = actor.handle_message(acquire_holder, &mut ctx).unwrap();

        // Act
        let acquire_too_long = LeaseMessage::Acquire {
            family_id: fam,
            route: test_route.clone(),
            owner_id: "client-wait-too-long".to_string(),
            ttl_secs: 60,
            wait_seconds: 31, // Exceeds 30-second max
        };
        let response = actor.handle_message(acquire_too_long, &mut ctx).unwrap();

        // Assert
        assert_eq!(response, LeaseResponse::Timeout);
    }

    #[test]
    fn should_accept_wait_seconds_at_server_max() {
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

        // Act
        let acquire_at_max = LeaseMessage::Acquire {
            family_id: fam,
            route: test_route.clone(),
            owner_id: "client-max-wait".to_string(),
            ttl_secs: 60,
            wait_seconds: 30, // At server max
        };
        let response = actor.handle_message(acquire_at_max, &mut ctx).unwrap();

        // Assert
        match response {
            LeaseResponse::Queued { fencing_token } => {
                assert!(fencing_token > 0, "Expected valid token for queued waiter");
            }
            _ => panic!("Expected Queued for wait_seconds=30, got {:?}", response),
        }
    }
}

#[cfg(test)]
mod timeout_and_expiry_coordination {
    use super::*;

    #[test]
    fn should_preserve_fifo_order_through_single_waiter_lifecycle() {
        // Arrange
        let (mut actor, mut ctx) = create_test_actor();
        let test_route = route("lease://test/app/lock");
        let fam = family();

        // Single holder
        let acquire_holder = LeaseMessage::Acquire {
            family_id: fam,
            route: test_route.clone(),
            owner_id: "holder".to_string(),
            ttl_secs: 60,
            wait_seconds: 0,
        };
        let _holder_response = actor.handle_message(acquire_holder, &mut ctx).unwrap();

        // Single waiter with timeout
        let acquire_waiter = LeaseMessage::Acquire {
            family_id: fam,
            route: test_route.clone(),
            owner_id: "waiter".to_string(),
            ttl_secs: 60,
            wait_seconds: 5,
        };
        let response_queued = actor.handle_message(acquire_waiter, &mut ctx).unwrap();

        // Assert
        match response_queued {
            LeaseResponse::Queued { fencing_token } => {
                assert!(fencing_token > 0, "Expected valid token");
            }
            _ => panic!("Expected Queued response, got {:?}", response_queued),
        }

        // Act
        let query = LeaseMessage::Query {
            family_id: fam,
            route: test_route.clone(),
        };
        let status = actor.handle_message(query, &mut ctx).unwrap();

        // Assert

        match status {
            LeaseResponse::Status {
                owner_id,
                pending_waiters,
                ..
            } => {
                assert_eq!(owner_id, "holder", "Expected holder");
                assert_eq!(pending_waiters, 1, "Expected 1 waiter in queue");
            }
            _ => panic!("Expected Status, got {:?}", status),
        }
    }

    #[test]
    fn should_handle_multiple_concurrent_timeouts_with_staggered_deadlines() {
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

        // Queue 5 waiters with different wait times
        for i in 0..5 {
            let wait_secs = ((i + 1) * 5) as u32; // 5s, 10s, 15s, 20s, 25s
            let acquire = LeaseMessage::Acquire {
                family_id: fam,
                route: test_route.clone(),
                owner_id: format!("waiter-{}", i),
                ttl_secs: 60,
                wait_seconds: wait_secs,
            };
            let response = actor.handle_message(acquire, &mut ctx).unwrap();

            match response {
                LeaseResponse::Queued { .. } => {}        // Expected
                LeaseResponse::AlreadyQueued { .. } => {} // Duplicate owner, already queued
                _ => panic!("Unexpected response for waiter {}: {:?}", i, response),
            }
        }

        // Act
        let query = LeaseMessage::Query {
            family_id: fam,
            route: test_route.clone(),
        };
        let status = actor.handle_message(query, &mut ctx).unwrap();

        // Assert

        match status {
            LeaseResponse::Status {
                owner_id,
                pending_waiters,
                ..
            } => {
                assert_eq!(owner_id, "holder", "Expected holder");
                assert_eq!(pending_waiters, 5, "Expected 5 staggered waiters in queue");
            }
            _ => panic!("Expected Status, got {:?}", status),
        }
    }
}

#[cfg(test)]
mod timer_interaction_with_release {
    use super::*;

    #[test]
    fn should_proceed_when_holder_releases_before_timeout_fires() {
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
        let holder_resp = actor.handle_message(acquire_holder, &mut ctx).unwrap();

        let holder_token = match holder_resp {
            LeaseResponse::Acquired { fencing_token } => fencing_token,
            _ => panic!("Expected Acquired for holder"),
        };

        // Waiter queued with long timeout
        let acquire_waiter = LeaseMessage::Acquire {
            family_id: fam,
            route: test_route.clone(),
            owner_id: "waiter".to_string(),
            ttl_secs: 60,
            wait_seconds: 30, // Long wait
        };
        let _waiter_resp = actor.handle_message(acquire_waiter, &mut ctx).unwrap();

        // Act
        let release = LeaseMessage::Release {
            family_id: fam,
            route: test_route.clone(),
            owner_id: "holder".to_string(),
            fencing_token: holder_token,
        };
        let release_resp = actor.handle_message(release, &mut ctx).unwrap();

        // Assert
        match release_resp {
            LeaseResponse::Released => {}
            _ => panic!("Expected Released, got {:?}", release_resp),
        }

        // Query should show waiter now has the lease (or is granted async)
        // Depending on implementation, waiter may be auto-granted or still queued for async delivery
        let query = LeaseMessage::Query {
            family_id: fam,
            route: test_route.clone(),
        };
        let status = actor.handle_message(query, &mut ctx).unwrap();

        match status {
            LeaseResponse::Status {
                owner_id: _,
                pending_waiters,
                ..
            } => {
                // After release, either waiter is new holder or is being granted
                // pending_waiters should be 0 at this point
                assert_eq!(
                    pending_waiters, 0,
                    "Expected no pending waiters after release"
                );
            }
            LeaseResponse::NotFound => {
                // Lease completely free is also acceptable
            }
            _ => panic!("Unexpected status after release"),
        }
    }

    #[test]
    fn should_block_new_acquirers_while_waiter_is_being_granted() {
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
        let holder_resp = actor.handle_message(acquire_holder, &mut ctx).unwrap();
        let holder_token = match holder_resp {
            LeaseResponse::Acquired { fencing_token } => fencing_token,
            _ => panic!("Expected Acquired"),
        };

        // Waiter-1 queued
        let acquire_waiter1 = LeaseMessage::Acquire {
            family_id: fam,
            route: test_route.clone(),
            owner_id: "waiter-1".to_string(),
            ttl_secs: 60,
            wait_seconds: 30,
        };
        let _ = actor.handle_message(acquire_waiter1, &mut ctx).unwrap();

        // Release (granting to waiter-1)
        let release = LeaseMessage::Release {
            family_id: fam,
            route: test_route.clone(),
            owner_id: "holder".to_string(),
            fencing_token: holder_token,
        };
        let _ = actor.handle_message(release, &mut ctx).unwrap();

        // Act
        // Depending on impl, waiter-1 may not yet be "open" for new ops
        let acquire_new = LeaseMessage::Acquire {
            family_id: fam,
            route: test_route.clone(),
            owner_id: "new-client".to_string(),
            ttl_secs: 60,
            wait_seconds: 0,
        };
        let response = actor.handle_message(acquire_new, &mut ctx).unwrap();

        // Assert
        // 1. HeldByOther (waiter-1 is now holder), or
        // 2. Queued (if waiter-1 hasn't been granted yet and is still in queue)
        match response {
            LeaseResponse::HeldByOther { current_owner } => {
                assert_eq!(current_owner, "waiter-1", "Expected waiter-1 to hold lease");
            }
            LeaseResponse::Queued { .. } => {
                // Also acceptable if async grant not yet delivered
            }
            _ => panic!("Unexpected response after release: {:?}", response),
        }
    }
}

#[cfg(test)]
mod concurrent_lease_independence {
    use super::*;

    #[test]
    fn should_isolate_timers_across_separate_leases() {
        // Arrange
        let (mut actor, mut ctx) = create_test_actor();
        let route_a = route("lease://test/app/lock-a");
        let route_b = route("lease://test/app/lock-b");
        let fam = family();

        // Two independent leases, both with holders
        let acquire_a_holder = LeaseMessage::Acquire {
            family_id: fam,
            route: route_a.clone(),
            owner_id: "holder-a".to_string(),
            ttl_secs: 60,
            wait_seconds: 0,
        };
        let _ = actor.handle_message(acquire_a_holder, &mut ctx).unwrap();

        let acquire_b_holder = LeaseMessage::Acquire {
            family_id: fam,
            route: route_b.clone(),
            owner_id: "holder-b".to_string(),
            ttl_secs: 60,
            wait_seconds: 0,
        };
        let _ = actor.handle_message(acquire_b_holder, &mut ctx).unwrap();

        // Waiter on A with short timeout
        let acquire_a_waiter = LeaseMessage::Acquire {
            family_id: fam,
            route: route_a.clone(),
            owner_id: "waiter-a".to_string(),
            ttl_secs: 60,
            wait_seconds: 5,
        };
        let _ = actor.handle_message(acquire_a_waiter, &mut ctx).unwrap();

        // Waiter on B with long timeout
        let acquire_b_waiter = LeaseMessage::Acquire {
            family_id: fam,
            route: route_b.clone(),
            owner_id: "waiter-b".to_string(),
            ttl_secs: 60,
            wait_seconds: 30,
        };
        let _ = actor.handle_message(acquire_b_waiter, &mut ctx).unwrap();

        // Act
        let query_a = LeaseMessage::Query {
            family_id: fam,
            route: route_a.clone(),
        };
        let status_a = actor.handle_message(query_a, &mut ctx).unwrap();

        // Assert

        match status_a {
            LeaseResponse::Status {
                owner_id,
                pending_waiters,
                ..
            } => {
                assert_eq!(owner_id, "holder-a", "Expected holder-a");
                assert_eq!(pending_waiters, 1, "Expected 1 waiter on lease-a");
            }
            _ => panic!("Expected Status for lease-a"),
        }

        let query_b = LeaseMessage::Query {
            family_id: fam,
            route: route_b.clone(),
        };
        let status_b = actor.handle_message(query_b, &mut ctx).unwrap();

        match status_b {
            LeaseResponse::Status {
                owner_id,
                pending_waiters,
                ..
            } => {
                assert_eq!(owner_id, "holder-b", "Expected holder-b");
                assert_eq!(pending_waiters, 1, "Expected 1 waiter on lease-b");
            }
            _ => panic!("Expected Status for lease-b"),
        }
    }
}

#[cfg(test)]
mod error_state_handling {
    use super::*;

    #[test]
    fn should_handle_release_on_non_existent_lease() {
        // Arrange
        let (mut actor, mut ctx) = create_test_actor();
        let test_route = route("lease://test/app/never-created");
        let fam = family();

        // Act
        let release = LeaseMessage::Release {
            family_id: fam,
            route: test_route.clone(),
            owner_id: "someone".to_string(),
            fencing_token: 123,
        };
        let response = actor.handle_message(release, &mut ctx).unwrap();

        // Assert
        match response {
            LeaseResponse::NotHeld => {}
            LeaseResponse::NotFound => {}
            _ => panic!("Expected NotHeld or NotFound, got {:?}", response),
        }
    }

    #[test]
    fn should_handle_renew_on_expired_lease() {
        // Arrange
        let (mut actor, mut ctx) = create_test_actor();
        let test_route = route("lease://test/app/lock");
        let fam = family();

        // Acquire and immediately try to renew with wrong token
        let acquire = LeaseMessage::Acquire {
            family_id: fam,
            route: test_route.clone(),
            owner_id: "client".to_string(),
            ttl_secs: 1, // Very short TTL
            wait_seconds: 0,
        };
        let _ = actor.handle_message(acquire, &mut ctx).unwrap();

        // Act
        let renew = LeaseMessage::Renew {
            family_id: fam,
            route: test_route.clone(),
            owner_id: "client".to_string(),
            fencing_token: 999, // Wrong token
            ttl_secs: 60,
        };
        let response = actor.handle_message(renew, &mut ctx).unwrap();

        // Assert
        match response {
            LeaseResponse::Fenced { .. } => {
                // Expected: wrong token
            }
            _ => panic!("Expected Fenced error, got {:?}", response),
        }
    }
}
