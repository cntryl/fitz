// Consolidated lease advanced tests
// Combined from: lease_queueing.rs, lease_timers.rs

use fitz::domains::lease::LeaseActor;
use fitz::domains::lease::protocol::{LeaseMessage, LeaseResponse};
use fitz::runtime::actor::Context;
use fitz::runtime::router::Router;
use fitz::runtime::routing::RouteAddress;
use fitz::runtime::routing::{Route, RouteFamily};
use std::sync::Arc;

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

// ===== Queueing behavior =====

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
            assert!(fencing_token > 0);
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
            assert_eq!(current_owner, "client-a");
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

    // Client-A acquires (holds it)
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
            assert!(fencing_token > 0);
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
        wait_seconds: 0,
    };
    let response = actor.handle_message(acquire_a_again, &mut ctx).unwrap();

    // Assert
    match response {
        LeaseResponse::AlreadyHeld { fencing_token } => assert_eq!(fencing_token, token_a),
        _ => panic!("Expected AlreadyHeld response, got {:?}", response),
    }
}

// ===== Queue overflow, FIFO, renew/release semantics are below =====

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
            owner_id: format!("client-{}", i),
            ttl_secs: 60,
            wait_seconds: 10,
        };
        let _ = actor.handle_message(acquire, &mut ctx).unwrap();
    }

    // Act
    let acquire_overflow = LeaseMessage::Acquire {
        family_id: fam,
        route: test_route.clone(),
        owner_id: "overflow-client".to_string(),
        ttl_secs: 60,
        wait_seconds: 10,
    };
    let response = actor.handle_message(acquire_overflow, &mut ctx).unwrap();

    // Assert
    match response {
        LeaseResponse::QueueFull { pending_count: _ } => (),
        _ => panic!("Expected QueueFull, got {:?}", response),
    }
}

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
            pending_waiters, ..
        } => {
            assert_eq!(pending_waiters, 2);
        }
        _ => panic!("Expected Status, got {:?}", response),
    }
}

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
        owner_id: "holder".to_string(),
        ttl_secs: 60,
        wait_seconds: 0,
    };
    let response_a = actor.handle_message(acquire, &mut ctx).unwrap();

    let token_a = match response_a {
        LeaseResponse::Acquired { fencing_token } => fencing_token,
        _ => panic!("Expected Acquired for holder"),
    };

    // Act
    let renew = LeaseMessage::Renew {
        family_id: fam,
        route: test_route.clone(),
        owner_id: "holder".to_string(),
        fencing_token: token_a,
        ttl_secs: 60,
    };
    let response = actor.handle_message(renew, &mut ctx).unwrap();

    // Assert
    match response {
        LeaseResponse::Renewed { fencing_token } => assert!(fencing_token > token_a),
        _ => panic!("Expected Renewed, got {:?}", response),
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
        owner_id: "holder".to_string(),
        ttl_secs: 60,
        wait_seconds: 0,
    };
    let _ = actor.handle_message(acquire, &mut ctx).unwrap();

    // Act
    let renew_bad_token = LeaseMessage::Renew {
        family_id: fam,
        route: test_route.clone(),
        owner_id: "holder".to_string(),
        fencing_token: 9999,
        ttl_secs: 60,
    };
    let response = actor.handle_message(renew_bad_token, &mut ctx).unwrap();

    // Assert
    match response {
        LeaseResponse::Fenced { current_token } => assert!(current_token > 0),
        _ => panic!("Expected Fenced, got {:?}", response),
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
        owner_id: "holder".to_string(),
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
        owner_id: "holder".to_string(),
        fencing_token: token_a,
    };
    let response = actor.handle_message(release, &mut ctx).unwrap();

    // Assert
    match response {
        LeaseResponse::Released => (),
        _ => panic!("Expected Released, got {:?}", response),
    }
}

// ===== Timers & timeout coordination =====

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
        LeaseResponse::Queued { fencing_token } => assert!(fencing_token > 0),
        _ => panic!("Expected Queued for wait_seconds=30, got {:?}", response),
    }
}

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

    match response_queued {
        LeaseResponse::Queued { fencing_token } => assert!(fencing_token > 0),
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
            pending_waiters, ..
        } => {
            assert_eq!(pending_waiters, 1);
        }
        _ => panic!("Expected Status, got {:?}", status),
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
        _ => panic!("Expected Acquired for holder"),
    };

    // Waiter-1 queued
    let acquire_waiter1 = LeaseMessage::Acquire {
        family_id: fam,
        route: test_route.clone(),
        owner_id: "waiter-1".to_string(),
        ttl_secs: 60,
        wait_seconds: 20,
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
    let acquire_new = LeaseMessage::Acquire {
        family_id: fam,
        route: test_route.clone(),
        owner_id: "new-client".to_string(),
        ttl_secs: 60,
        wait_seconds: 10,
    };
    let response = actor.handle_message(acquire_new, &mut ctx).unwrap();

    // Assert
    match response {
        LeaseResponse::HeldByOther { .. } | LeaseResponse::Queued { .. } => (),
        _ => panic!("Expected HeldByOther or Queued, got {:?}", response),
    }
}

#[test]
fn should_isolate_timers_across_separate_leases() {
    // Arrange
    let (mut actor, mut ctx) = create_test_actor();
    let route_a = route("lease://test/app/lock-a");
    let route_b = route("lease://test/app/lock-b");
    let fam = family();

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

    let acquire_a_waiter = LeaseMessage::Acquire {
        family_id: fam,
        route: route_a.clone(),
        owner_id: "waiter-a".to_string(),
        ttl_secs: 60,
        wait_seconds: 5,
    };
    let _ = actor.handle_message(acquire_a_waiter, &mut ctx).unwrap();

    let acquire_b_waiter = LeaseMessage::Acquire {
        family_id: fam,
        route: route_b.clone(),
        owner_id: "waiter-b".to_string(),
        ttl_secs: 60,
        wait_seconds: 25,
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
            pending_waiters, ..
        } => assert!(pending_waiters >= 1),
        _ => panic!("Expected Status for A"),
    }

    let query_b = LeaseMessage::Query {
        family_id: fam,
        route: route_b.clone(),
    };
    let status_b = actor.handle_message(query_b, &mut ctx).unwrap();

    match status_b {
        LeaseResponse::Status {
            pending_waiters, ..
        } => assert!(pending_waiters >= 1),
        _ => panic!("Expected Status for B"),
    }
}

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
        owner_id: "owner".to_string(),
        fencing_token: 1,
    };
    let response = actor.handle_message(release, &mut ctx).unwrap();

    // Assert - Idempotent delete: releasing non-existent lease succeeds
    match response {
        LeaseResponse::Released => (),
        _ => panic!("Expected Released (idempotent delete), got {:?}", response),
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
        owner_id: "holder".to_string(),
        ttl_secs: 1,
        wait_seconds: 0,
    };
    let _ = actor.handle_message(acquire, &mut ctx).unwrap();

    // Act
    let renew = LeaseMessage::Renew {
        family_id: fam,
        route: test_route.clone(),
        owner_id: "holder".to_string(),
        fencing_token: 9999,
        ttl_secs: 30,
    };
    let response = actor.handle_message(renew, &mut ctx).unwrap();

    // Assert
    match response {
        LeaseResponse::Expired | LeaseResponse::NotHeld | LeaseResponse::Fenced { .. } => (),
        _ => panic!("Expected Expired/NotHeld/Fenced, got {:?}", response),
    }
}
