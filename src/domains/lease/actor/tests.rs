use super::*;
use crate::runtime::routing::RouteFamily;
use crate::runtime::{DeliveryError, Envelope, MailboxSink};
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
    fn now_instant(&self) -> Instant {
        *self.now.lock()
    }

    fn now_epoch_ms(&self) -> u64 {
        0
    }
}

struct CapturedPublishSink {
    events: Arc<Mutex<Vec<crate::runtime::DomainPublishEvent>>>,
}

impl MailboxSink for CapturedPublishSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        if let Some(event) = envelope.payload::<crate::runtime::DomainPublishEvent>() {
            self.events.lock().push(event.clone());
        }
        Ok(())
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
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
    let LeaseResponse::Acquired {
        fencing_token: first_token,
    } = first
    else {
        panic!("Expected Acquired");
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
fn should_publish_each_expired_lease_route_given_batched_tick() {
    // Arrange
    let clock = MockClock::new();
    let clock_ref = Arc::new(clock);
    let mut actor = LeaseActor::with_clock(
        RouteFamily::new(1),
        Box::new(MockClock {
            now: clock_ref.now.clone(),
        }),
    );
    let router = Arc::new(crate::runtime::router::Router::new());
    let captured_events = Arc::new(Mutex::new(Vec::new()));
    router.register_domain_pattern(
        "lease",
        Arc::new(CapturedPublishSink {
            events: captured_events.clone(),
        }),
    );
    let address = crate::runtime::routing::RouteAddress::new(
        RouteFamily::new(1),
        crate::runtime::routing::Route::new("lease://actor"),
    );
    let mut ctx = Context::new(address, router);
    let key_a = test_key("acme", "locks", "resource-a");
    let key_b = test_key("acme", "locks", "resource-b");
    actor.handle_acquire(key_a.clone(), "owner-a".to_string(), 5, 0, None, &mut ctx);
    actor.handle_acquire(key_b.clone(), "owner-b".to_string(), 5, 0, None, &mut ctx);
    clock_ref.advance(Duration::from_secs(10));

    // Act
    actor.handle_message(LeaseMessage::Tick, &mut ctx);
    let timer_id = actor.notify_timer.expect("notification timer");
    actor.on_timer(timer_id, &mut ctx);

    // Assert
    let mut event_routes = captured_events
        .lock()
        .iter()
        .map(|event| event.route.as_str().to_string())
        .collect::<Vec<_>>();
    event_routes.sort();
    assert_eq!(
        event_routes,
        vec![
            "lease://acme/locks/resource-a".to_string(),
            "lease://acme/locks/resource-b".to_string(),
        ]
    );
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
    let LeaseResponse::Acquired {
        fencing_token: token1,
    } = response1
    else {
        panic!("Expected Acquired");
    };

    // Expire and takeover
    clock_ref.advance(Duration::from_secs(10));
    let response2 = test_acquire(
        &mut actor,
        test_key("acme", "locks", "test1"),
        "owner2".to_string(),
        5,
    );
    let LeaseResponse::Acquired {
        fencing_token: token2,
    } = response2
    else {
        panic!("Expected Acquired");
    };

    // Assert
    assert!(token2 > token1);
}

#[test]
fn should_renew_lease_with_valid_token() {
    // Arrange
    let mut actor = LeaseActor::new(RouteFamily::new(1));
    let mut ctx = crate::testkit::lease::create_test_lease_context(None);
    let response = test_acquire(
        &mut actor,
        test_key("acme", "locks", "test1"),
        "owner1".to_string(),
        60,
    );
    let LeaseResponse::Acquired {
        fencing_token: token,
    } = response
    else {
        panic!("Expected Acquired");
    };

    // Act
    let renew_response = actor.handle_extend(
        &test_key("acme", "locks", "test1"),
        "owner1",
        token,
        60,
        &mut ctx,
    );

    // Assert
    match renew_response {
        LeaseResponse::Extended { fencing_token } => {
            assert!(fencing_token > token);
        }
        _ => panic!("Expected Extended"),
    }
}

#[test]
fn should_reject_renew_with_wrong_token() {
    // Arrange
    let mut actor = LeaseActor::new(RouteFamily::new(1));
    let mut ctx = crate::testkit::lease::create_test_lease_context(None);
    test_acquire(
        &mut actor,
        test_key("acme", "locks", "test1"),
        "owner1".to_string(),
        60,
    );

    // Act
    let response = actor.handle_extend(
        &test_key("acme", "locks", "test1"),
        "owner1",
        999,
        60,
        &mut ctx,
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
    let mut ctx = crate::testkit::lease::create_test_lease_context(None);
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
    let LeaseResponse::Acquired {
        fencing_token: token,
    } = response
    else {
        panic!("Expected Acquired");
    };

    // Advance time past expiration
    clock_ref.advance(Duration::from_secs(10));

    // Act
    let renew_response = actor.handle_extend(
        &test_key("acme", "locks", "test1"),
        "owner1",
        token,
        60,
        &mut ctx,
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
    let LeaseResponse::Acquired {
        fencing_token: token,
    } = response
    else {
        panic!("Expected Acquired");
    };

    // Act
    let release_response =
        actor.handle_release(&test_key("acme", "locks", "test1"), "owner1", token);

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
    let response = actor.handle_release(&test_key("acme", "locks", "test1"), "owner1", 999);

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
    let LeaseResponse::Acquired {
        fencing_token: token,
    } = response
    else {
        panic!("Expected Acquired");
    };
    actor.handle_release(&test_key("acme", "locks", "test1"), "owner1", token);

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
    let response = actor.handle_query(&test_key("acme", "locks", "test1"));

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
fn should_enqueue_waiter_when_lease_held() {
    // Arrange
    let mut actor = LeaseActor::new(RouteFamily::new(1));
    let mut ctx = crate::testkit::lease::create_test_lease_context(None);
    let key = test_key("acme", "locks", "queue-test");

    // Act
    let r1 = actor.handle_acquire(key.clone(), "owner1".to_string(), 30, 0, None, &mut ctx);
    let queued = actor.handle_acquire(key.clone(), "owner2".to_string(), 30, 5, None, &mut ctx);

    // Assert
    assert!(matches!(r1, LeaseResponse::Acquired { .. }));
    assert!(matches!(queued, LeaseResponse::Queued { .. }));
}

#[test]
fn should_grant_lease_to_queued_waiter_on_release() {
    // Arrange
    let mut actor = LeaseActor::new(RouteFamily::new(1));
    let mut ctx = crate::testkit::lease::create_test_lease_context(None);
    let key = test_key("acme", "locks", "queue-test");

    let r1 = actor.handle_acquire(key.clone(), "owner1".to_string(), 30, 0, None, &mut ctx);
    let queued = actor.handle_acquire(key.clone(), "owner2".to_string(), 30, 5, None, &mut ctx);

    let LeaseResponse::Queued {
        fencing_token: queued_token,
    } = queued
    else {
        panic!("Expected Queued response");
    };

    // Act
    let release_msg = LeaseMessage::Release {
        family_id: RouteFamily::new(1),
        route: crate::runtime::routing::Route::new("lease://acme/locks/queue-test/release"),
        owner_id: "owner1".to_string(),
        fencing_token: 1,
    };
    let released = actor.handle_message(release_msg, &mut ctx);

    // Assert
    assert!(matches!(r1, LeaseResponse::Acquired { .. }));
    assert_eq!(released, Some(LeaseResponse::Released));

    let status = actor.handle_query(&key);
    match status {
        LeaseResponse::Status {
            owner_id,
            fencing_token,
            ..
        } => {
            assert_eq!(owner_id, "owner2");
            assert_eq!(fencing_token, queued_token);
        }
        _ => panic!("Expected Status after promotion"),
    }
}

#[test]
fn should_isolate_leases_across_realm_boundaries() {
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
    let s1 = actor.handle_query(&test_key("realm_a", "locks", "shared-resource"));
    let s2 = actor.handle_query(&test_key("realm_b", "locks", "shared-resource"));

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
fn should_queue_multiple_waiters_in_fifo_order() {
    // Arrange
    let mut actor = LeaseActor::new(RouteFamily::new(1));
    let mut ctx = crate::testkit::lease::create_test_lease_context(None);
    let key = test_key("race", "locks", "res");

    // Act - owner1 acquires immediately; owner2 and owner3 queue
    let a1 = actor.handle_acquire(key.clone(), "owner1".to_string(), 30, 0, None, &mut ctx);
    let q2 = actor.handle_acquire(key.clone(), "owner2".to_string(), 30, 10, None, &mut ctx);
    let q3 = actor.handle_acquire(key.clone(), "owner3".to_string(), 30, 10, None, &mut ctx);

    // Assert - initial acquire succeeded, others queued
    assert!(matches!(a1, LeaseResponse::Acquired { .. }));
    let LeaseResponse::Queued { fencing_token: t2 } = q2 else {
        panic!("expected queued");
    };
    let LeaseResponse::Queued { fencing_token: t3 } = q3 else {
        panic!("expected queued");
    };
    assert!(t3 > t2, "queued tokens should be monotonic");
}

#[test]
fn should_promote_first_waiter_when_holder_releases() {
    // Arrange
    let mut actor = LeaseActor::new(RouteFamily::new(1));
    let mut ctx = crate::testkit::lease::create_test_lease_context(None);
    let key = test_key("race", "locks", "res");

    let _a1 = actor.handle_acquire(key.clone(), "owner1".to_string(), 30, 0, None, &mut ctx);
    let q2 = actor.handle_acquire(key.clone(), "owner2".to_string(), 30, 10, None, &mut ctx);
    let _q3 = actor.handle_acquire(key.clone(), "owner3".to_string(), 30, 10, None, &mut ctx);

    let LeaseResponse::Queued { fencing_token: t2 } = q2 else {
        panic!("expected queued");
    };

    // Act - release owner1 via the public message path so the waiter is promoted
    let release_msg = LeaseMessage::Release {
        family_id: RouteFamily::new(1),
        route: crate::runtime::routing::Route::new("lease://race/locks/res/release"),
        owner_id: "owner1".to_string(),
        fencing_token: 1,
    };
    let released = actor.handle_message(release_msg, &mut ctx);

    // Assert
    assert_eq!(released, Some(LeaseResponse::Released));

    let status = actor.handle_query(&key);
    match status {
        LeaseResponse::Status {
            owner_id,
            fencing_token,
            pending_waiters,
            ..
        } => {
            assert_eq!(owner_id, "owner2");
            assert_eq!(fencing_token, t2);
            assert_eq!(pending_waiters, 1);
        }
        _ => panic!("expected status after promotion"),
    }
}

#[test]
fn should_promote_next_waiter_when_current_holder_releases() {
    // Arrange
    let mut actor = LeaseActor::new(RouteFamily::new(1));
    let mut ctx = crate::testkit::lease::create_test_lease_context(None);
    let key = test_key("race", "locks", "res");

    let _a1 = actor.handle_acquire(key.clone(), "owner1".to_string(), 30, 0, None, &mut ctx);
    let q2 = actor.handle_acquire(key.clone(), "owner2".to_string(), 30, 10, None, &mut ctx);
    let q3 = actor.handle_acquire(key.clone(), "owner3".to_string(), 30, 10, None, &mut ctx);

    let t2 = match q2 {
        LeaseResponse::Queued { fencing_token } => fencing_token,
        _ => panic!("expected queued"),
    };
    let t3 = match q3 {
        LeaseResponse::Queued { fencing_token } => fencing_token,
        _ => panic!("expected queued"),
    };

    // First release: owner1 -> owner2 is promoted
    let release_msg = LeaseMessage::Release {
        family_id: RouteFamily::new(1),
        route: crate::runtime::routing::Route::new("lease://race/locks/res/release"),
        owner_id: "owner1".to_string(),
        fencing_token: 1,
    };
    let _released = actor.handle_message(release_msg, &mut ctx);

    // Act - release owner2 via public message path so waiter promotion runs
    let release_msg2 = LeaseMessage::Release {
        family_id: RouteFamily::new(1),
        route: crate::runtime::routing::Route::new("lease://race/locks/res/release"),
        owner_id: "owner2".to_string(),
        fencing_token: t2,
    };
    let released2 = actor.handle_message(release_msg2, &mut ctx);

    // Assert
    assert_eq!(released2, Some(LeaseResponse::Released));

    let status2 = actor.handle_query(&key);
    match status2 {
        LeaseResponse::Status {
            owner_id,
            fencing_token,
            pending_waiters,
            ..
        } => {
            assert_eq!(owner_id, "owner3");
            assert_eq!(fencing_token, t3);
            assert_eq!(pending_waiters, 0);
        }
        _ => panic!("expected status after second promotion"),
    }
}

#[test]
fn should_promote_waiter_when_expired_before_new_acquire() {
    // Arrange
    let clock = MockClock::new();
    let clock_ref = Arc::new(clock);
    let mut actor = LeaseActor::with_clock(
        RouteFamily::new(1),
        Box::new(MockClock {
            now: clock_ref.now.clone(),
        }),
    );
    let mut ctx = crate::testkit::lease::create_test_lease_context(None);
    let key = test_key("race", "locks", "expire-queue");

    let _ = actor.handle_acquire(key.clone(), "owner1".to_string(), 5, 0, None, &mut ctx);
    let _ = actor.handle_acquire(key.clone(), "owner2".to_string(), 30, 10, None, &mut ctx);

    clock_ref.advance(Duration::from_secs(10));

    // Act
    let response = actor.handle_acquire(key.clone(), "owner3".to_string(), 30, 0, None, &mut ctx);

    // Assert
    assert!(matches!(
        response,
        LeaseResponse::HeldByOther {
            current_owner
        } if current_owner == "owner2"
    ));

    let status = actor.handle_query(&key);
    match status {
        LeaseResponse::Status {
            owner_id,
            pending_waiters,
            ..
        } => {
            assert_eq!(owner_id, "owner2");
            assert_eq!(pending_waiters, 0);
        }
        _ => panic!("Expected Status after promotion"),
    }
}

#[test]
fn should_promote_waiter_when_extend_observes_expired_holder() {
    // Arrange
    let clock = MockClock::new();
    let clock_ref = Arc::new(clock);
    let mut actor = LeaseActor::with_clock(
        RouteFamily::new(1),
        Box::new(MockClock {
            now: clock_ref.now.clone(),
        }),
    );
    let mut ctx = crate::testkit::lease::create_test_lease_context(None);
    let key = test_key("race", "locks", "extend-expired");

    let acquired = actor.handle_acquire(key.clone(), "owner1".to_string(), 5, 0, None, &mut ctx);
    let holder_token = match acquired {
        LeaseResponse::Acquired { fencing_token } => fencing_token,
        _ => panic!("expected holder acquire"),
    };
    let queued = actor.handle_acquire(key.clone(), "owner2".to_string(), 30, 10, None, &mut ctx);
    let waiter_token = match queued {
        LeaseResponse::Queued { fencing_token } => fencing_token,
        _ => panic!("expected queued waiter"),
    };
    clock_ref.advance(Duration::from_secs(10));

    // Act
    let response = actor.handle_extend(&key, "owner1", holder_token, 30, &mut ctx);

    // Assert
    assert_eq!(response, LeaseResponse::Expired);
    match actor.handle_query(&key) {
        LeaseResponse::Status {
            owner_id,
            fencing_token,
            pending_waiters,
            ..
        } => {
            assert_eq!(owner_id, "owner2");
            assert_eq!(fencing_token, waiter_token);
            assert_eq!(pending_waiters, 0);
        }
        _ => panic!("expected waiter to own lease after expired extend"),
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
        let owner = format!("w{i:03}");
        let _ = actor.handle_acquire(key.clone(), owner, 30, 10, None, &mut ctx);
    }

    // Assert - queue depth reflects the enqueued waiters
    let status = actor.handle_query(&key);
    match status {
        LeaseResponse::Status {
            pending_waiters, ..
        } => {
            assert!(
                pending_waiters >= 200,
                "expected at least 200 waiters, got {pending_waiters}"
            );
        }
        _ => panic!("expected status"),
    }
}
