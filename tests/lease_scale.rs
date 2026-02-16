use fitz::domains::lease::{LeaseActor, LeaseMessage, LeaseResponse};
use fitz::runtime::routing::RouteFamily;
use fitz::testkit::lease::create_test_lease_context;
use bytes::Bytes;

#[test]
fn should_scale_under_high_contention_queueing() {
    // Arrange
    let mut actor = LeaseActor::new(RouteFamily::new(1));
    let mut ctx = create_test_lease_context(Some("lease://bench/app/contend"));
    let route = crate::runtime::routing::Route::new("lease://bench/app/contend/0");

    // Act - rapidly enqueue many waiters behind an existing holder
    let key = crate::domains::lease::LeaseKey::from_route(RouteFamily::new(1), &route).unwrap();
    let _ = actor.handle_acquire(key.clone(), "holder".to_string(), 30, 0, None, &mut ctx);

    for i in 0..50 {
        let owner = format!("waiter{:02}", i);
        let _ = actor.handle_acquire(key.clone(), owner, 30, 10, None, &mut ctx);
    }

    // Assert - queue depth reflects the enqueued waiters
    let status = actor.handle_query(key.clone());
    match status {
        LeaseResponse::Status { pending_waiters, .. } => {
            assert!(pending_waiters >= 50);
        }
        _ => panic!("expected status"),
    }
}
