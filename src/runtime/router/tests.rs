use super::*;
use crate::observability as obs;
use crate::runtime::actor::{Actor, Context};
use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
use crate::runtime::ManagedActor;
use parking_lot::Mutex;

/// Helper to create test route addresses
fn test_address(family: u64, route: &str) -> RouteAddress {
    RouteAddress::new(RouteFamily::new(family), Route::new(route))
}

/// Mock sink for testing
struct MockSink {
    delivered: Mutex<Vec<Envelope>>,
    should_fail: bool,
}

struct PanicSink;

struct ManagedHighLaneFullSink;

enum ManagedRouterTestMessage {
    Work,
}

struct ManagedRouterTestActor;

impl MockSink {
    fn new() -> Self {
        Self {
            delivered: Mutex::new(Vec::new()),
            should_fail: false,
        }
    }

    fn failing() -> Self {
        Self {
            delivered: Mutex::new(Vec::new()),
            should_fail: true,
        }
    }

    fn count(&self) -> usize {
        self.delivered.lock().len()
    }
}

impl MailboxSink for PanicSink {
    fn deliver(&self, _envelope: Envelope) -> Result<(), DeliveryError> {
        panic!("router sink panic");
    }

    fn deliver_high_priority(&self, _envelope: Envelope) -> Result<(), DeliveryError> {
        panic!("router high-priority sink panic");
    }
}

impl MailboxSink for ManagedHighLaneFullSink {
    fn deliver(&self, _envelope: Envelope) -> Result<(), DeliveryError> {
        Ok(())
    }

    fn deliver_high_priority(&self, _envelope: Envelope) -> Result<(), DeliveryError> {
        Err(DeliveryError::HighLaneFull {
            capacity: 1,
            current_len: 1,
        })
    }
}

impl Actor for ManagedRouterTestActor {
    type Message = ManagedRouterTestMessage;

    fn receive(&mut self, _msg: Self::Message, _ctx: &mut Context<Self>) {}
}

impl MailboxSink for MockSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        if self.should_fail {
            return Err(DeliveryError::MailboxFull {
                capacity: 10,
                current_len: 10,
            });
        }
        self.delivered.lock().push(envelope);
        Ok(())
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        // For tests, just use normal delivery
        self.deliver(envelope)
    }
}

#[test]
fn should_route_to_domain_pattern_across_families() {
    // Arrange
    let router = Router::new();
    let sink = Arc::new(MockSink::new());

    // Register "kv" domain pattern (NOT specific to any family)
    router.register_domain_pattern("kv", sink.clone());

    // Act - Send messages from different tenants (route families)
    let tenant_acme = test_address(1, "kv://acme/app/users");
    let tenant_xyz = test_address(2, "kv://xyz/app/users");

    router.route(Envelope::new(tenant_acme, "msg1")).unwrap();
    router.route(Envelope::new(tenant_xyz, "msg2")).unwrap();

    // Assert - Both messages delivered to same domain sink
    assert_eq!(sink.delivered.lock().len(), 2);
}

#[test]
fn should_prefer_exact_match_over_domain_pattern() {
    // Arrange
    let router = Router::new();
    let exact_sink = Arc::new(MockSink::new());
    let pattern_sink = Arc::new(MockSink::new());

    let address = test_address(1, "kv://acme/app/users");

    // Register both exact and pattern
    router.register(address.clone(), exact_sink.clone());
    router.register_domain_pattern("kv", pattern_sink.clone());

    // Act
    router.route(Envelope::new(address, "msg")).unwrap();

    // Assert - Exact match wins
    assert_eq!(exact_sink.delivered.lock().len(), 1);
    assert_eq!(pattern_sink.delivered.lock().len(), 0);
}

#[test]
fn should_route_directly_to_known_domain_pattern() {
    // Arrange
    let router = Router::new();
    let sink = Arc::new(MockSink::new());
    let address = test_address(7, "queue://inbound");

    router.register_domain_pattern("queue", sink.clone());

    // Act
    let result = router.route_to_domain("queue", Envelope::new(address, "msg"));

    // Assert
    assert!(result.is_ok());
    assert_eq!(sink.count(), 1);
}

#[test]
fn should_return_error_for_unknown_domain() {
    // Arrange
    let router = Router::new();
    let address = test_address(1, "unknown://acme/app/users/get");

    // Act
    let result = router.route(Envelope::new(address.clone(), "msg"));

    // Assert
    assert!(matches!(result, Err(RouteError::RouteNotFound(_))));
}

#[test]
fn should_register_route() {
    // Arrange
    let router = Router::new();
    let address = test_address(1, "/user/123");
    let sink = Arc::new(MockSink::new());

    // Act
    router.register(address.clone(), sink);

    // Assert
    assert!(router.contains(&address));
    assert_eq!(router.len(), 1);
}

#[test]
fn should_unregister_route() {
    // Arrange
    let router = Router::new();
    let address = test_address(1, "/user/123");
    let sink = Arc::new(MockSink::new());
    router.register(address.clone(), sink);

    // Act
    router.unregister(&address);

    // Assert
    assert!(!router.contains(&address));
    assert!(router.is_empty());
}

#[test]
fn should_route_envelope_to_registered_route() {
    // Arrange
    let router = Router::new();
    let address = test_address(1, "/user/123");
    let sink = Arc::new(MockSink::new());
    router.register(address.clone(), sink.clone());
    let envelope = Envelope::new(address, "test message");

    // Act
    let result = router.route(envelope);

    // Assert
    assert!(result.is_ok());
    assert_eq!(sink.count(), 1);
}

#[test]
fn should_return_error_for_unregistered_route() {
    // Arrange
    let router = Router::new();
    let address = test_address(1, "/user/123");
    let envelope = Envelope::new(address.clone(), "test message");

    // Act
    let result = router.route(envelope);

    // Assert
    assert_eq!(result, Err(RouteError::RouteNotFound(address)));
}

#[test]
fn should_return_error_for_failed_delivery() {
    // Arrange
    let router = Router::new();
    let address = test_address(1, "/user/123");
    let sink = Arc::new(MockSink::failing());
    router.register(address.clone(), sink);
    let envelope = Envelope::new(address.clone(), "test message");

    // Act
    let result = router.route(envelope);

    // Assert
    assert!(matches!(
        result,
        Err(RouteError::DeliveryFailed(
            _,
            DeliveryError::MailboxFull { .. }
        ))
    ));
}

#[test]
fn should_catch_panicking_sink_during_delivery() {
    // Arrange
    let router = Router::new();
    let address = test_address(1, "/user/panic");
    router.register(address.clone(), Arc::new(PanicSink));
    let envelope = Envelope::new(address.clone(), "test message");

    // Act
    let result = router.route(envelope);

    // Assert
    assert!(matches!(
        result,
        Err(RouteError::DeliveryFailed(
            target,
            DeliveryError::SinkPanicked
        )) if target == address
    ));
}

#[test]
fn should_catch_panicking_sink_during_high_priority_delivery() {
    // Arrange
    let router = Router::new();
    let address = test_address(1, "/system/panic");
    router.register(address.clone(), Arc::new(PanicSink));
    let envelope = Envelope::new(address.clone(), "test message");

    // Act
    let result = router.route_high_priority(envelope);

    // Assert
    assert!(matches!(
        result,
        Err(RouteError::DeliveryFailed(
            target,
            DeliveryError::SinkPanicked
        )) if target == address
    ));
}

#[test]
fn should_record_high_lane_backpressure_metric_on_high_priority_delivery_failure() {
    // Arrange
    struct HighLaneBackpressuredSink;

    impl MailboxSink for HighLaneBackpressuredSink {
        fn deliver(&self, _envelope: Envelope) -> Result<(), DeliveryError> {
            Ok(())
        }

        fn deliver_high_priority(&self, _envelope: Envelope) -> Result<(), DeliveryError> {
            Err(DeliveryError::HighLaneFull {
                capacity: 1,
                current_len: 1,
            })
        }
    }

    let metrics = crate::observability::metrics();
    let before = metrics.counter_get(obs::METRIC_ROUTER_HIGH_LANE_BACKPRESSURE);
    let router = Router::new();
    let address = test_address(1, "/system/control");
    router.register(address.clone(), Arc::new(HighLaneBackpressuredSink));

    // Act
    let result = router.route_high_priority(Envelope::new(address.clone(), "msg"));

    // Assert
    assert!(matches!(
        result,
        Err(RouteError::DeliveryFailed(
            target,
            DeliveryError::HighLaneFull { .. }
        )) if target == address
    ));
    assert!(
        metrics.counter_get(obs::METRIC_ROUTER_HIGH_LANE_BACKPRESSURE) > before,
        "expected router high-lane backpressure metric to increase"
    );
}

#[test]
fn should_route_managed_actor_high_priority_delivery_through_router() {
    // Arrange
    let router = Arc::new(Router::new());
    let address = test_address(1, "managed://router/high-priority");
    let managed = ManagedActor::spawn(router.clone(), address.clone(), ManagedRouterTestActor, 8);
    let replacement_sink = Arc::new(MockSink::new());
    router.register(address, replacement_sink.clone());

    // Act
    let result = managed.try_send_high_priority(ManagedRouterTestMessage::Work);

    // Assert
    assert!(result.is_ok());
    assert_eq!(replacement_sink.count(), 1);
}

#[test]
fn should_report_high_lane_full_from_managed_actor_high_priority_router_path() {
    // Arrange
    let router = Arc::new(Router::new());
    let address = test_address(1, "managed://router/high-lane-full");
    let managed = ManagedActor::spawn(router.clone(), address.clone(), ManagedRouterTestActor, 8);
    router.register(address, Arc::new(ManagedHighLaneFullSink));

    // Act
    let result = managed.try_send_high_priority(ManagedRouterTestMessage::Work);

    // Assert
    assert!(matches!(
        result,
        Err(DeliveryError::HighLaneFull {
            capacity: 1,
            current_len: 1
        })
    ));
}

#[test]
fn should_contain_sink_panic_from_managed_actor_high_priority_router_path() {
    // Arrange
    let router = Arc::new(Router::new());
    let address = test_address(1, "managed://router/panic");
    let managed = ManagedActor::spawn(router.clone(), address.clone(), ManagedRouterTestActor, 8);
    router.register(address, Arc::new(PanicSink));

    // Act
    let result = managed.try_send_high_priority(ManagedRouterTestMessage::Work);

    // Assert
    assert!(matches!(result, Err(DeliveryError::SinkPanicked)));
}

#[test]
fn should_support_multiple_routes() {
    // Arrange
    let router = Router::new();
    let addr1 = test_address(1, "/user/123");
    let addr2 = test_address(1, "/user/456");
    let sink1 = Arc::new(MockSink::new());
    let sink2 = Arc::new(MockSink::new());
    router.register(addr1.clone(), sink1.clone());
    router.register(addr2.clone(), sink2.clone());

    // Act
    router.route(Envelope::new(addr1, "msg1")).unwrap();
    router.route(Envelope::new(addr2, "msg2")).unwrap();

    // Assert
    assert_eq!(sink1.count(), 1);
    assert_eq!(sink2.count(), 1);
    assert_eq!(router.len(), 2);
}

#[test]
fn should_clone_router() {
    // Arrange
    let router = Router::new();
    let address = test_address(1, "/user/123");
    let sink = Arc::new(MockSink::new());
    router.register(address.clone(), sink);

    // Act
    let cloned = router.clone();

    // Assert
    assert!(cloned.contains(&address));
    assert_eq!(cloned.len(), router.len());
}

#[test]
fn should_handle_concurrent_routing() {
    // Arrange
    let router = Router::new();
    let address = test_address(1, "/user/123");
    let sink = Arc::new(MockSink::new());
    router.register(address.clone(), sink.clone());

    let router_clone = router.clone();
    let addr_clone = address.clone();
    let handle = std::thread::spawn(move || {
        for i in 0..10 {
            let envelope = Envelope::new(addr_clone.clone(), i);
            router_clone.route(envelope).unwrap();
        }
    });

    // Act - route from main thread concurrently
    for i in 10..20 {
        let envelope = Envelope::new(address.clone(), i);
        router.route(envelope).unwrap();
    }

    handle.join().unwrap();

    // Assert
    assert_eq!(sink.count(), 20);
}

#[test]
fn should_isolate_same_route_in_different_families() {
    // Arrange
    let router = Router::new();
    let addr_family1 = test_address(1, "/user/123");
    let addr_family2 = test_address(2, "/user/123");
    let sink1 = Arc::new(MockSink::new());
    let sink2 = Arc::new(MockSink::new());
    router.register(addr_family1.clone(), sink1.clone());
    router.register(addr_family2.clone(), sink2.clone());

    // Act
    router.route(Envelope::new(addr_family1, "msg1")).unwrap();
    router.route(Envelope::new(addr_family2, "msg2")).unwrap();

    // Assert
    assert_eq!(sink1.count(), 1, "Family 1 should receive its message");
    assert_eq!(sink2.count(), 1, "Family 2 should receive its message");
    assert_eq!(
        router.len(),
        2,
        "Both routes should be registered independently"
    );
}
