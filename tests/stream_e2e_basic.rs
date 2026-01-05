use bytes::Bytes;
use fitz::domains::stream::{StreamActor, StreamMessage, StreamRecord};
use fitz::prelude::Actor;
use crate::runtime::actor::Context;
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use std::sync::Arc;

// This file tests basic Stream golden paths: simple append/read cycles
// and fundamental event stream interactions.
// It MUST NOT test implementation details.

fn make_ctx() -> Context<StreamActor> {
    let router = Arc::new(fitz::runtime::router::Router::new());
    let addr = RouteAddress::new(
        RouteFamily::new(1),
        Route::new("stream://acme/orders/checkout"),
    );
    Context::new(addr, router)
}

#[test]
fn should_append_single_event_successfully() {
    // Arrange
    let mut actor = StreamActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    let append_msg = StreamMessage::Append {
        family_id: RouteFamily::new(1),
        route: Route::new("stream://acme/orders/checkout/append"),
        resource_offset: 0,
        body: Bytes::from(b"order-placed-event".to_vec()),
        metadata: None,
    };

    // Act
    actor.receive(append_msg, &mut ctx);

    // Assert
    assert_eq!(actor.event_count(), 1);
}

#[test]
fn should_read_previously_appended_event() {
    // Arrange
    let mut actor = StreamActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    let append_msg = StreamMessage::Append {
        family_id: RouteFamily::new(1),
        route: Route::new("stream://acme/orders/checkout/append"),
        resource_offset: 0,
        body: Bytes::from(b"order-placed-event".to_vec()),
        metadata: None,
    };
    actor.receive(append_msg, &mut ctx);

    // Act
    let read_msg = StreamMessage::Read {
        family_id: RouteFamily::new(1),
        route: Route::new("stream://acme/orders/checkout/read"),
        from_offset: 0,
        limit: 100,
    };
    actor.receive(read_msg, &mut ctx);

    // Assert
    assert_eq!(actor.last_read_count(), 1);
}

#[test]
fn should_append_multiple_events_in_sequence() {
    // Arrange
    let mut actor = StreamActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    let events = vec![
        b"order-placed".to_vec(),
        b"payment-received".to_vec(),
        b"order-confirmed".to_vec(),
    ];

    // Act
    for (i, event) in events.iter().enumerate() {
        let msg = StreamMessage::Append {
            family_id: RouteFamily::new(1),
            route: Route::new("stream://acme/orders/checkout/append"),
            resource_offset: i as u64,
            body: Bytes::from(event.clone()),
            metadata: None,
        };
        actor.receive(msg, &mut ctx);
    }

    // Assert
    assert_eq!(actor.event_count(), 3);
}

#[test]
fn should_read_events_from_specific_offset() {
    // Arrange
    let mut actor = StreamActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    // Append 5 events
    for i in 0..5 {
        let msg = StreamMessage::Append {
            family_id: RouteFamily::new(1),
            route: Route::new("stream://acme/orders/checkout/append"),
            resource_offset: i,
            body: Bytes::from(format!("event-{}", i).into_bytes()),
            metadata: None,
        };
        actor.receive(msg, &mut ctx);
    }

    // Act - Read from offset 2
    let read_msg = StreamMessage::Read {
        family_id: RouteFamily::new(1),
        route: Route::new("stream://acme/orders/checkout/read"),
        from_offset: 2,
        limit: 100,
    };
    actor.receive(read_msg, &mut ctx);

    // Assert
    // Should read events 2, 3, 4 (3 events)
    assert_eq!(actor.last_read_count(), 3);
}

#[test]
fn should_respect_read_limit() {
    // Arrange
    let mut actor = StreamActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    // Append 10 events
    for i in 0..10 {
        let msg = StreamMessage::Append {
            family_id: RouteFamily::new(1),
            route: Route::new("stream://acme/orders/checkout/append"),
            resource_offset: i,
            body: Bytes::from(format!("event-{}", i).into_bytes()),
            metadata: None,
        };
        actor.receive(msg, &mut ctx);
    }

    // Act - Read with limit of 3
    let read_msg = StreamMessage::Read {
        family_id: RouteFamily::new(1),
        route: Route::new("stream://acme/orders/checkout/read"),
        from_offset: 0,
        limit: 3,
    };
    actor.receive(read_msg, &mut ctx);

    // Assert
    assert_eq!(actor.last_read_count(), 3);
}

#[test]
fn should_append_event_with_metadata() {
    // Arrange
    let mut actor = StreamActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    let metadata = Bytes::from(b"{\"user_id\":\"123\",\"correlation_id\":\"abc-xyz\"}".to_vec());

    let msg = StreamMessage::Append {
        family_id: RouteFamily::new(1),
        route: Route::new("stream://acme/orders/checkout/append"),
        resource_offset: 0,
        body: Bytes::from(b"order-placed".to_vec()),
        metadata: Some(metadata),
    };

    // Act
    actor.receive(msg, &mut ctx);

    // Assert
    assert_eq!(actor.event_count(), 1);
}

#[test]
fn should_return_empty_result_for_read_beyond_end() {
    // Arrange
    let mut actor = StreamActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    // Append only 3 events
    for i in 0..3 {
        let msg = StreamMessage::Append {
            family_id: RouteFamily::new(1),
            route: Route::new("stream://acme/orders/checkout/append"),
            resource_offset: i,
            body: Bytes::from(format!("event-{}", i).into_bytes()),
            metadata: None,
        };
        actor.receive(msg, &mut ctx);
    }

    // Act - Read from offset 10 (beyond end)
    let read_msg = StreamMessage::Read {
        family_id: RouteFamily::new(1),
        route: Route::new("stream://acme/orders/checkout/read"),
        from_offset: 10,
        limit: 100,
    };
    actor.receive(read_msg, &mut ctx);

    // Assert
    assert_eq!(actor.last_read_count(), 0);
}

#[test]
fn should_handle_zero_limit_gracefully() {
    // Arrange
    let mut actor = StreamActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    // Append events
    for i in 0..5 {
        let msg = StreamMessage::Append {
            family_id: RouteFamily::new(1),
            route: Route::new("stream://acme/orders/checkout/append"),
            resource_offset: i,
            body: Bytes::from(format!("event-{}", i).into_bytes()),
            metadata: None,
        };
        actor.receive(msg, &mut ctx);
    }

    // Act - Read with limit of 0
    let read_msg = StreamMessage::Read {
        family_id: RouteFamily::new(1),
        route: Route::new("stream://acme/orders/checkout/read"),
        from_offset: 0,
        limit: 0,
    };
    actor.receive(read_msg, &mut ctx);

    // Assert
    assert_eq!(actor.last_read_count(), 0);
}

#[test]
fn should_support_large_event_payloads() {
    // Arrange
    let mut actor = StreamActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    // Create 100KB payload
    let large_payload = vec![b'X'; 100_000];

    let msg = StreamMessage::Append {
        family_id: RouteFamily::new(1),
        route: Route::new("stream://acme/orders/checkout/append"),
        resource_offset: 0,
        body: Bytes::from(large_payload),
        metadata: None,
    };

    // Act
    actor.receive(msg, &mut ctx);

    // Assert
    assert_eq!(actor.event_count(), 1);
}

#[test]
fn should_maintain_independent_event_counts_per_resource() {
    // Arrange
    let mut actor1 = StreamActor::new(RouteFamily::new(1));
    let mut actor2 = StreamActor::new(RouteFamily::new(2));
    let mut ctx1 = make_ctx();
    let mut ctx2 = make_ctx();

    let msg1 = StreamMessage::Append {
        family_id: RouteFamily::new(1),
        route: Route::new("stream://acme/orders/checkout/append"),
        resource_offset: 0,
        body: Bytes::from(b"checkout-event".to_vec()),
        metadata: None,
    };

    let msg2 = StreamMessage::Append {
        family_id: RouteFamily::new(2),
        route: Route::new("stream://acme/orders/payment/append"),
        resource_offset: 0,
        body: Bytes::from(b"payment-event".to_vec()),
        metadata: None,
    };

    // Act
    actor1.receive(msg1, &mut ctx1);
    actor2.receive(msg2, &mut ctx2);

    // Assert
    assert_eq!(actor1.event_count(), 1);
    assert_eq!(actor2.event_count(), 1);
}

#[test]
fn should_handle_rapid_sequential_appends() {
    // Arrange
    let mut actor = StreamActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    // Act - Append 100 events rapidly
    for i in 0..100 {
        let msg = StreamMessage::Append {
            family_id: RouteFamily::new(1),
            route: Route::new("stream://acme/orders/checkout/append"),
            resource_offset: i,
            body: Bytes::from(format!("event-{}", i).into_bytes()),
            metadata: None,
        };
        actor.receive(msg, &mut ctx);
    }

    // Assert
    assert_eq!(actor.event_count(), 100);
}

#[test]
fn should_read_all_events_when_limit_exceeds_count() {
    // Arrange
    let mut actor = StreamActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    // Append 5 events
    for i in 0..5 {
        let msg = StreamMessage::Append {
            family_id: RouteFamily::new(1),
            route: Route::new("stream://acme/orders/checkout/append"),
            resource_offset: i,
            body: Bytes::from(format!("event-{}", i).into_bytes()),
            metadata: None,
        };
        actor.receive(msg, &mut ctx);
    }

    // Act - Read with limit of 1000
    let read_msg = StreamMessage::Read {
        family_id: RouteFamily::new(1),
        route: Route::new("stream://acme/orders/checkout/read"),
        from_offset: 0,
        limit: 1000,
    };
    actor.receive(read_msg, &mut ctx);

    // Assert - Should return all 5 events
    assert_eq!(actor.last_read_count(), 5);
}
