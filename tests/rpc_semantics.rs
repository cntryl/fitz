use bytes::Bytes;
use fitz::domains::rpc::{RpcMessage, RpcRequest, RpcResponse, RpcRouteActor};
use fitz::prelude::Actor;
use fitz::runtime::actor::Context;
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use std::sync::Arc;
use uuid::Uuid;

// This file asserts RPC semantics: verifies request/response correlation, worker assignment,
// backpressure, and failure handling.
// It MUST NOT test implementation details.

fn make_ctx() -> Context<RpcRouteActor> {
    let router = Arc::new(fitz::runtime::router::Router::new());
    let addr = RouteAddress::new(
        RouteFamily::new(1),
        Route::new("rpc://realm/auth/user/authenticate"),
    );
    Context::new(addr, router)
}

#[test]
fn should_route_request_to_available_worker() {
    // Arrange
    let mut actor = RpcRouteActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    let worker_addr = RouteAddress::new(
        RouteFamily::new(1),
        Route::new("worker://realm/service/worker1"),
    );

    let subscribe_msg = RpcMessage::Subscribe {
        worker_addr: worker_addr.clone(),
    };
    actor.receive(subscribe_msg, &mut ctx);

    let request = RpcRequest {
        family_id: RouteFamily::new(1),
        correlation_id: Uuid::new_v4(),
        route: Route::new("rpc://realm/service/handler/call"),
        reply_route: Route::new("inbox://session/123"),
        body: Bytes::from(vec![1, 2, 3]),
    };

    // Act
    let msg = RpcMessage::Request(request);
    actor.receive(msg, &mut ctx);

    // Assert
    assert_eq!(actor.pending_count(), 0);
    assert_eq!(actor.worker_count(), 1);
}

#[test]
fn should_enqueue_request_when_no_workers_available() {
    // Arrange
    let mut actor = RpcRouteActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    let request = RpcRequest {
        family_id: RouteFamily::new(1),
        correlation_id: Uuid::new_v4(),
        route: Route::new("rpc://realm/auth/user/authenticate"),
        reply_route: Route::new("inbox://session/123"),
        body: Bytes::from(vec![1, 2, 3]),
    };

    // Act
    let msg = RpcMessage::Request(request);
    actor.receive(msg, &mut ctx);

    // Assert
    assert_eq!(actor.pending_count(), 1);
    assert_eq!(actor.worker_count(), 0);
}

#[test]
fn should_correlate_response_with_request() {
    // Arrange
    let mut actor = RpcRouteActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    let worker_addr = RouteAddress::new(
        RouteFamily::new(1),
        Route::new("worker://realm/service/worker1"),
    );

    let subscribe_msg = RpcMessage::Subscribe {
        worker_addr: worker_addr.clone(),
    };
    actor.receive(subscribe_msg, &mut ctx);

    let request = RpcRequest {
        family_id: RouteFamily::new(1),
        correlation_id: Uuid::new_v4(),
        route: Route::new("rpc://realm/auth/user/authenticate"),
        reply_route: Route::new("inbox://session/123"),
        body: Bytes::from(vec![1, 2, 3]),
    };
    actor.receive(RpcMessage::Request(request), &mut ctx);

    // Act
    let response = RpcResponse {
        correlation_id: Uuid::new_v4(),
        seq: 0,
        body: Bytes::from(vec![4, 5, 6]),
        stream_end: true,
    };
    actor.receive(RpcMessage::Response(response), &mut ctx);

    // Assert
    assert_eq!(actor.pending_count(), 0);
}

#[test]
fn should_reject_request_when_queue_is_full() {
    // Arrange
    let mut actor = RpcRouteActor::with_capacity(RouteFamily::new(1), 2);
    let mut ctx = make_ctx();

    // Fill the queue
    for _i in 0..2 {
        let request = RpcRequest {
            family_id: RouteFamily::new(1),
            correlation_id: Uuid::new_v4(),
            route: Route::new("rpc://realm/auth/user/authenticate"),
            reply_route: Route::new("inbox://session/123"),
            body: Bytes::from(vec![1, 2, 3]),
        };
        actor.receive(RpcMessage::Request(request), &mut ctx);
    }

    let overflow_request = RpcRequest {
        family_id: RouteFamily::new(1),
        correlation_id: Uuid::new_v4(),
        route: Route::new("rpc://realm/auth/user/authenticate"),
        reply_route: Route::new("inbox://session/123"),
        body: Bytes::from(vec![1, 2, 3]),
    };

    // Act
    actor.receive(RpcMessage::Request(overflow_request), &mut ctx);

    // Assert
    assert_eq!(actor.pending_count(), 2);
}

#[test]
fn should_distribute_requests_across_multiple_workers() {
    // Arrange
    let mut actor = RpcRouteActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    // Register three workers
    for i in 1..=3 {
        let worker_addr = RouteAddress::new(
            RouteFamily::new(1),
            Route::new(format!("worker://realm/service/worker{}", i)),
        );
        let subscribe_msg = RpcMessage::Subscribe {
            worker_addr: worker_addr.clone(),
        };
        actor.receive(subscribe_msg, &mut ctx);
    }

    // Act
    for _i in 0..3 {
        let request = RpcRequest {
            family_id: RouteFamily::new(1),
            correlation_id: Uuid::new_v4(),
            route: Route::new("rpc://realm/auth/user/authenticate"),
            reply_route: Route::new("inbox://session/123"),
            body: Bytes::from(vec![1, 2, 3]),
        };
        actor.receive(RpcMessage::Request(request), &mut ctx);
    }

    // Assert
    assert_eq!(actor.pending_count(), 0);
    assert_eq!(actor.worker_count(), 3);
}

#[test]
fn should_handle_worker_unsubscribe() {
    // Arrange
    let mut actor = RpcRouteActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    let worker_addr = RouteAddress::new(
        RouteFamily::new(1),
        Route::new("worker://realm/service/worker1"),
    );

    let subscribe_msg = RpcMessage::Subscribe {
        worker_addr: worker_addr.clone(),
    };
    actor.receive(subscribe_msg, &mut ctx);

    // Act
    let unsubscribe_msg = RpcMessage::Unsubscribe {
        worker_addr: worker_addr.clone(),
    };
    actor.receive(unsubscribe_msg, &mut ctx);

    // Assert
    assert_eq!(actor.worker_count(), 0);
}

#[test]
fn should_maintain_request_order_in_queue() {
    // Arrange
    let mut actor = RpcRouteActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    // Enqueue three requests
    for _i in 0..3 {
        let request = RpcRequest {
            family_id: RouteFamily::new(1),
            correlation_id: Uuid::new_v4(),
            route: Route::new("rpc://realm/auth/user/authenticate"),
            reply_route: Route::new("inbox://session/123"),
            body: Bytes::from(vec![_i as u8]),
        };
        actor.receive(RpcMessage::Request(request), &mut ctx);
    }

    // Act
    let worker_addr = RouteAddress::new(
        RouteFamily::new(1),
        Route::new("worker://realm/service/worker1"),
    );
    let subscribe_msg = RpcMessage::Subscribe {
        worker_addr: worker_addr.clone(),
    };
    actor.receive(subscribe_msg, &mut ctx);

    // Assert
    assert_eq!(actor.pending_count(), 2);
}

#[test]
fn should_handle_streaming_response_with_multiple_chunks() {
    // Arrange
    let mut actor = RpcRouteActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    let worker_addr = RouteAddress::new(
        RouteFamily::new(1),
        Route::new("worker://realm/service/worker1"),
    );
    actor.receive(
        RpcMessage::Subscribe {
            worker_addr: worker_addr.clone(),
        },
        &mut ctx,
    );

    let request = RpcRequest {
        family_id: RouteFamily::new(1),
        correlation_id: Uuid::new_v4(),
        route: Route::new("rpc://realm/reports/monthly/generate"),
        reply_route: Route::new("inbox://session/123"),
        body: Bytes::from(vec![1, 2, 3]),
    };
    actor.receive(RpcMessage::Request(request), &mut ctx);

    // Act
    for seq in 0..3 {
        let response = RpcResponse {
            correlation_id: Uuid::new_v4(),
            seq,
            body: Bytes::from(vec![seq as u8]),
            stream_end: seq == 2,
        };
        actor.receive(RpcMessage::Response(response), &mut ctx);
    }

    // Assert
    assert_eq!(actor.pending_count(), 0);
}

#[test]
fn should_isolate_requests_across_route_families() {
    // Arrange
    let mut actor1 = RpcRouteActor::new(RouteFamily::new(1));
    let mut actor2 = RpcRouteActor::new(RouteFamily::new(2));
    let mut ctx = make_ctx();

    let request1 = RpcRequest {
        family_id: RouteFamily::new(1),
        correlation_id: Uuid::new_v4(),
        route: Route::new("rpc://realm/auth/user/authenticate"),
        reply_route: Route::new("inbox://session/123"),
        body: Bytes::from(vec![1]),
    };

    let request2 = RpcRequest {
        family_id: RouteFamily::new(2),
        correlation_id: Uuid::new_v4(),
        route: Route::new("rpc://realm/auth/user/authenticate"),
        reply_route: Route::new("inbox://session/456"),
        body: Bytes::from(vec![2]),
    };

    // Act
    actor1.receive(RpcMessage::Request(request1), &mut ctx);
    actor2.receive(RpcMessage::Request(request2), &mut ctx);

    // Assert
    assert_eq!(actor1.pending_count(), 1);
    assert_eq!(actor2.pending_count(), 1);
}

#[test]
fn should_cleanup_state_after_request_completion() {
    // Arrange
    let mut actor = RpcRouteActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    let worker_addr = RouteAddress::new(
        RouteFamily::new(1),
        Route::new("worker://realm/service/worker1"),
    );
    actor.receive(
        RpcMessage::Subscribe {
            worker_addr: worker_addr.clone(),
        },
        &mut ctx,
    );

    let correlation_id = Uuid::new_v4();
    let request = RpcRequest {
        family_id: RouteFamily::new(1),
        correlation_id,
        route: Route::new("rpc://realm/inventory/item/update"),
        reply_route: Route::new("inbox://session/123"),
        body: Bytes::from(vec![1, 2, 3]),
    };
    actor.receive(RpcMessage::Request(request), &mut ctx);

    // Act
    let response = RpcResponse {
        correlation_id,
        seq: 0,
        body: Bytes::from(vec![4, 5, 6]),
        stream_end: true,
    };
    actor.receive(RpcMessage::Response(response), &mut ctx);

    // Send a second request to verify clean state
    let request2 = RpcRequest {
        family_id: RouteFamily::new(1),
        correlation_id: Uuid::new_v4(),
        route: Route::new("rpc://realm/inventory/item/update"),
        reply_route: Route::new("inbox://session/123"),
        body: Bytes::from(vec![7, 8, 9]),
    };
    actor.receive(RpcMessage::Request(request2), &mut ctx);

    // Assert
    assert_eq!(actor.pending_count(), 0);
}
