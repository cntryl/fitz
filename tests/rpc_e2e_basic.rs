use fitz::domains::rpc::{RpcRouteActor, RpcMessage, RpcRequest, RpcResponse};
use fitz::prelude::Actor;
use fitz::runtime::actor::Context;
use fitz::runtime::routing::{Route, RouteFamily, RouteAddress};
use std::sync::Arc;

// This file tests basic RPC golden paths: simple request/response cycles
// and fundamental worker interactions.

fn make_ctx() -> Context<RpcRouteActor> {
    let router = Arc::new(fitz::runtime::router::Router::new());
    let addr = RouteAddress::new(
        RouteFamily::new(1),
        Route::new("rpc://acme/auth/user/create"),
    );
    Context::new(addr, router)
}

#[test]
fn should_complete_basic_request_response_cycle() {
    // Arrange
    let mut actor = RpcRouteActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    let worker_addr = RouteAddress::new(
        RouteFamily::new(1),
        Route::new("worker://acme/auth/worker1"),
    );
    
    actor.receive(
        RpcMessage::Subscribe {
            worker_addr: worker_addr.clone(),
        },
        &mut ctx,
    );

    let request = RpcRequest {
        correlation_id: "req-create-user-001".to_string(),
        route: Route::new("rpc://acme/auth/user/create"),
        reply_route: Route::new("inbox://session/abc123"),
        body: b"{ \"username\": \"alice\", \"email\": \"alice@example.com\" }".to_vec(),
    };

    // Act
    actor.receive(RpcMessage::Request(request), &mut ctx);

    let response = RpcResponse {
        correlation_id: "req-create-user-001".to_string(),
        seq: 0,
        body: b"{ \"user_id\": \"12345\", \"status\": \"created\" }".to_vec(),
        stream_end: true,
    };
    actor.receive(RpcMessage::Response(response), &mut ctx);

    // Assert
    assert_eq!(actor.pending_count(), 0);
    assert_eq!(actor.worker_count(), 1);
}

#[test]
fn should_handle_streaming_report_generation() {
    // Arrange
    let mut actor = RpcRouteActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    let worker_addr = RouteAddress::new(
        RouteFamily::new(1),
        Route::new("worker://acme/reports/worker1"),
    );
    
    actor.receive(
        RpcMessage::Subscribe {
            worker_addr: worker_addr.clone(),
        },
        &mut ctx,
    );

    let request = RpcRequest {
        correlation_id: "req-report-monthly".to_string(),
        route: Route::new("rpc://acme/reports/monthly/generate"),
        reply_route: Route::new("inbox://session/xyz789"),
        body: b"{ \"month\": \"2025-12\" }".to_vec(),
    };

    actor.receive(RpcMessage::Request(request), &mut ctx);

    // Act - Stream multiple chunks
    let chunks = [
        b"Page 1 data...".to_vec(),
        b"Page 2 data...".to_vec(),
        b"Page 3 data...".to_vec(),
    ];

    for (seq, chunk) in chunks.iter().enumerate() {
        let response = RpcResponse {
            correlation_id: "req-report-monthly".to_string(),
            seq: seq as u64,
            body: chunk.clone(),
            stream_end: seq == chunks.len() - 1,
        };
        actor.receive(RpcMessage::Response(response), &mut ctx);
    }

    // Assert
    assert_eq!(actor.pending_count(), 0);
}

#[test]
fn should_maintain_isolation_across_realms() {
    // Arrange
    let mut actor_acme = RpcRouteActor::new(RouteFamily::new(1));
    let mut actor_corp = RpcRouteActor::new(RouteFamily::new(2));
    let mut ctx = make_ctx();

    let worker_acme = RouteAddress::new(
        RouteFamily::new(1),
        Route::new("worker://acme/inventory/worker1"),
    );
    actor_acme.receive(
        RpcMessage::Subscribe {
            worker_addr: worker_acme.clone(),
        },
        &mut ctx,
    );

    let worker_corp = RouteAddress::new(
        RouteFamily::new(2),
        Route::new("worker://corp/inventory/worker1"),
    );
    actor_corp.receive(
        RpcMessage::Subscribe {
            worker_addr: worker_corp.clone(),
        },
        &mut ctx,
    );

    let request_acme = RpcRequest {
        correlation_id: "req-acme-001".to_string(),
        route: Route::new("rpc://acme/inventory/item/update"),
        reply_route: Route::new("inbox://session/acme123"),
        body: b"{ \"item_id\": \"widget-1\" }".to_vec(),
    };

    let request_corp = RpcRequest {
        correlation_id: "req-corp-001".to_string(),
        route: Route::new("rpc://corp/inventory/item/update"),
        reply_route: Route::new("inbox://session/corp456"),
        body: b"{ \"item_id\": \"gadget-1\" }".to_vec(),
    };

    // Act
    actor_acme.receive(RpcMessage::Request(request_acme), &mut ctx);
    actor_corp.receive(RpcMessage::Request(request_corp), &mut ctx);

    // Assert - Each realm has processed independently
    assert_eq!(actor_acme.pending_count(), 0);
    assert_eq!(actor_corp.pending_count(), 0);
    assert_eq!(actor_acme.worker_count(), 1);
    assert_eq!(actor_corp.worker_count(), 1);
}

#[test]
fn should_handle_multiple_workers_for_same_route() {
    // Arrange
    let mut actor = RpcRouteActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    // Register 5 workers for load distribution
    for i in 1..=5 {
        let worker_addr = RouteAddress::new(
            RouteFamily::new(1),
            Route::new(format!("worker://acme/ai/embedding/worker{}", i)),
        );
        actor.receive(
            RpcMessage::Subscribe {
                worker_addr: worker_addr.clone(),
            },
            &mut ctx,
        );
    }

    // Act - Send 10 requests (more than workers)
    for i in 0..10 {
        let request = RpcRequest {
            correlation_id: format!("req-embedding-{:03}", i),
            route: Route::new("rpc://acme/ai/embedding/generate"),
            reply_route: Route::new("inbox://session/ai789"),
            body: format!("{{ \"text\": \"sample text {}\" }}", i).into_bytes(),
        };
        actor.receive(RpcMessage::Request(request), &mut ctx);
    }

    // Assert - Some requests dispatched, rest queued (5 workers, 10 requests)
    assert!(actor.pending_count() <= 10);
    assert_eq!(actor.worker_count(), 5);
}

#[test]
fn should_queue_requests_when_worker_unregisters() {
    // Arrange
    let mut actor = RpcRouteActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    let worker_addr = RouteAddress::new(
        RouteFamily::new(1),
        Route::new("worker://acme/analytics/query/worker1"),
    );

    // Act - Subscribe
    actor.receive(
        RpcMessage::Subscribe {
            worker_addr: worker_addr.clone(),
        },
        &mut ctx,
    );

    let request1 = RpcRequest {
        correlation_id: "req-query-001".to_string(),
        route: Route::new("rpc://acme/analytics/query/run"),
        reply_route: Route::new("inbox://session/analytics1"),
        body: b"SELECT * FROM events".to_vec(),
    };
    actor.receive(RpcMessage::Request(request1), &mut ctx);

    // Unsubscribe
    actor.receive(
        RpcMessage::Unsubscribe {
            worker_addr: worker_addr.clone(),
        },
        &mut ctx,
    );

    let request2 = RpcRequest {
        correlation_id: "req-query-002".to_string(),
        route: Route::new("rpc://acme/analytics/query/run"),
        reply_route: Route::new("inbox://session/analytics2"),
        body: b"SELECT * FROM logs".to_vec(),
    };
    actor.receive(RpcMessage::Request(request2), &mut ctx);

    // Assert - Worker gone, second request queued
    assert_eq!(actor.worker_count(), 0);
    assert_eq!(actor.pending_count(), 1);
}
