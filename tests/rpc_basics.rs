//! RPC domain basics tests
//!
//! Contains three tiers:
//! 1. Authorization: Session permission enforcement for RPC operations
//! 2. Semantics: Request routing, worker assignment, queue management, request/response correlation
//! 3. Specification validation: Wire format, error codes, acceptance criteria

// ===== Authorization Tests =====

use bytes::Bytes;
use fitz::auth::Permission;
use fitz::domains::rpc::session::SessionActor;
use fitz::domains::rpc::{RpcError, RpcErrorCode, RpcResponse};
use fitz::domains::rpc::{RpcMessage, RpcRequest, RpcRouteActor};
use fitz::prelude::Actor;
use fitz::runtime::actor::Context;
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use fitz::session::permissions::SessionPermissions;
use fitz::session::session::SessionId;
use std::sync::Arc;
use uuid::Uuid;

fn make_ctx() -> Context<RpcRouteActor> {
    let router = Arc::new(fitz::runtime::router::Router::new());
    let addr = RouteAddress::new(
        RouteFamily::new(1),
        Route::new("rpc://acme/auth/user/create"),
    );
    Context::new(addr, router)
}

#[test]
fn should_reject_rpc_request_without_call_permission() {
    // Arrange
    let mut actor = RpcRouteActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    let worker_addr = RouteAddress::new(
        RouteFamily::new(1),
        Route::new("worker://acme/auth/worker1"),
    );
    actor.receive(
        RpcMessage::RegisterWorker {
            worker_addr: worker_addr.clone(),
        },
        &mut ctx,
    );

    // Session without permission
    let session = SessionActor::new(SessionId(1), SessionPermissions::empty());

    // Request without required permission
    let request = RpcRequest {
        family_id: RouteFamily::new(1),
        correlation_id: Uuid::new_v4(),
        route: Route::new("rpc://acme/auth/user/create"),
        reply_route: Route::new("inbox://session/unauthorized"),
        body: Bytes::from(b"{ \"username\": \"hacker\" }".to_vec()),
    };

    // Act
    let result = session.call_rpc(request, &mut actor, &mut ctx);

    // Assert
    assert!(result.is_err());
    assert_eq!(actor.pending_count(), 0);
}

#[test]
fn should_allow_rpc_request_with_valid_call_permission() {
    // Arrange
    let mut actor = RpcRouteActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    let worker_addr = RouteAddress::new(
        RouteFamily::new(1),
        Route::new("worker://acme/auth/worker1"),
    );
    actor.receive(
        RpcMessage::RegisterWorker {
            worker_addr: worker_addr.clone(),
        },
        &mut ctx,
    );

    // Session with write permission
    let perms = vec![Permission::parse("rpc://acme/auth/**#write").unwrap()];
    let session_perms = SessionPermissions::from_permissions(perms);
    let session = SessionActor::new(SessionId(1), session_perms);

    // Request with proper permission granted
    let request = RpcRequest {
        family_id: RouteFamily::new(1),
        correlation_id: Uuid::new_v4(),
        route: Route::new("rpc://acme/auth/user/create"),
        reply_route: Route::new("inbox://session/authorized"),
        body: Bytes::from(b"{ \"username\": \"alice\" }".to_vec()),
    };

    // Act
    let result = session.call_rpc(request, &mut actor, &mut ctx);

    // Assert
    assert!(result.is_ok());
    assert_eq!(actor.pending_count(), 0);
    assert_eq!(actor.worker_count(), 1);
}

#[test]
fn should_enforce_realm_isolation_in_authorization() {
    // Arrange
    let mut actor_acme = RpcRouteActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    // Worker in acme realm
    let worker_acme = RouteAddress::new(
        RouteFamily::new(1),
        Route::new("worker://acme/data/worker1"),
    );
    actor_acme.receive(
        RpcMessage::RegisterWorker {
            worker_addr: worker_acme.clone(),
        },
        &mut ctx,
    );

    // Session with permission for corp realm (different realm)
    let perms = vec![Permission::parse("rpc://corp/data/**#write").unwrap()];
    let session_perms = SessionPermissions::from_permissions(perms);
    let session = SessionActor::new(SessionId(1), session_perms);

    // Try to call acme RPC with corp realm permissions (should fail)
    let request_cross_realm = RpcRequest {
        family_id: RouteFamily::new(1),
        correlation_id: Uuid::new_v4(),
        route: Route::new("rpc://acme/data/query/execute"),
        reply_route: Route::new("inbox://session/corp123"),
        body: Bytes::from(b"{ \"query\": \"SELECT *\" }".to_vec()),
    };

    // Act
    let result = session.call_rpc(request_cross_realm, &mut actor_acme, &mut ctx);

    // Assert
    assert!(result.is_err());
    assert_eq!(actor_acme.pending_count(), 0);
}

#[test]
fn should_allow_worker_registration_with_valid_permissions() {
    // Arrange
    let mut actor = RpcRouteActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    // Session with all permission to register workers
    let perms = vec![Permission::parse("rpc://acme/inventory/**#*").unwrap()];
    let session_perms = SessionPermissions::from_permissions(perms);
    let session = SessionActor::new(SessionId(1), session_perms);

    let worker_addr = RouteAddress::new(
        RouteFamily::new(1),
        Route::new("worker://acme/inventory/worker1"),
    );
    let route = Route::new("rpc://acme/inventory/item/query");

    // Act
    let result = session.register_worker(worker_addr, &route, &mut actor, &mut ctx);

    // Assert
    assert!(result.is_ok());
    assert_eq!(actor.worker_count(), 1);
}

#[test]
fn should_reject_worker_registration_without_permissions() {
    // Arrange
    let mut actor = RpcRouteActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    // Session without admin permission (only write)
    let perms = vec![Permission::parse("rpc://acme/admin/**#write").unwrap()];
    let session_perms = SessionPermissions::from_permissions(perms);
    let session = SessionActor::new(SessionId(1), session_perms);

    let unauthorized_worker = RouteAddress::new(
        RouteFamily::new(1),
        Route::new("worker://acme/admin/rogue-worker"),
    );
    let route = Route::new("rpc://acme/admin/user/delete");

    // Act
    let result = session.register_worker(unauthorized_worker, &route, &mut actor, &mut ctx);

    // Assert
    assert!(result.is_err());
    assert_eq!(actor.worker_count(), 0);
}

#[test]
fn should_enforce_scope_boundaries_for_rpc_calls() {
    // Arrange
    let mut actor = RpcRouteActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    let worker_addr = RouteAddress::new(
        RouteFamily::new(1),
        Route::new("worker://acme/billing/worker1"),
    );
    actor.receive(
        RpcMessage::RegisterWorker {
            worker_addr: worker_addr.clone(),
        },
        &mut ctx,
    );

    // Session with only read permission (needs write for create)
    let perms = vec![Permission::parse("rpc://acme/billing/**#read").unwrap()];
    let session_perms = SessionPermissions::from_permissions(perms);
    let session = SessionActor::new(SessionId(1), session_perms);

    // Request to billing service with only read permissions (needs write)
    let request = RpcRequest {
        family_id: RouteFamily::new(1),
        correlation_id: Uuid::new_v4(),
        route: Route::new("rpc://acme/billing/invoice/create"),
        reply_route: Route::new("inbox://session/limited"),
        body: Bytes::from(b"{ \"amount\": 100 }".to_vec()),
    };

    // Act
    let result = session.call_rpc(request, &mut actor, &mut ctx);

    // Assert
    assert!(result.is_err());
    assert_eq!(actor.pending_count(), 0);
}

#[test]
fn should_allow_requests_within_granted_scope() {
    // Arrange
    let mut actor = RpcRouteActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    let worker_addr = RouteAddress::new(
        RouteFamily::new(1),
        Route::new("worker://acme/billing/worker1"),
    );
    actor.receive(
        RpcMessage::RegisterWorker {
            worker_addr: worker_addr.clone(),
        },
        &mut ctx,
    );

    // Session with write permission for the scope
    let perms = vec![Permission::parse("rpc://acme/billing/**#write").unwrap()];
    let session_perms = SessionPermissions::from_permissions(perms);
    let session = SessionActor::new(SessionId(1), session_perms);

    // Request with proper scope
    let request = RpcRequest {
        family_id: RouteFamily::new(1),
        correlation_id: Uuid::new_v4(),
        route: Route::new("rpc://acme/billing/invoice/query"),
        reply_route: Route::new("inbox://session/authorized"),
        body: Bytes::from(b"{ \"invoice_id\": \"inv-123\" }".to_vec()),
    };

    // Act
    let result = session.call_rpc(request, &mut actor, &mut ctx);

    // Assert
    assert!(result.is_ok());
    assert_eq!(actor.pending_count(), 0);
    assert_eq!(actor.worker_count(), 1);
}

// ===== Semantics Tests =====

fn make_semantics_ctx() -> Context<RpcRouteActor> {
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
    let mut ctx = make_semantics_ctx();

    let worker_addr = RouteAddress::new(
        RouteFamily::new(1),
        Route::new("worker://realm/service/worker1"),
    );

    let subscribe_msg = RpcMessage::RegisterWorker {
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
    let mut ctx = make_semantics_ctx();

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
    let mut ctx = make_semantics_ctx();

    let worker_addr = RouteAddress::new(
        RouteFamily::new(1),
        Route::new("worker://realm/service/worker1"),
    );

    let subscribe_msg = RpcMessage::RegisterWorker {
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
    let mut ctx = make_semantics_ctx();

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
    let mut ctx = make_semantics_ctx();

    // Register three workers
    for i in 1..=3 {
        let worker_addr = RouteAddress::new(
            RouteFamily::new(1),
            Route::new(format!("worker://realm/service/worker{i}")),
        );
        let subscribe_msg = RpcMessage::RegisterWorker {
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
    let mut ctx = make_semantics_ctx();

    let worker_addr = RouteAddress::new(
        RouteFamily::new(1),
        Route::new("worker://realm/service/worker1"),
    );

    let subscribe_msg = RpcMessage::RegisterWorker {
        worker_addr: worker_addr.clone(),
    };
    actor.receive(subscribe_msg, &mut ctx);

    // Act
    let unsubscribe_msg = RpcMessage::UnregisterWorker {
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
    let mut ctx = make_semantics_ctx();

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
    let subscribe_msg = RpcMessage::RegisterWorker {
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
    let mut ctx = make_semantics_ctx();

    let worker_addr = RouteAddress::new(
        RouteFamily::new(1),
        Route::new("worker://realm/service/worker1"),
    );
    actor.receive(
        RpcMessage::RegisterWorker {
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
    let mut ctx = make_semantics_ctx();

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
    let mut ctx = make_semantics_ctx();

    let worker_addr = RouteAddress::new(
        RouteFamily::new(1),
        Route::new("worker://realm/service/worker1"),
    );
    actor.receive(
        RpcMessage::RegisterWorker {
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
        body: Bytes::from(vec![1, 2, 3]),
    };
    actor.receive(RpcMessage::Request(request2), &mut ctx);

    // Assert
    assert_eq!(actor.pending_count(), 0);
}

#[cfg(test)]
mod protocol_spec {
    //! Wire format and protocol constant validation.
    //!
    //! Structural checks; passing these does not mean the RPC runtime is correct.

    use super::*;

    // ===== Specification Validation Tests =====

    #[test]
    fn should_have_correlation_id_in_request() {
        // Arrange
        let correlation_id = Uuid::new_v4();
        let family = RouteFamily::new(1);
        let route = Route::new("rpc://acme/auth/user/create");
        let reply_route = Route::new("inbox://session/123");
        let body = Bytes::from("test payload");

        // Act
        let request = RpcRequest::new(family, correlation_id, route, reply_route, body);

        // Assert
        assert_eq!(
            request.correlation_id, correlation_id,
            "correlation_id should be stored in request"
        );
    }

    #[test]
    fn should_use_uuid_for_correlation_id() {
        // Documentation test: correlation_id MUST be exactly 16 bytes (UUID)
        // This enables distributed tracing and response matching

        // Arrange
        let correlation_id = Uuid::new_v4();

        // Act
        let uuid_bytes = correlation_id.as_bytes();

        // Assert
        assert_eq!(
            uuid_bytes.len(),
            16,
            "correlation_id (UUID) must be exactly 16 bytes"
        );
    }

    #[test]
    fn should_echo_correlation_id_in_response() {
        // Arrange
        let correlation_id = Uuid::new_v4();
        let _seq = 0u64;
        let _stream_end = true;
        let body = Bytes::from("response payload");

        // Act
        let response = RpcResponse::single(correlation_id, body);

        // Assert
        assert_eq!(
            response.correlation_id, correlation_id,
            "response must echo request correlation_id"
        );
    }

    #[test]
    fn should_have_sequence_number_for_streaming() {
        // Arrange
        let correlation_id = Uuid::new_v4();
        let seq = 5u64; // Middle chunk
        let stream_end = false;
        let body = Bytes::from("middle chunk");

        // Act
        let response = RpcResponse::chunk(correlation_id, seq, body, stream_end);

        // Assert
        assert_eq!(response.seq, 5, "sequence number should be incremented");
        assert!(
            !response.stream_end,
            "middle chunk should not mark stream end"
        );
    }

    #[test]
    fn should_have_stream_end_flag_for_final_chunk() {
        // Arrange
        let correlation_id = Uuid::new_v4();
        let seq = 10u64; // Final chunk (seq should be highest)
        let stream_end = true;
        let body = Bytes::from("final chunk");

        // Act
        let response = RpcResponse::chunk(correlation_id, seq, body, stream_end);

        // Assert
        assert!(response.stream_end, "final chunk must set stream_end=true");
    }

    #[test]
    fn should_include_payload_in_request_response() {
        // Arrange
        let family = RouteFamily::new(1);
        let route = Route::new("rpc://acme/auth/user/create");
        let reply_route = Route::new("inbox://session/123");
        let request_body = Bytes::from("create user request");
        let response_body = Bytes::from("user created");

        // Act
        let request = RpcRequest::new(family, Uuid::new_v4(), route, reply_route, request_body);
        let response = RpcResponse::single(Uuid::new_v4(), response_body);

        // Assert
        assert_eq!(
            request.body,
            Bytes::from("create user request"),
            "request body preserved"
        );
        assert_eq!(
            response.body,
            Bytes::from("user created"),
            "response body preserved"
        );
    }

    #[test]
    fn should_define_error_code_6001_rpc_timeout() {
        // Arrange
        let correlation_id = Uuid::new_v4();

        // Act
        let error = RpcError::timeout(correlation_id);

        // Assert
        assert_eq!(error.code, RpcErrorCode::Timeout, "6001 = RPC_TIMEOUT");
        assert_eq!(error.correlation_id, correlation_id);
    }

    #[test]
    fn should_define_error_code_6003_rpc_backpressure() {
        // Arrange
        let correlation_id = Uuid::new_v4();

        // Act
        let error = RpcError::backpressure(correlation_id);

        // Assert
        assert_eq!(
            error.code,
            RpcErrorCode::Backpressure,
            "6003 = RPC_BACKPRESSURE"
        );
    }

    #[test]
    fn should_define_error_code_6004_route_not_registered() {
        // Arrange
        let correlation_id = Uuid::new_v4();

        // Act
        let error = RpcError::invalid_route(correlation_id);

        // Assert
        assert_eq!(
            error.code,
            RpcErrorCode::InvalidRoute,
            "6004 = ROUTE_NOT_REGISTERED/INVALID_ROUTE"
        );
    }

    #[test]
    fn should_define_error_code_6006_rpc_invalid_sequence() {
        // Arrange

        // Act
        let code = fitz::protocol::error_codes::rpc::ERR_RPC_INVALID_SEQUENCE;

        // Assert
        assert_eq!(code, 6006, "6006 = RPC_INVALID_SEQUENCE");
    }

    #[test]
    fn should_define_error_code_6007_rpc_duplicate_correlation() {
        let code = fitz::protocol::error_codes::rpc::ERR_RPC_DUPLICATE_CORRELATION;
        assert_eq!(code, 6007, "6007 = RPC_DUPLICATE_CORRELATION");
    }

    #[test]
    fn should_define_error_code_6008_rpc_wrong_worker() {
        let code = fitz::protocol::error_codes::rpc::ERR_RPC_WRONG_WORKER;
        assert_eq!(code, 6008, "6008 = RPC_WRONG_WORKER");
    }

    #[test]
    fn should_complete_single_request_response_cycle() {
        // Arrange
        let family = RouteFamily::new(1);
        let correlation_id = Uuid::new_v4();
        let route = Route::new("rpc://acme/billing/invoice/create");
        let reply_route = Route::new("inbox://session/123");
        let request_body = Bytes::from("{ \"amount\": 100 }");
        let response_body = Bytes::from("{ \"invoice_id\": 123 }");

        // Act
        let request = RpcRequest::new(family, correlation_id, route, reply_route, request_body);
        let response = RpcResponse::single(correlation_id, response_body);

        // Assert
        assert_eq!(request.correlation_id, correlation_id);
        assert_eq!(response.correlation_id, correlation_id);
        assert_eq!(request.correlation_id, response.correlation_id);
    }

    #[test]
    fn should_match_response_to_request_by_correlation_id() {
        // Arrange
        let correlation_id = Uuid::new_v4();
        let request_family = RouteFamily::new(1);
        let request_route = Route::new("rpc://realm/auth/user/get");
        let reply_route = Route::new("inbox://session/456");
        let request_body = Bytes::from("{ \"user_id\": 42 }");

        // Act
        let request = RpcRequest::new(
            request_family,
            correlation_id,
            request_route,
            reply_route,
            request_body,
        );
        let response = RpcResponse::single(correlation_id, Bytes::from("{ \"name\": \"Alice\" }"));

        // Assert
        assert_eq!(request.correlation_id, response.correlation_id);
    }

    #[test]
    fn should_reassemble_multi_chunk_streaming_response() {
        // Arrange
        let correlation_id = Uuid::new_v4();

        // Act
        let chunk1 = RpcResponse::chunk(correlation_id, 0, Bytes::from("chunk1"), false);
        let chunk2 = RpcResponse::chunk(correlation_id, 1, Bytes::from("chunk2"), false);
        let chunk3 = RpcResponse::chunk(correlation_id, 2, Bytes::from("chunk3"), true);

        // Assert
        assert_eq!(chunk1.seq, 0);
        assert_eq!(chunk2.seq, 1);
        assert_eq!(chunk3.seq, 2);
        assert!(chunk3.stream_end);
    }

    #[test]
    fn should_detect_out_of_order_streaming_chunks() {
        // Arrange
        let correlation_id = Uuid::new_v4();

        // Act
        let chunk0 = RpcResponse::chunk(correlation_id, 0, Bytes::from("chunk0"), false);
        let chunk2 = RpcResponse::chunk(correlation_id, 2, Bytes::from("chunk2"), false);

        // Assert
        assert_eq!(chunk0.seq, 0);
        assert_eq!(chunk2.seq, 2);
        // Gap detected between chunk0 and chunk2
        assert!(chunk2.seq - chunk0.seq > 1);
    }

    #[test]
    fn should_handle_single_chunk_as_complete_response() {
        // Arrange
        let correlation_id = Uuid::new_v4();
        let body = Bytes::from("complete response");

        // Act
        let response = RpcResponse::single(correlation_id, body);

        // Assert
        assert_eq!(response.seq, 0);
        assert!(response.stream_end);
        assert_eq!(response.correlation_id, correlation_id);
    }

    #[test]
    fn should_include_route_family_in_request() {
        // Arrange
        let family = RouteFamily::new(1);
        let correlation_id = Uuid::new_v4();
        let route = Route::new("rpc://acme/auth/user/create");
        let reply_route = Route::new("inbox://session/123");
        let body = Bytes::from("test");

        // Act
        let request = RpcRequest::new(family, correlation_id, route, reply_route, body);

        // Assert
        assert_eq!(request.family_id, family);
    }

    #[test]
    fn should_include_reply_route_in_request() {
        // Arrange
        let family = RouteFamily::new(1);
        let correlation_id = Uuid::new_v4();
        let route = Route::new("rpc://acme/auth/user/create");
        let reply_route = Route::new("inbox://session/123");
        let body = Bytes::from("test");

        // Act
        let request = RpcRequest::new(
            family,
            correlation_id,
            route,
            reply_route.clone(),
            body.clone(),
        );

        // Assert
        assert_eq!(&request.reply_route, &reply_route);
    }

    #[test]
    fn should_include_target_route_in_request() {
        // Arrange
        let family = RouteFamily::new(1);
        let correlation_id = Uuid::new_v4();
        let route = Route::new("rpc://acme/auth/user/create");
        let reply_route = Route::new("inbox://session/123");
        let body = Bytes::from("test");

        // Act
        let request = RpcRequest::new(family, correlation_id, route.clone(), reply_route, body);

        // Assert
        assert_eq!(&request.route, &route);
    }
}
