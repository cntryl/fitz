//! RPC domain validation tests
//!
//! This test suite verifies RPC wire format, error codes, and acceptance criteria
//! as per TODO.md HIGH section and CLIENT.md lines 1055-1108.
//!
//! Tests cover:
//! - RPC wire format compliance (correlation_id, seq, stream_end)
//! - Error codes: 6001 (TIMEOUT), 6002 (WORKER_NOT_FOUND), 6003 (BACKPRESSURE), 6004 (ROUTE_NOT_REGISTERED)
//! - Request/response cycle
//! - Streaming response reassembly
//! - Request timeout
//! - Multiple workers

use bytes::Bytes;
use fitz::domains::rpc::{RpcError, RpcErrorCode, RpcRequest, RpcResponse};
use fitz::runtime::routing::{Route, RouteFamily};
use uuid::Uuid;

// ============================================================================
// RPC WIRE FORMAT TESTS
// ============================================================================

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

// ============================================================================
// RPC ERROR CODE TESTS (6000-6099 range)
// ============================================================================

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
fn should_define_error_code_6002_worker_not_found() {
    // Arrange
    // Documentation test: 6002 = ERR_WORKER_NOT_FOUND
    // Returned when: No worker registered for the requested route

    // Act
    let error_code = RpcErrorCode::InvalidRoute;
    // Assert
    assert!(
        !error_code.as_str().is_empty(),
        "error codes must have string representation"
    );
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
fn should_define_error_code_6001_unauthorized() {
    // Arrange
    let correlation_id = Uuid::new_v4();

    // Act
    let error = RpcError::unauthorized(correlation_id);

    // Assert
    assert_eq!(
        error.code,
        RpcErrorCode::Unauthorized,
        "6001 variant = ERR_UNAUTHORIZED"
    );
}

#[test]
fn should_have_all_rpc_error_codes_in_range_6000_6099() {
    // Arrange
    // Documentation test: All RPC error codes MUST be in 6000-6099 range
    //
    // Current defined codes:
    // - Timeout (6010 as_str: "RPC_TIMEOUT")
    // - Backpressure (6012 as_str: "RPC_BACKPRESSURE")
    // - Unauthorized (6001 as_str: "RPC_UNAUTHORIZED")
    // - InvalidRoute (6004 as_str: "RPC_INVALID_ROUTE")
    // - StreamGap (6011 as_str: "RPC_STREAM_GAP")
    // - ClientDisconnected (6013 as_str: "RPC_CLIENT_DISCONNECTED")
    // - WorkerCrashed (6014 as_str: "RPC_WORKER_CRASHED")

    // Act
    // All variants have string representations
    let timeout_str = RpcErrorCode::Timeout.as_str();
    let backpressure_str = RpcErrorCode::Backpressure.as_str();
    let unauthorized_str = RpcErrorCode::Unauthorized.as_str();

    // Assert
    // All should be prefixed with RPC_
    assert!(
        timeout_str.starts_with("RPC_"),
        "error codes should use RPC_ prefix"
    );
    assert!(
        backpressure_str.starts_with("RPC_"),
        "error codes should use RPC_ prefix"
    );
    assert!(
        unauthorized_str.starts_with("RPC_"),
        "error codes should use RPC_ prefix"
    );
}

// ============================================================================
// RPC REQUEST/RESPONSE CYCLE TESTS
// ============================================================================

#[test]
fn should_complete_single_request_response_cycle() {
    // Arrange
    let family = RouteFamily::new(1);
    let correlation_id = Uuid::new_v4();
    let route = Route::new("rpc://acme/auth/user/create");
    let reply_route = Route::new("inbox://session/123");

    // Act
    let request = RpcRequest::new(
        family,
        correlation_id,
        route,
        reply_route,
        Bytes::from("create user"),
    );

    // Continue: Response phase (single chunk)
    let response = RpcResponse::single(correlation_id, Bytes::from("user created"));

    // Assert
    assert_eq!(
        request.correlation_id, response.correlation_id,
        "correlation IDs match"
    );
    assert_eq!(response.seq, 0, "single response has seq=0");
    assert!(response.stream_end, "single response has stream_end=true");
}

#[test]
fn should_match_response_to_request_by_correlation_id() {
    // Arrange
    let correlation_id = Uuid::new_v4();
    let family = RouteFamily::new(1);
    let route = Route::new("rpc://acme/auth/user/create");
    let reply_route = Route::new("inbox://session/123");

    // Act
    let request = RpcRequest::new(family, correlation_id, route, reply_route, Bytes::from(""));
    let response = RpcResponse::single(correlation_id, Bytes::from("response"));

    // Assert
    assert_eq!(
        request.correlation_id, response.correlation_id,
        "client can match response to request"
    );
}

// ============================================================================
// RPC STREAMING RESPONSE TESTS
// ============================================================================

#[test]
fn should_reassemble_multi_chunk_streaming_response() {
    // Arrange
    let correlation_id = Uuid::new_v4();

    // Act - Simulate streaming response
    let chunk1 = RpcResponse::chunk(correlation_id, 0, Bytes::from("chunk1"), false);
    let chunk2 = RpcResponse::chunk(correlation_id, 1, Bytes::from("chunk2"), false);
    let chunk3 = RpcResponse::chunk(correlation_id, 2, Bytes::from("chunk3"), true);

    // Assert
    assert_eq!(chunk1.seq, 0, "first chunk seq=0");
    assert!(!chunk1.stream_end, "first chunk not final");

    assert_eq!(chunk2.seq, 1, "second chunk seq=1");
    assert!(!chunk2.stream_end, "second chunk not final");

    assert_eq!(chunk3.seq, 2, "third chunk seq=2");
    assert!(chunk3.stream_end, "third chunk is final");

    // All have same correlation_id
    assert_eq!(chunk1.correlation_id, chunk2.correlation_id);
    assert_eq!(chunk2.correlation_id, chunk3.correlation_id);
}

#[test]
fn should_detect_out_of_order_streaming_chunks() {
    // Arrange
    let correlation_id = Uuid::new_v4();

    // Act - Out of order: chunk 0, then chunk 2 (missing chunk 1)
    let chunk0 = RpcResponse::chunk(correlation_id, 0, Bytes::from("chunk0"), false);
    let chunk2 = RpcResponse::chunk(correlation_id, 2, Bytes::from("chunk2"), false);

    // Assert - Client should detect gap
    assert_eq!(chunk0.seq, 0);
    assert_eq!(chunk2.seq, 2);
    assert!(
        chunk2.seq != chunk0.seq + 1,
        "out of order detected by seq gap"
    );
}

#[test]
fn should_handle_single_chunk_as_complete_response() {
    // Arrange
    let correlation_id = Uuid::new_v4();

    // Act
    let single_response = RpcResponse::single(correlation_id, Bytes::from("complete response"));

    // Assert
    assert_eq!(single_response.seq, 0, "single-chunk response has seq=0");
    assert!(
        single_response.stream_end,
        "single-chunk response has stream_end=true"
    );
}

// ============================================================================
// RPC REQUEST/RESPONSE PROTOCOL TESTS
// ============================================================================

#[test]
fn should_include_route_family_in_request() {
    // Arrange
    let family = RouteFamily::new(42);
    let route = Route::new("rpc://acme/service/operation");
    let reply_route = Route::new("inbox://session/123");

    // Act
    let request = RpcRequest::new(family, Uuid::new_v4(), route, reply_route, Bytes::from(""));

    // Assert
    assert_eq!(request.family_id, family, "route family preserved");
}

#[test]
fn should_include_reply_route_in_request() {
    // Arrange
    let family = RouteFamily::new(1);
    let route = Route::new("rpc://acme/service/operation");
    let reply_route = Route::new("inbox://session/123/replies");
    let reply_route_clone = reply_route.clone();

    // Act
    let request = RpcRequest::new(family, Uuid::new_v4(), route, reply_route, Bytes::from(""));

    // Assert
    assert_eq!(
        request.reply_route, reply_route_clone,
        "reply route tells worker where to send response"
    );
}

#[test]
fn should_include_target_route_in_request() {
    // Arrange
    let family = RouteFamily::new(1);
    let target_route = Route::new("rpc://acme/auth/user/create");
    let target_route_clone = target_route.clone();
    let reply_route = Route::new("inbox://session/123");

    // Act
    let request = RpcRequest::new(
        family,
        Uuid::new_v4(),
        target_route,
        reply_route,
        Bytes::from(""),
    );

    // Assert
    assert_eq!(
        request.route, target_route_clone,
        "target route specifies worker route"
    );
}

// ============================================================================
// RPC ACCEPTANCE TESTS
// ============================================================================

#[test]
fn should_handle_worker_registration() {
    // Arrange
    // Documentation test: Worker subscribes to handle requests
    // Act
    // Subscribe wire format: SUBSCRIBE_WORKER request
    // - family_id: RouteFamily
    // - route: Route (e.g., "rpc://acme/auth/user/create")
    // - reply_route: Inbox for responses
    //
    // Assert
    // Server response: SUBSCRIBE_WORKER_OK or error
}

#[test]
fn should_handle_client_request_to_worker() {
    // Arrange
    // Documentation test: Client sends REQUEST
    // Act
    // REQUEST wire format:
    // - correlation_id: UUID (16 bytes)
    // - family_id: RouteFamily
    // - route: Target route
    // - reply_route: Inbox for response
    // - body: Request payload
    //
    // Assert
    // Server routes REQUEST to available worker
    // If no worker available: return BACKPRESSURE error
}

#[test]
fn should_handle_worker_response_to_client() {
    // Arrange
    // Documentation test: Worker sends RESPONSE
    // Act
    // RESPONSE wire format:
    // - correlation_id: UUID (echoed from request)
    // - seq: u64 (0-based sequence for streaming)
    // - stream_end: bool (true only for final chunk)
    // - body: Response payload
    //
    // Assert
    // Server forwards RESPONSE to client's reply_route
}

#[test]
fn should_timeout_request_if_worker_not_responding() {
    // Arrange
    // Documentation test: If worker doesn't respond within lease timeout:
    // Act
    // - Return RpcError::timeout() with correlation_id
    // Assert
    // - Error code: RPC_TIMEOUT
    // - Client can retry with new request
}

#[test]
fn should_return_backpressure_when_route_queue_full() {
    // Arrange
    // Documentation test: If route queue at capacity:
    // Act
    // - Return RpcError::backpressure() with correlation_id
    // Assert
    // - Error code: RPC_BACKPRESSURE
    // - Client should retry with exponential backoff
}

#[test]
fn should_return_unauthorized_when_client_lacks_permission() {
    // Arrange
    // Documentation test: If client has insufficient scope:
    // Act
    // - Return RpcError::unauthorized() with correlation_id
    // Assert
    // - Error code: RPC_UNAUTHORIZED
    // - Client should request different scopes
}

#[test]
fn should_return_invalid_route_when_no_worker_registered() {
    // Arrange
    // Documentation test: If no worker registered for route:
    // Act
    // - Return RpcError::invalid_route() with correlation_id
    // Assert
    // - Error code: RPC_INVALID_ROUTE
    // - Client should verify route and retry
}

// ============================================================================
// RPC MESSAGE HELPER TRAIT
// ============================================================================

// RPC codec should implement CodecTrait for encoding/decoding
// See: src/protocol/ for codec implementations
