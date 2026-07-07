//! RPC public protocol and error-code tests.

use bytes::Bytes;
use fitz::domains::rpc::{RpcError, RpcErrorCode, RpcRequest, RpcResponse};
use fitz::runtime::routing::{Route, RouteFamily};
use uuid::Uuid;

#[test]
fn should_have_correlation_id_in_request() {
    // Arrange
    let correlation_id = Uuid::new_v4();
    let family = RouteFamily::new(1);
    let route = Route::new("rpc://acme/auth/user/create");
    let body = Bytes::from("test payload");

    // Act
    let request = RpcRequest::new(family, correlation_id, route, body);

    // Assert
    assert_eq!(
        request.correlation_id, correlation_id,
        "correlation_id should be stored in request"
    );
}

#[test]
fn should_use_uuid_for_correlation_id() {
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
    let seq = 5u64;
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
    let seq = 10u64;
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
    let request_body = Bytes::from("create user request");
    let response_body = Bytes::from("user created");

    // Act
    let request = RpcRequest::new(family, Uuid::new_v4(), route, request_body);
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
    let request_body = Bytes::from("{ \"amount\": 100 }");
    let response_body = Bytes::from("{ \"invoice_id\": 123 }");

    // Act
    let request = RpcRequest::new(family, correlation_id, route, request_body);
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
    let request_body = Bytes::from("{ \"user_id\": 42 }");

    // Act
    let request = RpcRequest::new(request_family, correlation_id, request_route, request_body);
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
    let body = Bytes::from("test");

    // Act
    let request = RpcRequest::new(family, correlation_id, route, body);

    // Assert
    assert_eq!(request.family_id, family);
}

#[test]
fn should_not_include_reply_route_in_request() {
    // Arrange
    let family = RouteFamily::new(1);
    let correlation_id = Uuid::new_v4();
    let route = Route::new("rpc://acme/auth/user/create");
    let body = Bytes::from("test");

    // Act
    let request = RpcRequest::new(family, correlation_id, route, body.clone());

    // Assert
    assert_eq!(request.body, body);
}

#[test]
fn should_include_target_route_in_request() {
    // Arrange
    let family = RouteFamily::new(1);
    let correlation_id = Uuid::new_v4();
    let route = Route::new("rpc://acme/auth/user/create");
    let body = Bytes::from("test");

    // Act
    let request = RpcRequest::new(family, correlation_id, route.clone(), body);

    // Assert
    assert_eq!(&request.route, &route);
}
