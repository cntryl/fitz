use super::*;
use crate::protocol::tlv::MessageType;

fn decode_single_frame(frame: &[u8]) -> (MessageType, Bytes) {
    let (header, value) =
        crate::protocol::wire2::header::decode_message(frame, u32::MAX).expect("decode frame");
    assert_eq!(header.correlation, None, "control frames answer no request");
    (
        MessageType::new(header.message_type),
        Bytes::copy_from_slice(value),
    )
}

#[test]
fn should_encode_client_response_payload() {
    // Arrange
    let response = RpcClientResponseBody::Ok {
        data: b"accepted".to_vec(),
    };
    let expected_payload = encode_response(&response);

    // Act

    let payload = encode_client_response_payload(&response);

    // Assert
    assert_eq!(payload.as_ref(), expected_payload.as_slice());
}

#[test]
fn should_encode_worker_request_payload() {
    // Arrange
    let request = RpcRequest::new(
        RouteFamily::new(1),
        Uuid::new_v4(),
        Route::new("rpc://bench/service"),
        Bytes::from_static(b"ping"),
    );
    let mut encoder = PayloadEncoder::with_capacity(request_payload_capacity(&request));
    let expected_payload = encode_request_with_budget_into(&request, 1_500, &mut encoder);

    // Act

    let payload = encode_worker_request_payload(&request, 1_500);

    // Assert
    assert_eq!(payload.as_ref(), expected_payload.as_slice());
}

#[test]
fn should_encode_response_message_payload() {
    // Arrange
    let response = RpcResponse::single(Uuid::new_v4(), Bytes::from_static(b"pong"));
    let expected_payload = encode_response_message(&response);

    // Act

    let payload = encode_response_message_payload(&response);

    // Assert
    assert_eq!(payload.as_ref(), expected_payload.as_slice());
}

#[test]
fn should_encode_terminal_error_response_payload() {
    // Arrange
    let correlation_id = Uuid::new_v4();
    let message = "worker unavailable";
    let mut response_encoder =
        PayloadEncoder::with_capacity(terminal_error_response_message_capacity(message));
    let mut error_encoder = PayloadEncoder::with_capacity(error_body_capacity(message));
    let expected_payload = encode_terminal_error_response_message_into(
        &correlation_id,
        crate::protocol::error_codes::rpc::ERR_WORKER_NOT_FOUND,
        message,
        &mut response_encoder,
        &mut error_encoder,
    );

    // Act

    let payload = encode_terminal_error_response_message_payload(
        &correlation_id,
        crate::protocol::error_codes::rpc::ERR_WORKER_NOT_FOUND,
        message,
    );

    // Assert
    assert_eq!(payload.as_ref(), expected_payload.as_slice());
}

#[test]
fn should_preserve_rpc_wire_golden_vectors() {
    // Arrange
    let correlation_id = Uuid::from_bytes([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]);
    let request = RpcRequest::new(
        RouteFamily::new(1),
        correlation_id,
        Route::new("rpc://r/a/x"),
        Bytes::from_static(b"hi"),
    );

    // Act
    let mut encoder = PayloadEncoder::new();
    let delivery = encode_request_into(&request, &mut encoder);
    let parsed_route = extract_auth_route(302, &delivery).expect("parse golden RPC request");
    let response = encode_response(&RpcClientResponseBody::Ok {
        data: b"ok".to_vec(),
    });
    let error = encode_error_body(
        crate::protocol::error_codes::rpc::ERR_WORKER_NOT_FOUND,
        "gone",
    );

    // Assert
    assert_eq!(parsed_route, Some("rpc://r/a/x"));
    assert_eq!(
        delivery,
        [
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 0, 0, 0, 11, b'r', b'p', b'c',
            b':', b'/', b'/', b'r', b'/', b'a', b'/', b'x', 0, 0, 0, 2, b'h', b'i', 0,
        ]
    );
    assert_eq!(response, [0, 0, 0, 0, 2, b'o', b'k']);
    let decoded = crate::protocol::error_codes::decode_wire_error(&error).expect("error");
    assert_eq!(
        (decoded.code, decoded.message.as_str()),
        (
            crate::protocol::error_codes::rpc::ERR_WORKER_NOT_FOUND,
            "gone"
        )
    );
}

#[test]
fn should_keep_cancellation_control_out_of_domain_rpc_messages() {
    // Arrange
    let frame = FrameContext::new(
        1,
        crate::protocol::frame::ChannelId::Rpc,
        MessageType::new(304),
        Bytes::new(),
        RouteFamily::new(1),
    );

    // Act
    let result = parse_request(&frame, &frame.payload, RouteFamily::new(1));

    // Assert
    assert!(result.is_err());
}

#[test]
fn should_parse_worker_registration_flags_for_cancellation_support() {
    // Arrange
    let family = RouteFamily::new(1);
    let route = Route::new("rpc://bench/system/resource/operation");
    let mut payload = BytesMut::new();
    put_payload_string(&mut payload, route.as_str());
    payload.put_u32(2);
    payload.put_u8(RPC_WORKER_CANCELLATION_SUPPORTED);
    let context = FrameContext::new(
        42,
        crate::protocol::frame::ChannelId::Rpc,
        MessageType::new(300),
        payload.clone().freeze(),
        family,
    );

    // Act
    let registration =
        parse_request(&context, &payload, family).expect("parse supporting worker registration");
    let supports_cancellation =
        extract_registration_cancellation_support(&payload).expect("extract worker capability");

    // Assert
    assert!(matches!(
        registration,
        RpcMessage::RegisterWorker {
            max_concurrent: 2,
            ..
        }
    ));
    assert!(supports_cancellation);
}

#[test]
fn should_parse_request_with_remaining_budget() {
    // Arrange
    let family = RouteFamily::new(1);
    let route = Route::new("rpc://bench/system/resource/operation");
    let request = RpcRequest::new(
        family,
        Uuid::new_v4(),
        route.clone(),
        Bytes::from_static(b"request"),
    );
    let payload = encode_request_with_budget_into(&request, 1_234, &mut PayloadEncoder::new());
    let context = FrameContext::new(
        1,
        crate::protocol::frame::ChannelId::Rpc,
        MessageType::new(302),
        Bytes::from(payload.clone()),
        family,
    );

    // Act
    let parsed_request =
        parse_request(&context, &payload, family).expect("parse request with remaining budget");
    let remaining_budget =
        extract_request_remaining_budget_ms(&payload).expect("extract request budget");
    let auth_route = extract_auth_route(302, &payload).expect("extract request route");

    // Assert
    assert_eq!(remaining_budget, Some(1_234));
    assert!(matches!(parsed_request, RpcMessage::Request(parsed)
        if parsed.correlation_id == request.correlation_id
            && parsed.route == route && parsed.body == request.body));
    assert_eq!(auth_route, Some(route.as_str()));
}

#[test]
fn should_parse_explicit_caller_cancellation_control() {
    // Arrange
    let correlation_id = Uuid::new_v4();
    let mut payload = BytesMut::with_capacity(18);
    payload.put_u8(1);
    put_uuid(&mut payload, &correlation_id);
    payload.put_u8(RpcCancellationReason::Explicit as u8);

    // Act
    let cancel = parse_cancellation_message(&payload).expect("parse caller cancel");

    // Assert
    assert_eq!(
        cancel,
        RpcCancellationMessage::CallerCancel {
            correlation_id,
            reason: RpcCancellationReason::Explicit,
        }
    );
}

#[test]
fn should_round_trip_worker_cleanup_ack_control() {
    // Arrange
    let correlation_id = Uuid::new_v4();

    // Act
    let frame = encode_cancel_ack_frame(&correlation_id);
    let (message_type, payload) = decode_single_frame(&frame);
    let ack = parse_cancellation_message(&payload).expect("parse worker cleanup ack");

    // Assert
    assert_eq!(
        ack,
        RpcCancellationMessage::WorkerCleanupAck { correlation_id }
    );
    assert_eq!(message_type.as_u16(), 304);
}

#[test]
fn should_encode_broker_deadline_worker_cancellation_control() {
    // Arrange
    let correlation_id = Uuid::new_v4();

    // Act
    let frame = encode_worker_cancel_frame(&correlation_id, RpcCancellationReason::BrokerDeadline);
    let (message_type, payload) = decode_single_frame(&frame);

    // Assert
    assert_eq!(message_type.as_u16(), 305);
    assert_eq!(payload[0], 2);
    assert_eq!(payload[17], RpcCancellationReason::BrokerDeadline as u8);
}

#[test]
fn should_encode_forwarded_cancellation_result_control() {
    // Arrange
    let correlation_id = Uuid::new_v4();

    // Act
    let frame = encode_cancel_result_frame(&correlation_id, RpcCancellationResult::Forwarded);
    let (message_type, payload) = decode_single_frame(&frame);

    // Assert
    assert_eq!(message_type.as_u16(), 305);
    assert_eq!(payload[0], 4);
    assert_eq!(payload[17], RpcCancellationResult::Forwarded as u8);
}
