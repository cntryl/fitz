use super::*;
use crate::protocol::tlv::TlvDecoder;

fn decode_single_frame(frame: &[u8]) -> (MessageType, Bytes) {
    let decoder = TlvDecoder::new();
    let (record, consumed) = decoder.decode_one(frame).expect("decode frame");
    assert_eq!(consumed, frame.len());
    (record.msg_type, record.value)
}

#[test]
fn should_encode_client_response_tlv_frame() {
    // Arrange
    let response = RpcClientResponseBody::Ok {
        data: b"accepted".to_vec(),
    };
    let expected_payload = encode_response(&response);

    // Act
    let frame = encode_client_response_tlv_frame(MessageType::new(302), &response);
    let (msg_type, payload) = decode_single_frame(&frame);

    // Assert
    assert_eq!(msg_type.as_u16(), 302);
    assert_eq!(payload.as_ref(), expected_payload.as_slice());
}

#[test]
fn should_encode_worker_request_tlv_frame() {
    // Arrange
    let request = RpcRequest::new(
        RouteFamily::new(1),
        Uuid::new_v4(),
        Route::new("rpc://bench/service"),
        Bytes::from_static(b"ping"),
    );
    let mut encoder = PayloadEncoder::with_capacity(request_payload_capacity(&request));
    let expected_payload = encode_request_into(&request, &mut encoder);

    // Act
    let frame = encode_worker_request_tlv_frame(&request);
    let (msg_type, payload) = decode_single_frame(&frame);

    // Assert
    assert_eq!(msg_type.as_u16(), 302);
    assert_eq!(payload.as_ref(), expected_payload.as_slice());
}

#[test]
fn should_encode_response_message_tlv_frame() {
    // Arrange
    let response = RpcResponse::single(Uuid::new_v4(), Bytes::from_static(b"pong"));
    let expected_payload = encode_response_message(&response);

    // Act
    let frame = encode_response_message_tlv_frame(&response);
    let (msg_type, payload) = decode_single_frame(&frame);

    // Assert
    assert_eq!(msg_type.as_u16(), 303);
    assert_eq!(payload.as_ref(), expected_payload.as_slice());
}

#[test]
fn should_encode_terminal_error_response_tlv_frame() {
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
    let frame = encode_terminal_error_response_message_tlv_frame(
        &correlation_id,
        crate::protocol::error_codes::rpc::ERR_WORKER_NOT_FOUND,
        message,
    );
    let (msg_type, payload) = decode_single_frame(&frame);

    // Assert
    assert_eq!(msg_type.as_u16(), 303);
    assert_eq!(payload.as_ref(), expected_payload.as_slice());
}

#[test]
fn should_preserve_rpc_request_response_delivery_and_error_golden_bytes() {
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
            b':', b'/', b'/', b'r', b'/', b'a', b'/', b'x', 0, 0, 0, 2, b'h', b'i',
        ]
    );
    assert_eq!(response, [0, 0, 0, 0, 2, b'o', b'k']);
    assert_eq!(
        error,
        [1, 0, 0, 23, 114, 0, 0, 0, 4, b'g', b'o', b'n', b'e']
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
fn should_parse_negotiated_registration_and_remaining_budget_extensions() {
    // Arrange
    let family = RouteFamily::new(1);
    let route = Route::new("rpc://bench/system/resource/operation");
    let mut registration_payload = BytesMut::new();
    put_payload_string(&mut registration_payload, route.as_str());
    registration_payload.put_u32(2);
    registration_payload.put_u8(RPC_LIFECYCLE_EXTENSION_VERSION);
    registration_payload.put_u8(RPC_WORKER_CANCELLATION_SUPPORTED);
    let registration_context = FrameContext::new(
        42,
        crate::protocol::frame::ChannelId::Rpc,
        MessageType::new(300),
        registration_payload.clone().freeze(),
        family,
    );
    let request = RpcRequest::new(
        family,
        Uuid::new_v4(),
        route.clone(),
        Bytes::from_static(b"request"),
    );
    let mut request_payload = encode_request_into(&request, &mut PayloadEncoder::new());
    request_payload.extend_from_slice(&[RPC_LIFECYCLE_EXTENSION_VERSION, 0x01, 0, 0, 4, 210]);
    let request_context = FrameContext::new(
        1,
        crate::protocol::frame::ChannelId::Rpc,
        MessageType::new(302),
        Bytes::from(request_payload.clone()),
        family,
    );

    // Act
    let registration = parse_request(&registration_context, &registration_payload, family)
        .expect("parse supporting worker registration");
    let parsed_request = parse_request(&request_context, &request_payload, family)
        .expect("parse request with remaining budget");
    let supports_cancellation = extract_registration_cancellation_support(&registration_payload)
        .expect("extract worker capability");
    let remaining_budget =
        extract_request_remaining_budget_ms(&request_payload).expect("extract request budget");

    // Assert
    assert!(matches!(
        registration,
        RpcMessage::RegisterWorker {
            max_concurrent: 2,
            ..
        }
    ));
    assert!(supports_cancellation);
    assert_eq!(remaining_budget, Some(1_234));
    assert!(matches!(
        parsed_request,
        RpcMessage::Request(parsed)
            if parsed.correlation_id == request.correlation_id
                && parsed.route == route
                && parsed.body == request.body
    ));
    assert_eq!(
        extract_auth_route(302, &request_payload).expect("extract request route"),
        Some(route.as_str())
    );
}

#[test]
fn should_parse_caller_cancel_and_worker_cleanup_ack_controls() {
    // Arrange
    let correlation_id = Uuid::new_v4();
    let mut cancel_payload = BytesMut::with_capacity(18);
    cancel_payload.put_u8(1);
    put_uuid(&mut cancel_payload, &correlation_id);
    cancel_payload.put_u8(RpcCancellationReason::Explicit as u8);
    let ack_frame = encode_cancel_ack_tlv_frame(&correlation_id);
    let (ack_type, ack_payload) = decode_single_frame(&ack_frame);

    // Act
    let cancel = parse_cancellation_message(&cancel_payload).expect("parse caller cancel");
    let ack = parse_cancellation_message(&ack_payload).expect("parse worker cleanup ack");
    let worker_signal =
        encode_worker_cancel_tlv_frame(&correlation_id, RpcCancellationReason::BrokerDeadline);
    let result = encode_cancel_result_tlv_frame(&correlation_id, RpcCancellationResult::Forwarded);
    let (worker_signal_type, worker_signal_payload) = decode_single_frame(&worker_signal);
    let (result_type, result_payload) = decode_single_frame(&result);

    // Assert
    assert_eq!(
        cancel,
        RpcCancellationMessage::CallerCancel {
            correlation_id,
            reason: RpcCancellationReason::Explicit,
        }
    );
    assert_eq!(
        ack,
        RpcCancellationMessage::WorkerCleanupAck { correlation_id }
    );
    assert_eq!(ack_type.as_u16(), 304);
    assert_eq!(worker_signal_type.as_u16(), 305);
    assert_eq!(result_type.as_u16(), 305);
    assert_eq!(worker_signal_payload[0], 2);
    assert_eq!(
        worker_signal_payload[17],
        RpcCancellationReason::BrokerDeadline as u8
    );
    assert_eq!(result_payload[0], 4);
    assert_eq!(result_payload[17], RpcCancellationResult::Forwarded as u8);
}
