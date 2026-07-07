use super::common::*;

#[test]
fn should_format_error_codes_correctly() {
    assert_eq!(RpcErrorCode::Timeout.as_str(), "RPC_TIMEOUT");
    assert_eq!(RpcErrorCode::Backpressure.as_str(), "RPC_BACKPRESSURE");
    assert_eq!(RpcErrorCode::Unauthorized.as_str(), "RPC_UNAUTHORIZED");
}

#[test]
fn should_create_backpressure_error_with_correlation_id() {
    // Arrange
    let correlation_id = Uuid::new_v4();

    // Act
    let error = RpcError::backpressure(correlation_id);

    // Assert
    assert_eq!(error.correlation_id, correlation_id);
    assert_eq!(error.code, RpcErrorCode::Backpressure);
    assert!(error.message.contains("capacity"));
}

#[test]
fn should_create_timeout_error_with_correlation_id() {
    // Arrange
    let correlation_id = Uuid::new_v4();

    // Act
    let error = RpcError::timeout(correlation_id);

    // Assert
    assert_eq!(error.correlation_id, correlation_id);
    assert_eq!(error.code, RpcErrorCode::Timeout);
    assert!(error.message.contains("timeout"));
}

#[test]
fn should_reject_rpc_request_when_pending_capacity_reached_given_rpc_sink() {
    // Arrange
    let router = Arc::new(Router::new());
    let sink = create_bench_rpc_sink_with_route_pending_capacity(router.clone(), 4096);
    let family = RouteFamily::new(1);
    let route = "rpc://test/services/backpressure";
    let request_addr = RouteAddress::new(family, Route::new(route));
    let worker_addr = session_inbox_address(family, 42);
    let rejected_caller_addr = session_inbox_address(family, 2);
    let worker_sink = register_counting_sink(&router, worker_addr.clone());
    let rejected_caller_sink = register_capture_sink(&router, rejected_caller_addr.clone());

    deliver_rpc_frame(
        sink.as_ref(),
        worker_addr,
        request_addr.clone(),
        42,
        family,
        &build_rpc_subscribe(route),
    );

    for _request_index in 0..4096 {
        deliver_rpc_frame(
            sink.as_ref(),
            session_inbox_address(family, 7),
            request_addr.clone(),
            7,
            family,
            &build_rpc_request(route, "getUser", b"fill"),
        );
    }

    // Act
    deliver_rpc_frame(
        sink.as_ref(),
        rejected_caller_addr,
        request_addr,
        2,
        family,
        &build_rpc_request(route, "getUser", b"reject"),
    );

    // Assert
    assert_eq!(sink.pending_request_count(), 4096);
    assert_eq!(sink.worker_count(), 1);
    assert_eq!(worker_sink.delivery_count(), 2);

    let rejected_frames = rejected_caller_sink.snapshot();
    assert_eq!(rejected_frames.len(), 1);
    let response = parse_rpc_response_delivery(&encode_captured_frame(&rejected_frames[0]))
        .expect("parse backpressure terminal response");
    assert_eq!(response.seq, 0);
    assert!(response.stream_end);

    let (code, message) = fitz::protocol::rpc_codec::decode_error_body(&response.body)
        .expect("decode backpressure error");
    assert_eq!(code, fitz::protocol::error_codes::rpc::ERR_RPC_BACKPRESSURE);
    assert_eq!(message, "RPC backpressure: too many pending requests");
}

#[test]
fn should_forward_worker_not_found_error_given_rpc_session_cleanup() {
    // Arrange
    let router = Arc::new(Router::new());
    let sink = create_bench_rpc_sink(router.clone());
    let family = RouteFamily::new(1);
    let route = "rpc://test/services/cleanup";
    let request_addr = RouteAddress::new(family, Route::new(route));
    let caller_addr = session_inbox_address(family, 1);
    let worker_addr = session_inbox_address(family, 42);
    let caller_sink = register_capture_sink(&router, caller_addr.clone());
    let worker_sink = register_capture_sink(&router, worker_addr.clone());

    deliver_rpc_frame(
        sink.as_ref(),
        worker_addr.clone(),
        request_addr.clone(),
        42,
        family,
        &build_rpc_subscribe(route),
    );
    deliver_rpc_frame(
        sink.as_ref(),
        caller_addr,
        request_addr,
        1,
        family,
        &build_rpc_request(route, "getUser", b"user-123"),
    );

    // Act
    sink.deliver(Envelope::new(
        RouteAddress::new(family, Route::new("rpc://cleanup")),
        SessionCleanup { session_id: 42 },
    ))
    .expect("cleanup worker session");

    // Assert
    assert_eq!(sink.pending_request_count(), 0);
    assert_eq!(sink.worker_count(), 0);

    let caller_frames = caller_sink.snapshot();
    assert_eq!(caller_frames.len(), 1);
    let forwarded_error = parse_rpc_response_delivery(&encode_captured_frame(&caller_frames[0]))
        .expect("parse forwarded rpc disconnect error");
    let (code, message) = fitz::protocol::rpc_codec::decode_error_body(&forwarded_error.body)
        .expect("decode worker not found error");
    assert_eq!(code, fitz::protocol::error_codes::rpc::ERR_WORKER_NOT_FOUND);
    assert_eq!(message, "Worker disconnected or unregistered");

    let worker_frames = worker_sink.snapshot();
    let delivered_request = worker_frames
        .iter()
        .find(|frame| frame.msg_type.as_u16() == 302)
        .expect("worker request delivery");
    let delivered_request = parse_rpc_request_delivery(&encode_captured_frame(delivered_request))
        .expect("parse worker request delivery");
    assert_eq!(
        forwarded_error.correlation_id,
        delivered_request.correlation_id
    );
}

#[tokio::test]
async fn should_reject_late_worker_response_after_timeout_given_rpc_sink() {
    // Arrange
    let router = Arc::new(Router::new());
    let sink = create_bench_rpc_sink_with_timeout(router.clone(), Duration::from_millis(20));
    let family = RouteFamily::new(1);
    let route = "rpc://test/services/timeout";
    let request_addr = RouteAddress::new(family, Route::new(route));
    let caller_addr = session_inbox_address(family, 1);
    let worker_addr = session_inbox_address(family, 42);
    let caller_sink = register_capture_sink(&router, caller_addr.clone());
    let worker_sink = register_capture_sink(&router, worker_addr.clone());

    let domains = domain_handles_with_rpc_sink(router.clone(), sink.clone());
    fitz::api::background::start_domain_background_tasks(&domains);
    deliver_rpc_frame(
        sink.as_ref(),
        worker_addr.clone(),
        request_addr.clone(),
        42,
        family,
        &build_rpc_subscribe(route),
    );
    deliver_rpc_frame(
        sink.as_ref(),
        caller_addr,
        request_addr.clone(),
        1,
        family,
        &build_rpc_request(route, "getUser", b"user-123"),
    );

    let delivered_request = worker_sink
        .snapshot()
        .into_iter()
        .find(|frame| frame.msg_type.as_u16() == 302)
        .expect("worker request delivery");
    let delivered_request = parse_rpc_request_delivery(&encode_captured_frame(&delivered_request))
        .expect("parse worker request delivery");

    // Act
    wait_for_pending_requests_to_clear(sink.as_ref()).await;
    deliver_rpc_frame(
        sink.as_ref(),
        worker_addr,
        request_addr,
        42,
        family,
        &build_rpc_response_delivery(delivered_request.correlation_id, 0, true, b"late"),
    );

    // Assert
    assert_eq!(sink.pending_request_count(), 0);

    let caller_frames = caller_sink.snapshot();
    assert_eq!(caller_frames.len(), 1);
    let timeout_response = parse_rpc_response_delivery(&encode_captured_frame(&caller_frames[0]))
        .expect("parse forwarded timeout response");
    let (timeout_code, timeout_message) =
        fitz::protocol::rpc_codec::decode_error_body(&timeout_response.body)
            .expect("decode timeout error");
    assert_eq!(
        timeout_code,
        fitz::protocol::error_codes::rpc::ERR_RPC_TIMEOUT
    );
    assert_eq!(
        timeout_message,
        "Worker did not reply within timeout period"
    );
    assert_eq!(
        timeout_response.correlation_id,
        delivered_request.correlation_id
    );

    let worker_frames = worker_sink.snapshot();
    assert_eq!(worker_frames.len(), 3);
    assert_eq!(worker_frames[0].msg_type.as_u16(), 300);
    assert_eq!(worker_frames[1].msg_type.as_u16(), 302);
    assert_eq!(worker_frames[2].msg_type.as_u16(), 303);
    let orphan_response = parse_rpc_response_delivery(&encode_captured_frame(&worker_frames[2]))
        .expect("parse orphan response error");
    let (orphan_code, orphan_message) =
        fitz::protocol::rpc_codec::decode_error_body(&orphan_response.body)
            .expect("decode orphan error");
    assert_eq!(
        orphan_code,
        fitz::protocol::error_codes::rpc::ERR_CORRELATION_NOT_FOUND
    );
    assert_eq!(
        orphan_message,
        "Correlation ID not found (orphaned response)"
    );
    assert_eq!(
        orphan_response.correlation_id,
        delivered_request.correlation_id
    );

    domains.stop();
}

// ============================================================================
//                      STREAMING & ORDERING TESTS
// ============================================================================
