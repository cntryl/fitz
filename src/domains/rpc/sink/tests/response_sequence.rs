use super::super::response_sink_impl::MAX_RESPONSE_DELIVERY_ATTEMPTS;
use super::*;

#[test]
fn should_retain_pending_request_before_stream_end_given_rpc_pending_table() {
    // Arrange
    let correlation_id = uuid::Uuid::new_v4();
    let caller_inbox_addr = session_inbox_address(RouteFamily::new(9), 84);
    let worker_addr = RouteAddress::new(
        RouteFamily::new(9),
        Route::new("rpc://bench/system/resource/operation"),
    );
    let mut pending = RpcPendingTable::new();
    let pending_len = pending.track_pending(
        correlation_id,
        RpcPendingRequest::new(RpcPendingRequestInit {
            route: worker_addr.route().clone(),
            caller_session_id: 84,
            caller_inbox_addr: caller_inbox_addr.clone(),
            registration_addr: worker_addr.clone(),
            registration_session_id: 99,
            registration_id: 0,
            submitted_at: test_rpc_timestamp(),
            submitted_at_instant: Instant::now(),
            expires_at: Instant::now() + Duration::from_secs(30),
        }),
    );

    // Act
    let result = pending.pending_for_response(&correlation_id, 99, 0, false);

    // Assert
    assert_eq!(pending_len, 1);
    match result {
        RpcPendingResponseDisposition::Forward {
            pending: tracked,
            stream_end,
        } => {
            assert_eq!(tracked.caller_session_id, 84);
            assert_eq!(tracked.caller_inbox_addr, Some(caller_inbox_addr));
            assert_eq!(&tracked.route, worker_addr.route());
            assert_eq!(tracked.registration_id, 0);
            assert!(!stream_end);
            assert_eq!(pending.len(), 1);
            let seq_key = RpcCorrelationKey {
                family: RouteFamily::new(1),
                correlation_id,
            };
            // The lookup does not move the cursor: delivery can still fail,
            // and advancing first is what lets a dropped chunk pass as
            // contiguous.
            assert_eq!(pending.pending[&seq_key].next_expected_seq, 0);
            assert!(pending.commit_response_delivery(RouteFamily::new(1), &correlation_id, false));
            assert_eq!(pending.pending[&seq_key].next_expected_seq, 1);
        }
        other => panic!("expected non-terminal response handling, found {other:?}"),
    }
}

#[test]
fn should_reject_invalid_response_sequence_given_rpc_pending_table() {
    // Arrange
    let correlation_id = uuid::Uuid::new_v4();
    let caller_inbox_addr = session_inbox_address(RouteFamily::new(11), 21);
    let worker_addr = RouteAddress::new(
        RouteFamily::new(11),
        Route::new("rpc://bench/system/resource/operation"),
    );
    let mut pending = RpcPendingTable::new();
    pending.track_pending(
        correlation_id,
        RpcPendingRequest::new(RpcPendingRequestInit {
            route: worker_addr.route().clone(),
            caller_session_id: 21,
            caller_inbox_addr,
            registration_addr: worker_addr,
            registration_session_id: 77,
            registration_id: 0,
            submitted_at: test_rpc_timestamp(),
            submitted_at_instant: Instant::now(),
            expires_at: Instant::now() + Duration::from_secs(30),
        }),
    );

    // Act
    let result = pending.pending_for_response(&correlation_id, 77, 1, false);

    // Assert
    match result {
        RpcPendingResponseDisposition::InvalidSequence {
            pending: tracked,
            expected_seq,
        } => {
            assert_eq!(tracked.caller_session_id, 21);
            assert_eq!(tracked.registration_id, 0);
            assert_eq!(expected_seq, 0);
            assert_eq!(pending.len(), 0);
        }
        other => panic!("expected invalid sequence handling, found {other:?}"),
    }
}

#[test]
fn should_reject_out_of_order_worker_response_given_rpc_sink() {
    // Arrange
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = Arc::new(RpcDomainSink::new(router.clone(), admin_read_model));
    let family = RouteFamily::new(1);
    let request_route = Route::new("rpc://bench/system/resource/operation");
    let request_addr = RouteAddress::new(family, request_route.clone());
    let request_source = session_inbox_address(family, 1);
    let worker_source = session_inbox_address(family, 42);
    let reply_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
    let worker_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
    let reply_sink = Arc::new(CaptureRpcFrameSink {
        frames: reply_frames.clone(),
    });
    let worker_sink = Arc::new(CaptureRpcFrameSink {
        frames: worker_frames.clone(),
    });
    router.register(request_source.clone(), reply_sink as Arc<dyn MailboxSink>);
    router.register(worker_source.clone(), worker_sink as Arc<dyn MailboxSink>);
    sink.register_registration_for_tests(test_rpc_worker(family, &request_route, 42));
    let request_frame = crate::benchkit::build_rpc_request(request_route.as_str(), b"ping");
    let (request_msg_type, request_payload) =
        crate::benchkit::extract_single_tlv_field(&request_frame);
    let request_ctx = FrameContext::new(
        1,
        crate::dispatch::protocol::frame::ChannelId::Rpc,
        crate::dispatch::protocol::tlv::MessageType::new(request_msg_type),
        request_payload.clone(),
        family,
    );
    let request = match crate::dispatch::protocol::rpc_codec::parse_request(
        &request_ctx,
        &request_payload,
        family,
    )
    .expect("parse rpc request")
    {
        crate::domains::rpc::protocol::RpcMessage::Request(request) => request,
        other => panic!("expected rpc request, found {other:?}"),
    };
    let invalid_response_payload = crate::dispatch::protocol::rpc_codec::encode_response_message(
        &crate::domains::rpc::protocol::RpcResponse::chunk(
            request.correlation_id,
            1,
            bytes::Bytes::from_static(b"gap"),
            false,
        ),
    );

    // Act
    sink.deliver(Envelope::from_route(
        request_source,
        request_addr.clone(),
        request_ctx,
    ))
    .expect("deliver request");
    sink.deliver(Envelope::from_route(
        worker_source,
        request_addr,
        FrameContext::new(
            42,
            crate::dispatch::protocol::frame::ChannelId::Rpc,
            crate::dispatch::protocol::tlv::MessageType::new(303),
            bytes::Bytes::from(invalid_response_payload),
            family,
        ),
    ))
    .expect("deliver invalid response");

    // Assert
    assert_eq!(sink.pending_request_count(), 0);

    let reply_frames = reply_frames.lock();
    assert_eq!(reply_frames.len(), 1);
    assert_eq!(reply_frames[0].msg_type.as_u16(), 303);
    let error_response = parse_forwarded_rpc_response(&reply_frames[0]);
    assert_eq!(error_response.correlation_id, request.correlation_id);
    assert_eq!(error_response.seq, 0);
    assert!(error_response.stream_end);
    assert_rpc_code_error(
        error_response.body.as_ref(),
        crate::dispatch::protocol::error_codes::rpc::ERR_RPC_INVALID_SEQUENCE,
        RPC_INVALID_SEQUENCE_ERROR,
    );

    let worker_frames = worker_frames.lock();
    assert_eq!(worker_frames.len(), 2);
    assert_eq!(worker_frames[0].msg_type.as_u16(), 302);
    assert_eq!(worker_frames[1].msg_type.as_u16(), 303);
    let worker_error = parse_forwarded_rpc_response(&worker_frames[1]);
    assert_eq!(worker_error.correlation_id, request.correlation_id);
    assert!(worker_error.stream_end);
    assert_rpc_code_error(
        worker_error.body.as_ref(),
        crate::dispatch::protocol::error_codes::rpc::ERR_RPC_INVALID_SEQUENCE,
        RPC_INVALID_SEQUENCE_ERROR,
    );
}

#[test]
#[allow(clippy::too_many_lines)]
fn should_reject_duplicate_worker_response_chunk_given_rpc_sink() {
    // Arrange
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = Arc::new(RpcDomainSink::new(router.clone(), admin_read_model));
    let family = RouteFamily::new(1);
    let request_route = Route::new("rpc://bench/system/resource/operation");
    let request_addr = RouteAddress::new(family, request_route.clone());
    let request_source = session_inbox_address(family, 1);
    let worker_source = session_inbox_address(family, 42);
    let reply_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
    let worker_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
    let reply_sink = Arc::new(CaptureRpcFrameSink {
        frames: reply_frames.clone(),
    });
    let worker_sink = Arc::new(CaptureRpcFrameSink {
        frames: worker_frames.clone(),
    });
    router.register(request_source.clone(), reply_sink as Arc<dyn MailboxSink>);
    router.register(worker_source.clone(), worker_sink as Arc<dyn MailboxSink>);
    sink.register_registration_for_tests(test_rpc_worker(family, &request_route, 42));
    let request_frame = crate::benchkit::build_rpc_request(request_route.as_str(), b"ping");
    let (request_msg_type, request_payload) =
        crate::benchkit::extract_single_tlv_field(&request_frame);
    let request_ctx = FrameContext::new(
        1,
        crate::dispatch::protocol::frame::ChannelId::Rpc,
        crate::dispatch::protocol::tlv::MessageType::new(request_msg_type),
        request_payload.clone(),
        family,
    );
    let request = match crate::dispatch::protocol::rpc_codec::parse_request(
        &request_ctx,
        &request_payload,
        family,
    )
    .expect("parse rpc request")
    {
        crate::domains::rpc::protocol::RpcMessage::Request(request) => request,
        other => panic!("expected rpc request, found {other:?}"),
    };
    let first_response_payload = crate::dispatch::protocol::rpc_codec::encode_response_message(
        &crate::domains::rpc::protocol::RpcResponse::chunk(
            request.correlation_id,
            0,
            bytes::Bytes::from_static(b"part-0"),
            false,
        ),
    );
    let duplicate_response_payload = crate::dispatch::protocol::rpc_codec::encode_response_message(
        &crate::domains::rpc::protocol::RpcResponse::chunk(
            request.correlation_id,
            0,
            bytes::Bytes::from_static(b"part-0-again"),
            false,
        ),
    );

    // Act
    sink.deliver(Envelope::from_route(
        request_source,
        request_addr.clone(),
        request_ctx,
    ))
    .expect("deliver request");
    sink.deliver(Envelope::from_route(
        worker_source.clone(),
        request_addr.clone(),
        FrameContext::new(
            42,
            crate::dispatch::protocol::frame::ChannelId::Rpc,
            crate::dispatch::protocol::tlv::MessageType::new(303),
            bytes::Bytes::from(first_response_payload),
            family,
        ),
    ))
    .expect("deliver first response chunk");
    sink.deliver(Envelope::from_route(
        worker_source,
        request_addr,
        FrameContext::new(
            42,
            crate::dispatch::protocol::frame::ChannelId::Rpc,
            crate::dispatch::protocol::tlv::MessageType::new(303),
            bytes::Bytes::from(duplicate_response_payload),
            family,
        ),
    ))
    .expect("deliver duplicate response chunk");

    // Assert
    assert_eq!(sink.pending_request_count(), 0);

    let reply_frames = reply_frames.lock();
    assert_eq!(reply_frames.len(), 2);
    let first_response = parse_forwarded_rpc_response(&reply_frames[0]);
    assert_eq!(first_response.correlation_id, request.correlation_id);
    assert_eq!(first_response.seq, 0);
    assert!(!first_response.stream_end);
    let terminal_error = parse_forwarded_rpc_response(&reply_frames[1]);
    assert_eq!(terminal_error.correlation_id, request.correlation_id);
    assert!(terminal_error.stream_end);
    assert_rpc_code_error(
        terminal_error.body.as_ref(),
        crate::dispatch::protocol::error_codes::rpc::ERR_RPC_INVALID_SEQUENCE,
        RPC_INVALID_SEQUENCE_ERROR,
    );

    let worker_frames = worker_frames.lock();
    assert_eq!(worker_frames.len(), 2);
    assert_eq!(worker_frames[0].msg_type.as_u16(), 302);
    assert_eq!(worker_frames[1].msg_type.as_u16(), 303);
    let worker_error = parse_forwarded_rpc_response(&worker_frames[1]);
    assert_eq!(worker_error.correlation_id, request.correlation_id);
    assert!(worker_error.stream_end);
    assert_rpc_code_error(
        worker_error.body.as_ref(),
        crate::dispatch::protocol::error_codes::rpc::ERR_RPC_INVALID_SEQUENCE,
        RPC_INVALID_SEQUENCE_ERROR,
    );
}

/// Fails the first N deliveries with backpressure, then captures the rest.
struct BackpressuredThenCapturingSink {
    failures_remaining: parking_lot::Mutex<usize>,
    frames: Arc<parking_lot::Mutex<Vec<FrameContext>>>,
}

impl MailboxSink for BackpressuredThenCapturingSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        {
            let mut remaining = self.failures_remaining.lock();
            if *remaining > 0 {
                *remaining -= 1;
                return Err(DeliveryError::MailboxFull {
                    capacity: 1_000,
                    current_len: 1_000,
                });
            }
        }
        let frame = envelope
            .payload::<FrameContext>()
            .expect("rpc frame payload")
            .clone();
        self.frames.lock().push(frame);
        Ok(())
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

/// Deliver one worker response chunk for `request`.
fn deliver_test_rpc_chunk(
    sink: &Arc<RpcDomainSink>,
    request: &crate::domains::rpc::protocol::RpcRequest,
    family: RouteFamily,
    worker_source: &RouteAddress,
    seq: u64,
    stream_end: bool,
) -> Result<(), DeliveryError> {
    let payload = crate::dispatch::protocol::rpc_codec::encode_response_message(
        &crate::domains::rpc::protocol::RpcResponse::chunk(
            request.correlation_id,
            seq,
            bytes::Bytes::from(format!("chunk-{seq}")),
            stream_end,
        ),
    );
    sink.deliver(Envelope::from_route(
        worker_source.clone(),
        RouteAddress::new(family, request.route.clone()),
        FrameContext::new(
            42,
            crate::dispatch::protocol::frame::ChannelId::Rpc,
            crate::dispatch::protocol::tlv::MessageType::new(303),
            bytes::Bytes::from(payload),
            family,
        ),
    ))
}

/// Build a well-formed RPC request, deliver it, and hand back the parsed form.
fn deliver_test_rpc_request(
    sink: &Arc<RpcDomainSink>,
    family: RouteFamily,
    request_route: &Route,
    request_source: RouteAddress,
) -> crate::domains::rpc::protocol::RpcRequest {
    let request_frame = crate::benchkit::build_rpc_request(request_route.as_str(), b"ping");
    let (request_msg_type, request_payload) =
        crate::benchkit::extract_single_tlv_field(&request_frame);
    let request_ctx = FrameContext::new(
        1,
        crate::dispatch::protocol::frame::ChannelId::Rpc,
        crate::dispatch::protocol::tlv::MessageType::new(request_msg_type),
        request_payload.clone(),
        family,
    );
    let request = match crate::dispatch::protocol::rpc_codec::parse_request(
        &request_ctx,
        &request_payload,
        family,
    )
    .expect("parse rpc request")
    {
        crate::domains::rpc::protocol::RpcMessage::Request(request) => request,
        other => panic!("expected rpc request, found {other:?}"),
    };
    sink.deliver(Envelope::from_route(
        request_source,
        RouteAddress::new(family, request_route.clone()),
        request_ctx,
    ))
    .expect("deliver request");
    request
}

#[test]
fn should_terminate_stream_when_a_response_chunk_cannot_be_delivered() {
    // Arrange
    // A full outbound channel makes the caller's inbox reject one chunk. The
    // broker may reject or terminate the RPC under backpressure, but it must
    // never drop a chunk and keep forwarding later ones - that hands the
    // caller a silently corrupted stream with a sequence gap.
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let metrics = crate::observability::metrics::MetricsCollector::new();
    let sink = Arc::new(
        RpcDomainSink::new(router.clone(), admin_read_model).with_metrics(metrics.clone()),
    );
    let family = RouteFamily::new(1);
    let request_route = Route::new("rpc://bench/system/resource/stream");
    let request_addr = RouteAddress::new(family, request_route.clone());
    let request_source = session_inbox_address(family, 1);
    let worker_source = session_inbox_address(family, 42);
    let reply_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
    router.register(
        request_source.clone(),
        Arc::new(BackpressuredThenCapturingSink {
            failures_remaining: parking_lot::Mutex::new(1),
            frames: reply_frames.clone(),
        }) as Arc<dyn MailboxSink>,
    );
    let worker_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
    router.register(
        worker_source.clone(),
        Arc::new(CaptureRpcFrameSink {
            frames: worker_frames.clone(),
        }) as Arc<dyn MailboxSink>,
    );
    sink.register_registration_for_tests(test_rpc_worker(family, &request_route, 42));
    let request = deliver_test_rpc_request(&sink, family, &request_route, request_source);

    let deliver_chunk = |seq: u64, body: &'static [u8], stream_end: bool| {
        let payload = crate::dispatch::protocol::rpc_codec::encode_response_message(
            &crate::domains::rpc::protocol::RpcResponse::chunk(
                request.correlation_id,
                seq,
                bytes::Bytes::from_static(body),
                stream_end,
            ),
        );
        sink.deliver(Envelope::from_route(
            worker_source.clone(),
            request_addr.clone(),
            FrameContext::new(
                42,
                crate::dispatch::protocol::frame::ChannelId::Rpc,
                crate::dispatch::protocol::tlv::MessageType::new(303),
                bytes::Bytes::from(payload),
                family,
            ),
        ))
    };

    // Act
    // The worker keeps resending chunk 0 while the caller stays saturated,
    // then tries to move on.
    for _ in 0..MAX_RESPONSE_DELIVERY_ATTEMPTS {
        deliver_chunk(0, b"chunk-zero", false).expect("deliver chunk 0");
    }
    deliver_chunk(1, b"chunk-one", false).expect("deliver chunk 1");

    // Assert
    assert_eq!(
        sink.pending_request_count(),
        0,
        "an undeliverable chunk must terminate the RPC, not leave the stream live"
    );
    let frames = reply_frames.lock();
    let forwarded = frames
        .iter()
        .map(parse_forwarded_rpc_response)
        .collect::<Vec<_>>();
    assert!(
        !forwarded
            .iter()
            .any(|response| response.seq == 1 && !response.stream_end),
        "chunk 1 must not be forwarded as a normal chunk after chunk 0 was dropped: {forwarded:?}"
    );
    assert!(
        forwarded.iter().any(|response| response.stream_end),
        "the caller must be told the stream terminated, got {forwarded:?}"
    );

    // The worker must also be cancelled, or it keeps producing into a stream
    // that no longer exists - the amplification that turned two corrupted
    // streams into tens of thousands of late dropped responses.
    let worker_frames = worker_frames.lock();
    let worker_cancels = worker_frames
        .iter()
        .filter(|frame| frame.msg_type.as_u16() == 303)
        .map(parse_forwarded_rpc_response)
        .filter(|response| response.correlation_id == request.correlation_id && response.stream_end)
        .count();
    assert!(
        worker_cancels >= 1,
        "the worker must be told to stop producing, got {worker_frames:?}"
    );

    // Health must not read green through a corrupted stream.
    assert!(
        metrics.counter_get(crate::domains::rpc::metrics::METRIC_FAILURE_TOTAL) >= 1,
        "a terminated stream must be counted as a failure, not silent success"
    );
    assert!(
        metrics.counter_get("rpc_response_delivery_retries_total") >= 1,
        "the failed delivery attempt must be observable"
    );
}

#[test]
fn should_retry_a_transiently_undeliverable_chunk_without_ending_the_stream() {
    // Arrange
    // A momentarily full outbound channel is backpressure, not corruption.
    // The chunk must stay retryable at the same sequence: the broker must not
    // advance past it, and must not tear down a stream that can still succeed
    // once the caller drains.
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = Arc::new(RpcDomainSink::new(router.clone(), admin_read_model));
    let family = RouteFamily::new(1);
    let request_route = Route::new("rpc://bench/system/resource/retry");
    let request_source = session_inbox_address(family, 1);
    let worker_source = session_inbox_address(family, 42);
    let reply_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
    router.register(
        request_source.clone(),
        Arc::new(BackpressuredThenCapturingSink {
            failures_remaining: parking_lot::Mutex::new(1),
            frames: reply_frames.clone(),
        }) as Arc<dyn MailboxSink>,
    );
    router.register(
        worker_source.clone(),
        Arc::new(CaptureRpcFrameSink {
            frames: Arc::new(parking_lot::Mutex::new(Vec::new())),
        }) as Arc<dyn MailboxSink>,
    );
    sink.register_registration_for_tests(test_rpc_worker(family, &request_route, 42));
    let request = deliver_test_rpc_request(&sink, family, &request_route, request_source);

    // Act
    // Chunk 0 hits the full channel, then the worker resends the same chunk.
    deliver_test_rpc_chunk(&sink, &request, family, &worker_source, 0, false)
        .expect("deliver chunk 0");
    deliver_test_rpc_chunk(&sink, &request, family, &worker_source, 0, false)
        .expect("resend chunk 0");
    deliver_test_rpc_chunk(&sink, &request, family, &worker_source, 1, true)
        .expect("deliver chunk 1");

    // Assert
    let frames = reply_frames.lock();
    let forwarded = frames
        .iter()
        .map(parse_forwarded_rpc_response)
        .collect::<Vec<_>>();
    let sequences = forwarded
        .iter()
        .filter(|response| response.correlation_id == request.correlation_id)
        .map(|response| response.seq)
        .collect::<Vec<_>>();
    assert_eq!(
        sequences,
        vec![0, 1],
        "the retried chunk must be delivered at its own sequence, then the stream continues"
    );
    assert!(
        forwarded
            .iter()
            .any(|response| response.seq == 1 && response.stream_end),
        "the stream must still complete normally: {forwarded:?}"
    );
}
