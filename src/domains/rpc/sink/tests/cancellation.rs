use super::*;
use crate::domains::rpc::protocol::RpcLifecycleControlDelivery;
use crate::protocol::rpc_codec::encode_cancel_ack_tlv_frame;
use bytes::Bytes;

mod safety;

#[derive(Default)]
struct CaptureRpcEndpoint {
    frames: parking_lot::Mutex<Vec<FrameContext>>,
    lifecycle_frames: parking_lot::Mutex<Vec<Bytes>>,
    close_reasons: parking_lot::Mutex<Vec<&'static str>>,
}

impl MailboxSink for CaptureRpcEndpoint {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        if let Some(frame) = envelope.payload::<FrameContext>() {
            self.frames.lock().push(frame.clone());
        }
        if let Some(delivery) = envelope.payload::<RpcLifecycleControlDelivery>() {
            self.lifecycle_frames.lock().push(delivery.frame.clone());
        }
        if let Some(request) = envelope.payload::<crate::runtime::SessionCloseRequest>() {
            self.close_reasons.lock().push(request.reason);
        }
        Ok(())
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

fn rpc_frame(
    session_id: u64,
    family: RouteFamily,
    message_type: u16,
    payload: Bytes,
) -> FrameContext {
    FrameContext::new(
        session_id,
        crate::dispatch::protocol::frame::ChannelId::Rpc,
        crate::dispatch::protocol::tlv::MessageType::new(message_type),
        payload,
        family,
    )
}

fn deliver_request(
    sink: &RpcDomain,
    family: RouteFamily,
    route: &Route,
    source: RouteAddress,
    session_id: u64,
    correlation_id: uuid::Uuid,
) {
    let request = crate::domains::rpc::protocol::RpcRequest::new(
        family,
        correlation_id,
        route.clone(),
        Bytes::from_static(b"request"),
    );
    let mut encoder = crate::dispatch::protocol::payload_codec::PayloadEncoder::new();
    let payload = crate::dispatch::protocol::rpc_codec::encode_request_into(&request, &mut encoder);
    sink.deliver(Envelope::from_route(
        source,
        RouteAddress::new(family, route.clone()),
        rpc_frame(session_id, family, 302, Bytes::from(payload)),
    ))
    .expect("deliver RPC request");
}

fn deliver_caller_cancel(
    sink: &RpcDomain,
    family: RouteFamily,
    route: &Route,
    caller_source: RouteAddress,
    caller_session_id: u64,
    correlation_id: uuid::Uuid,
) {
    let mut payload = Vec::with_capacity(18);
    payload.push(1);
    payload.extend_from_slice(correlation_id.as_bytes());
    payload.push(1);
    sink.deliver(Envelope::from_route(
        caller_source,
        RouteAddress::new(family, route.clone()),
        rpc_frame(caller_session_id, family, 304, Bytes::from(payload)),
    ))
    .expect("deliver caller cancellation");
}

fn deliver_worker_ack(
    sink: &RpcDomain,
    family: RouteFamily,
    route: &Route,
    worker_source: RouteAddress,
    correlation_id: uuid::Uuid,
) {
    let frame = encode_cancel_ack_tlv_frame(&correlation_id);
    let (message_type, payload) = crate::benchkit::extract_single_tlv_field(&frame);
    sink.deliver(Envelope::from_route(
        worker_source,
        RouteAddress::new(family, route.clone()),
        rpc_frame(42, family, message_type, payload),
    ))
    .expect("deliver worker cleanup acknowledgement");
}

fn deliver_worker_completion(
    sink: &RpcDomain,
    family: RouteFamily,
    route: &Route,
    worker_source: RouteAddress,
    correlation_id: uuid::Uuid,
) {
    let response = crate::domains::rpc::protocol::RpcResponse::single(
        correlation_id,
        Bytes::from_static(b"late completion"),
    );
    let payload = crate::dispatch::protocol::rpc_codec::encode_response_message(&response);
    sink.deliver(Envelope::from_route(
        worker_source,
        RouteAddress::new(family, route.clone()),
        rpc_frame(42, family, 303, Bytes::from(payload)),
    ))
    .expect("deliver worker completion");
}

fn deliver_worker_progress(
    sink: &RpcDomain,
    family: RouteFamily,
    route: &Route,
    worker_source: RouteAddress,
    correlation_id: uuid::Uuid,
) {
    let response = crate::domains::rpc::protocol::RpcResponse::chunk(
        correlation_id,
        0,
        Bytes::from_static(b"late progress"),
        false,
    );
    let payload = crate::dispatch::protocol::rpc_codec::encode_response_message(&response);
    sink.deliver(Envelope::from_route(
        worker_source,
        RouteAddress::new(family, route.clone()),
        rpc_frame(42, family, 303, Bytes::from(payload)),
    ))
    .expect("deliver worker progress");
}

fn lifecycle_payload_for(frame: &Bytes, kind: u8, correlation_id: uuid::Uuid) -> Bytes {
    let (message_type, payload) = crate::benchkit::extract_single_tlv_field(frame);
    assert_eq!(message_type, 305);
    assert_eq!(payload.first(), Some(&kind));
    assert_eq!(&payload[1..17], correlation_id.as_bytes());
    payload
}

#[test]
#[allow(clippy::too_many_lines)]
fn should_hold_worker_credit_until_cleanup_ack_and_ignore_late_duplicate_release() {
    // Arrange
    let router = Arc::new(Router::new());
    let sink = RpcDomain::new(
        router.clone(),
        crate::control::admin::read_model::AdminReadModel::new(),
    );
    let family = RouteFamily::new(1);
    let route = Route::new("rpc://bench/system/resource/cancel");
    let caller_one = session_inbox_address(family, 1);
    let caller_two = session_inbox_address(family, 2);
    let caller_three = session_inbox_address(family, 3);
    let worker_addr = session_inbox_address(family, 42);
    let caller_one_capture = Arc::new(CaptureRpcEndpoint::default());
    let caller_two_capture = Arc::new(CaptureRpcEndpoint::default());
    let caller_three_capture = Arc::new(CaptureRpcEndpoint::default());
    let worker_capture = Arc::new(CaptureRpcEndpoint::default());
    router.register(caller_one.clone(), caller_one_capture.clone());
    router.register(caller_two.clone(), caller_two_capture.clone());
    router.register(caller_three.clone(), caller_three_capture.clone());
    router.register(worker_addr.clone(), worker_capture.clone());
    sink.register_registration_for_tests(
        test_rpc_worker(family, &route, 42).with_cancellation_support(true),
    );
    let cancelled_id = uuid::Uuid::new_v4();
    let queued_id = uuid::Uuid::new_v4();
    let remaining_queued_id = uuid::Uuid::new_v4();

    // Act
    deliver_request(&sink, family, &route, caller_one.clone(), 1, cancelled_id);
    deliver_request(&sink, family, &route, caller_two.clone(), 2, queued_id);
    deliver_request(&sink, family, &route, caller_three, 3, remaining_queued_id);
    deliver_caller_cancel(&sink, family, &route, caller_two, 2, queued_id);
    let queued_cancel_result = caller_two_capture.lifecycle_frames.lock().clone();
    deliver_caller_cancel(&sink, family, &route, caller_one.clone(), 1, cancelled_id);
    let live_while_worker_stops = sink.pending_request_count();
    let caller_result = caller_one_capture.lifecycle_frames.lock().clone();
    let worker_cancellation = worker_capture.lifecycle_frames.lock().clone();
    deliver_worker_ack(&sink, family, &route, worker_addr.clone(), cancelled_id);
    let live_after_ack = sink.pending_request_count();
    deliver_worker_ack(&sink, family, &route, worker_addr.clone(), cancelled_id);
    deliver_worker_completion(&sink, family, &route, worker_addr, cancelled_id);
    let live_after_late_completion = sink.pending_request_count();

    // Assert
    assert_eq!(live_while_worker_stops, 2);
    assert_eq!(live_after_ack, 1);
    assert_eq!(live_after_late_completion, 1);
    assert_eq!(caller_result.len(), 1);
    assert_eq!(queued_cancel_result.len(), 1);
    assert_eq!(
        lifecycle_payload_for(&queued_cancel_result[0], 4, queued_id)[17],
        crate::protocol::rpc_codec::RpcCancellationResult::QueuedRemoved as u8
    );
    assert_eq!(
        lifecycle_payload_for(&caller_result[0], 4, cancelled_id)[17],
        crate::protocol::rpc_codec::RpcCancellationResult::Forwarded as u8
    );
    assert_eq!(worker_cancellation.len(), 1);
    let worker_cancel = lifecycle_payload_for(&worker_cancellation[0], 2, cancelled_id);
    assert_eq!(
        worker_cancel[17],
        crate::protocol::rpc_codec::RpcCancellationReason::Explicit as u8
    );
    let worker_requests = worker_capture
        .frames
        .lock()
        .iter()
        .filter(|frame| frame.msg_type.as_u16() == 302)
        .count();
    assert_eq!(
        worker_requests, 2,
        "the surviving queued call dispatches after one ack"
    );
}

#[test]
fn should_retain_cancelled_call_credit_when_terminal_response_precedes_cleanup_ack() {
    // Arrange
    let router = Arc::new(Router::new());
    let sink = RpcDomain::new(
        router.clone(),
        crate::control::admin::read_model::AdminReadModel::new(),
    );
    let family = RouteFamily::new(1);
    let route = Route::new("rpc://bench/system/resource/cancel-race");
    let caller = session_inbox_address(family, 1);
    let worker = session_inbox_address(family, 42);
    let caller_capture = Arc::new(CaptureRpcEndpoint::default());
    let worker_capture = Arc::new(CaptureRpcEndpoint::default());
    router.register(caller.clone(), caller_capture.clone());
    router.register(worker.clone(), worker_capture.clone());
    sink.register_registration_for_tests(
        test_rpc_worker(family, &route, 42).with_cancellation_support(true),
    );
    let correlation_id = uuid::Uuid::new_v4();
    deliver_request(&sink, family, &route, caller.clone(), 1, correlation_id);

    // Act
    deliver_caller_cancel(&sink, family, &route, caller, 1, correlation_id);
    deliver_worker_progress(&sink, family, &route, worker.clone(), correlation_id);
    let live_after_progress = sink.pending_request_count();
    let caller_response_count = caller_capture
        .frames
        .lock()
        .iter()
        .filter(|frame| frame.msg_type.as_u16() == 303)
        .count();
    deliver_worker_completion(&sink, family, &route, worker.clone(), correlation_id);
    let live_after_completion = sink.pending_request_count();
    deliver_worker_ack(&sink, family, &route, worker, correlation_id);
    let live_after_late_ack = sink.pending_request_count();

    // Assert
    assert_eq!(live_after_progress, 1);
    assert_eq!(caller_response_count, 0);
    assert_eq!(live_after_completion, 1);
    assert_eq!(live_after_late_ack, 0);
    assert_eq!(worker_capture.lifecycle_frames.lock().len(), 1);
}

#[test]
fn should_close_worker_only_after_deadline_cancellation_grace_expires() {
    // Arrange
    let router = Arc::new(Router::new());
    let sink = RpcDomain::new(
        router.clone(),
        crate::control::admin::read_model::AdminReadModel::new(),
    )
    .with_request_timeout(Duration::from_millis(10));
    let family = RouteFamily::new(1);
    let route = Route::new("rpc://bench/system/resource/deadline-cancel");
    let caller = session_inbox_address(family, 1);
    let worker = session_inbox_address(family, 42);
    let caller_capture = Arc::new(CaptureRpcEndpoint::default());
    let worker_capture = Arc::new(CaptureRpcEndpoint::default());
    router.register(caller.clone(), caller_capture.clone());
    router.register(worker.clone(), worker_capture.clone());
    sink.register_registration_for_tests(
        test_rpc_worker(family, &route, 42).with_cancellation_support(true),
    );
    let correlation_id = uuid::Uuid::new_v4();
    deliver_request(&sink, family, &route, caller, 1, correlation_id);
    let timeout_check = Instant::now() + Duration::from_secs(1);

    // Act
    sink.expire_timed_out_requests_at(timeout_check);
    let live_during_grace = sink.pending_request_count();
    let timeout_frames = worker_capture
        .lifecycle_frames
        .lock()
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    sink.expire_timed_out_requests_at(
        timeout_check + Duration::from_secs(5) + Duration::from_millis(1),
    );
    let live_after_close_request = sink.pending_request_count();
    let close_reasons = worker_capture.close_reasons.lock().clone();
    let cleanup = sink.apply_session_cleanup(42);

    // Assert
    assert_eq!(live_during_grace, 1);
    assert_eq!(live_after_close_request, 1);
    assert_eq!(timeout_frames.len(), 1);
    let timeout_cancel = lifecycle_payload_for(&timeout_frames[0], 2, correlation_id);
    assert_eq!(
        timeout_cancel[17],
        crate::protocol::rpc_codec::RpcCancellationReason::BrokerDeadline as u8
    );
    let caller_frames = caller_capture.frames.lock();
    assert_eq!(caller_frames.len(), 1);
    assert_rpc_terminal_code_error(
        &caller_frames[0],
        correlation_id,
        crate::dispatch::protocol::error_codes::rpc::ERR_RPC_TIMEOUT,
        RPC_TIMEOUT_ERROR,
    );
    assert_eq!(close_reasons, ["RPC cancellation grace period expired"]);
    assert_eq!(cleanup.removed_pending, 1);
    assert_eq!(sink.pending_request_count(), 0);
}
