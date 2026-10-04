use super::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[test]
fn should_reject_stale_cleanup_ack_after_caller_correlation_is_reused() {
    // Arrange
    let h = Harness::new();
    let id = uuid::Uuid::new_v4();
    h.request(id);
    let first_worker_id = uuid::Uuid::from_slice(&h.worker.frames.lock()[0].payload[..16]).unwrap();
    deliver_worker_completion(
        &h.sink,
        h.family,
        &h.route,
        session_inbox_address(h.family, 42),
        first_worker_id,
    );
    h.request(id);
    let second_worker_id =
        uuid::Uuid::from_slice(&h.worker.frames.lock()[1].payload[..16]).unwrap();
    h.cancel(id);

    // Act
    h.ack(first_worker_id);

    // Assert
    assert_eq!(h.capacity(), 1);
    assert_eq!(h.sink.pending_request_count(), 1);
    assert_ne!(first_worker_id, second_worker_id);
    assert_ne!(id, second_worker_id);
    h.ack(second_worker_id);
    assert_eq!(h.capacity(), 0);
}

#[test]
fn should_ignore_stale_worker_responses_after_caller_correlation_is_reused() {
    // Arrange
    let h = Harness::new();
    let id = uuid::Uuid::new_v4();
    h.request(id);
    let first_worker_id = uuid::Uuid::from_slice(&h.worker.frames.lock()[0].payload[..16]).unwrap();
    deliver_worker_completion(
        &h.sink,
        h.family,
        &h.route,
        session_inbox_address(h.family, 42),
        first_worker_id,
    );
    h.request(id);

    // Act
    deliver_worker_progress(
        &h.sink,
        h.family,
        &h.route,
        session_inbox_address(h.family, 42),
        first_worker_id,
    );
    deliver_worker_completion(
        &h.sink,
        h.family,
        &h.route,
        session_inbox_address(h.family, 42),
        first_worker_id,
    );

    // Assert
    assert_eq!(h.capacity(), 1);
    assert_eq!(h.caller.frames.lock().len(), 1);
    assert_eq!(&h.caller.frames.lock()[0].payload[..16], id.as_bytes());
}

struct Harness {
    sink: RpcDomain,
    router: Arc<Router>,
    family: RouteFamily,
    route: Route,
    caller: Arc<CaptureRpcEndpoint>,
    worker: Arc<CaptureRpcEndpoint>,
}

impl Harness {
    fn new() -> Self {
        let router = Arc::new(Router::new());
        let family = RouteFamily::new(1);
        let route = Route::new("rpc://realm/area/resource/cancellation");
        let sink = RpcDomain::new_with_families(
            router.clone(),
            crate::control::admin::read_model::AdminReadModel::new(),
            &[family, RouteFamily::new(2)],
        );
        let caller = Arc::new(CaptureRpcEndpoint::default());
        let worker = Arc::new(CaptureRpcEndpoint::default());
        router.register(session_inbox_address(family, 1), caller.clone());
        router.register(session_inbox_address(family, 42), worker.clone());
        sink.register_registration_for_tests(
            test_rpc_worker(family, &route, 42).with_cancellation_support(true),
        );
        Self {
            sink,
            router,
            family,
            route,
            caller,
            worker,
        }
    }

    fn request(&self, id: uuid::Uuid) {
        deliver_request(
            &self.sink,
            self.family,
            &self.route,
            session_inbox_address(self.family, 1),
            1,
            id,
        );
    }

    fn cancel(&self, id: uuid::Uuid) {
        deliver_caller_cancel(
            &self.sink,
            self.family,
            &self.route,
            session_inbox_address(self.family, 1),
            1,
            id,
        );
    }

    fn ack(&self, id: uuid::Uuid) {
        deliver_worker_ack(
            &self.sink,
            self.family,
            &self.route,
            session_inbox_address(self.family, 42),
            id,
        );
    }

    fn capacity(&self) -> usize {
        self.sink
            .config
            .global_pending_count
            .load(Ordering::Acquire)
    }

    fn worker_request_count(&self) -> usize {
        self.worker
            .frames
            .lock()
            .iter()
            .filter(|frame| frame.msg_type.as_u16() == 302)
            .count()
    }
}

#[test]
fn should_hold_cancelled_execution_credit_until_ack_before_dispatching_queued_call() {
    // Arrange
    let h = Harness::new();
    let first = uuid::Uuid::new_v4();
    let queued = uuid::Uuid::new_v4();
    h.request(first);
    h.request(queued);

    // Act
    h.cancel(first);
    deliver_worker_completion(
        &h.sink,
        h.family,
        &h.route,
        session_inbox_address(h.family, 42),
        first,
    );
    let requests_before_cleanup = h.worker_request_count();
    let capacity_before_cleanup = h.capacity();
    h.ack(first);

    // Assert
    assert_eq!(requests_before_cleanup, 1);
    assert_eq!(capacity_before_cleanup, 2);
    assert_eq!(h.worker_request_count(), 2);
    assert_eq!(h.capacity(), 1);
}

#[test]
fn should_retain_cancelled_execution_when_worker_unregisters_before_cleanup_ack() {
    // Arrange
    let h = Harness::new();
    let id = uuid::Uuid::new_v4();
    h.request(id);
    h.cancel(id);

    // Act
    let cleanup = h
        .sink
        .apply_worker_unsubscribe(&RouteAddress::new(h.family, h.route.clone()), 42);
    let retained_capacity = h.capacity();
    h.ack(id);

    // Assert
    assert_eq!(cleanup.removed_registrations, 1);
    assert_eq!(cleanup.removed_pending, 0);
    assert_eq!(retained_capacity, 1);
    assert_eq!(h.capacity(), 0);
}

#[test]
fn should_reject_other_caller_cancellation_without_exposing_live_call_state() {
    // Arrange
    let h = Harness::new();
    let id = uuid::Uuid::new_v4();
    h.request(id);
    let other = Arc::new(CaptureRpcEndpoint::default());
    let other_addr = session_inbox_address(h.family, 2);
    h.router.register(other_addr.clone(), other.clone());

    // Act
    deliver_caller_cancel(&h.sink, h.family, &h.route, other_addr.clone(), 2, id);
    deliver_caller_cancel(
        &h.sink,
        h.family,
        &h.route,
        other_addr,
        2,
        uuid::Uuid::new_v4(),
    );

    // Assert
    let results = other.lifecycle_frames.lock();
    assert_eq!(results.len(), 2);
    for result in results.iter() {
        let (_, payload) = crate::benchkit::extract_single_tlv_field(result);
        assert_eq!(
            payload[17],
            crate::protocol::rpc_codec::RpcCancellationResult::UnauthorizedOrUnknown as u8
        );
    }
    assert!(h.worker.lifecycle_frames.lock().is_empty());
    assert_eq!(h.capacity(), 1);
}

#[test]
fn should_cancel_only_originating_family_when_identical_correlations_are_live() {
    // Arrange
    let h = Harness::new();
    let id = uuid::Uuid::new_v4();
    let other_family = RouteFamily::new(2);
    let other_worker = Arc::new(CaptureRpcEndpoint::default());
    let other_caller = Arc::new(CaptureRpcEndpoint::default());
    h.router.register(
        session_inbox_address(other_family, 42),
        other_worker.clone(),
    );
    h.router
        .register(session_inbox_address(other_family, 1), other_caller);
    h.sink.register_registration_for_tests(
        test_rpc_worker(other_family, &h.route, 42).with_cancellation_support(true),
    );
    h.request(id);
    deliver_request(
        &h.sink,
        other_family,
        &h.route,
        session_inbox_address(other_family, 1),
        1,
        id,
    );

    // Act
    h.cancel(id);
    h.ack(id);

    // Assert
    assert_eq!(h.worker.lifecycle_frames.lock().len(), 1);
    assert!(other_worker.lifecycle_frames.lock().is_empty());
    assert_eq!(h.capacity(), 1);
    assert_eq!(h.sink.pending_request_count(), 1);
}

#[test]
fn should_ignore_wrong_worker_and_duplicate_cleanup_ack_without_releasing_credit() {
    // Arrange
    let h = Harness::new();
    let id = uuid::Uuid::new_v4();
    h.request(id);
    h.cancel(id);
    let frame = encode_cancel_ack_tlv_frame(&id);
    let (message_type, payload) = crate::benchkit::extract_single_tlv_field(&frame);

    // Act
    h.sink
        .deliver(Envelope::from_route(
            session_inbox_address(h.family, 43),
            RouteAddress::new(h.family, h.route.clone()),
            rpc_frame(43, h.family, message_type, payload),
        ))
        .expect("wrong worker ack is ignored");
    let after_wrong_worker = h.capacity();
    h.ack(id);
    h.ack(id);

    // Assert
    assert_eq!(after_wrong_worker, 1);
    assert_eq!(h.capacity(), 0);
}

#[test]
fn should_keep_completed_call_terminal_when_cancellation_arrives_later() {
    // Arrange
    let h = Harness::new();
    let id = uuid::Uuid::new_v4();
    h.request(id);

    // Act
    deliver_worker_completion(
        &h.sink,
        h.family,
        &h.route,
        session_inbox_address(h.family, 42),
        id,
    );
    h.cancel(id);
    h.ack(id);

    // Assert
    assert_eq!(h.capacity(), 0);
    assert_eq!(h.caller.frames.lock().len(), 1);
    assert!(h.worker.lifecycle_frames.lock().is_empty());
    let result = h.caller.lifecycle_frames.lock();
    assert_eq!(
        lifecycle_payload_for(&result[0], 4, id)[17],
        crate::protocol::rpc_codec::RpcCancellationResult::UnauthorizedOrUnknown as u8
    );
}

#[test]
fn should_remove_queued_call_once_and_leave_dispatched_capacity_reserved() {
    // Arrange
    let h = Harness::new();
    let dispatched = uuid::Uuid::new_v4();
    let queued = uuid::Uuid::new_v4();
    h.request(dispatched);
    h.request(queued);

    // Act
    h.cancel(queued);
    h.cancel(queued);
    h.sink
        .expire_timed_out_requests_at(Instant::now() + Duration::from_secs(1));

    // Assert
    assert_eq!(h.capacity(), 1);
    assert_eq!(h.worker_request_count(), 1);
    assert!(h.worker.lifecycle_frames.lock().is_empty());
    let results = h.caller.lifecycle_frames.lock();
    assert_eq!(
        lifecycle_payload_for(&results[0], 4, queued)[17],
        crate::protocol::rpc_codec::RpcCancellationResult::QueuedRemoved as u8
    );
    assert_eq!(
        lifecycle_payload_for(&results[1], 4, queued)[17],
        crate::protocol::rpc_codec::RpcCancellationResult::UnauthorizedOrUnknown as u8
    );
}

#[test]
fn should_forward_dispatched_cancellation_once_when_caller_repeats_cancel() {
    // Arrange
    let h = Harness::new();
    let id = uuid::Uuid::new_v4();
    h.request(id);

    // Act
    h.cancel(id);
    h.cancel(id);

    // Assert
    assert_eq!(h.capacity(), 1);
    assert_eq!(h.worker.lifecycle_frames.lock().len(), 1);
    let results = h.caller.lifecycle_frames.lock();
    assert_eq!(
        lifecycle_payload_for(&results[1], 4, id)[17],
        crate::protocol::rpc_codec::RpcCancellationResult::AlreadyTerminal as u8
    );
}

struct FailingControlEndpoint {
    capture: CaptureRpcEndpoint,
    reject_controls: AtomicBool,
    rejected_closes: AtomicUsize,
}

impl MailboxSink for FailingControlEndpoint {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        if envelope.payload::<RpcLifecycleControlDelivery>().is_some()
            && self.reject_controls.load(Ordering::Acquire)
        {
            return Err(DeliveryError::MailboxFull {
                capacity: 1,
                current_len: 1,
            });
        }
        if envelope
            .payload::<crate::runtime::SessionCloseRequest>()
            .is_some()
            && self
                .rejected_closes
                .try_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
        {
            return Err(DeliveryError::ActorStopped);
        }
        self.capture.deliver(envelope)
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

#[test]
fn should_retry_failed_worker_close_without_releasing_cancelled_capacity() {
    // Arrange
    let h = Harness::new();
    let endpoint = Arc::new(FailingControlEndpoint {
        capture: CaptureRpcEndpoint::default(),
        reject_controls: AtomicBool::new(true),
        rejected_closes: AtomicUsize::new(1),
    });
    h.router
        .register(session_inbox_address(h.family, 42), endpoint.clone());
    let id = uuid::Uuid::new_v4();
    h.request(id);

    // Act
    h.cancel(id);
    let capacity_after_failed_close = h.capacity();
    h.sink
        .expire_timed_out_requests_at(Instant::now() + Duration::from_secs(1));
    let capacity_after_close_request = h.capacity();
    h.sink.apply_session_cleanup(42);

    // Assert
    let results = h.caller.lifecycle_frames.lock();
    assert_eq!(
        lifecycle_payload_for(&results[0], 4, id)[17],
        crate::protocol::rpc_codec::RpcCancellationResult::ForwardingFailed as u8
    );
    assert_eq!(capacity_after_failed_close, 1);
    assert_eq!(capacity_after_close_request, 1);
    assert_eq!(endpoint.capture.close_reasons.lock().len(), 1);
    assert_eq!(h.capacity(), 0);
}
