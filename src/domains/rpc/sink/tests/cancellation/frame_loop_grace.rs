//! Cancellation grace runs only while the worker's frame loop can read the ACK.

use super::*;

const GRACE: Duration = Duration::from_secs(5);

/// Worker inbox whose transport reports a test-controlled frame-loop busy time.
#[derive(Default)]
struct BusyWorkerEndpoint {
    capture: CaptureRpcEndpoint,
    busy_time: parking_lot::Mutex<Duration>,
}

impl MailboxSink for BusyWorkerEndpoint {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.capture.deliver(envelope)
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }

    fn frame_loop_busy_time(&self, _now: Instant) -> Option<Duration> {
        Some(*self.busy_time.lock())
    }
}

struct CancelledCall {
    sink: RpcDomain,
    family: RouteFamily,
    route: Route,
    worker: Arc<BusyWorkerEndpoint>,
    correlation_id: uuid::Uuid,
    cancelled_at: Instant,
}

impl CancelledCall {
    fn start() -> Self {
        let router = Arc::new(Router::new());
        let family = RouteFamily::new(1);
        let route = Route::new("rpc://realm/area/resource/frame-loop-grace");
        let sink = RpcDomain::new(
            router.clone(),
            crate::control::admin::read_model::AdminReadModel::new(),
        );
        let worker = Arc::new(BusyWorkerEndpoint::default());
        router.register(
            session_inbox_address(family, 1),
            Arc::new(CaptureRpcEndpoint::default()),
        );
        router.register(session_inbox_address(family, 42), worker.clone());
        sink.register_registration_for_tests(
            test_rpc_worker(family, &route, 42).with_cancellation_support(true),
        );
        let correlation_id = uuid::Uuid::new_v4();
        let caller = session_inbox_address(family, 1);
        deliver_request(&sink, family, &route, caller.clone(), 1, correlation_id);
        deliver_caller_cancel(&sink, family, &route, caller, 1, correlation_id);
        Self {
            sink,
            family,
            route,
            worker,
            correlation_id,
            cancelled_at: Instant::now(),
        }
    }

    fn hold_frame_loop_for(&self, busy: Duration) {
        *self.worker.busy_time.lock() += busy;
    }

    fn keep_frame_loop_busy_until(&self, elapsed: Duration) {
        *self.worker.busy_time.lock() = elapsed;
    }

    fn sweep_after(&self, elapsed: Duration) {
        self.sink
            .expire_timed_out_requests_at(self.cancelled_at + elapsed);
    }

    fn ack(&self) {
        deliver_worker_ack(
            &self.sink,
            self.family,
            &self.route,
            session_inbox_address(self.family, 42),
            self.correlation_id,
        );
    }

    fn close_reasons(&self) -> Vec<&'static str> {
        self.worker.capture.close_reasons.lock().clone()
    }
}

#[test]
fn should_not_close_worker_whose_frame_loop_was_held_past_grace_before_ack() {
    // Arrange
    let call = CancelledCall::start();
    call.hold_frame_loop_for(Duration::from_secs(60));

    // Act
    call.sweep_after(GRACE + Duration::from_millis(1));
    call.sweep_after(Duration::from_secs(30));
    call.ack();
    call.sweep_after(Duration::from_secs(120));

    // Assert
    assert_eq!(call.close_reasons(), [] as [&str; 0]);
    assert_eq!(call.sink.pending_request_count(), 0);
}

#[test]
fn should_close_worker_whose_frame_loop_stays_busy_past_grace_ceiling() {
    // Arrange
    let call = CancelledCall::start();
    let mut closed_at = None;

    // Act
    for second in 1..=70 {
        let elapsed = Duration::from_secs(second);
        call.keep_frame_loop_busy_until(elapsed);
        call.sweep_after(elapsed);
        if closed_at.is_none() && !call.close_reasons().is_empty() {
            closed_at = Some(elapsed);
        }
    }

    // Assert
    assert_eq!(closed_at, Some(GRACE + RPC_MAX_CANCELLATION_GRACE_DEFERRAL));
}

#[test]
fn should_close_idle_worker_that_never_acks_after_grace() {
    // Arrange
    let call = CancelledCall::start();

    // Act
    call.sweep_after(GRACE + Duration::from_millis(1));

    // Assert
    assert_eq!(
        call.close_reasons(),
        ["RPC cancellation grace period expired"]
    );
    assert_eq!(call.sink.pending_request_count(), 1);
}

#[test]
fn should_resume_grace_once_held_frame_loop_is_free_again() {
    // Arrange
    let call = CancelledCall::start();
    call.hold_frame_loop_for(Duration::from_secs(3));

    // Act
    call.sweep_after(GRACE + Duration::from_millis(1));
    let closed_while_owed_free_time = call.close_reasons();
    call.sweep_after(GRACE + Duration::from_secs(3) + Duration::from_millis(1));

    // Assert
    assert_eq!(closed_while_owed_free_time, [] as [&str; 0]);
    assert_eq!(
        call.close_reasons(),
        ["RPC cancellation grace period expired"]
    );
}
