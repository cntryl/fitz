use super::*;

struct BackpressuredWorkerSink {
    error: DeliveryError,
}

impl MailboxSink for BackpressuredWorkerSink {
    fn deliver(&self, _envelope: Envelope) -> Result<(), DeliveryError> {
        Err(self.error.clone())
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

fn exercise_detached_caller_queued_dispatch_backpressure(error: DeliveryError) {
    let router = Arc::new(Router::new());
    let metrics = crate::observability::metrics::MetricsCollector::new();
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = RpcDomainSink::new(router.clone(), admin_read_model).with_metrics(metrics.clone());
    let family = RouteFamily::new(1);
    let route = Route::new("rpc://bench/system/resource/queued-disconnect");
    let worker_inbox = session_inbox_address(family, 42);
    router.register(
        worker_inbox.clone(),
        Arc::new(BackpressuredWorkerSink { error }) as Arc<dyn MailboxSink>,
    );
    sink.register_registration_for_tests(RpcWorker::with_stats(
        RouteAddress::new(family, route.clone()),
        worker_inbox,
        42,
        "2026-03-14T12:00:00Z",
        0,
        0,
    ));
    let correlation_id = uuid::Uuid::new_v4();
    sink.queue_request_for_tests(
        correlation_id,
        RpcQueuedRequest::from_request(
            crate::domains::rpc::protocol::RpcRequest::new(
                family,
                correlation_id,
                route.clone(),
                bytes::Bytes::from_static(b"queued"),
            ),
            7,
            session_inbox_address(family, 7),
            Instant::now() + Duration::from_secs(30),
        ),
    );
    let dispatch = {
        let mut state = sink.core.state.lock();
        state.next_queued_dispatch(&route).expect("queued dispatch")
    };
    let cleanup = sink.apply_session_cleanup(7);

    sink.runtime().forward_queued_dispatch(&dispatch);

    assert_eq!(cleanup.detached_callers, 1);
    assert_eq!(sink.pending_request_count(), 0);
    assert_eq!(
        metrics.counter_get("rpc_backpressure_errors_forwarded_total"),
        0
    );
    assert_eq!(
        metrics.counter_get("rpc_backpressure_errors_dropped_total"),
        1
    );
    assert_eq!(
        metrics.counter_get("rpc_responses_dropped_closed_caller_total"),
        1
    );
}

#[test]
fn should_return_to_family_drain_loop_after_queued_dispatch_backpressure() {
    // Arrange
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = RpcDomainSink::new(router.clone(), admin_read_model);
    let family = RouteFamily::new(1);
    let route = Route::new("rpc://bench/system/resource/queued-backpressure");
    let worker_inbox = session_inbox_address(family, 42);
    router.register(
        worker_inbox.clone(),
        Arc::new(BackpressuredWorkerSink {
            error: DeliveryError::MailboxFull {
                capacity: 1,
                current_len: 1,
            },
        }) as Arc<dyn MailboxSink>,
    );
    sink.register_registration_for_tests(RpcWorker::with_stats(
        RouteAddress::new(family, route.clone()),
        worker_inbox,
        42,
        "2026-03-14T12:00:00Z",
        0,
        0,
    ));
    for caller_session_id in [7, 8] {
        let correlation_id = uuid::Uuid::new_v4();
        sink.queue_request_for_tests(
            correlation_id,
            RpcQueuedRequest::from_request(
                crate::domains::rpc::protocol::RpcRequest::new(
                    family,
                    correlation_id,
                    route.clone(),
                    bytes::Bytes::from_static(b"queued"),
                ),
                caller_session_id,
                session_inbox_address(family, caller_session_id),
                Instant::now() + Duration::from_secs(30),
            ),
        );
    }
    let dispatch = {
        let mut state = sink.core.state.lock();
        state.next_queued_dispatch(&route).expect("queued dispatch")
    };

    // Act
    sink.runtime().forward_queued_dispatch(&dispatch);

    // Assert
    assert_eq!(sink.queued_request_count_for_tests(), 1);
    assert_eq!(sink.pending_table_len_for_tests(), 0);
}

#[test]
fn should_drop_backpressure_error_without_unwind_given_detached_queued_request_caller() {
    // Arrange
    let error = DeliveryError::MailboxFull {
        capacity: 1,
        current_len: 1,
    };

    // Act
    exercise_detached_caller_queued_dispatch_backpressure(error);

    // Assert
    // The shared exercise asserts bounded error handling and metrics.
}

#[test]
fn should_drop_high_lane_error_without_unwind_given_detached_queued_request_caller() {
    // Arrange
    let error = DeliveryError::HighLaneFull {
        capacity: 1,
        current_len: 1,
    };

    // Act
    exercise_detached_caller_queued_dispatch_backpressure(error);

    // Assert
    // The shared exercise asserts bounded error handling and metrics.
}
