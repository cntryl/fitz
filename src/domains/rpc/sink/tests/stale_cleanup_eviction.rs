use super::*;

#[test]
fn should_not_dispatch_closed_caller_request_after_cleanup_marker_eviction() {
    assert_dispatch_after_cleanup_churn(false, true);
}

#[test]
fn should_not_dispatch_closed_caller_via_known_domain_after_cleanup_marker_eviction() {
    assert_dispatch_after_cleanup_churn(true, true);
}

#[test]
fn should_dispatch_live_older_caller_after_unrelated_cleanup_churn() {
    assert_dispatch_after_cleanup_churn(true, false);
}

fn assert_dispatch_after_cleanup_churn(known_domain: bool, close_caller: bool) {
    // Arrange
    let family = RouteFamily::new(1);
    let router = Arc::new(Router::new());
    let sink = Arc::new(RpcDomain::new_with_families(
        router.clone(),
        crate::control::admin::read_model::AdminReadModel::new(),
        &[family],
    ));
    let admitted = Arc::new(parking_lot::Mutex::new(None));
    router.register_domain_pattern(
        "rpc",
        Arc::new(HoldRpcRequest {
            sink: sink.clone(),
            admitted: admitted.clone(),
        }),
    );
    let route = Route::new("rpc://acme/jobs/worker/run");
    let destination = RouteAddress::new(family, route.clone());
    let worker_inbox = session_inbox_address(family, 42);
    let worker_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
    router.register(
        worker_inbox.clone(),
        Arc::new(CaptureRpcFrameSink {
            frames: worker_frames.clone(),
        }) as Arc<dyn MailboxSink>,
    );
    sink.register_registration_for_tests(RpcWorker::with_stats(
        destination.clone(),
        worker_inbox,
        42,
        "2026-10-01T12:00:00Z",
        0,
        0,
    ));
    let request = crate::domains::rpc::protocol::RpcRequest::new(
        family,
        uuid::Uuid::new_v4(),
        route,
        bytes::Bytes::from_static(b"side-effecting work"),
    );
    let mut encoder = crate::dispatch::protocol::payload_codec::PayloadEncoder::new();
    let payload = crate::dispatch::protocol::rpc_codec::encode_request_into(&request, &mut encoder);
    let stale_request = Envelope::from_route(
        session_inbox_address(family, 1),
        destination,
        FrameContext::new(
            1,
            crate::dispatch::protocol::frame::ChannelId::Rpc,
            crate::dispatch::protocol::tlv::MessageType::new(302),
            bytes::Bytes::from(payload),
            family,
        ),
    );

    if known_domain {
        router
            .route_to_domain("rpc", stale_request)
            .expect("admit caller request");
    } else {
        router.route(stale_request).expect("admit caller request");
    }

    // Act
    for session_id in close_caller.then_some(1).into_iter().chain(
        100..100 + u64::try_from(crate::domains::DOMAIN_ACTOR_MAILBOX_CAPACITY).expect("capacity"),
    ) {
        router
            .route_high_priority(Envelope::new(
                RouteAddress::new(family, Route::new("rpc://cleanup")),
                crate::runtime::SessionCleanup { session_id },
            ))
            .expect("complete control-lane cleanup");
    }
    sink.deliver(admitted.lock().take().expect("queued caller request"))
        .expect("process stale request");
    let dispatched = worker_frames.lock().len();
    let pending = sink.pending_request_count();

    // Assert
    assert_eq!(
        (dispatched, pending),
        if close_caller { (0, 0) } else { (1, 1) },
        "cleanup churn must reject closed callers and preserve live older callers",
    );
}

struct HoldRpcRequest {
    sink: Arc<RpcDomain>,
    admitted: Arc<parking_lot::Mutex<Option<Envelope>>>,
}

impl MailboxSink for HoldRpcRequest {
    fn deliver(&self, envelope: Envelope) -> Result<(), crate::runtime::DeliveryError> {
        *self.admitted.lock() = Some(envelope);
        Ok(())
    }

    fn deliver_high_priority(
        &self,
        envelope: Envelope,
    ) -> Result<(), crate::runtime::DeliveryError> {
        self.sink.deliver_high_priority(envelope)
    }
}
