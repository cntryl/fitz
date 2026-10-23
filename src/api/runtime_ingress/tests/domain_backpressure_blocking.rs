use super::*;
use std::time::Instant;

struct BlockingDomainSink {
    entered: crossbeam_channel::Sender<()>,
    release: crossbeam_channel::Receiver<()>,
}

impl MailboxSink for BlockingDomainSink {
    fn deliver(&self, _envelope: Envelope) -> Result<(), DeliveryError> {
        self.entered.send(()).expect("signal domain delivery");
        self.release.recv().expect("release domain delivery");
        Ok(())
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

fn dispatch_while_sink_blocks(domain: &'static str) -> (bool, IngressDecision) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .max_blocking_threads(2)
        .enable_all()
        .build()
        .expect("single-worker runtime");
    let case = blocking_case(domain);
    let (entered_tx, entered_rx) = crossbeam_channel::bounded(1);
    let (release_tx, release_rx) = crossbeam_channel::bounded(1);
    let router = Arc::new(crate::runtime::Router::new());
    router.register_domain_pattern(
        sink_domain(domain),
        Arc::new(BlockingDomainSink {
            entered: entered_tx,
            release: release_rx,
        }),
    );
    let ingress = Arc::new(RuntimeIngress::new(false).with_router(router));
    let session_id = 9_000;

    rt.block_on(ingress.on_open(make_session_info(session_id, TransportKind::Tcp)))
        .expect("open session");
    let dispatch = rt.spawn(async move {
        ingress
            .on_frame(
                session_id,
                case.channel_id,
                crate::protocol::message::MessageType::new(case.msg_type),
                case.payload,
                None,
            )
            .await
    });
    entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("domain sink entered");
    let (canary_tx, canary_rx) = crossbeam_channel::bounded(1);
    rt.spawn(async move { canary_tx.send(()).expect("signal Tokio canary") });
    let canary_ran = canary_rx.recv_timeout(Duration::from_secs(2)).is_ok();
    release_tx.send(()).expect("release domain sink");
    let decision = rt.block_on(dispatch).expect("domain dispatch task");

    (canary_ran, decision)
}

#[test]
fn should_keep_tokio_worker_available_while_stream_sink_waits() {
    // Arrange
    let domain = "stream";

    // Act
    let (canary_ran, decision) = dispatch_while_sink_blocks(domain);

    // Assert
    assert!(
        canary_ran,
        "a stalled Stream dispatch blocked the Tokio worker"
    );
    assert_eq!(decision, IngressDecision::Accept);
}

/// Test case names may carry an operation suffix; the sink registers under the
/// bare domain scheme.
fn sink_domain(case_domain: &'static str) -> &'static str {
    case_domain
        .split_once('-')
        .map_or(case_domain, |(domain, _)| domain)
}

/// Blocking-handoff cases: the shared table's request for each domain, plus a
/// Notice SUBSCRIBE, which waits for an actor reply unlike Notice PUBLISH.
fn blocking_case(domain: &'static str) -> DomainIngressCase {
    if domain == "notice-subscribe" {
        let (msg_type, payload) = crate::benchkit::extract_single_tlv_field(
            &crate::benchkit::build_notice_subscribe("notice://test/events/*"),
        );
        return DomainIngressCase {
            domain,
            channel_id: ChannelId::Pub,
            msg_type,
            payload,
        };
    }
    domain_ingress_cases()
        .into_iter()
        .find(|case| case.domain == domain)
        .expect("domain request case")
}

#[test]
fn should_keep_tokio_worker_available_while_rpc_sink_waits() {
    // Arrange
    let domain = "rpc";

    // Act
    let (canary_ran, decision) = dispatch_while_sink_blocks(domain);

    // Assert
    assert!(
        canary_ran,
        "a stalled RPC dispatch blocked the Tokio worker"
    );
    assert_eq!(decision, IngressDecision::Accept);
}

#[test]
fn should_keep_tokio_worker_available_while_notice_subscribe_waits() {
    // Arrange
    let domain = "notice-subscribe";

    // Act
    let (canary_ran, decision) = dispatch_while_sink_blocks(domain);

    // Assert
    assert!(
        canary_ran,
        "a stalled Notice subscribe dispatch blocked the Tokio worker"
    );
    assert_eq!(decision, IngressDecision::Accept);
}

#[test]
fn should_keep_tokio_worker_available_while_queue_sink_waits() {
    // Arrange
    let domain = "queue";

    // Act
    let (canary_ran, decision) = dispatch_while_sink_blocks(domain);

    // Assert
    assert!(
        canary_ran,
        "a stalled Queue dispatch blocked the Tokio worker"
    );
    assert_eq!(decision, IngressDecision::Accept);
}

fn assert_rejected_before_enqueue_when_handoff_full(domain: &'static str) {
    // Arrange
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let case = blocking_case(domain);
    let router = Arc::new(crate::runtime::Router::new());
    let sink = Arc::new(TransientBackpressuredSink::new(0));
    router.register_domain_pattern(sink_domain(domain), sink.clone());
    let session_id = 9_001;
    let client_frames = Arc::new(Mutex::new(Vec::<FrameContext>::new()));
    router.register(
        crate::runtime::routing::RouteAddress::new(
            RouteFamily::new(1),
            crate::runtime::routing::Route::new(format!("inbox://session/{session_id}")),
        ),
        Arc::new(CapturingInboxSink {
            frames: client_frames.clone(),
        }),
    );
    let ingress = RuntimeIngress::new(false).with_router(router);
    let permits = match domain {
        "stream" => &ingress.dispatcher.stream_dispatch_permits,
        "queue" => &ingress.dispatcher.queue_dispatch_permits,
        "rpc" => &ingress.dispatcher.rpc_dispatch_permits,
        "notice-subscribe" => &ingress.dispatcher.notice_dispatch_permits,
        _ => unreachable!("only blocking domains have a handoff"),
    };
    let _all_permits = permits
        .clone()
        .try_acquire_many_owned(
            u32::try_from(
                crate::api::runtime_ingress::domain_frame_dispatcher::BLOCKING_DOMAIN_DISPATCH_CONCURRENCY,
            )
            .expect("domain handoff capacity fits u32"),
        )
        .expect("occupy domain blocking handoff");

    // Act
    let decision = rt.block_on(async {
        ingress
            .on_open(make_session_info(session_id, TransportKind::Tcp))
            .await
            .expect("open session");
        ingress
            .on_frame(
                session_id,
                case.channel_id,
                crate::protocol::message::MessageType::new(case.msg_type),
                case.payload.clone(),
                None,
            )
            .await
    });

    // Assert
    assert_eq!(decision, IngressDecision::Accept);
    assert_eq!(sink.accepted.load(Ordering::SeqCst), 0);
    let frames = client_frames.lock().expect("captured frames");
    assert_eq!(frames.len(), 1);
    assert!(documented_retryable(synthesized_error_code(
        &case, &frames[0]
    )));
}

#[test]
fn should_reject_stream_before_enqueue_when_blocking_handoff_is_full() {
    assert_rejected_before_enqueue_when_handoff_full("stream");
}

#[test]
fn should_reject_queue_before_enqueue_when_blocking_handoff_is_full() {
    assert_rejected_before_enqueue_when_handoff_full("queue");
}

#[test]
fn should_reject_rpc_before_enqueue_when_blocking_handoff_is_full() {
    assert_rejected_before_enqueue_when_handoff_full("rpc");
}

#[test]
fn should_reject_notice_subscribe_before_enqueue_when_blocking_handoff_is_full() {
    assert_rejected_before_enqueue_when_handoff_full("notice-subscribe");
}

#[test]
fn should_retain_rpc_handoff_permit_after_transport_waiter_is_canceled() {
    // Arrange
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let (entered_tx, entered_rx) = crossbeam_channel::bounded(1);
    let (release_tx, release_rx) = crossbeam_channel::bounded(1);
    let router = Arc::new(crate::runtime::Router::new());
    router.register_domain_pattern(
        "rpc",
        Arc::new(BlockingDomainSink {
            entered: entered_tx,
            release: release_rx,
        }),
    );
    let ingress = Arc::new(RuntimeIngress::new(false).with_router(router.clone()));
    let permits = ingress.dispatcher.rpc_dispatch_permits.clone();
    let capacity =
        crate::api::runtime_ingress::domain_frame_dispatcher::BLOCKING_DOMAIN_DISPATCH_CONCURRENCY;
    let dispatch = {
        let ingress = ingress.clone();
        rt.spawn(async move {
            let destination = crate::runtime::routing::RouteAddress::new(
                RouteFamily::new(1),
                crate::runtime::routing::Route::new("rpc://inbound"),
            );
            ingress
                .dispatcher
                .route_client_domain(
                    &router,
                    DispatchDomain::Rpc,
                    Envelope::new(destination, ()),
                    Instant::now(),
                )
                .await
        })
    };
    entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("RPC sink entered");

    // Act
    dispatch.abort();
    let held_while_sink_blocked = permits.available_permits();
    release_tx.send(()).expect("release RPC sink");
    let deadline = Instant::now() + Duration::from_secs(2);
    while permits.available_permits() != capacity && Instant::now() < deadline {
        std::thread::yield_now();
    }

    // Assert
    assert_eq!(
        held_while_sink_blocked,
        capacity - 1,
        "cancelling the transport waiter must not recycle an active RPC handoff"
    );
    assert_eq!(permits.available_permits(), capacity);
}
