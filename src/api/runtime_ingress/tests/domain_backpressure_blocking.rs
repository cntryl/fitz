use super::*;

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
    let case = domain_ingress_cases()
        .into_iter()
        .find(|case| case.domain == domain)
        .expect("domain request case");
    let (entered_tx, entered_rx) = crossbeam_channel::bounded(1);
    let (release_tx, release_rx) = crossbeam_channel::bounded(1);
    let router = Arc::new(crate::runtime::Router::new());
    router.register_domain_pattern(
        domain,
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
                crate::protocol::tlv::MessageType::new(case.msg_type),
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
    let case = domain_ingress_cases()
        .into_iter()
        .find(|case| case.domain == domain)
        .expect("domain request case");
    let router = Arc::new(crate::runtime::Router::new());
    let sink = Arc::new(TransientBackpressuredSink::new(0));
    router.register_domain_pattern(domain, sink.clone());
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
                crate::protocol::tlv::MessageType::new(case.msg_type),
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
    assert!(DOCUMENTED_RETRYABLE_CODES.contains(&synthesized_error_code(&case, &frames[0])));
}

#[test]
fn should_reject_stream_before_enqueue_when_blocking_handoff_is_full() {
    assert_rejected_before_enqueue_when_handoff_full("stream");
}

#[test]
fn should_reject_queue_before_enqueue_when_blocking_handoff_is_full() {
    assert_rejected_before_enqueue_when_handoff_full("queue");
}
