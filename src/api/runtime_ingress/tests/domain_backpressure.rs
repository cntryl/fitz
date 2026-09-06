use super::*;
use crate::api::runtime_ingress::domain_frame_dispatcher::DomainFrameDispatcher;

struct AlwaysBackpressuredSink;

impl MailboxSink for AlwaysBackpressuredSink {
    fn deliver(&self, _envelope: Envelope) -> Result<(), DeliveryError> {
        Err(DeliveryError::MailboxFull {
            capacity: 1,
            current_len: 1,
        })
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

struct AlwaysHighLaneBackpressuredSink;

impl MailboxSink for AlwaysHighLaneBackpressuredSink {
    fn deliver(&self, _envelope: Envelope) -> Result<(), DeliveryError> {
        Err(DeliveryError::HighLaneFull {
            capacity: 1,
            current_len: 1,
        })
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

struct TransientBackpressuredSink {
    failures_remaining: AtomicUsize,
    accepted: AtomicUsize,
}

impl TransientBackpressuredSink {
    fn new(failures: usize) -> Self {
        Self {
            failures_remaining: AtomicUsize::new(failures),
            accepted: AtomicUsize::new(0),
        }
    }
}

impl MailboxSink for TransientBackpressuredSink {
    fn deliver(&self, _envelope: Envelope) -> Result<(), DeliveryError> {
        let previous = self
            .failures_remaining
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .unwrap_or(0);

        if previous > 0 {
            return Err(DeliveryError::MailboxFull {
                capacity: 1,
                current_len: 1,
            });
        }

        self.accepted.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

struct DomainIngressCase {
    domain: &'static str,
    channel_id: ChannelId,
    msg_type: u16,
    payload: Bytes,
}

fn domain_ingress_cases() -> Vec<DomainIngressCase> {
    [
        (
            "kv",
            ChannelId::Pub,
            crate::benchkit::build_kv_begin("kv://test/app/users", 1, 0),
        ),
        (
            "queue",
            ChannelId::Pub,
            crate::benchkit::build_queue_enqueue("queue://test/app/jobs", b"job"),
        ),
        (
            "notice",
            ChannelId::Pub,
            crate::benchkit::build_notice_publish("notice://test/events", b"event"),
        ),
        (
            "stream",
            ChannelId::Pub,
            crate::benchkit::build_stream_begin("stream://test/logs/events/append"),
        ),
        (
            "rpc",
            ChannelId::Rpc,
            crate::benchkit::build_rpc_request("rpc://test/tasks/worker", b"run"),
        ),
        (
            "lease",
            ChannelId::Lease,
            crate::benchkit::build_lease_acquire_immediate(
                "lease://test/locks/resource",
                "owner",
                30,
            ),
        ),
        (
            "schedule",
            ChannelId::Pub,
            crate::benchkit::build_schedule_create(
                "schedule://test/jobs/hourly/run",
                "0 * * * *",
                b"task",
            ),
        ),
    ]
    .into_iter()
    .map(|(domain, channel_id, frame)| {
        let (msg_type, payload) = crate::benchkit::extract_single_tlv_field(&frame);
        DomainIngressCase {
            domain,
            channel_id,
            msg_type,
            payload,
        }
    })
    .collect()
}

#[test]
fn should_absorb_transient_domain_mailbox_backpressure_for_each_domain() {
    // Arrange
    let rt = tokio::runtime::Runtime::new().unwrap();

    for (index, case) in domain_ingress_cases().into_iter().enumerate() {
        let router = Arc::new(crate::runtime::Router::new());
        let sink = Arc::new(TransientBackpressuredSink::new(1));
        router.register_domain_pattern(case.domain, sink.clone());
        let ingress = RuntimeIngress::new(false).with_router(router);
        let session_id = 1_000 + u64::try_from(index).unwrap();
        let session = make_session_info(session_id, TransportKind::Tcp);

        // Act
        let decision = rt.block_on(async {
            ingress.on_open(session).await.unwrap();
            ingress
                .on_frame(
                    session_id,
                    case.channel_id,
                    crate::protocol::tlv::MessageType::new(case.msg_type),
                    case.payload,
                )
                .await
        });

        // Assert
        assert_eq!(
            decision,
            IngressDecision::Accept,
            "transient backpressure should be absorbed for {}",
            case.domain
        );
        assert_eq!(
            sink.accepted.load(Ordering::SeqCst),
            1,
            "domain frame should be accepted after retry for {}",
            case.domain
        );
    }
}

struct CapturingInboxSink {
    frames: Arc<Mutex<Vec<FrameContext>>>,
}

impl MailboxSink for CapturingInboxSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        let frame = envelope
            .payload::<FrameContext>()
            .expect("client frame payload")
            .clone();
        self.frames.lock().unwrap().push(frame);
        Ok(())
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

/// `REQ-PROTO-012`'s retryable set. A timeout must never answer with one of
/// these: they tell a compliant client the request was never accepted. A
/// backpressure rejection must always answer with one, for the same reason.
const DOCUMENTED_RETRYABLE_CODES: [u32; 12] = [
    1004, 1014, 2014, 3006, 4005, 5001, 5007, 6001, 6002, 6003, 6004, 7010,
];

struct AlwaysTimingOutSink;

impl MailboxSink for AlwaysTimingOutSink {
    fn deliver(&self, _envelope: Envelope) -> Result<(), DeliveryError> {
        Err(DeliveryError::Timeout)
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

struct QueueReplyThenTimeoutSink {
    router: Arc<crate::runtime::Router>,
    session_id: u64,
}

impl MailboxSink for QueueReplyThenTimeoutSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        assert!(envelope.try_claim_reply(), "queue response claim");
        let response = envelope
            .try_reply_to(FrameContext::new(
                self.session_id,
                ChannelId::Pub,
                crate::protocol::tlv::MessageType::new(200),
                Bytes::from_static(&[0]),
                RouteFamily::new(1),
            ))
            .expect("queue response envelope");
        self.router.route(response).expect("route queue response");
        Err(DeliveryError::Timeout)
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

#[test]
fn should_not_emit_a_second_queue_terminal_response_after_the_domain_replied() {
    // Arrange
    // A Queue command can finish at the same instant its mailbox reply wait
    // expires. Its response and ingress' indeterminate timeout compete for one
    // terminal-response slot; emitting both shifts the client's per-type FIFO
    // and can make a later accepted enqueue look retryably rejected.
    let rt = tokio::runtime::Runtime::new().unwrap();
    let router = Arc::new(crate::runtime::Router::new());
    let session_id = 6_500;
    let client_frames = Arc::new(Mutex::new(Vec::<FrameContext>::new()));
    router.register(
        crate::runtime::routing::RouteAddress::new(
            RouteFamily::new(1),
            crate::runtime::routing::Route::new(format!("inbox://session/{session_id}")),
        ),
        Arc::new(CapturingInboxSink {
            frames: client_frames.clone(),
        }) as Arc<dyn MailboxSink>,
    );
    router.register_domain_pattern(
        "queue",
        Arc::new(QueueReplyThenTimeoutSink {
            router: router.clone(),
            session_id,
        }),
    );
    let ingress = RuntimeIngress::new(false).with_router(router);
    let (_, payload) = crate::benchkit::extract_single_tlv_field(
        &crate::benchkit::build_queue_enqueue("queue://test/app/jobs", b"job"),
    );

    // Act
    let decision = rt.block_on(async {
        ingress
            .on_open(make_session_info(session_id, TransportKind::Tcp))
            .await
            .unwrap();
        ingress
            .on_frame(
                session_id,
                ChannelId::Pub,
                crate::protocol::tlv::MessageType::new(200),
                payload,
            )
            .await
    });

    // Assert
    assert_eq!(decision, IngressDecision::Accept);
    let frames = client_frames.lock().unwrap();
    assert_eq!(
        frames.len(),
        1,
        "one request must produce exactly one terminal response"
    );
    assert_eq!(frames[0].payload.as_ref(), &[0]);
}

#[test]
fn should_surface_sustained_high_lane_domain_mailbox_backpressure_for_each_domain() {
    // Arrange
    let rt = tokio::runtime::Runtime::new().unwrap();

    for (index, case) in domain_ingress_cases().into_iter().enumerate() {
        let router = Arc::new(crate::runtime::Router::new());
        router.register_domain_pattern(case.domain, Arc::new(AlwaysHighLaneBackpressuredSink));
        let session_id = 3_000 + u64::try_from(index).unwrap();
        router.register(
            crate::runtime::routing::RouteAddress::new(
                RouteFamily::new(1),
                crate::runtime::routing::Route::new(format!("inbox://session/{session_id}")),
            ),
            Arc::new(CapturingInboxSink {
                frames: Arc::new(Mutex::new(Vec::new())),
            }) as Arc<dyn MailboxSink>,
        );
        let ingress = RuntimeIngress::new(false).with_router(router);
        let session = make_session_info(session_id, TransportKind::Tcp);

        // Act
        let decision = rt.block_on(async {
            ingress.on_open(session).await.unwrap();
            ingress
                .on_frame(
                    session_id,
                    case.channel_id,
                    crate::protocol::tlv::MessageType::new(case.msg_type),
                    case.payload,
                )
                .await
        });

        // Assert
        assert_eq!(
            decision,
            IngressDecision::Accept,
            "sustained high-lane backpressure should reject the frame, not the session, for {}",
            case.domain
        );
    }
}

#[test]
fn should_not_close_session_when_a_domain_command_times_out() {
    // Arrange
    // A domain actor that is merely slow must not cost the client its whole
    // connection. The WebSocket is multiplexed, so closing it destroys every
    // other domain's in-flight work on that session too - which is how one
    // saturated Queue took down unrelated KV, Stream and Schedule traffic.
    //
    // The frame is answered with an indeterminate-outcome code, never a
    // "queue full"/backpressure code: the command was already enqueued and may
    // still execute, so telling the client it was rejected would invite a
    // duplicate.
    let rt = tokio::runtime::Runtime::new().unwrap();

    for (index, case) in domain_ingress_cases().into_iter().enumerate() {
        let router = Arc::new(crate::runtime::Router::new());
        router.register_domain_pattern(case.domain, Arc::new(AlwaysTimingOutSink));
        let session_id = 7_000 + u64::try_from(index).unwrap();
        // A real session has an inbox; the indeterminate error frame is written to
        // it instead of the session being torn down.
        let client_frames = Arc::new(Mutex::new(Vec::<FrameContext>::new()));
        router.register(
            crate::runtime::routing::RouteAddress::new(
                RouteFamily::new(1),
                crate::runtime::routing::Route::new(format!("inbox://session/{session_id}")),
            ),
            Arc::new(CapturingInboxSink {
                frames: client_frames.clone(),
            }) as Arc<dyn MailboxSink>,
        );
        let ingress = RuntimeIngress::new(false).with_router(router);
        let session = make_session_info(session_id, TransportKind::Tcp);

        // Act
        let decision = rt.block_on(async {
            ingress.on_open(session).await.unwrap();
            ingress
                .on_frame(
                    session_id,
                    case.channel_id,
                    crate::protocol::tlv::MessageType::new(case.msg_type),
                    case.payload,
                )
                .await
        });

        // Assert
        assert!(
            !matches!(decision, IngressDecision::Close(_)),
            "a slow {} actor must not close the session, got {decision:?}",
            case.domain
        );
        let frames = client_frames.lock().unwrap();
        assert_eq!(
            frames.len(),
            1,
            "{} should answer the one frame with an error, got {frames:?}",
            case.domain
        );
        // Error body: [u8 flag][u32 code][string message].
        let body = &frames[0].payload;
        assert_eq!(
            body[0],
            if case.domain == "stream" { 2 } else { 1 },
            "{} should send an error body",
            case.domain
        );
        let code = u32::from_be_bytes([body[1], body[2], body[3], body[4]]);
        // A timed-out command was already enqueued and may still run, so the
        // code must not be one `REQ-PROTO-012` classifies as retryable. Those
        // tell a compliant SDK the request was never accepted, and its
        // `IsRetryable` helper (REQ-ERR-006) erases any prose caveat - so the
        // client re-sends and duplicates the side effect. Only queue ACK is
        // deduplicated.
        assert!(
            !DOCUMENTED_RETRYABLE_CODES.contains(&code),
            "{} answered a timeout with retryable code {code}; a compliant client \
             would re-send a command that may already have applied",
            case.domain
        );
    }
}

#[test]
fn should_log_domain_dispatch_timeout_as_indeterminate() {
    // Arrange

    // Act
    let outcome = DomainFrameDispatcher::dispatch_timeout_outcome();

    // Assert
    assert_eq!(outcome, "indeterminate");
}

#[test]
fn should_answer_sustained_mailbox_backpressure_without_killing_the_session() {
    // Arrange
    // A full mailbox means the command was never enqueued, which is the one
    // failure a client can safely retry. Closing the connection throws that
    // information away: the caller cannot tell a rejected request from one that
    // may have applied, so it must stop rather than risk a duplicate. The
    // 2ms retry budget is far shorter than a saturated actor takes to drain,
    // so this is reached under ordinary load.
    let rt = tokio::runtime::Runtime::new().unwrap();

    for (index, case) in domain_ingress_cases().into_iter().enumerate() {
        let router = Arc::new(crate::runtime::Router::new());
        router.register_domain_pattern(case.domain, Arc::new(AlwaysBackpressuredSink));
        let session_id = 8_000 + u64::try_from(index).unwrap();
        let client_frames = Arc::new(Mutex::new(Vec::<FrameContext>::new()));
        router.register(
            crate::runtime::routing::RouteAddress::new(
                RouteFamily::new(1),
                crate::runtime::routing::Route::new(format!("inbox://session/{session_id}")),
            ),
            Arc::new(CapturingInboxSink {
                frames: client_frames.clone(),
            }) as Arc<dyn MailboxSink>,
        );
        let ingress = RuntimeIngress::new(false).with_router(router);
        let session = make_session_info(session_id, TransportKind::Tcp);

        // Act
        let decision = rt.block_on(async {
            ingress.on_open(session).await.unwrap();
            ingress
                .on_frame(
                    session_id,
                    case.channel_id,
                    crate::protocol::tlv::MessageType::new(case.msg_type),
                    case.payload,
                )
                .await
        });

        // Assert
        // `Backpressure` is not good enough: the transport turns it into a
        // close (see `should_treat_websocket_backpressure_as_terminal_session_error`).
        assert_eq!(
            decision,
            IngressDecision::Accept,
            "{} should answer the frame and keep the session",
            case.domain
        );
        let frames = client_frames.lock().unwrap();
        assert_eq!(
            frames.len(),
            1,
            "{} should answer with a rejection frame, got {frames:?}",
            case.domain
        );
        // The mirror of the timeout guard. Nothing was enqueued, and the
        // message tells the client to retry with backoff - so the code must be
        // one `REQ-PROTO-012` classifies as retryable. A fatal code here makes
        // a compliant client give up on a request it could safely re-send.
        let body = &frames[0].payload;
        assert_eq!(
            body[0],
            if case.domain == "stream" { 2 } else { 1 },
            "{} should send an error body",
            case.domain
        );
        let code = u32::from_be_bytes([body[1], body[2], body[3], body[4]]);
        assert!(
            DOCUMENTED_RETRYABLE_CODES.contains(&code),
            "{} rejected a never-enqueued request with fatal code {code}; a compliant \
             client will not retry",
            case.domain
        );
    }
}

struct FixedErrorSink(DeliveryError);

impl MailboxSink for FixedErrorSink {
    fn deliver(&self, _envelope: Envelope) -> Result<(), DeliveryError> {
        Err(self.0.clone())
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

#[test]
fn should_answer_terminal_delivery_failures_without_killing_the_session() {
    // Arrange
    // None of these is a client protocol violation, so none of them justifies
    // destroying a multiplexed session: a dead queue actor must not take
    // unrelated KV/Stream/RPC work with it, and an unframable response is a
    // server-side bug the client should not pay for with its connection.
    let rt = tokio::runtime::Runtime::new().unwrap();
    let failures = [
        DeliveryError::ActorStopped,
        DeliveryError::SinkPanicked,
        DeliveryError::InvalidPayload {
            len: 70_000,
            max: 65_535,
        },
    ];

    for (index, failure) in failures.into_iter().enumerate() {
        let case = domain_ingress_cases()
            .into_iter()
            .next()
            .expect("at least one domain case");
        let router = Arc::new(crate::runtime::Router::new());
        router.register_domain_pattern(case.domain, Arc::new(FixedErrorSink(failure.clone())));
        let session_id = 9_500 + u64::try_from(index).unwrap();
        let client_frames = Arc::new(Mutex::new(Vec::<FrameContext>::new()));
        router.register(
            crate::runtime::routing::RouteAddress::new(
                RouteFamily::new(1),
                crate::runtime::routing::Route::new(format!("inbox://session/{session_id}")),
            ),
            Arc::new(CapturingInboxSink {
                frames: client_frames.clone(),
            }) as Arc<dyn MailboxSink>,
        );
        let ingress = RuntimeIngress::new(false).with_router(router);
        let session = make_session_info(session_id, TransportKind::Tcp);

        // Act
        let decision = rt.block_on(async {
            ingress.on_open(session).await.unwrap();
            ingress
                .on_frame(
                    session_id,
                    case.channel_id,
                    crate::protocol::tlv::MessageType::new(case.msg_type),
                    case.payload,
                )
                .await
        });

        // Assert
        assert_eq!(
            decision,
            IngressDecision::Accept,
            "{failure:?} should be answered on the channel, not close the session"
        );
        assert_eq!(
            client_frames.lock().unwrap().len(),
            1,
            "{failure:?} should produce one error frame"
        );
    }
}

#[test]
fn should_answer_unroutable_domain_frame_without_killing_the_session() {
    // Arrange
    // No sink is registered for the domain, so the router cannot find a route.
    // That is a permanent condition for this request but says nothing about the
    // session's other channels.
    let rt = tokio::runtime::Runtime::new().unwrap();
    let case = domain_ingress_cases()
        .into_iter()
        .next()
        .expect("at least one domain case");
    let router = Arc::new(crate::runtime::Router::new());
    let session_id = 9_600;
    let client_frames = Arc::new(Mutex::new(Vec::<FrameContext>::new()));
    router.register(
        crate::runtime::routing::RouteAddress::new(
            RouteFamily::new(1),
            crate::runtime::routing::Route::new(format!("inbox://session/{session_id}")),
        ),
        Arc::new(CapturingInboxSink {
            frames: client_frames.clone(),
        }) as Arc<dyn MailboxSink>,
    );
    let ingress = RuntimeIngress::new(false).with_router(router);
    let session = make_session_info(session_id, TransportKind::Tcp);

    // Act
    let decision = rt.block_on(async {
        ingress.on_open(session).await.unwrap();
        ingress
            .on_frame(
                session_id,
                case.channel_id,
                crate::protocol::tlv::MessageType::new(case.msg_type),
                case.payload,
            )
            .await
    });

    // Assert
    assert_eq!(decision, IngressDecision::Accept);
    assert_eq!(client_frames.lock().unwrap().len(), 1);
}
