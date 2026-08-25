use super::*;

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

#[test]
fn should_surface_sustained_high_lane_domain_mailbox_backpressure_for_each_domain() {
    // Arrange
    let rt = tokio::runtime::Runtime::new().unwrap();

    for (index, case) in domain_ingress_cases().into_iter().enumerate() {
        let router = Arc::new(crate::runtime::Router::new());
        router.register_domain_pattern(case.domain, Arc::new(AlwaysHighLaneBackpressuredSink));
        let ingress = RuntimeIngress::new(false).with_router(router);
        let session_id = 3_000 + u64::try_from(index).unwrap();
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
            IngressDecision::Backpressure,
            "sustained high-lane backpressure should remain visible for {}",
            case.domain
        );
    }
}

#[test]
fn should_surface_sustained_domain_mailbox_backpressure_for_each_domain() {
    // Arrange
    let rt = tokio::runtime::Runtime::new().unwrap();

    for (index, case) in domain_ingress_cases().into_iter().enumerate() {
        let router = Arc::new(crate::runtime::Router::new());
        router.register_domain_pattern(case.domain, Arc::new(AlwaysBackpressuredSink));
        let ingress = RuntimeIngress::new(false).with_router(router);
        let session_id = 2_000 + u64::try_from(index).unwrap();
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
            IngressDecision::Backpressure,
            "sustained backpressure should remain visible for {}",
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

struct AlwaysTimingOutSink;

impl MailboxSink for AlwaysTimingOutSink {
    fn deliver(&self, _envelope: Envelope) -> Result<(), DeliveryError> {
        Err(DeliveryError::Timeout)
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
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
        // A real session has an inbox; the retryable error frame is written to
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
        assert_eq!(body[0], 1, "{} should send an error body", case.domain);
        let code = u32::from_be_bytes([body[1], body[2], body[3], body[4]]);
        // A timed-out command was already enqueued and may still run. Reporting
        // a backpressure/"full" code would tell the client it was rejected and
        // invite a retry that duplicates the side effect; only queue ACK is
        // deduplicated.
        let retryable_rejection_codes = [
            u32::from(crate::protocol::error_codes::queue::ERR_QUEUE_FULL),
            u32::from(crate::protocol::error_codes::rpc::ERR_RPC_BACKPRESSURE),
            u32::from(crate::protocol::error_codes::lease::ERR_QUEUE_FULL),
        ];
        assert!(
            !retryable_rejection_codes.contains(&code),
            "{} answered a timeout with rejection code {code}, which invites a duplicate",
            case.domain
        );
    }
}
