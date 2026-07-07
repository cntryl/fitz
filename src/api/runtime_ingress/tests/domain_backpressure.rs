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
