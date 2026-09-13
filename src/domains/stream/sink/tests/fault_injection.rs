use super::*;
use crate::benchkit::{
    build_stream_append, build_stream_begin, build_stream_commit, build_stream_subscribe,
    extract_single_tlv_field, parse_stream_session_id,
};
use crate::dispatch::protocol::frame::ChannelId;
use crate::dispatch::protocol::frame_context::FrameContext;
use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
use crate::runtime::Mailbox;

struct FaultHarness {
    sink: StreamDomain,
    destination: RouteAddress,
    family: RouteFamily,
}

impl FaultHarness {
    fn new(family: RouteFamily, route: &str, router: Arc<Router>) -> Self {
        let sink = StreamDomain::try_new(
            crate::benchkit::create_bench_store(),
            router,
            crate::control::admin::read_model::AdminReadModel::new(),
            StreamStorageWriteOptions::local(),
        )
        .expect("create Stream test sink");
        Self {
            sink,
            destination: RouteAddress::new(family, Route::new(route)),
            family,
        }
    }

    fn deliver_and_receive(
        &self,
        source: &RouteAddress,
        reply_mailbox: &Mailbox,
        session_id: u64,
        msg_type: u16,
        payload: Bytes,
        label: &str,
    ) -> Bytes {
        self.sink
            .deliver(Envelope::from_route(
                source.clone(),
                self.destination.clone(),
                FrameContext::new(
                    session_id,
                    ChannelId::Pub,
                    crate::dispatch::protocol::tlv::MessageType::new(msg_type),
                    payload,
                    self.family,
                ),
            ))
            .unwrap_or_else(|_| panic!("deliver {label}"));

        reply_mailbox
            .receiver()
            .recv_timeout(Duration::from_secs(1))
            .unwrap_or_else(|_| panic!("receive {label} reply"))
            .into_payload::<FrameContext>()
            .unwrap_or_else(|| panic!("{label} reply frame"))
            .payload
    }

    fn begin(&self, source: &RouteAddress, mailbox: &Mailbox, session_id: u64, route: &str) -> u64 {
        let (msg_type, payload) = extract_single_tlv_field(&build_stream_begin(route));
        let response =
            self.deliver_and_receive(source, mailbox, session_id, msg_type, payload, "begin");
        parse_stream_session_id(response.as_ref()).expect("stream session id")
    }

    fn append(
        &self,
        source: &RouteAddress,
        mailbox: &Mailbox,
        session_id: u64,
        stream_session_id: u64,
        expected_offset: u64,
        body: &'static [u8],
    ) {
        let (msg_type, payload) = extract_single_tlv_field(&build_stream_append(
            stream_session_id,
            expected_offset,
            body,
        ));
        let _ = self.deliver_and_receive(source, mailbox, session_id, msg_type, payload, "append");
    }

    fn commit(
        &self,
        source: &RouteAddress,
        mailbox: &Mailbox,
        session_id: u64,
        stream_session_id: u64,
    ) {
        let (msg_type, payload) =
            extract_single_tlv_field(&build_stream_commit(stream_session_id, 1));
        let _ = self.deliver_and_receive(source, mailbox, session_id, msg_type, payload, "commit");
    }
}

/// A subscriber whose mailbox is temporarily full when a commit notify is
/// routed must not be torn out of the subscription table: the notify is
/// best-effort per the Stream contract, and a single dropped notify must not
/// turn into a permanently lost subscription. Once the mailbox has room
/// again, the same subscription must still deliver the next commit.
#[test]
fn should_keep_subscription_alive_after_one_commit_notify_delivery_fails() {
    // Arrange
    let family = RouteFamily::new(1);
    let route = "stream://bench/events/notify-full";
    let owner_address = RouteAddress::new(family, Route::new("inbox://session/1"));
    let owner_mailbox = Arc::new(Mailbox::new(8));
    let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/2"));
    let subscriber_mailbox = Arc::new(Mailbox::new(1));
    let router = Arc::new(Router::new());
    router.register(owner_address.clone(), owner_mailbox.clone());
    router.register(subscriber_address.clone(), subscriber_mailbox.clone());
    let harness = FaultHarness::new(family, route, router);

    let (subscribe_type, subscribe_payload) =
        extract_single_tlv_field(&build_stream_subscribe(route));
    let _subscribe_ack = harness.deliver_and_receive(
        &subscriber_address,
        &subscriber_mailbox,
        2,
        subscribe_type,
        subscribe_payload,
        "subscribe",
    );
    assert_eq!(harness.sink.subscription_count(), 1);

    // Fill the subscriber's one mailbox slot so the upcoming commit notify
    // is rejected with MailboxFull.
    subscriber_mailbox
        .sender()
        .try_send(Envelope::new(subscriber_address.clone(), 0_u8))
        .expect("fill subscriber mailbox");

    let session_id = harness.begin(&owner_address, &owner_mailbox, 1, route);
    harness.append(&owner_address, &owner_mailbox, 1, session_id, 0, b"first");

    // Act
    harness.commit(&owner_address, &owner_mailbox, 1, session_id);
    let subscription_count_after_dropped_notify = harness.sink.subscription_count();

    // Drain the stale dummy frame and prove the subscription still works.
    let _ = subscriber_mailbox.receiver().try_recv();
    let second_session_id = harness.begin(&owner_address, &owner_mailbox, 1, route);
    harness.append(
        &owner_address,
        &owner_mailbox,
        1,
        second_session_id,
        1,
        b"second",
    );
    harness.commit(&owner_address, &owner_mailbox, 1, second_session_id);
    let second_notify = subscriber_mailbox
        .receiver()
        .recv_timeout(Duration::from_secs(1))
        .expect("subscription must still be live for the second commit");

    // Assert
    assert_eq!(
        subscription_count_after_dropped_notify, 1,
        "a dropped notify must not remove the subscription"
    );
    let notify_frame = second_notify
        .into_payload::<FrameContext>()
        .expect("second notify frame");
    assert_eq!(notify_frame.msg_type.as_u16(), 609);
}

/// `BEGIN` rolls the append session back when its response cannot be
/// delivered (see `should_not_retain_append_session_when_begin_response_cannot_be_delivered`).
/// This asserts what `APPEND` actually does in the same situation: the
/// append is a mutate-then-send with no matching rollback path, so a lost
/// append ack leaves the event staged and the session alive with an
/// offset the caller was never told about.
#[test]
fn should_retain_staged_event_and_active_session_when_append_response_cannot_be_delivered() {
    // Arrange
    let family = RouteFamily::new(1);
    let route = "stream://bench/events/append-undeliverable";
    let owner_address = RouteAddress::new(family, Route::new("inbox://session/1"));
    let owner_mailbox = Arc::new(Mailbox::new(1));
    let router = Arc::new(Router::new());
    router.register(owner_address.clone(), owner_mailbox.clone());
    let harness = FaultHarness::new(family, route, router);

    let session_id = harness.begin(&owner_address, &owner_mailbox, 1, route);

    // Fill the owner's one mailbox slot so the append ack cannot be delivered.
    owner_mailbox
        .sender()
        .try_send(Envelope::new(owner_address.clone(), 0_u8))
        .expect("fill owner mailbox");

    // Act
    let (append_type, append_payload) =
        extract_single_tlv_field(&build_stream_append(session_id, 0, b"orphaned-ack"));
    harness
        .sink
        .deliver(Envelope::from_route(
            owner_address.clone(),
            harness.destination.clone(),
            FrameContext::new(
                1,
                ChannelId::Pub,
                crate::dispatch::protocol::tlv::MessageType::new(append_type),
                append_payload,
                family,
            ),
        ))
        .expect("deliver append despite undeliverable response");
    // Drain the dummy frame so the follow-up commit's own response can land.
    let _ = owner_mailbox.receiver().try_recv();

    harness.commit(&owner_address, &owner_mailbox, 1, session_id);
    let committed = harness
        .sink
        .read_area_records_for_tests(family, "bench", "events", 0, 10)
        .unwrap_or_default();

    // Assert
    // The staged event survived the lost ack and was committed -
    // documenting that APPEND, unlike BEGIN, has no undeliverable-response
    // rollback path.
    assert_eq!(committed.len(), 1);
    match &committed[0] {
        crate::domains::stream::StreamReadItem::Event(record) => {
            assert_eq!(record.body, Bytes::from_static(b"orphaned-ack"));
        }
        other => panic!("expected a committed event, got {other:?}"),
    }
}
