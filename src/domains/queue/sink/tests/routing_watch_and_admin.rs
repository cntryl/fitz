use super::*;
pub(super) use crate::protocol::frame::ChannelId;
pub(super) use crate::protocol::tlv::MessageType;
pub(super) use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
pub(super) use crate::runtime::Mailbox;
pub(super) use bytes::{BufMut, Bytes};

fn len_to_u32(len: usize) -> u32 {
    u32::try_from(len).expect("payload length should fit in u32")
}

pub(super) fn encode_route_pattern(pattern: &str) -> Bytes {
    let mut payload = Vec::new();
    payload.put_u32(len_to_u32(pattern.len()));
    payload.put_slice(pattern.as_bytes());
    Bytes::from(payload)
}

pub(super) fn encode_queue_send(route: &str, body: &[u8]) -> Bytes {
    let mut payload = Vec::new();
    payload.put_u32(len_to_u32(route.len()));
    payload.put_slice(route.as_bytes());
    payload.put_u32(len_to_u32(body.len()));
    payload.put_slice(body);
    Bytes::from(payload)
}

pub(super) fn encode_queue_send_with_delay(route: &str, body: &[u8], delay_seconds: u64) -> Bytes {
    let mut payload = Vec::new();
    payload.put_u32(len_to_u32(route.len()));
    payload.put_slice(route.as_bytes());
    payload.put_u32(len_to_u32(body.len()));
    payload.put_slice(body);
    payload.put_u8(1);
    payload.put_u64(delay_seconds);
    Bytes::from(payload)
}

pub(super) fn encode_queue_reserve(route: &str, inflight_seconds: u64, batch_size: u32) -> Bytes {
    let mut payload = Vec::new();
    payload.put_u32(len_to_u32(route.len()));
    payload.put_slice(route.as_bytes());
    payload.put_u64(inflight_seconds);
    payload.put_u8(1);
    payload.put_u32(batch_size);
    Bytes::from(payload)
}

pub(super) fn encode_queue_watch(pattern: &str) -> Bytes {
    let mut payload = Vec::new();
    payload.put_u32(len_to_u32(pattern.len()));
    payload.put_slice(pattern.as_bytes());
    Bytes::from(payload)
}

pub(super) fn encode_queue_unwatch(pattern: &str) -> Bytes {
    let mut payload = Vec::new();
    payload.put_u32(len_to_u32(pattern.len()));
    payload.put_slice(pattern.as_bytes());
    Bytes::from(payload)
}

pub(super) fn encode_queue_extend(
    route: &str,
    id: u64,
    token: u64,
    inflight_seconds: u64,
) -> Bytes {
    let mut payload = Vec::new();
    payload.put_u32(len_to_u32(route.len()));
    payload.put_slice(route.as_bytes());
    payload.put_u64(id);
    payload.put_u64(token);
    payload.put_u64(inflight_seconds);
    Bytes::from(payload)
}

pub(super) fn encode_queue_ack(route: &str, id: u64, token: u64) -> Bytes {
    let mut payload = Vec::new();
    payload.put_u32(len_to_u32(route.len()));
    payload.put_slice(route.as_bytes());
    payload.put_u64(id);
    payload.put_u64(token);
    Bytes::from(payload)
}

pub(super) fn bad_request_reason(frame: &FrameContext) -> String {
    let (code, message) = crate::protocol::error_codes::decode_error_body(frame.payload.as_ref())
        .expect("bad request error envelope");
    assert_eq!(code, crate::protocol::error_codes::queue::ERR_BAD_REQUEST);
    message
}

pub(super) fn new_queue_domain_sink(
    store: Arc<cntryl_midge::Engine>,
    router: Arc<Router>,
    admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
    queue_write_options: cntryl_midge::WriteOptions,
) -> QueueDomainSink {
    QueueDomainSink::new(
        store,
        router,
        admin_read_model,
        queue_write_options,
        crate::utils::idempotency::default_dedup_store(),
    )
}

pub(super) fn receive_response_message_count(frame: &FrameContext) -> u32 {
    assert_eq!(frame.payload[0], 0, "expected success status");
    u32::from_be_bytes(
        frame.payload[1..5]
            .try_into()
            .expect("receive payload should include count"),
    )
}

pub(super) fn receive_response_first_message(frame: &FrameContext) -> (u64, u64) {
    assert_eq!(receive_response_message_count(frame), 1);
    let id = u64::from_be_bytes(
        frame.payload[5..13]
            .try_into()
            .expect("receive payload should include message id"),
    );
    let token = u64::from_be_bytes(
        frame.payload[13..21]
            .try_into()
            .expect("receive payload should include token"),
    );
    (id, token)
}

pub(super) fn queue_simple_error_code(frame: &FrameContext) -> u16 {
    let (code, _) = crate::protocol::error_codes::decode_error_body(frame.payload.as_ref())
        .expect("queue error envelope");
    code
}

pub(super) fn receive_queue_frame(mailbox: &Mailbox, label: &str) -> FrameContext {
    mailbox
        .receiver()
        .try_recv()
        .expect(label)
        .into_payload::<FrameContext>()
        .expect("queue response frame")
}

pub(super) fn watch_response_subscription_id(frame: &FrameContext) -> u64 {
    assert_eq!(frame.payload[0], 0, "expected success status");
    u64::from_be_bytes(
        frame.payload[1..9]
            .try_into()
            .expect("watch payload should include subscription id"),
    )
}

pub(super) fn decode_queue_watch_delivery(frame: &FrameContext) -> (u64, String, u64) {
    let (subscription_id, route, ready_messages, _, _) = decode_queue_watch_delivery_counts(frame);
    (subscription_id, route, ready_messages)
}

pub(super) fn decode_queue_watch_delivery_counts(
    frame: &FrameContext,
) -> (u64, String, u64, u64, u64) {
    let subscription_id = u64::from_be_bytes(frame.payload[0..8].try_into().unwrap());
    let route_len = usize::try_from(u32::from_be_bytes(frame.payload[8..12].try_into().unwrap()))
        .expect("route length should fit in usize");
    let route = String::from_utf8(frame.payload[12..12 + route_len].to_vec())
        .expect("queue watch route should be utf-8");
    let offset = 12 + route_len;
    let ready_messages = u64::from_be_bytes(frame.payload[offset..offset + 8].try_into().unwrap());
    let delayed_messages =
        u64::from_be_bytes(frame.payload[offset + 8..offset + 16].try_into().unwrap());
    let inflight_messages =
        u64::from_be_bytes(frame.payload[offset + 16..offset + 24].try_into().unwrap());
    (
        subscription_id,
        route,
        ready_messages,
        delayed_messages,
        inflight_messages,
    )
}

struct QueueWatchHarness {
    family: RouteFamily,
    queue_address: RouteAddress,
    watcher_address: RouteAddress,
    sender_address: RouteAddress,
    worker_address: RouteAddress,
    watcher_mailbox: Arc<Mailbox>,
    sender_mailbox: Arc<Mailbox>,
    worker_mailbox: Arc<Mailbox>,
    sink: QueueDomainSink,
}

impl QueueWatchHarness {
    fn new() -> Self {
        let family = RouteFamily::new(1);
        let queue_address = RouteAddress::new(family, Route::new("queue://inbound"));
        let watcher_address = RouteAddress::new(family, Route::new("inbox://session/1"));
        let sender_address = RouteAddress::new(family, Route::new("inbox://session/2"));
        let worker_address = RouteAddress::new(family, Route::new("inbox://session/3"));
        let watcher_mailbox = Arc::new(Mailbox::new(16));
        let sender_mailbox = Arc::new(Mailbox::new(16));
        let worker_mailbox = Arc::new(Mailbox::new(16));
        let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
        let router = Arc::new(Router::new());
        router.register(watcher_address.clone(), watcher_mailbox.clone());
        router.register(sender_address.clone(), sender_mailbox.clone());
        router.register(worker_address.clone(), worker_mailbox.clone());
        let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
        let sink = new_queue_domain_sink(
            store,
            router,
            admin_read_model,
            cntryl_midge::WriteOptions::best_effort(),
        );

        Self {
            family,
            queue_address,
            watcher_address,
            sender_address,
            worker_address,
            watcher_mailbox,
            sender_mailbox,
            worker_mailbox,
            sink,
        }
    }

    fn watch(&self, pattern: &str) -> u64 {
        self.sink
            .deliver(Envelope::from_route(
                self.watcher_address.clone(),
                self.queue_address.clone(),
                FrameContext::new(
                    1,
                    ChannelId::Pub,
                    MessageType::new(207),
                    encode_queue_watch(pattern),
                    self.family,
                ),
            ))
            .expect("watch queue readiness");
        let frame = receive_queue_frame(&self.watcher_mailbox, "watch response");
        watch_response_subscription_id(&frame)
    }

    fn send(&self, route: &str, body: &[u8]) {
        self.sink
            .deliver(Envelope::from_route(
                self.sender_address.clone(),
                self.queue_address.clone(),
                FrameContext::new(
                    2,
                    ChannelId::Pub,
                    MessageType::new(200),
                    encode_queue_send(route, body),
                    self.family,
                ),
            ))
            .expect("enqueue watched queue message");
        let response = receive_queue_frame(&self.sender_mailbox, "send response");
        assert_eq!(response.msg_type.as_u16(), 200);
    }

    fn send_delayed(&self, route: &str, body: &[u8], delay_seconds: u64) {
        self.sink
            .deliver(Envelope::from_route(
                self.sender_address.clone(),
                self.queue_address.clone(),
                FrameContext::new(
                    2,
                    ChannelId::Pub,
                    MessageType::new(200),
                    encode_queue_send_with_delay(route, body, delay_seconds),
                    self.family,
                ),
            ))
            .expect("enqueue delayed watched queue message");
        let response = receive_queue_frame(&self.sender_mailbox, "delayed send response");
        assert_eq!(response.msg_type.as_u16(), 200);
    }

    fn reserve_one(&self, route: &str) -> (u64, u64) {
        self.sink
            .deliver(Envelope::from_route(
                self.worker_address.clone(),
                self.queue_address.clone(),
                FrameContext::new(
                    3,
                    ChannelId::Pub,
                    MessageType::new(202),
                    encode_queue_reserve(route, 30, 1),
                    self.family,
                ),
            ))
            .expect("reserve watched queue message");
        let response = receive_queue_frame(&self.worker_mailbox, "reserve response");
        assert_eq!(response.msg_type.as_u16(), 202);
        receive_response_first_message(&response)
    }

    fn complete(&self, route: &str, id: u64, token: u64) {
        self.sink
            .deliver(Envelope::from_route(
                self.worker_address.clone(),
                self.queue_address.clone(),
                FrameContext::new(
                    3,
                    ChannelId::Pub,
                    MessageType::new(204),
                    encode_queue_ack(route, id, token),
                    self.family,
                ),
            ))
            .expect("complete watched queue message");
        let response = receive_queue_frame(&self.worker_mailbox, "complete response");
        assert_eq!(response.msg_type.as_u16(), 204);
    }

    fn next_watch_notification(&self) -> (u64, String, u64, u64, u64) {
        let notify = receive_queue_frame(&self.watcher_mailbox, "queue watch notify");
        assert_eq!(notify.msg_type.as_u16(), 209);
        decode_queue_watch_delivery_counts(&notify)
    }

    fn assert_no_watch_notification(&self) {
        assert!(self.watcher_mailbox.receiver().try_recv().is_err());
    }
}

pub(super) fn force_actor_idle(sink: &QueueDomainSink, queue_route: &str, family: RouteFamily) {
    sink.force_actor_idle_for_tests(family, queue_route);
}

mod admin;
mod routing;
