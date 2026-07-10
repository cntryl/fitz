#![allow(dead_code)] // Standalone Notice targets use focused subsets of this fixture API.

use bytes::Bytes;
use fitz::benchkit::{
    create_bench_notice_sink, parse_notice_delivery, register_session_queue_sink,
    route_frame_to_address, shared_bench_runtime, FrameQueueSink,
};
use fitz::domains::notice::protocol::{
    NoticeClientRequest, NotificationMessage, PublishMessage, SubscribeMessage, UnsubscribeMessage,
};
use fitz::domains::notice::sink::NoticeDomainSink;
use fitz::protocol::frame::ChannelId;
use fitz::protocol::frame_context::FrameContext;
use fitz::protocol::payload_codec::PayloadDecoder;
use fitz::runtime::envelope::Envelope;
use fitz::runtime::router::{MailboxSink, Router};
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use fitz::runtime::{ClientChannel, ClientFrameMeta};
use fitz::testkit::transport::TlvFrameParser;
use fitz::testkit::{TestClient, TestServer, TestWebSocketClient};
use futures_util::future::join_all;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub(crate) const NOTICE_RESPONSE_TIMEOUT_MS: u64 = 5_000;
pub(crate) const NOTICE_RESPONSE_TIMEOUT: Duration = Duration::from_secs(5);
pub(crate) const PUBLISHER_SESSION_ID: u64 = 10_000;

pub(crate) enum NoticeBenchClient {
    Tcp(TestClient),
    WebSocket(Box<TestWebSocketClient>),
}

impl NoticeBenchClient {
    async fn connect(
        server: &TestServer,
        transport: crate::tier4_support::TransportKind,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        match transport {
            crate::tier4_support::TransportKind::Tcp => {
                TestClient::new(server.tcp_addr).await.map(Self::Tcp)
            }
            crate::tier4_support::TransportKind::WebSocket => {
                TestWebSocketClient::connect(&format!("ws://{}", server.ws_addr))
                    .await
                    .map(|client| Self::WebSocket(Box::new(client)))
            }
        }
    }

    pub(crate) async fn request(&mut self, frame: &[u8]) -> Vec<u8> {
        match self {
            Self::Tcp(client) => client.request(frame, NOTICE_RESPONSE_TIMEOUT_MS).await,
            Self::WebSocket(client) => client.request(frame, NOTICE_RESPONSE_TIMEOUT_MS).await,
        }
        .expect("Notice request response")
    }

    async fn send(&mut self, frame: &[u8]) {
        match self {
            Self::Tcp(client) => client.send_frame(frame).await,
            Self::WebSocket(client) => client.send_frame(frame).await,
        }
        .expect("Notice send frame")
    }

    pub(crate) async fn recv_frame(&mut self) -> Vec<u8> {
        match self {
            Self::Tcp(client) => client.recv_frame(NOTICE_RESPONSE_TIMEOUT_MS).await,
            Self::WebSocket(client) => client.recv_frame(NOTICE_RESPONSE_TIMEOUT_MS).await,
        }
        .expect("Notice delivery frame")
    }

    async fn close(self) {
        match self {
            Self::Tcp(client) => client.close().await.expect("close Notice TCP client"),
            Self::WebSocket(mut client) => {
                client.close().await.expect("close Notice WebSocket client")
            }
        }
    }
}

pub(crate) fn with_notice_clients<R>(
    transport: crate::tier4_support::TransportKind,
    client_count: usize,
    run: impl FnOnce(&tokio::runtime::Runtime, &mut [NoticeBenchClient]) -> R,
) -> R {
    let runtime = shared_bench_runtime();
    let server = runtime
        .block_on(TestServer::start())
        .expect("start Notice benchmark server");
    let mut clients = (0..client_count)
        .map(|_| {
            runtime
                .block_on(NoticeBenchClient::connect(&server, transport))
                .expect("connect Notice benchmark client")
        })
        .collect::<Vec<_>>();

    let result = run(runtime, &mut clients);

    for client in clients {
        runtime.block_on(client.close());
    }
    runtime
        .block_on(server.shutdown())
        .expect("shutdown Notice benchmark server");
    result
}

pub(crate) async fn subscribe_network_client(
    client: &mut NoticeBenchClient,
    subscribe_frame: &[u8],
) -> u64 {
    let response = client.request(subscribe_frame).await;
    parse_wire_subscribe_response(&response)
}

pub(crate) async fn complete_network_publish(
    publisher: &mut NoticeBenchClient,
    subscribers: &mut [NoticeBenchClient],
    publish_frame: &[u8],
    subscription_ids: &[u64],
    expected_route: &str,
    expected_payload: &[u8],
) -> Duration {
    assert_eq!(subscribers.len(), subscription_ids.len());
    let started = Instant::now();
    let delivery_futures: Vec<_> = subscribers
        .iter_mut()
        .map(|client| client.recv_frame())
        .collect();
    publisher.send(publish_frame).await;
    let deliveries = join_all(delivery_futures).await;
    for (delivery_frame, subscription_id) in deliveries.iter().zip(subscription_ids) {
        let delivery = parse_notice_delivery(delivery_frame).expect("valid Notice delivery");
        assert_eq!(delivery.msg_type, 504);
        assert_eq!(delivery.subscription_id, *subscription_id);
        assert_eq!(delivery.route, expected_route);
        assert_eq!(delivery.body.as_slice(), expected_payload);
    }
    started.elapsed()
}

pub(crate) struct MutableNoticeUnsubscribeFrame {
    frame: Vec<u8>,
    payload_offset: usize,
}

impl MutableNoticeUnsubscribeFrame {
    pub(crate) fn new() -> Self {
        let frame = fitz::benchkit::build_notice_unsubscribe(0);
        let payload_offset = match frame.first().copied() {
            Some(0xFF) => 5,
            Some(_) => 3,
            None => panic!("Notice unsubscribe frame should not be empty"),
        };
        Self {
            frame,
            payload_offset,
        }
    }

    pub(crate) fn set_subscription_id(&mut self, subscription_id: u64) {
        self.frame[self.payload_offset..self.payload_offset + 8]
            .copy_from_slice(&subscription_id.to_be_bytes());
    }

    pub(crate) fn as_slice(&self) -> &[u8] {
        &self.frame
    }
}

pub(crate) async fn complete_network_control_lifecycle(
    client: &mut NoticeBenchClient,
    subscribe_frame: &[u8],
    unsubscribe_frame: &mut MutableNoticeUnsubscribeFrame,
) -> Duration {
    let started = Instant::now();
    let subscribe_response = client.request(subscribe_frame).await;
    let subscription_id = parse_wire_subscribe_response(&subscribe_response);
    unsubscribe_frame.set_subscription_id(subscription_id);
    let unsubscribe_response = client.request(unsubscribe_frame.as_slice()).await;
    assert_wire_ok_response(&unsubscribe_response, 502);
    started.elapsed()
}

pub(crate) struct InProcessNoticeFixture {
    sink: Arc<NoticeDomainSink>,
    router: Arc<Router>,
    family: RouteFamily,
    destination: RouteAddress,
    publisher_source: RouteAddress,
    publisher_inbox: Arc<FrameQueueSink>,
    subscriber_inboxes: Vec<(u64, Arc<FrameQueueSink>)>,
    publish_message: NotificationMessage,
    publish_frame: Vec<u8>,
    expected_route: String,
    expected_payload: Bytes,
}

impl InProcessNoticeFixture {
    pub(crate) fn new(pattern: &str, route: &str, payload: &[u8], subscriber_count: usize) -> Self {
        let family = RouteFamily::new(1);
        let router = Arc::new(Router::new());
        let sink = create_bench_notice_sink(router.clone());
        router.register_domain_pattern("notice", sink.clone() as Arc<dyn MailboxSink>);
        let destination = RouteAddress::new(family, Route::new(route));
        let (publisher_source, publisher_inbox) =
            register_session_queue_sink(&router, family, PUBLISHER_SESSION_ID);
        let mut subscriber_inboxes = Vec::with_capacity(subscriber_count);

        for index in 0..subscriber_count {
            let session_id = u64::try_from(index + 1).expect("subscriber session id fits u64");
            let (source, inbox) = register_session_queue_sink(&router, family, session_id);
            let meta = ClientFrameMeta::new(session_id, ClientChannel::Pub, 501, family);
            let subscribe = SubscribeMessage::new(
                family,
                Route::new(pattern),
                fitz::session::SessionId(session_id),
                source.clone(),
            );
            sink.deliver(Envelope::from_route(
                source,
                destination.clone(),
                NoticeClientRequest::new(meta, Ok(NotificationMessage::Subscribe(subscribe))),
            ))
            .expect("direct Notice subscribe");
            let response = inbox.drain_after_count(1, NOTICE_RESPONSE_TIMEOUT);
            let subscription_id = parse_direct_subscribe_response(&response);
            subscriber_inboxes.push((subscription_id, inbox));
        }

        let expected_payload = Bytes::copy_from_slice(payload);
        Self {
            sink,
            router,
            family,
            destination,
            publisher_source,
            publisher_inbox,
            subscriber_inboxes,
            publish_message: NotificationMessage::Publish(PublishMessage::new(
                family,
                Route::new(route),
                expected_payload.clone(),
            )),
            publish_frame: fitz::benchkit::build_notice_publish(route, payload),
            expected_route: route.to_string(),
            expected_payload,
        }
    }

    pub(crate) fn complete_direct_publish(&self) -> Duration {
        let started = Instant::now();
        let meta = ClientFrameMeta::new(PUBLISHER_SESSION_ID, ClientChannel::Pub, 500, self.family);
        self.sink
            .deliver(Envelope::from_route(
                self.publisher_source.clone(),
                self.destination.clone(),
                NoticeClientRequest::new(meta, Ok(self.publish_message.clone())),
            ))
            .expect("direct Notice publish");
        self.validate_direct_completion();
        started.elapsed()
    }

    pub(crate) fn complete_encoded_publish(&self) -> Duration {
        let started = Instant::now();
        let mut parser = TlvFrameParser::new(&self.publish_frame);
        let (message_type, payload) = parser.next_field_ref().expect("Notice publish TLV field");
        assert!(
            parser.next_field_ref().is_none(),
            "expected one Notice publish field"
        );
        route_frame_to_address(
            &self.router,
            &self.publisher_source,
            &self.destination,
            PUBLISHER_SESSION_ID,
            ChannelId::Pub,
            message_type,
            Bytes::copy_from_slice(payload),
        )
        .expect("encoded Notice publish");
        self.validate_direct_completion();
        started.elapsed()
    }

    fn validate_direct_completion(&self) {
        for (subscription_id, inbox) in &self.subscriber_inboxes {
            let deliveries = inbox.drain_after_count(1, NOTICE_RESPONSE_TIMEOUT);
            assert_eq!(deliveries.len(), 1, "expected one Notice delivery");
            assert_direct_delivery(
                &deliveries[0],
                *subscription_id,
                &self.expected_route,
                self.expected_payload.as_ref(),
            );
        }
    }

    pub(crate) fn stop(&self) {
        self.sink.stop();
    }
}

pub(crate) struct InProcessNoticeControlFixture {
    sink: Arc<NoticeDomainSink>,
    router: Arc<Router>,
    family: RouteFamily,
    destination: RouteAddress,
    client_source: RouteAddress,
    client_inbox: Arc<FrameQueueSink>,
    subscribe_message: NotificationMessage,
    subscribe_frame: Vec<u8>,
    unsubscribe_frame: MutableNoticeUnsubscribeFrame,
}

impl InProcessNoticeControlFixture {
    pub(crate) fn new(pattern: &str) -> Self {
        let family = RouteFamily::new(1);
        let router = Arc::new(Router::new());
        let sink = create_bench_notice_sink(router.clone());
        router.register_domain_pattern("notice", sink.clone() as Arc<dyn MailboxSink>);
        let destination = RouteAddress::new(family, Route::new(pattern));
        let session_id = 1;
        let (client_source, client_inbox) =
            register_session_queue_sink(&router, family, session_id);
        let subscribe_message = NotificationMessage::Subscribe(SubscribeMessage::new(
            family,
            Route::new(pattern),
            fitz::session::SessionId(session_id),
            client_source.clone(),
        ));
        Self {
            sink,
            router,
            family,
            destination,
            client_source,
            client_inbox,
            subscribe_message,
            subscribe_frame: fitz::benchkit::build_notice_subscribe(pattern),
            unsubscribe_frame: MutableNoticeUnsubscribeFrame::new(),
        }
    }

    pub(crate) fn complete_direct_lifecycle(&mut self) -> Duration {
        let started = Instant::now();
        let subscribe_meta = ClientFrameMeta::new(1, ClientChannel::Pub, 501, self.family);
        self.sink
            .deliver(Envelope::from_route(
                self.client_source.clone(),
                self.destination.clone(),
                NoticeClientRequest::new(subscribe_meta, Ok(self.subscribe_message.clone())),
            ))
            .expect("direct Notice subscribe");
        let subscription_id = parse_direct_subscribe_response(
            &self
                .client_inbox
                .drain_after_count(1, NOTICE_RESPONSE_TIMEOUT),
        );
        let unsubscribe_meta = ClientFrameMeta::new(1, ClientChannel::Pub, 502, self.family);
        let unsubscribe = NotificationMessage::Unsubscribe(UnsubscribeMessage::new(
            self.family,
            subscription_id,
            fitz::session::SessionId(1),
        ));
        self.sink
            .deliver(Envelope::from_route(
                self.client_source.clone(),
                self.destination.clone(),
                NoticeClientRequest::new(unsubscribe_meta, Ok(unsubscribe)),
            ))
            .expect("direct Notice unsubscribe");
        assert_direct_ok_response(
            &self
                .client_inbox
                .drain_after_count(1, NOTICE_RESPONSE_TIMEOUT),
            502,
        );
        started.elapsed()
    }

    pub(crate) fn complete_encoded_lifecycle(&mut self) -> Duration {
        let started = Instant::now();
        route_encoded_frame(
            &self.router,
            &self.client_source,
            &self.destination,
            1,
            &self.subscribe_frame,
        );
        let subscription_id = parse_direct_subscribe_response(
            &self
                .client_inbox
                .drain_after_count(1, NOTICE_RESPONSE_TIMEOUT),
        );
        self.unsubscribe_frame.set_subscription_id(subscription_id);
        route_encoded_frame(
            &self.router,
            &self.client_source,
            &self.destination,
            1,
            self.unsubscribe_frame.as_slice(),
        );
        assert_direct_ok_response(
            &self
                .client_inbox
                .drain_after_count(1, NOTICE_RESPONSE_TIMEOUT),
            502,
        );
        started.elapsed()
    }

    pub(crate) fn stop(&self) {
        self.sink.stop();
    }
}

fn route_encoded_frame(
    router: &Router,
    source: &RouteAddress,
    destination: &RouteAddress,
    session_id: u64,
    frame: &[u8],
) {
    let mut parser = TlvFrameParser::new(frame);
    let (message_type, payload) = parser.next_field_ref().expect("Notice TLV field");
    assert!(
        parser.next_field_ref().is_none(),
        "expected one Notice TLV field"
    );
    route_frame_to_address(
        router,
        source,
        destination,
        session_id,
        ChannelId::Pub,
        message_type,
        Bytes::copy_from_slice(payload),
    )
    .expect("route encoded Notice frame");
}

fn parse_direct_subscribe_response(frames: &[FrameContext]) -> u64 {
    assert_eq!(frames.len(), 1, "expected one Notice subscribe response");
    let frame = &frames[0];
    assert_eq!(frame.msg_type.as_u16(), 501);
    let mut decoder = PayloadDecoder::new(&frame.payload);
    assert_eq!(decoder.get_u8().expect("Notice response status"), 0);
    let subscription_id = decoder
        .get_optional_u64()
        .expect("Notice subscription id")
        .expect("Notice subscription id value");
    assert!(
        decoder.is_complete(),
        "unexpected Notice subscribe response bytes"
    );
    subscription_id
}

fn parse_wire_subscribe_response(frame: &[u8]) -> u64 {
    let (message_type, payload) = single_wire_field(frame);
    assert_eq!(message_type, 501);
    let mut decoder = PayloadDecoder::new(payload);
    assert_eq!(decoder.get_u8().expect("Notice response status"), 0);
    let subscription_id = decoder
        .get_optional_u64()
        .expect("Notice subscription id")
        .expect("Notice subscription id value");
    assert!(
        decoder.is_complete(),
        "unexpected Notice subscribe response bytes"
    );
    subscription_id
}

fn assert_wire_ok_response(frame: &[u8], expected_message_type: u16) {
    let (message_type, payload) = single_wire_field(frame);
    assert_eq!(message_type, expected_message_type);
    let mut decoder = PayloadDecoder::new(payload);
    assert_eq!(decoder.get_u8().expect("Notice response status"), 0);
    assert!(decoder.is_complete(), "unexpected Notice response bytes");
}

fn single_wire_field(frame: &[u8]) -> (u16, &[u8]) {
    let mut parser = TlvFrameParser::new(frame);
    let (message_type, payload) = parser.next_field_ref().expect("Notice TLV field");
    assert!(
        parser.next_field_ref().is_none(),
        "expected one Notice TLV field"
    );
    (message_type, payload)
}

fn assert_direct_ok_response(frames: &[FrameContext], expected_message_type: u16) {
    assert_eq!(frames.len(), 1, "expected one Notice response");
    assert_eq!(frames[0].msg_type.as_u16(), expected_message_type);
    let mut decoder = PayloadDecoder::new(&frames[0].payload);
    assert_eq!(decoder.get_u8().expect("Notice response status"), 0);
    assert!(decoder.is_complete(), "unexpected Notice response bytes");
}

fn assert_direct_delivery(
    frame: &FrameContext,
    expected_subscription_id: u64,
    expected_route: &str,
    expected_payload: &[u8],
) {
    assert_eq!(frame.msg_type.as_u16(), 504);
    let mut decoder = PayloadDecoder::new(&frame.payload);
    assert_eq!(
        decoder.get_u64().expect("Notice delivery subscription id"),
        expected_subscription_id
    );
    assert_eq!(
        decoder.get_string().expect("Notice delivery route"),
        expected_route
    );
    assert_eq!(
        decoder
            .get_bytes()
            .expect("Notice delivery payload")
            .as_ref(),
        expected_payload
    );
    assert!(decoder.is_complete(), "unexpected Notice delivery bytes");
}
