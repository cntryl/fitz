use super::*;
use crate::control::admin::{NoticeRouteInfo, NoticeSubscription as AdminNoticeSubscription};
use crate::dispatch::protocol::frame::ChannelId;
use crate::dispatch::protocol::frame_context::FrameContext;
use crate::dispatch::protocol::payload_codec::{PayloadDecoder, PayloadEncoder};
use crate::dispatch::protocol::tlv::MessageType;
use crate::runtime::mailbox::Mailbox;
use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
use bytes::Bytes;
use std::sync::Arc;

mod cleanup;
mod correctness;
mod publish;

fn encode_notice_subscribe(pattern: &str) -> Bytes {
    let mut encoder = PayloadEncoder::new();
    encoder.put_string(pattern);
    Bytes::from(encoder.finish())
}

fn encode_notice_publish(route: &str, payload: &[u8]) -> Bytes {
    let mut encoder = PayloadEncoder::new();
    encoder.put_string(route);
    encoder.put_bytes(payload);
    Bytes::from(encoder.finish())
}

fn encode_notice_unsubscribe(subscription_id: u64) -> Bytes {
    let mut encoder = PayloadEncoder::new();
    encoder.put_u64(subscription_id);
    Bytes::from(encoder.finish())
}

fn drain_mailbox(mailbox: &Mailbox) {
    while mailbox.receiver().try_recv().is_ok() {}
}

struct NoticeResponsePayload {
    status: u8,
    subscription_id: Option<u64>,
    error: Option<String>,
}

struct FailingSink;

impl MailboxSink for FailingSink {
    fn deliver(&self, _envelope: Envelope) -> Result<(), DeliveryError> {
        Err(DeliveryError::ActorStopped)
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

struct RetrySink {
    state: Arc<Mutex<Vec<Result<(), DeliveryError>>>>,
}

impl MailboxSink for RetrySink {
    fn deliver(&self, _envelope: Envelope) -> Result<(), DeliveryError> {
        self.state.lock().remove(0)
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

fn decode_notice_response(mailbox: &Mailbox) -> NoticeResponsePayload {
    let response_envelope = mailbox
        .receiver()
        .try_recv()
        .expect("notice response envelope");
    let response_frame = response_envelope
        .into_payload::<FrameContext>()
        .expect("notice response frame");
    let mut decoder = PayloadDecoder::new(&response_frame.payload);
    let status = decoder.get_u8().expect("notice response status");

    if status == 0 {
        let subscription_id = if decoder.remaining() > 0 {
            Some(
                decoder
                    .get_optional_u64()
                    .expect("notice response subscription id")
                    .expect("notice response subscription id value"),
            )
        } else {
            None
        };
        assert!(decoder.is_complete());
        NoticeResponsePayload {
            status,
            subscription_id,
            error: None,
        }
    } else {
        let _error_code = decoder.get_u32().expect("notice response error code");
        let error = decoder.get_string().expect("notice response error");
        assert!(decoder.is_complete());
        NoticeResponsePayload {
            status,
            subscription_id: None,
            error: Some(error),
        }
    }
}

fn subscribe_notice_pattern(
    sink: &NoticeDomainSink,
    subscriber_address: &RouteAddress,
    notice_address: &RouteAddress,
    session_id: u64,
    pattern: &str,
    family: RouteFamily,
) {
    sink.deliver(Envelope::from_route(
        subscriber_address.clone(),
        notice_address.clone(),
        FrameContext::new(
            session_id,
            ChannelId::Sub,
            MessageType::new(501),
            encode_notice_subscribe(pattern),
            family,
        ),
    ))
    .expect("subscribe notice pattern");
}

fn unsubscribe_notice_pattern(
    sink: &NoticeDomainSink,
    subscriber_address: &RouteAddress,
    notice_address: &RouteAddress,
    session_id: u64,
    subscription_id: u64,
    family: RouteFamily,
) {
    sink.deliver(Envelope::from_route(
        subscriber_address.clone(),
        notice_address.clone(),
        FrameContext::new(
            session_id,
            ChannelId::Sub,
            MessageType::new(502),
            encode_notice_unsubscribe(subscription_id),
            family,
        ),
    ))
    .expect("unsubscribe notice pattern");
}

fn assert_notice_admin_subscriptions(
    actual: &[AdminNoticeSubscription],
    expected_patterns: &[&str],
) {
    let mut actual_patterns: Vec<&str> =
        actual.iter().map(|entry| entry.pattern.as_str()).collect();
    actual_patterns.sort_unstable();

    let mut expected_patterns = expected_patterns.to_vec();
    expected_patterns.sort_unstable();

    assert_eq!(actual_patterns, expected_patterns);
}

fn assert_notice_admin_routes(actual: &[NoticeRouteInfo], expected_routes: &[&str]) {
    let mut actual_routes: Vec<&str> = actual.iter().map(|entry| entry.route.as_str()).collect();
    actual_routes.sort_unstable();

    let mut expected_routes = expected_routes.to_vec();
    expected_routes.sort_unstable();

    assert_eq!(actual_routes, expected_routes);
}

fn refresh_notice_admin_snapshot(sink: &NoticeDomainSink) {
    sink.refresh_admin_snapshot_if_dirty();
}

mod delivery_and_limits;
mod lifecycle_and_admin;
