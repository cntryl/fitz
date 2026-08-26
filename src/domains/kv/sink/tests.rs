use super::*;
use crate::dispatch::protocol::error_codes;
use crate::dispatch::protocol::frame::ChannelId;
use crate::dispatch::protocol::frame_context::FrameContext;
use crate::dispatch::protocol::tlv::MessageType;
use crate::domains::kv::KvResourceScope;
use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
use crate::runtime::{Envelope, Mailbox, MailboxSink, Router};
use bytes::{BufMut, Bytes};
use std::sync::Arc;
use std::time::{Duration, Instant};

mod configuration;
mod correctness;
mod lifecycle;
mod subscriptions;

#[inline]
fn usize_to_u32_saturating(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

fn encode_kv_begin(route: &str, mode: u8, durability: u8) -> Bytes {
    let mut payload = Vec::new();
    payload.put_u32(usize_to_u32_saturating(route.len()));
    payload.put_slice(route.as_bytes());
    payload.put_u8(mode);
    payload.put_u8(durability);
    Bytes::from(payload)
}

fn encode_kv_put(tx_id: u64, route: &str, key: &[u8], value: &[u8]) -> Bytes {
    let mut payload = Vec::new();
    payload.put_u64(tx_id);
    payload.put_u32(usize_to_u32_saturating(route.len()));
    payload.put_slice(route.as_bytes());
    payload.put_u32(usize_to_u32_saturating(key.len()));
    payload.put_slice(key);
    payload.put_u32(usize_to_u32_saturating(value.len()));
    payload.put_slice(value);
    Bytes::from(payload)
}

fn encode_kv_commit(tx_id: u64, route: &str) -> Bytes {
    let mut payload = Vec::new();
    payload.put_u64(tx_id);
    payload.put_u32(usize_to_u32_saturating(route.len()));
    payload.put_slice(route.as_bytes());
    Bytes::from(payload)
}

fn encode_kv_subscribe(pattern: &str) -> Bytes {
    let mut payload = Vec::new();
    payload.put_u32(usize_to_u32_saturating(pattern.len()));
    payload.put_slice(pattern.as_bytes());
    Bytes::from(payload)
}

fn encode_kv_unsubscribe(pattern: &str) -> Bytes {
    let mut payload = Vec::new();
    payload.put_u32(usize_to_u32_saturating(pattern.len()));
    payload.put_slice(pattern.as_bytes());
    Bytes::from(payload)
}

fn decode_kv_begin_tx_id(payload: &[u8]) -> u64 {
    let tx_id_bytes: [u8; 8] = payload[1..9]
        .try_into()
        .expect("begin response tx_id bytes");
    u64::from_be_bytes(tx_id_bytes)
}

fn decode_kv_subscription_id(payload: &[u8]) -> u64 {
    let subscription_id_bytes: [u8; 8] = payload[1..9]
        .try_into()
        .expect("subscribe response subscription_id bytes");
    u64::from_be_bytes(subscription_id_bytes)
}

fn decode_kv_watch_delivery(frame: &FrameContext) -> (u64, String, u64) {
    let subscription_id = u64::from_be_bytes(frame.payload[0..8].try_into().unwrap());
    let route_len = u32::from_be_bytes(frame.payload[8..12].try_into().unwrap()) as usize;
    let route = String::from_utf8(frame.payload[12..12 + route_len].to_vec())
        .expect("KV watch route should be utf-8");
    let mutation_offset = 12 + route_len;
    let mutation_count = u64::from_be_bytes(
        frame.payload[mutation_offset..mutation_offset + 8]
            .try_into()
            .unwrap(),
    );
    (subscription_id, route, mutation_count)
}

fn decode_error_code(payload: &[u8]) -> u16 {
    error_codes::decode_error_body(payload)
        .expect("error payload")
        .0
}

fn drain_mailbox(mailbox: &Mailbox) {
    while mailbox.receiver().try_recv().is_ok() {}
}

fn receive_envelope(mailbox: &Mailbox, label: &str) -> Envelope {
    mailbox
        .receiver()
        .recv_timeout(Duration::from_secs(1))
        .unwrap_or_else(|_| panic!("{label}"))
}

fn receive_frame(mailbox: &Mailbox, label: &str) -> FrameContext {
    receive_envelope(mailbox, label)
        .into_payload::<FrameContext>()
        .unwrap_or_else(|| panic!("{label} frame"))
}

fn assert_no_envelope(mailbox: &Mailbox) {
    assert!(mailbox
        .receiver()
        .recv_timeout(Duration::from_millis(50))
        .is_err());
}

fn wait_for_active_transaction_count(sink: &KvDomainSink, expected: usize) {
    let deadline = Instant::now() + Duration::from_secs(1);
    while Instant::now() < deadline {
        if sink.active_transaction_count() == expected {
            return;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(sink.active_transaction_count(), expected);
}
