#![allow(dead_code)] // Each standalone workload uses a focused subset of shared Stream helpers.

use crate::tier4_support::{tag_dimensions, Tier4Dimensions};
use bytes::{BufMut, Bytes};
use cntryl_stress::StressContext;
use fitz::benchkit::{build_stream_append, build_stream_commit};
use fitz::domains::stream::protocol::{StreamFilterClause, StreamFilterSet};
use fitz::protocol::payload_codec::{PayloadDecoder, PayloadEncoder};
use fitz::testkit::{TlvFrameBuilder, TlvFrameParser};

pub(crate) const CANONICAL_HISTORY_DEPTH: usize = 100;
pub(crate) const CANONICAL_PAYLOAD_SIZE: usize = 1024;
pub(crate) const CANONICAL_READ_LIMIT: usize = 100;
pub(crate) const MIXED_OPERATIONS_PER_CLIENT: usize = 10;
pub(crate) const MIXED_READS_PER_CLIENT: usize = 8;
pub(crate) const MIXED_WRITES_PER_CLIENT: usize = 2;
pub(crate) const RESPONSE_TIMEOUT_MS: u64 = 5_000;
pub(crate) const STREAM_APPEND_MSG_TYPE: u16 = 601;
pub(crate) const STREAM_NOTIFY_MSG_TYPE: u16 = 609;
pub(crate) const STREAM_READ_MSG_TYPE: u16 = 604;
pub(crate) const STREAM_SYNC_COMMIT_MODE: u8 = 1;
pub(crate) const WIRE_READ_PAGE_LIMIT: usize = 48;

pub(crate) use crate::tier4_support::{
    measure_operations, LayerKind, StorageProfile, TransportKind,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReadScope {
    None,
    Resource,
    Area,
    Realm,
}

impl ReadScope {
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Resource => "resource_exact",
            Self::Area => "area",
            Self::Realm => "realm",
        }
    }

    pub(crate) fn route(self, realm: &str) -> String {
        match self {
            Self::None => panic!("write-only rows do not have a read route"),
            Self::Resource => format!("stream://{realm}/orders/resource-0"),
            Self::Area => format!("stream://{realm}/orders/*"),
            Self::Realm => format!("stream://{realm}/*/*"),
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct RowDimensions<'a> {
    pub(crate) scenario: &'a str,
    pub(crate) storage_profile: StorageProfile,
    pub(crate) layer: LayerKind,
    pub(crate) write_mode: &'a str,
    pub(crate) write_operation: &'a str,
    pub(crate) payload_size: usize,
    pub(crate) history_depth: usize,
    pub(crate) read_limit: usize,
    pub(crate) read_scope: ReadScope,
    pub(crate) route_count: usize,
    pub(crate) filter_match_count: &'a str,
    pub(crate) client_count: usize,
    pub(crate) workload_mix: &'a str,
    pub(crate) completed_unit: &'a str,
    pub(crate) gate_class: &'a str,
}

pub(crate) fn tag_row(ctx: &mut StressContext, dimensions: &RowDimensions<'_>) {
    tag_dimensions(
        ctx,
        &Tier4Dimensions {
            domain: "stream",
            scenario: dimensions.scenario,
            storage_profile: dimensions.storage_profile,
            layer: dimensions.layer,
            write_mode: dimensions.write_mode,
            payload_size: dimensions.payload_size,
            history_depth: dimensions.history_depth,
            read_limit: dimensions.read_limit,
            read_scope: dimensions.read_scope.label(),
            route_count: dimensions.route_count,
            filter_selectivity: dimensions.filter_match_count,
            client_count: dimensions.client_count,
            workload_mix: dimensions.workload_mix,
            completed_unit: dimensions.completed_unit,
            gate_class: dimensions.gate_class,
        },
    );
    ctx.parameter("write_operation", dimensions.write_operation);
    // Preserve the established Stream discriminator while the shared dimension
    // names make selectivity comparable across domains.
    ctx.parameter("filter_match_count", dimensions.filter_match_count);
}

pub(crate) struct MutableAppendFrame {
    frame: Vec<u8>,
    payload_offset: usize,
}

impl MutableAppendFrame {
    pub(crate) fn new(session_id: u64, expected_offset: u64, body: &[u8]) -> Self {
        let frame = build_stream_append(session_id, expected_offset, body);
        let payload_offset = tlv_payload_offset(&frame);
        assert!(
            frame.len() >= payload_offset + 16,
            "stream append frame should contain session and offset fields"
        );
        Self {
            frame,
            payload_offset,
        }
    }

    pub(crate) fn set_session_id(&mut self, session_id: u64) {
        self.frame[self.payload_offset..self.payload_offset + 8]
            .copy_from_slice(&session_id.to_be_bytes());
    }

    pub(crate) fn set_expected_offset(&mut self, expected_offset: u64) {
        self.frame[self.payload_offset + 8..self.payload_offset + 16]
            .copy_from_slice(&expected_offset.to_be_bytes());
    }

    pub(crate) fn as_slice(&self) -> &[u8] {
        &self.frame
    }
}

pub(crate) struct MutableCommitFrame {
    frame: Vec<u8>,
    payload_offset: usize,
}

impl MutableCommitFrame {
    pub(crate) fn new(session_id: u64, write_mode: u8) -> Self {
        let frame = build_stream_commit(session_id, write_mode);
        let payload_offset = tlv_payload_offset(&frame);
        assert!(
            frame.len() >= payload_offset + 8,
            "stream commit frame should contain a session id"
        );
        Self {
            frame,
            payload_offset,
        }
    }

    pub(crate) fn set_session_id(&mut self, session_id: u64) {
        self.frame[self.payload_offset..self.payload_offset + 8]
            .copy_from_slice(&session_id.to_be_bytes());
    }

    pub(crate) fn as_slice(&self) -> &[u8] {
        &self.frame
    }
}

pub(crate) fn build_stream_read_with_filter(
    route: &str,
    from_offset: u64,
    limit: u64,
    filter: Option<&StreamFilterSet>,
) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.put_u32(usize_to_u32(route.len()));
    payload.put_slice(route.as_bytes());
    payload.put_u64(from_offset);
    payload.put_u64(limit);
    payload.put_u8(0);
    if let Some(filter) = filter {
        let encoded = filter.encode();
        payload.put_u8(1);
        payload.put_u32(usize_to_u32(encoded.len()));
        payload.put_slice(&encoded);
    }

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(STREAM_READ_MSG_TYPE, &payload);
    builder.build()
}

pub(crate) fn equality_filter(value: &str) -> StreamFilterSet {
    StreamFilterSet {
        clauses: vec![StreamFilterClause::Equals(value.to_string())],
    }
}

pub(crate) fn assert_stream_notify(frame: &[u8], expected_route: &str) {
    let mut parser = TlvFrameParser::new(frame);
    let (msg_type, payload) = parser.next_field_ref().expect("stream notify frame");
    assert_eq!(msg_type, STREAM_NOTIFY_MSG_TYPE, "expected stream notify");

    let mut decoder = PayloadDecoder::new(payload);
    decoder.get_u64().expect("stream notify subscription id");
    let route = decoder.get_string().expect("stream notify route");
    let body = decoder.get_bytes().expect("stream notify payload");
    assert_eq!(route, expected_route, "unexpected stream notify route");
    assert!(
        !body.is_empty(),
        "stream notify payload should not be empty"
    );
    assert!(decoder.is_complete(), "expected complete stream notify");
}

pub(crate) fn build_stream_append_with_discriminator(
    session_id: u64,
    expected_offset: u64,
    body: &[u8],
    discriminator: &str,
) -> Vec<u8> {
    let mut encoder = PayloadEncoder::with_capacity(body.len() + discriminator.len() + 32);
    encoder.put_u64(session_id);
    encoder.put_u64(expected_offset);
    encoder.put_bytes(body);
    encoder.put_u8(0);
    encoder.put_optional_string(Some(discriminator));

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(STREAM_APPEND_MSG_TYPE, &encoder.finish());
    builder.build()
}

pub(crate) fn tlv_field(frame: &[u8]) -> (u16, Bytes) {
    let mut parser = TlvFrameParser::new(frame);
    let (msg_type, payload) = parser.next_field_ref().expect("one TLV field");
    assert!(parser.next_field_ref().is_none(), "expected one TLV field");
    (msg_type, Bytes::copy_from_slice(payload))
}

fn tlv_payload_offset(frame: &[u8]) -> usize {
    match frame.first().copied() {
        Some(0xFF) => 5,
        Some(_) => 3,
        None => panic!("TLV frame should not be empty"),
    }
}

fn usize_to_u32(value: usize) -> u32 {
    u32::try_from(value).expect("benchmark value should fit u32")
}
