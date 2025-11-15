//! Hotpath benchmarks for TLV (Type-Length-Value) operations
//!
//! These are the most fundamental performance-critical operations in Fitz.
//! TLV parsing/encoding happens on every message, so these need to be blazing fast.

use criterion::{criterion_group, criterion_main, Criterion};
use fitz::protocol::frame::{build_tlv, find_tlv};
use fitz::protocol::tags::*;
use std::sync::OnceLock;

#[path = "../config.rs"]
mod config;

// ---------------------------------------------------------
// Shared test data
// ---------------------------------------------------------
static SMALL_PAYLOAD: OnceLock<Vec<u8>> = OnceLock::new();
fn small_payload() -> &'static [u8] {
    SMALL_PAYLOAD.get_or_init(|| {
        let mut payload = Vec::new();
        build_tlv(TAG_BODY, b"hello world", &mut payload);
        build_tlv(TAG_ID, b"correlation_123", &mut payload);
        build_tlv(TAG_ROUTE, b"test://route", &mut payload);
        payload
    })
}

static LARGE_PAYLOAD: OnceLock<Vec<u8>> = OnceLock::new();
fn large_payload() -> &'static [u8] {
    LARGE_PAYLOAD.get_or_init(|| {
        let mut payload = Vec::new();
        let large_body = vec![b'x'; 64 * 1024]; // 64KB
        let large_id = format!("correlation_{}", "x".repeat(1000));
        build_tlv(TAG_BODY, &large_body, &mut payload);
        build_tlv(TAG_ID, large_id.as_bytes(), &mut payload);
        build_tlv(TAG_ROUTE, b"test://large/route", &mut payload);
        payload
    })
}

// ---------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------

fn bench_build_tlv_small(c: &mut Criterion) {
    c.bench_function("tlv_build_small", |b| {
        b.iter(|| {
            let mut payload = Vec::new();
            build_tlv(TAG_BODY, b"hello world", &mut payload);
            build_tlv(TAG_ID, b"correlation_123", &mut payload);
            build_tlv(TAG_ROUTE, b"test://route", &mut payload);
        })
    });
}

fn bench_build_tlv_large(c: &mut Criterion) {
    let large_body = vec![b'x'; 64 * 1024];
    let large_id = format!("correlation_{}", "x".repeat(1000));

    c.bench_function("tlv_build_large", |b| {
        b.iter(|| {
            let mut payload = Vec::new();
            build_tlv(TAG_BODY, &large_body, &mut payload);
            build_tlv(TAG_ID, large_id.as_bytes(), &mut payload);
            build_tlv(TAG_ROUTE, b"test://large/route", &mut payload);
        })
    });
}

fn bench_find_tlv_small(c: &mut Criterion) {
    let payload = small_payload();

    c.bench_function("tlv_find_small", |b| {
        b.iter(|| {
            let _body = find_tlv(payload, TAG_BODY);
            let _id = find_tlv(payload, TAG_ID);
            let _route = find_tlv(payload, TAG_ROUTE);
        })
    });
}

fn bench_find_tlv_large(c: &mut Criterion) {
    let payload = large_payload();

    c.bench_function("tlv_find_large", |b| {
        b.iter(|| {
            let _body = find_tlv(payload, TAG_BODY);
            let _id = find_tlv(payload, TAG_ID);
            let _route = find_tlv(payload, TAG_ROUTE);
        })
    });
}

fn bench_parse_string_small(c: &mut Criterion) {
    let payload = small_payload();

    c.bench_function("tlv_parse_string_small", |b| {
        b.iter(|| {
            let _route = fitz::protocol::frame::parse_string(payload, TAG_ROUTE);
            let _id = fitz::protocol::frame::parse_string(payload, TAG_ID);
        })
    });
}

fn bench_parse_bytes_small(c: &mut Criterion) {
    let payload = small_payload();

    c.bench_function("tlv_parse_bytes_small", |b| {
        b.iter(|| {
            let _body = fitz::protocol::frame::parse_bytes(payload, TAG_BODY);
        })
    });
}

fn bench_parse_u32_small(c: &mut Criterion) {
    let mut payload = Vec::new();
    build_tlv(TAG_SEQ, 42u32, &mut payload);

    c.bench_function("tlv_parse_u32_small", |b| {
        b.iter(|| {
            let _seq = fitz::protocol::frame::parse_u32(&payload, TAG_SEQ);
        })
    });
}

criterion_group!(
    name = tlv_hotpath;
    config = config::criterion_config();
    targets =
        bench_build_tlv_small,
        bench_build_tlv_large,
        bench_find_tlv_small,
        bench_find_tlv_large,
        bench_parse_string_small,
        bench_parse_bytes_small,
        bench_parse_u32_small
);

criterion_main!(tlv_hotpath);