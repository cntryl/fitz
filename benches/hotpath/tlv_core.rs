//! Hotpath microbenchmarks for TLV/frame helpers.
use criterion::{criterion_group, criterion_main, Criterion};
use fitz::protocol::frame::{build_frame, build_tlv, find_tlv, parse_frame};
use fitz::protocol::tags::{TAG_BODY, TAG_ID};

#[path = "../config.rs"]
mod config;

fn bench_build_tlv(c: &mut Criterion) {
    let mut buf = Vec::with_capacity(256);
    let id = b"id-012345";

    c.bench_function("tlv_build_id", |b| {
        b.iter(|| {
            buf.clear();
            build_tlv(TAG_ID, id, &mut buf);
        })
    });
}

fn bench_find_tlv(c: &mut Criterion) {
    let mut payload = Vec::new();
    build_tlv(TAG_ID, b"id-1", &mut payload);
    build_tlv(TAG_BODY, b"hello", &mut payload);

    c.bench_function("tlv_find_body", |b| {
        b.iter(|| {
            let _ = find_tlv(&payload, TAG_BODY);
        })
    });
}

fn bench_build_frame_parse(c: &mut Criterion) {
    let mut payload = Vec::new();
    build_tlv(TAG_BODY, b"hello world", &mut payload);

    c.bench_function("frame_build_parse", |b| {
        b.iter(|| {
            let frame = build_frame(0x07, 0, 1, &payload);
            let _ = parse_frame(&frame).unwrap();
        })
    });
}

criterion_group!(
    name = hotpath_tlv_core;
    config = config::criterion_config();
    targets = bench_build_tlv, bench_find_tlv, bench_build_frame_parse
);
criterion_main!(hotpath_tlv_core);
