//! Subsystem benchmarks for Notice domain: Engine → Domain → Response
//!
//! Measures only the synchronous engine path (no async, no threads, no mpsc):
//!   1. Raw frame bytes
//!   2. parse_frame()
//!   3. parse_route()
//!   4. DomainRegistry.dispatch()
//!   5. NoticeDomain.handle()
//!   6. Response encoding
//!
//! This is the "correct" subsystem layer: full engine+domain logic,
//! but without transport threads, channels, or async runtimes.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::sync::Arc;

use fitz::core::domain::DomainContext;
use fitz::core::registry::DomainRegistry;
use fitz::protocol::frame::{build_frame, build_tlv, find_tlv, parse_frame};
use fitz::protocol::route::parse_route;
use fitz::protocol::tags::*;

#[path = "../config.rs"]
mod config;

const CHANNEL_ID: u32 = 1;

// -----------------------------------------------------------------------------
// Frame builders
// -----------------------------------------------------------------------------

fn build_subscribe_frame(route: &str, channel_id: u32) -> Vec<u8> {
    let mut payload = Vec::new();
    build_tlv(TAG_ROUTE, route.as_bytes(), &mut payload);
    build_tlv(TAG_SUBSCRIBE, &[], &mut payload);
    build_frame(FRAME_REG, 0, channel_id, &payload)
}

fn build_publish_frame(route: &str, id: &str, body: &[u8], channel_id: u32) -> Vec<u8> {
    let mut payload = Vec::new();
    build_tlv(TAG_ROUTE, route.as_bytes(), &mut payload);
    build_tlv(TAG_ID, id.as_bytes(), &mut payload);
    build_tlv(TAG_BODY, body, &mut payload);
    build_frame(FRAME_PUB, 0, channel_id, &payload)
}

fn build_publish_frame_no_ack(route: &str, id: &str, body: &[u8], channel_id: u32) -> Vec<u8> {
    let mut payload = Vec::new();
    build_tlv(TAG_ROUTE, route.as_bytes(), &mut payload);
    build_tlv(TAG_NO_ACK, &[], &mut payload);
    build_tlv(TAG_ID, id.as_bytes(), &mut payload);
    build_tlv(TAG_BODY, body, &mut payload);
    build_frame(FRAME_PUB, 0, channel_id, &payload)
}

fn build_unsubscribe_frame(route: &str, channel_id: u32) -> Vec<u8> {
    let mut payload = Vec::new();
    build_tlv(TAG_ROUTE, route.as_bytes(), &mut payload);
    build_tlv(TAG_UNSUBSCRIBE, &[], &mut payload);
    build_frame(FRAME_REG, 0, channel_id, &payload)
}

// -----------------------------------------------------------------------------
// Test harness — fully synchronous, real domain, no async/tokio
// -----------------------------------------------------------------------------

struct NoticeSubsystemBench {
    domains: Arc<DomainRegistry>,
}

impl NoticeSubsystemBench {
    fn new() -> Self {
        let domains = Arc::new(DomainRegistry::new());
        Self { domains }
    }

    /// Pure synchronous subsystem path: frame parse → route parse → domain dispatch
    fn handle_frame(&self, bytes: &[u8]) {
        let parsed = parse_frame(bytes).expect("frame");
        let payload = parsed.payload;

        let route_str = find_tlv(payload, TAG_ROUTE)
            .and_then(|b| std::str::from_utf8(b).ok())
            .expect("route");

        let route = parse_route(route_str).expect("valid");

        let ctx = DomainContext {
            route: route.clone(),
            route_str: route_str.to_owned(),
            payload: payload.to_vec(),
            channel_id: parsed.header.channel_id,
            route_family: 1,
        };

        // synchronous domain dispatch (response is returned but ignored)
        let _ = self.domains.dispatch(route.scheme.as_str(), ctx);
    }
}

// -----------------------------------------------------------------------------
// Benchmark definitions
// -----------------------------------------------------------------------------

fn bench_subscribe(c: &mut Criterion) {
    let bench = NoticeSubsystemBench::new();

    let frame = build_subscribe_frame("notice://realm/area/events/update", CHANNEL_ID);

    let mut group = c.benchmark_group("notice_subsys_subscribe");
    group.bench_function("subscribe", |b| {
        b.iter(|| {
            bench.handle_frame(black_box(&frame));
        })
    });
    group.finish();
}

fn bench_unsubscribe(c: &mut Criterion) {
    let bench = NoticeSubsystemBench::new();

    let sub = build_subscribe_frame("notice://realm/area/events/update", CHANNEL_ID);
    bench.handle_frame(&sub); // setup

    let unsub = build_unsubscribe_frame("notice://realm/area/events/update", CHANNEL_ID);

    let mut group = c.benchmark_group("notice_subsys_unsubscribe");
    group.bench_function("unsubscribe", |b| {
        b.iter(|| {
            bench.handle_frame(black_box(&unsub));
        })
    });
    group.finish();
}

fn bench_publish_no_subscribers(c: &mut Criterion) {
    let bench = NoticeSubsystemBench::new();

    let body = b"hello world";

    let pub_ack = build_publish_frame("notice://realm/area/alerts/crit", "id1", body, CHANNEL_ID);
    let pub_noack =
        build_publish_frame_no_ack("notice://realm/area/alerts/crit", "id1", body, CHANNEL_ID);

    let mut group = c.benchmark_group("notice_subsys_publish_no_subs");
    group.bench_function("with_ack", |b| {
        b.iter(|| bench.handle_frame(black_box(&pub_ack)))
    });
    group.bench_function("no_ack", |b| {
        b.iter(|| bench.handle_frame(black_box(&pub_noack)))
    });
    group.finish();
}

fn bench_publish_with_fanout(c: &mut Criterion) {
    let mut group = c.benchmark_group("notice_subsys_publish_fanout");

    for &sub_count in &[1, 10, 100, 1000] {
        let bench = NoticeSubsystemBench::new();

        for ch in 1..=sub_count {
            let sub = build_subscribe_frame("notice://realm/area/broadcast/alert", ch);
            bench.handle_frame(&sub);
        }

        let body = vec![0u8; 128];
        let frame = build_publish_frame(
            "notice://realm/area/broadcast/alert",
            "id",
            &body,
            CHANNEL_ID,
        );

        group.throughput(Throughput::Elements(sub_count as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(sub_count),
            &sub_count,
            |b, _| {
                b.iter(|| bench.handle_frame(black_box(&frame)));
            },
        );
    }

    group.finish();
}

fn bench_wildcard_matching(c: &mut Criterion) {
    let bench = NoticeSubsystemBench::new();

    // install wildcard patterns
    let patterns = [
        ("notice://realm/area/*/update", 10),
        ("notice://realm/*/events/update", 11),
        ("notice://*/area/events/update", 12),
        ("notice://*/*/events/update", 13),
    ];

    for (route, ch) in &patterns {
        let sub = build_subscribe_frame(route, *ch);
        bench.handle_frame(&sub);
    }

    let test_routes = [
        "notice://realm/area/events/update",
        "notice://realm/area/specific/update",
        "notice://realm2/area/events/update",
        "notice://realm/area2/other/update",
    ];

    let mut group = c.benchmark_group("notice_subsys_wildcard");

    for r in &test_routes {
        let frame = build_publish_frame(r, "id", b"p", CHANNEL_ID);

        group.bench_function(*r, |b| {
            b.iter(|| bench.handle_frame(black_box(&frame)));
        });
    }

    group.finish();
}

fn bench_message_sizes(c: &mut Criterion) {
    let bench = NoticeSubsystemBench::new();

    let sub = build_subscribe_frame("notice://realm/area/data", 100);
    bench.handle_frame(&sub);

    let mut group = c.benchmark_group("notice_subsys_body_sizes");

    for size in [64, 256, 1024, 4096, 16384] {
        let body = vec![0u8; size];
        let frame = build_publish_frame("notice://realm/area/data", "id", &body, CHANNEL_ID);

        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter(|| bench.handle_frame(black_box(&frame)));
        });
    }

    group.finish();
}

fn bench_authorization(c: &mut Criterion) {
    let bench = NoticeSubsystemBench::new();

    let frame = build_subscribe_frame("notice://realm/area/events/u", CHANNEL_ID);

    let mut group = c.benchmark_group("notice_subsys_authz");
    group.bench_function("with_authz", |b| {
        b.iter(|| bench.handle_frame(black_box(&frame)));
    });
    group.finish();
}

// -----------------------------------------------------------------------------
// Registration
// -----------------------------------------------------------------------------

criterion_group!(
    name = subsystem_notice;
    config = config::criterion_config();
    targets =
        bench_subscribe,
        bench_unsubscribe,
        bench_publish_no_subscribers,
        bench_publish_with_fanout,
        bench_wildcard_matching,
        bench_message_sizes,
        bench_authorization
);
criterion_main!(subsystem_notice);
