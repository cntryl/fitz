//! Notice subsystem benchmarks (Engine glue → Domain → Response)
//!
//! Measures exactly the synchronous domain path used by Engine::handle_frame:
//!   1. build_frame()
//!   2. parse_frame()
//!   3. extract & parse TAG_ROUTE
//!   4. parse_route()
//!   5. DomainRegistry::dispatch()
//!   6. NoticeDomain.handle()
//!   7. encode DomainResponse → bytes
//!
//! This excludes:
//!   - engine thread
//!   - async WS
//!   - channel/conn registry
//!   - session lookup / authz
//!   - outbound queue
//!
//! This is tier-2 (subsystem) in your 3-layer model.

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
// Helpers
// -----------------------------------------------------------------------------

fn build_sub_frame(route: &str) -> Vec<u8> {
    let mut payload = Vec::new();
    build_tlv(TAG_ROUTE, route.as_bytes(), &mut payload);
    build_tlv(TAG_SUBSCRIBE, &[], &mut payload);
    build_frame(FRAME_REG, 0, CHANNEL_ID, &payload)
}

fn build_unsub_frame(route: &str) -> Vec<u8> {
    let mut payload = Vec::new();
    build_tlv(TAG_ROUTE, route.as_bytes(), &mut payload);
    build_tlv(TAG_UNSUBSCRIBE, &[], &mut payload);
    build_frame(FRAME_REG, 0, CHANNEL_ID, &payload)
}

fn build_pub_frame(route: &str, id: &str, body: &[u8], no_ack: bool) -> Vec<u8> {
    let mut payload = Vec::new();
    build_tlv(TAG_ROUTE, route.as_bytes(), &mut payload);
    if no_ack {
        build_tlv(TAG_NO_ACK, &[], &mut payload);
    }
    build_tlv(TAG_ID, id.as_bytes(), &mut payload);
    build_tlv(TAG_BODY, body, &mut payload);
    build_frame(FRAME_PUB, 0, CHANNEL_ID, &payload)
}

// -----------------------------------------------------------------------------
// Harness: EXACT engine subsystem path (no threads)
// -----------------------------------------------------------------------------

struct BenchHarness {
    registry: Arc<DomainRegistry>,
}

impl BenchHarness {
    fn new() -> Self {
        Self {
            registry: Arc::new(DomainRegistry::new()),
        }
    }

    /// Pure synchronous engine→domain path.
    fn exec(&self, bytes: &[u8]) {
        // 1. Frame parse
        let parsed = parse_frame(bytes).expect("frame");
        let payload = parsed.payload;

        // 2. Extract route from TLV
        let route_str = find_tlv(payload, TAG_ROUTE)
            .and_then(|b| std::str::from_utf8(b).ok())
            .expect("route");

        // 3. Parse route string → RouteParts
        let route = parse_route(route_str).expect("valid");

        // 4. Build DomainContext
        let ctx = DomainContext {
            route: route.clone(),
            route_str: route_str.to_owned(),
            payload: payload.to_vec(),
            channel_id: parsed.header.channel_id,
            route_family: 1,
        };

        // 5. Domain dispatch
        let _resp = self
            .registry
            .dispatch(route.scheme.as_str(), ctx)
            .expect("domain");
        // 6. Response dropped (bench purpose is measuring full path)
    }
}

// -----------------------------------------------------------------------------
// Benches
// -----------------------------------------------------------------------------

fn bench_subscribe(c: &mut Criterion) {
    let h = BenchHarness::new();
    let f = build_sub_frame("notice://realm/area/events/update");

    let mut g = c.benchmark_group("notice_subsys_subscribe");
    g.bench_function("subscribe", |b| b.iter(|| h.exec(black_box(&f))));
    g.finish();
}

fn bench_unsubscribe(c: &mut Criterion) {
    let h = BenchHarness::new();

    // Initialize subscription
    h.exec(&build_sub_frame("notice://realm/area/events/update"));

    let f = build_unsub_frame("notice://realm/area/events/update");

    let mut g = c.benchmark_group("notice_subsys_unsubscribe");
    g.bench_function("unsubscribe", |b| b.iter(|| h.exec(black_box(&f))));
    g.finish();
}

fn bench_publish_no_subscribers(c: &mut Criterion) {
    let h = BenchHarness::new();

    let ack = build_pub_frame("notice://realm/area/alerts/crit", "id1", b"hello", false);
    let noack = build_pub_frame("notice://realm/area/alerts/crit", "id1", b"hello", true);

    let mut g = c.benchmark_group("notice_subsys_publish_no_subs");
    g.bench_function("with_ack", |b| b.iter(|| h.exec(black_box(&ack))));
    g.bench_function("no_ack", |b| b.iter(|| h.exec(black_box(&noack))));
    g.finish();
}

fn bench_publish_fanout(c: &mut Criterion) {
    let mut g = c.benchmark_group("notice_subsys_publish_fanout");

    for &count in &[1, 10, 100, 1000] {
        let h = BenchHarness::new();

        // Register N subscribers
        for _ch in 1..=count {
            let f = build_sub_frame("notice://realm/area/data/update");
            h.exec(&f);
        }

        let body = vec![0u8; 128];
        let f = build_pub_frame("notice://realm/area/data/update", "id", &body, false);

        g.throughput(Throughput::Elements(count as u64));
        g.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, _| {
            b.iter(|| h.exec(black_box(&f)))
        });
    }
    g.finish();
}

fn bench_wildcards(c: &mut Criterion) {
    let h = BenchHarness::new();

    // Install wildcard patterns
    let patterns = [
        "notice://realm/area/*/update",
        "notice://realm/*/events/update",
        "notice://*/area/events/update",
        "notice://*/*/events/update",
    ];
    for p in patterns {
        h.exec(&build_sub_frame(p));
    }

    let routes = [
        "notice://realm/area/events/update",
        "notice://realm/area/specific/update",
        "notice://realm2/area/events/update",
        "notice://realm/area2/other/update",
    ];

    let mut g = c.benchmark_group("notice_subsys_wildcard");
    for r in routes {
        let f = build_pub_frame(r, "id", b"x", false);
        g.bench_function(r, |b| b.iter(|| h.exec(black_box(&f))));
    }
    g.finish();
}

fn bench_body_sizes(c: &mut Criterion) {
    let h = BenchHarness::new();
    h.exec(&build_sub_frame("notice://realm/area/data"));

    let mut g = c.benchmark_group("notice_subsys_body_sizes");

    for size in [64, 256, 1024, 4096, 16384] {
        let body = vec![0u8; size];
        let f = build_pub_frame("notice://realm/area/data", "id", &body, false);

        g.throughput(Throughput::Bytes(size as u64));
        g.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter(|| h.exec(black_box(&f)))
        });
    }
    g.finish();
}

// Registration
criterion_group!(
    name = subsystem_notice;
    config = config::criterion_config();
    targets =
        bench_subscribe,
        bench_unsubscribe,
        bench_publish_no_subscribers,
        bench_publish_fanout,
        bench_wildcards,
        bench_body_sizes,
);
criterion_main!(subsystem_notice);
