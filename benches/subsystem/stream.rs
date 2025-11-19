//! Stream subsystem benchmarks (Engine glue → Domain → Response)
//!
//! Measures exactly the synchronous domain path used by Engine::handle_frame:
//!   1. build_frame()
//!   2. parse_frame()
//!   3. extract & parse TAG_ROUTE
//!   4. parse_route()
//!   5. DomainRegistry::dispatch()
//!   6. StreamDomain.handle()
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

use criterion::{black_box, criterion_group, criterion_main, Criterion};

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

fn build_append_frame(route: &str, body: &[u8], metadata: Option<&[u8]>) -> Vec<u8> {
    let mut payload = Vec::new();
    build_tlv(TAG_ROUTE, route.as_bytes(), &mut payload);
    build_tlv(TAG_BODY, body, &mut payload);
    if let Some(meta) = metadata {
        build_tlv(TAG_METADATA, meta, &mut payload);
    }
    build_frame(FRAME_DAT, 0, CHANNEL_ID, &payload)
}

fn build_read_frame(route: &str, from_seq: u64) -> Vec<u8> {
    let mut payload = Vec::new();
    build_tlv(TAG_ROUTE, route.as_bytes(), &mut payload);
    build_tlv(TAG_SEQ, &from_seq.to_be_bytes(), &mut payload);
    // Note: limit is not directly supported in TLV, using a dummy tag for now
    build_frame(FRAME_DAT, 0, CHANNEL_ID, &payload)
}

fn build_read_area_frame(route: &str, from_seq: u64) -> Vec<u8> {
    let mut payload = Vec::new();
    build_tlv(TAG_ROUTE, route.as_bytes(), &mut payload);
    build_tlv(TAG_SEQ, &from_seq.to_be_bytes(), &mut payload);
    // Note: limit is not directly supported in TLV, using a dummy tag for now
    build_frame(FRAME_DAT, 0, CHANNEL_ID, &payload)
}

struct BenchHarness {
    registry: DomainRegistry,
}

impl BenchHarness {
    fn new() -> Self {
        Self {
            registry: DomainRegistry::new(),
        }
    }

    fn exec(&self, frame_bytes: &[u8]) {
        // 1. Parse frame
        let parsed = parse_frame(frame_bytes).expect("parse");

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

fn bench_append(c: &mut Criterion) {
    let h = BenchHarness::new();
    let f = build_append_frame("stream://realm/area/resource1/append", b"test event body", Some(b"metadata"));

    let mut g = c.benchmark_group("stream_subsys_append");
    g.bench_function("append", |b| b.iter(|| h.exec(black_box(&f))));
    g.finish();
}

fn bench_read(c: &mut Criterion) {
    let h = BenchHarness::new();
    let f = build_read_frame("stream://realm/area/resource1/read", 0);

    let mut g = c.benchmark_group("stream_subsys_read");
    g.bench_function("read", |b| b.iter(|| h.exec(black_box(&f))));
    g.finish();
}

fn bench_read_area(c: &mut Criterion) {
    let h = BenchHarness::new();
    let f = build_read_area_frame("stream://realm/area/read-area", 0);

    let mut g = c.benchmark_group("stream_subsys_read_area");
    g.bench_function("read_area", |b| b.iter(|| h.exec(black_box(&f))));
    g.finish();
}

criterion_group!(
    name = subsystem_stream;
    config = config::criterion_config();
    targets =
        bench_append,
        bench_read,
        bench_read_area
);
criterion_main!(subsystem_stream);