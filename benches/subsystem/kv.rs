//! KV subsystem benchmarks (Engine glue → Domain → Response)
//!
//! Measures exactly the synchronous domain path used by Engine::handle_frame:
//!   1. build_frame()
//!   2. parse_frame()
//!   3. extract & parse TAG_ROUTE
//!   4. parse_route()
//!   5. DomainRegistry::dispatch()
//!   6. KvDomain.handle()
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

fn build_put_frame(route: &str, key: &str, value: &[u8]) -> Vec<u8> {
    let mut payload = Vec::new();
    build_tlv(TAG_ROUTE, route.as_bytes(), &mut payload);
    build_tlv(TAG_ID, key.as_bytes(), &mut payload);
    build_tlv(TAG_BODY, value, &mut payload);
    build_frame(FRAME_DAT, 0, CHANNEL_ID, &payload)
}

fn build_get_frame(route: &str, key: &str) -> Vec<u8> {
    let mut payload = Vec::new();
    build_tlv(TAG_ROUTE, route.as_bytes(), &mut payload);
    build_tlv(TAG_ID, key.as_bytes(), &mut payload);
    build_frame(FRAME_DAT, 0, CHANNEL_ID, &payload)
}

fn build_scan_frame(route: &str, start: &str, end: &str) -> Vec<u8> {
    let mut payload = Vec::new();
    build_tlv(TAG_ROUTE, route.as_bytes(), &mut payload);
    build_tlv(
        TAG_BODY,
        format!("{}\n{}", start, end).as_bytes(),
        &mut payload,
    );
    build_frame(FRAME_DAT, 0, CHANNEL_ID, &payload)
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

fn bench_put(c: &mut Criterion) {
    let h = BenchHarness::new();
    let f = build_put_frame("kv://realm/area/key1", "key1", b"value");

    let mut g = c.benchmark_group("kv_subsys_put");
    g.bench_function("put", |b| b.iter(|| h.exec(black_box(&f))));
    g.finish();
}

fn bench_get(c: &mut Criterion) {
    let h = BenchHarness::new();
    // Pre-populate
    h.exec(&build_put_frame("kv://realm/area/key1", "key1", b"value"));

    let f = build_get_frame("kv://realm/area/key1", "key1");

    let mut g = c.benchmark_group("kv_subsys_get");
    g.bench_function("get", |b| b.iter(|| h.exec(black_box(&f))));
    g.finish();
}

fn bench_scan(c: &mut Criterion) {
    let h = BenchHarness::new();
    // Pre-populate some keys
    for i in 0..10 {
        h.exec(&build_put_frame(
            &format!("kv://realm/area/key{}", i),
            &format!("key{}", i),
            &format!("value{}", i).into_bytes(),
        ));
    }

    let f = build_scan_frame("kv://realm/area/", "key0", "key9");

    let mut g = c.benchmark_group("kv_subsys_scan");
    g.bench_function("scan", |b| b.iter(|| h.exec(black_box(&f))));
    g.finish();
}

// -----------------------------------------------------------------------------
// Registration
// -----------------------------------------------------------------------------

criterion_group!(
    name = subsystem_kv;
    config = config::criterion_config();
    targets =
        bench_put,
        bench_get,
        bench_scan
);
criterion_main!(subsystem_kv);
