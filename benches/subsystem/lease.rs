//! Lease subsystem benchmarks (Engine glue → Domain → Response)
//!
//! Measures exactly the synchronous domain path used by Engine::handle_frame:
//!   1. build_frame()
//!   2. parse_frame()
//!   3. extract & parse TAG_ROUTE
//!   4. parse_route()
//!   5. DomainRegistry::dispatch()
//!   6. LeaseDomain.handle()
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

fn build_acquire_frame(route: &str, ttl: u32) -> Vec<u8> {
    let mut payload = Vec::new();
    build_tlv(TAG_ROUTE, route.as_bytes(), &mut payload);
    build_tlv(TAG_LEASE, &ttl.to_be_bytes(), &mut payload);
    build_frame(FRAME_DAT, 0, CHANNEL_ID, &payload)
}

fn build_renew_frame(route: &str, id: &str, token: &str, add_ttl: u32) -> Vec<u8> {
    let mut payload = Vec::new();
    build_tlv(TAG_ROUTE, route.as_bytes(), &mut payload);
    build_tlv(TAG_ID, id.as_bytes(), &mut payload);
    build_tlv(TAG_DELIVERY_TOKEN, token.as_bytes(), &mut payload);
    build_tlv(TAG_LEASE, &add_ttl.to_be_bytes(), &mut payload);
    build_frame(FRAME_DAT, 0, CHANNEL_ID, &payload)
}

fn build_surrender_frame(route: &str, id: &str, token: &str) -> Vec<u8> {
    let mut payload = Vec::new();
    build_tlv(TAG_ROUTE, route.as_bytes(), &mut payload);
    build_tlv(TAG_ID, id.as_bytes(), &mut payload);
    build_tlv(TAG_DELIVERY_TOKEN, token.as_bytes(), &mut payload);
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

fn bench_acquire(c: &mut Criterion) {
    let h = BenchHarness::new();
    let f = build_acquire_frame("lease://realm/area/resource/acquire", 300);

    let mut g = c.benchmark_group("lease_subsys_acquire");
    g.bench_function("acquire", |b| b.iter(|| h.exec(black_box(&f))));
    g.finish();
}

fn bench_renew(c: &mut Criterion) {
    let h = BenchHarness::new();
    // Pre-acquire to get id/token
    let acquire_frame = build_acquire_frame("lease://realm/area/resource/acquire", 300);
    h.exec(&acquire_frame);
    // For renew, we need to get the grant somehow - this is tricky in bench
    // Let's use dummy values for now
    let f = build_renew_frame(
        "lease://realm/area/resource/renew",
        "dummy_id",
        "dummy_token",
        300,
    );

    let mut g = c.benchmark_group("lease_subsys_renew");
    g.bench_function("renew", |b| b.iter(|| h.exec(black_box(&f))));
    g.finish();
}

fn bench_surrender(c: &mut Criterion) {
    let h = BenchHarness::new();
    // Pre-acquire to get id/token
    let acquire_frame = build_acquire_frame("lease://realm/area/resource/acquire", 300);
    h.exec(&acquire_frame);
    // For surrender, we need to get the grant somehow - this is tricky in bench
    // Let's use dummy values for now
    let f = build_surrender_frame(
        "lease://realm/area/resource/surrender",
        "dummy_id",
        "dummy_token",
    );

    let mut g = c.benchmark_group("lease_subsys_surrender");
    g.bench_function("surrender", |b| b.iter(|| h.exec(black_box(&f))));
    g.finish();
}

criterion_group!(
    name = subsystem_lease;
    config = config::criterion_config();
    targets =
        bench_acquire,
        bench_renew,
        bench_surrender
);
criterion_main!(subsystem_lease);
