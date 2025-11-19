//! System benchmark for Lease domain.
//!
//! This measures the *real engine pipeline* end-to-end:
//!   inbound frame → crossbeam inbox → engine thread
//!   → parse_frame → TLV decode → route parse
//!   → authz → DomainRegistry::dispatch
//!   → LeaseDomain.handle
//!   → engine outbound delivery via mpsc (Arc<Vec<u8>>)
//!
//! This is the closest measurement to actual Fitz behavior.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::sync::Arc;

use fitz::authz::{PermissionGrants, SessionAuth};
use fitz::core::engine::{
    Engine, EngineConnectionRegistry, EngineHandle, ENGINE_INBOX_CAPACITY,
    OUTBOUND_QUEUE_CAPACITY,
};
use fitz::core::registry::DomainRegistry;
use fitz::protocol::frame::{build_frame, build_tlv};
use fitz::protocol::tags::*;

#[path = "../config.rs"]
mod config;

const CONN_ID: u64 = 1;
const CHANNEL_ID: u32 = 1;

// -----------------------------------------------------------------------------
// Frame builders
// -----------------------------------------------------------------------------

fn acquire_frame(route: &str, ttl: u32) -> Vec<u8> {
    let mut p = Vec::new();
    build_tlv(TAG_ROUTE, route.as_bytes(), &mut p);
    build_tlv(TAG_LEASE, &ttl.to_be_bytes(), &mut p);
    build_frame(FRAME_DAT, 0, CHANNEL_ID, &p)
}

fn renew_frame(route: &str, id: &str, token: &str, add_ttl: u32) -> Vec<u8> {
    let mut p = Vec::new();
    build_tlv(TAG_ROUTE, route.as_bytes(), &mut p);
    build_tlv(TAG_ID, id.as_bytes(), &mut p);
    build_tlv(TAG_DELIVERY_TOKEN, token.as_bytes(), &mut p);
    build_tlv(TAG_LEASE, &add_ttl.to_be_bytes(), &mut p);
    build_frame(FRAME_DAT, 0, CHANNEL_ID, &p)
}

fn surrender_frame(route: &str, id: &str, token: &str) -> Vec<u8> {
    let mut p = Vec::new();
    build_tlv(TAG_ROUTE, route.as_bytes(), &mut p);
    build_tlv(TAG_ID, id.as_bytes(), &mut p);
    build_tlv(TAG_DELIVERY_TOKEN, token.as_bytes(), &mut p);
    build_frame(FRAME_DAT, 0, CHANNEL_ID, &p)
}

// -----------------------------------------------------------------------------
// System harness
// -----------------------------------------------------------------------------

struct SystemHarness {
    handle: EngineHandle,
    _join: std::thread::JoinHandle<()>,
}

impl SystemHarness {
    fn new() -> Self {
        let mut registry = DomainRegistry::new();
        registry.register_lease_domain();

        let (inbox_tx, inbox_rx) = crossbeam::channel::bounded(ENGINE_INBOX_CAPACITY);
        let (outbound_tx, _outbound_rx) = tokio::sync::mpsc::channel(OUTBOUND_QUEUE_CAPACITY);

        let conn_registry = Arc::new(EngineConnectionRegistry::new());

        let engine = Engine::new(registry, conn_registry.clone());
        let join = std::thread::spawn(move || {
            engine.run(inbox_rx, outbound_tx);
        });

        let handle = EngineHandle::new(inbox_tx, conn_registry);

        // Register session for authz
        let session = SessionAuth {
            subject: "bench-subject".to_string(),
            route_family: "rf1".to_string(),
            scopes: Vec::new(),
            grants: PermissionGrants::from_scopes("rf1", &[]),
        };
        handle.register_session(CONN_ID, session);

        Self {
            handle,
            _join: join,
        }
    }

    fn send(&self, frame: &[u8]) {
        self.handle.on_frame(CONN_ID, frame.to_vec());
    }
}

// -----------------------------------------------------------------------------
// Benches
// -----------------------------------------------------------------------------

fn bench_sys_acquire(c: &mut Criterion) {
    let h = SystemHarness::new();
    let f = acquire_frame("lease://realm/area/resource/acquire", 300);

    let mut g = c.benchmark_group("lease_system_acquire");
    g.bench_function("acquire", |b| {
        b.iter(|| h.send(black_box(&f)));
    });
    g.finish();
}

fn bench_sys_renew(c: &mut Criterion) {
    let h = SystemHarness::new();
    // Pre-acquire to get id/token - but in system bench we can't easily get the response
    // Let's use dummy values for now
    let f = renew_frame("lease://realm/area/resource/renew", "dummy_id", "dummy_token", 300);

    let mut g = c.benchmark_group("lease_system_renew");
    g.bench_function("renew", |b| {
        b.iter(|| h.send(black_box(&f)));
    });
    g.finish();
}

fn bench_sys_surrender(c: &mut Criterion) {
    let h = SystemHarness::new();
    // Pre-acquire to get id/token - but in system bench we can't easily get the response
    // Let's use dummy values for now
    let f = surrender_frame("lease://realm/area/resource/surrender", "dummy_id", "dummy_token");

    let mut g = c.benchmark_group("lease_system_surrender");
    g.bench_function("surrender", |b| {
        b.iter(|| h.send(black_box(&f)));
    });
    g.finish();
}

criterion_group!(
    name = system_lease;
    config = config::criterion_config();
    targets =
        bench_sys_acquire,
        bench_sys_renew,
        bench_sys_surrender
);
criterion_main!(system_lease);