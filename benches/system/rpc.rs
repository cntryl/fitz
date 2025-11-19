//! System benchmark for RPC domain.
//!
//! This measures the *real engine pipeline* end-to-end:
//!   inbound frame → crossbeam inbox → engine thread
//!   → parse_frame → TLV decode → route parse
//!   → authz → DomainRegistry::dispatch
//!   → RpcDomain.handle
//!   → engine outbound delivery via mpsc (Arc<Vec<u8>>)
//!
//! This is the closest measurement to actual Fitz behavior.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::sync::Arc;

use fitz::authz::{PermissionGrants, SessionAuth};
use fitz::core::engine::{
    Engine, EngineConnectionRegistry, EngineHandle, ENGINE_INBOX_CAPACITY, OUTBOUND_QUEUE_CAPACITY,
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

fn subscribe_frame(route: &str) -> Vec<u8> {
    let mut p = Vec::new();
    build_tlv(TAG_ROUTE, route.as_bytes(), &mut p);
    build_tlv(TAG_SUBSCRIBE, &[], &mut p);
    build_frame(FRAME_DAT, 0, CHANNEL_ID, &p)
}

fn unsubscribe_frame(route: &str) -> Vec<u8> {
    let mut p = Vec::new();
    build_tlv(TAG_ROUTE, route.as_bytes(), &mut p);
    build_tlv(TAG_UNSUBSCRIBE, &[], &mut p);
    build_frame(FRAME_DAT, 0, CHANNEL_ID, &p)
}

fn request_frame(route: &str, correlation_id: &str, reply_route: &str, body: &[u8]) -> Vec<u8> {
    let mut p = Vec::new();
    build_tlv(TAG_ROUTE, route.as_bytes(), &mut p);
    build_tlv(TAG_ID, correlation_id.as_bytes(), &mut p);
    build_tlv(TAG_ROUTE_REPLY, reply_route.as_bytes(), &mut p);
    build_tlv(TAG_BODY, body, &mut p);
    build_frame(FRAME_DAT, 0, CHANNEL_ID, &p)
}

fn reply_frame(route: &str, correlation_id: &str, body: &[u8]) -> Vec<u8> {
    let mut p = Vec::new();
    build_tlv(TAG_ROUTE, route.as_bytes(), &mut p);
    build_tlv(TAG_ID, correlation_id.as_bytes(), &mut p);
    build_tlv(TAG_BODY, body, &mut p);
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
        // Shared domain registry
        let domains = Arc::new(DomainRegistry::new());
        let registry = Arc::new(EngineConnectionRegistry::new());

        // Bounded inbox for this shard
        let (tx, rx) = crossbeam_channel::bounded(ENGINE_INBOX_CAPACITY);

        // Engine instance
        let engine = Engine::new(rx, Arc::clone(&registry), Arc::clone(&domains));

        // Spawn engine thread
        let join = std::thread::spawn(move || {
            engine.run();
        });

        // Outbound mpsc queue (just drains into void)
        let (_out_tx, mut out_rx) =
            tokio::sync::mpsc::channel::<Arc<Vec<u8>>>(OUTBOUND_QUEUE_CAPACITY);

        // Spawn a background task to drain outbound messages
        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async move {
                while let Some(_bytes) = out_rx.recv().await {
                    // Drain outbound messages
                }
            });
        });

        let handle = EngineHandle::new(tx, Arc::clone(&domains), Arc::clone(&registry));

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

fn bench_sys_subscribe(c: &mut Criterion) {
    let h = SystemHarness::new();
    let f = subscribe_frame("rpc://realm/area/handler");

    let mut g = c.benchmark_group("rpc_system_subscribe");
    g.bench_function("subscribe", |b| {
        b.iter(|| h.send(black_box(&f)));
    });
    g.finish();
}

fn bench_sys_unsubscribe(c: &mut Criterion) {
    let h = SystemHarness::new();
    let f = unsubscribe_frame("rpc://realm/area/handler");

    let mut g = c.benchmark_group("rpc_system_unsubscribe");
    g.bench_function("unsubscribe", |b| {
        b.iter(|| h.send(black_box(&f)));
    });
    g.finish();
}

fn bench_sys_route_request(c: &mut Criterion) {
    let h = SystemHarness::new();
    let f = request_frame(
        "rpc://realm/area/handler",
        "corr123",
        "inbox://client/inbox",
        b"test body",
    );

    let mut g = c.benchmark_group("rpc_system_route_request");
    g.bench_function("route_request", |b| {
        b.iter(|| h.send(black_box(&f)));
    });
    g.finish();
}

fn bench_sys_route_reply(c: &mut Criterion) {
    let h = SystemHarness::new();
    let f = reply_frame("inbox://client/inbox", "corr123", b"reply body");

    let mut g = c.benchmark_group("rpc_system_route_reply");
    g.bench_function("route_reply", |b| {
        b.iter(|| h.send(black_box(&f)));
    });
    g.finish();
}

criterion_group!(
    name = system_rpc;
    config = config::criterion_config();
    targets =
        bench_sys_subscribe,
        bench_sys_unsubscribe,
        bench_sys_route_request,
        bench_sys_route_reply
);
criterion_main!(system_rpc);
