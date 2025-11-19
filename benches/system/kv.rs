//! System benchmark for KV domain.
//!
//! This measures the *real engine pipeline* end-to-end:
//!   inbound frame → crossbeam inbox → engine thread
//!   → parse_frame → TLV decode → route parse
//!   → authz → DomainRegistry::dispatch
//!   → KvDomain.handle
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

fn put_frame(route: &str, key: &str, value: &[u8]) -> Vec<u8> {
    let mut p = Vec::new();
    build_tlv(TAG_ROUTE, route.as_bytes(), &mut p);
    build_tlv(TAG_ID, key.as_bytes(), &mut p);
    build_tlv(TAG_BODY, value, &mut p);
    build_frame(FRAME_DAT, 0, CHANNEL_ID, &p)
}

fn get_frame(route: &str, key: &str) -> Vec<u8> {
    let mut p = Vec::new();
    build_tlv(TAG_ROUTE, route.as_bytes(), &mut p);
    build_tlv(TAG_ID, key.as_bytes(), &mut p);
    build_frame(FRAME_DAT, 0, CHANNEL_ID, &p)
}

fn scan_frame(route: &str, start: &str, end: &str) -> Vec<u8> {
    let mut p = Vec::new();
    build_tlv(TAG_ROUTE, route.as_bytes(), &mut p);
    build_tlv(TAG_BODY, format!("{}\n{}", start, end).as_bytes(), &mut p);
    build_frame(FRAME_DAT, 0, CHANNEL_ID, &p)
}

// -----------------------------------------------------------------------------
// Test harness — real engine with its own thread
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
        let (out_tx, mut out_rx) =
            tokio::sync::mpsc::channel::<Arc<Vec<u8>>>(OUTBOUND_QUEUE_CAPACITY);

        // Spawn a background task to drain outbound messages
        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async move {
                while let Some(_bytes) = out_rx.recv().await {
                    // Drop output (bench only measures engine path)
                }
            });
        });

        let handle = EngineHandle::new(tx, domains, registry.clone());

        // Register connection + outbound queue
        handle.register_connection(CONN_ID, out_tx);

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

fn bench_sys_put(c: &mut Criterion) {
    let h = SystemHarness::new();
    let f = put_frame("kv://realm/area/key1", "key1", b"value");

    let mut g = c.benchmark_group("kv_system_put");
    g.bench_function("put", |b| {
        b.iter(|| h.send(black_box(&f)));
    });
    g.finish();
}

fn bench_sys_get(c: &mut Criterion) {
    let h = SystemHarness::new();
    h.send(&put_frame("kv://realm/area/key1", "key1", b"value"));

    let f = get_frame("kv://realm/area/key1", "key1");

    let mut g = c.benchmark_group("kv_system_get");
    g.bench_function("get", |b| {
        b.iter(|| h.send(black_box(&f)));
    });
    g.finish();
}

fn bench_sys_scan(c: &mut Criterion) {
    let h = SystemHarness::new();
    // Pre-populate some keys
    for i in 0..10 {
        h.send(&put_frame(&format!("kv://realm/area/key{}", i), &format!("key{}", i), &format!("value{}", i).into_bytes()));
    }

    let f = scan_frame("kv://realm/area/", "key0", "key9");

    let mut g = c.benchmark_group("kv_system_scan");
    g.bench_function("scan", |b| {
        b.iter(|| h.send(black_box(&f)));
    });
    g.finish();
}

// -----------------------------------------------------------------------------
// Registration
// -----------------------------------------------------------------------------

criterion_group!(
    name = system_kv;
    config = config::criterion_config();
    targets =
        bench_sys_put,
        bench_sys_get,
        bench_sys_scan
);

criterion_main!(system_kv);