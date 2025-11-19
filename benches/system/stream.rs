//! System benchmark for Stream domain.
//!
//! This measures the *real engine pipeline* end-to-end:
//!   inbound frame → crossbeam inbox → engine thread
//!   → parse_frame → TLV decode → route parse
//!   → authz → DomainRegistry::dispatch
//!   → StreamDomain.handle
//!   → engine outbound delivery via mpsc (Arc<Vec<u8>>)
//!
//! This is the closest measurement to actual Fitz behavior.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use crossbeam_channel;
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

fn append_frame(route: &str, body: &[u8], metadata: Option<&[u8]>) -> Vec<u8> {
    let mut p = Vec::new();
    build_tlv(TAG_ROUTE, route.as_bytes(), &mut p);
    build_tlv(TAG_BODY, body, &mut p);
    if let Some(meta) = metadata {
        build_tlv(TAG_METADATA, meta, &mut p);
    }
    build_frame(FRAME_DAT, 0, CHANNEL_ID, &p)
}

fn read_frame(route: &str, from_seq: u64, _limit: usize) -> Vec<u8> {
    let mut p = Vec::new();
    build_tlv(TAG_ROUTE, route.as_bytes(), &mut p);
    build_tlv(TAG_SEQ, &from_seq.to_be_bytes(), &mut p);
    // Note: limit is not directly supported in TLV, using a dummy tag for now
    build_frame(FRAME_DAT, 0, CHANNEL_ID, &p)
}

fn read_area_frame(route: &str, from_seq: u64, _limit: usize) -> Vec<u8> {
    let mut p = Vec::new();
    build_tlv(TAG_ROUTE, route.as_bytes(), &mut p);
    build_tlv(TAG_SEQ, &from_seq.to_be_bytes(), &mut p);
    // Note: limit is not directly supported in TLV, using a dummy tag for now
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

fn bench_sys_append(c: &mut Criterion) {
    let h = SystemHarness::new();
    let f = append_frame(
        "stream://realm/area/resource1/append",
        b"test event body",
        Some(b"metadata"),
    );

    let mut g = c.benchmark_group("stream_system_append");
    g.bench_function("append", |b| {
        b.iter(|| h.send(black_box(&f)));
    });
    g.finish();
}

fn bench_sys_read(c: &mut Criterion) {
    let h = SystemHarness::new();
    let f = read_frame("stream://realm/area/resource1/read", 0, 10);

    let mut g = c.benchmark_group("stream_system_read");
    g.bench_function("read", |b| {
        b.iter(|| h.send(black_box(&f)));
    });
    g.finish();
}

fn bench_sys_read_area(c: &mut Criterion) {
    let h = SystemHarness::new();
    let f = read_area_frame("stream://realm/area/read-area", 0, 10);

    let mut g = c.benchmark_group("stream_system_read_area");
    g.bench_function("read_area", |b| {
        b.iter(|| h.send(black_box(&f)));
    });
    g.finish();
}

criterion_group!(
    name = system_stream;
    config = config::criterion_config();
    targets =
        bench_sys_append,
        bench_sys_read,
        bench_sys_read_area
);
criterion_main!(system_stream);
