//! System benchmark for Notice domain.
//!
//! This measures the *real engine pipeline* end-to-end:
//!   inbound frame → crossbeam inbox → engine thread
//!   → parse_frame → TLV decode → route parse
//!   → authz → DomainRegistry::dispatch
//!   → NoticeDomain.handle
//!   → engine outbound delivery via mpsc (Arc<Vec<u8>>)
//!
//! This is the closest measurement to actual Fitz behavior.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
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

fn sub_frame(route: &str) -> Vec<u8> {
    let mut p = Vec::new();
    build_tlv(TAG_ROUTE, route.as_bytes(), &mut p);
    build_tlv(TAG_SUBSCRIBE, &[], &mut p);
    build_frame(FRAME_REG, 0, CHANNEL_ID, &p)
}

fn unsub_frame(route: &str) -> Vec<u8> {
    let mut p = Vec::new();
    build_tlv(TAG_ROUTE, route.as_bytes(), &mut p);
    build_tlv(TAG_UNSUBSCRIBE, &[], &mut p);
    build_frame(FRAME_REG, 0, CHANNEL_ID, &p)
}

fn pub_frame(route: &str, no_ack: bool) -> Vec<u8> {
    let mut p = Vec::new();
    build_tlv(TAG_ROUTE, route.as_bytes(), &mut p);
    if no_ack {
        build_tlv(TAG_NO_ACK, &[], &mut p);
    }
    build_tlv(TAG_ID, b"id1", &mut p);
    build_tlv(TAG_BODY, b"hello world", &mut p);
    build_frame(FRAME_PUB, 0, CHANNEL_ID, &p)
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

fn bench_sys_subscribe(c: &mut Criterion) {
    let h = SystemHarness::new();
    let f = sub_frame("notice://realm/area/events/update");

    let mut g = c.benchmark_group("notice_system_subscribe");
    g.bench_function("subscribe", |b| {
        b.iter(|| h.send(black_box(&f)));
    });
    g.finish();
}

fn bench_sys_unsubscribe(c: &mut Criterion) {
    let h = SystemHarness::new();
    h.send(&sub_frame("notice://realm/area/events/update"));

    let f = unsub_frame("notice://realm/area/events/update");

    let mut g = c.benchmark_group("notice_system_unsubscribe");
    g.bench_function("unsubscribe", |b| {
        b.iter(|| h.send(black_box(&f)));
    });
    g.finish();
}

fn bench_sys_publish_no_subs(c: &mut Criterion) {
    let h = SystemHarness::new();
    let ack = pub_frame("notice://realm/area/foo/bar", false);
    let noack = pub_frame("notice://realm/area/foo/bar", true);

    let mut g = c.benchmark_group("notice_system_publish_no_subs");
    g.bench_function("with_ack", |b| b.iter(|| h.send(black_box(&ack))));
    g.bench_function("no_ack", |b| b.iter(|| h.send(black_box(&noack))));
    g.finish();
}

fn bench_sys_fanout(c: &mut Criterion) {
    let mut g = c.benchmark_group("notice_system_fanout");

    for &count in &[1, 10, 100, 1000] {
        let h = SystemHarness::new();

        // Pre-install subscribers
        for _ch in 1..=count {
            let f = sub_frame("notice://realm/area/broadcast");
            h.send(&f);
        }

        let f = pub_frame("notice://realm/area/broadcast", false);

        g.throughput(Throughput::Elements(count as u64));
        g.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, _| {
            b.iter(|| h.send(black_box(&f)));
        });
    }

    g.finish();
}

fn bench_sys_wildcards(c: &mut Criterion) {
    let h = SystemHarness::new();

    for p in [
        "notice://realm/area/*/u",
        "notice://realm/*/events/u",
        "notice://*/area/events/u",
        "notice://*/*/events/u",
    ] {
        h.send(&sub_frame(p));
    }

    let mut g = c.benchmark_group("notice_system_wildcard");

    for r in [
        "notice://realm/area/events/u",
        "notice://realm/area/specific/u",
        "notice://realm2/area/events/u",
        "notice://realm/area2/other/u",
    ] {
        let f = pub_frame(r, false);
        g.bench_function(r, |b| b.iter(|| h.send(black_box(&f))));
    }

    g.finish();
}

// -----------------------------------------------------------------------------
// Registration
// -----------------------------------------------------------------------------

criterion_group!(
    name = system_notice;
    config = config::criterion_config();
    targets =
        bench_sys_subscribe,
        bench_sys_unsubscribe,
        bench_sys_publish_no_subs,
        bench_sys_fanout,
        bench_sys_wildcards
);

criterion_main!(system_notice);
