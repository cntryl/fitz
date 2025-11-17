//! Hotpath benchmarks for NoticeDomain
//!
//! This version:
//! - no setup in the timed region
//! - domains + subscribers built once
//! - payloads and contexts prebuilt
//! - pure handler cost

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::sync::Arc;

use fitz::core::domain::{Domain, DomainContext};
use fitz::core::notice::NoticeDomain;
use fitz::protocol::route::{Route, Scheme};
use fitz::protocol::tags::{TAG_BODY, TAG_ID, TAG_NO_ACK};

#[path = "../config.rs"]
mod config;

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

fn build_route_str(r: &str, a: &str, res: &str, op: &str) -> String {
    format!("notice://{}/{}/{}/{}", r, a, res, op)
}

fn build_route(r: &str, a: &str, res: &str, op: &str) -> Route {
    Route {
        scheme: Scheme::Notice,
        realm: Some(r.to_string()),
        area: Some(a.to_string()),
        resource: Some(res.to_string()),
        operation: Some(op.to_string()),
        raw: build_route_str(r, a, res, op),
    }
}

fn build_payload(id: Option<&str>, body: &[u8], no_ack: bool) -> Vec<u8> {
    let mut out = Vec::new();

    if no_ack {
        out.push(TAG_NO_ACK);
        out.push(0);
    }

    if let Some(id) = id {
        out.push(TAG_ID);
        out.push(id.len() as u8);
        out.extend_from_slice(id.as_bytes());
    }

    out.push(TAG_BODY);
    if body.len() <= 254 {
        out.push(body.len() as u8);
        out.extend_from_slice(body);
    } else {
        out.push(255);
        out.extend_from_slice(&(body.len() as u32).to_be_bytes());
        out.extend_from_slice(body);
    }

    out
}

// -----------------------------------------------------------------------------
// Benchmarks
// -----------------------------------------------------------------------------

// 1. Sequential publish (no subscribers)
fn bench_publish_no_subscribers(c: &mut Criterion) {
    let domain = NoticeDomain::new();
    let route = build_route("realm1", "area1", "alerts", "critical");
    let route_str = route.raw.clone();

    let payload = build_payload(Some("id"), b"hello", false);

    let ctx = DomainContext {
        route,
        route_str,
        payload,
        channel_id: 1,
        route_family: 0,
    };

    let mut group = c.benchmark_group("notice_publish_no_subscribers");

    group.bench_function("ack", |b| {
        b.iter(|| {
            black_box(domain.handle(ctx.clone()));
        });
    });

    let mut ctx_noack = ctx.clone();
    ctx_noack.payload = build_payload(Some("id"), b"hello", true);

    group.bench_function("no_ack", |b| {
        b.iter(|| {
            black_box(domain.handle(ctx_noack.clone()));
        });
    });

    group.finish();
}

// 2. Message size impact (prebuilt contexts per size)
fn bench_message_sizes(c: &mut Criterion) {
    let domain = NoticeDomain::new();

    let mut group = c.benchmark_group("notice_message_sizes");

    for size in [64, 256, 1024, 4096, 16384] {
        let body = vec![0u8; size];
        let payload = build_payload(Some("id"), &body, false);

        let route = build_route("realm1", "area1", "data", "stream");
        let route_str = route.raw.clone();

        let ctx = DomainContext {
            route,
            route_str,
            payload,
            channel_id: 1,
            route_family: 0,
        };

        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter(|| {
                black_box(domain.handle(ctx.clone()));
            });
        });
    }

    group.finish();
}

// 3. Wildcard match hotpath (subscriptions created once)
fn bench_wildcard_matching(c: &mut Criterion) {
    let domain = NoticeDomain::new();
    let service = domain.get_service();

    // create wildcard subscribers once
    {
        let mut svc = service.write();
        let _ = svc.subscribe(0, "notice://realm1/area1/*/update".to_string(), 1);
        let _ = svc.subscribe(0, "notice://realm1/*/events/update".to_string(), 2);
        let _ = svc.subscribe(0, "notice://*/area1/events/update".to_string(), 3);
        let _ = svc.subscribe(0, "notice://*/*/events/update".to_string(), 4);
    }

    let patterns = [
        ("realm1", "area1", "events", "update"),
        ("realm1", "area1", "*", "update"),
        ("realm1", "*", "*", "update"),
    ];

    let mut group = c.benchmark_group("notice_wildcard_matching");

    for (r, a, res, op) in patterns {
        let route = build_route(r, a, res, op);
        let route_str = route.raw.clone();
        let payload = build_payload(Some("id"), b"test", false);

        let ctx = DomainContext {
            route,
            route_str,
            payload,
            channel_id: 99,
            route_family: 0,
        };

        group.bench_function(format!("{}/{}/{}/{}", r, a, res, op), |b| {
            b.iter(|| {
                black_box(domain.handle(ctx.clone()));
            });
        });
    }

    group.finish();
}

// 4. Fanout: many subscribers (register once)
fn bench_broadcast_fanout(c: &mut Criterion) {
    let mut group = c.benchmark_group("notice_broadcast_fanout");

    for &sub_count in &[10, 100, 1000] {
        let domain = NoticeDomain::new();
        let service = domain.get_service();

        // register subscribers ONCE
        {
            let mut svc = service.write();
            for ch in 0..sub_count {
                let _ = svc.subscribe(
                    0,
                    "notice://realm1/area1/broadcast/alert".to_string(),
                    ch as u32,
                );
            }
        }

        // publish ctx
        let body = vec![0u8; 128];
        let payload = build_payload(Some("id"), &body, false);
        let route = build_route("realm1", "area1", "broadcast", "alert");
        let route_str = route.raw.clone();

        let ctx = DomainContext {
            route,
            route_str,
            payload,
            channel_id: 9999,
            route_family: 0,
        };

        group.bench_with_input(
            BenchmarkId::from_parameter(sub_count),
            &sub_count,
            |b, _| {
                b.iter(|| black_box(domain.handle(ctx.clone())));
            },
        );
    }

    group.finish();
}

// -----------------------------------------------------------------------------
// Criterion registration
// -----------------------------------------------------------------------------

criterion_group!(
    name = hotpath_notice_core;
    config = config::criterion_config();
    targets =
        bench_publish_no_subscribers,
        bench_message_sizes,
        bench_wildcard_matching,
        bench_broadcast_fanout
);
criterion_main!(hotpath_notice_core);
