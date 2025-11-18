//! Hotpath benchmarks for NoticeService.
//!
//! Measures ONLY the internal logic of the Notice domain:
//!   - subscriber registration
//!   - unsubscribe
//!   - route matching (exact + wildcard)
//!   - fanout list construction
//!
//! Zero frame parsing, zero engine, zero outbound delivery.
//! This is the true "business logic" bench.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use fitz::core::notice::NoticeService;
use fitz::routing::DEFAULT_RF;

#[path = "../config.rs"]
mod config;

const CHANNEL_ID: u32 = 1;

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

fn subscribe(svc: &mut NoticeService, pattern: &str, channel: u32) -> u64 {
    svc.subscribe(DEFAULT_RF, pattern.to_string(), channel)
}

fn unsubscribe(svc: &mut NoticeService, sub_id: u64) -> bool {
    svc.unsubscribe(DEFAULT_RF, sub_id)
}

fn publish_lookup(svc: &NoticeService, route: &str) {
    let _result = svc.publish(DEFAULT_RF, route, Some("msg-1"), b"test");
}

// -----------------------------------------------------------------------------
// Benchmarks
// -----------------------------------------------------------------------------

fn bench_hot_subscribe(c: &mut Criterion) {
    let mut group = c.benchmark_group("notice_hot_subscribe");
    group.bench_function("subscribe", |b| {
        b.iter_batched(
            || NoticeService::new(),
            |mut svc| {
                subscribe(&mut svc, black_box("notice://realm/area/events/update"), CHANNEL_ID)
            },
            criterion::BatchSize::SmallInput,
        )
    });
    group.finish();
}

fn bench_hot_unsubscribe(c: &mut Criterion) {
    let mut group = c.benchmark_group("notice_hot_unsubscribe");
    group.bench_function("unsubscribe", |b| {
        b.iter_batched(
            || {
                let mut svc = NoticeService::new();
                let sub_id = subscribe(&mut svc, "notice://realm/area/events/update", CHANNEL_ID);
                (svc, sub_id)
            },
            |(mut svc, sub_id)| unsubscribe(&mut svc, black_box(sub_id)),
            criterion::BatchSize::SmallInput,
        )
    });
    group.finish();
}

fn bench_hot_publish_no_subs(c: &mut Criterion) {
    let svc = NoticeService::new();
    let route = "notice://realm/area/events/no_subscribers";

    let mut group = c.benchmark_group("notice_hot_publish_no_subs");
    group.bench_function("publish_no_subscribers", |b| {
        b.iter(|| publish_lookup(&svc, black_box(route)))
    });
    group.finish();
}

fn bench_hot_publish_with_subs(c: &mut Criterion) {
    let mut group = c.benchmark_group("notice_hot_publish_with_subs");

    for &count in &[1, 10, 100, 1000] {
        let mut svc = NoticeService::new();
        let pattern = "notice://realm/area/broadcast/alert";

        for ch in 1..=count {
            subscribe(&mut svc, pattern, ch);
        }

        let target = "notice://realm/area/broadcast/alert";

        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, _| {
            b.iter(|| publish_lookup(&svc, black_box(target)));
        });
    }

    group.finish();
}

fn bench_hot_wildcards(c: &mut Criterion) {
    let mut svc = NoticeService::new();

    // wildcard patterns
    subscribe(&mut svc, "notice://realm/area/*/update", 10);
    subscribe(&mut svc, "notice://realm/*/events/update", 11);
    subscribe(&mut svc, "notice://*/area/events/update", 12);
    subscribe(&mut svc, "notice://*/*/events/update", 13);

    let routes = [
        "notice://realm/area/events/update",
        "notice://realm/area/specific/update",
        "notice://realm2/area/events/update",
        "notice://realm/area2/other/update",
    ];

    let mut group = c.benchmark_group("notice_hot_wildcards");

    for r in &routes {
        group.bench_function(*r, |b| {
            b.iter(|| publish_lookup(&svc, black_box(r)));
        });
    }

    group.finish();
}

// -----------------------------------------------------------------------------
// Registration
// -----------------------------------------------------------------------------

criterion_group!(
    name = hotpath_notice;
    config = config::criterion_config();
    targets =
        bench_hot_subscribe,
        bench_hot_unsubscribe,
        bench_hot_publish_no_subs,
        bench_hot_publish_with_subs,
        bench_hot_wildcards
);
criterion_main!(hotpath_notice);
