use criterion::{
    black_box, criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use fitz::domains::notification::bench::Matcher;
use fitz::domains::notification::protocol::NotifyMessage;
use fitz::protocol::tlv::MessageType;
use fitz::runtime::routing::Route;
use bytes::Bytes;
use std::sync::Arc;

#[path = "config.rs"]
mod config;

fn bench_matcher_lookup(c: &mut Criterion) {
    let mut matcher = Matcher::new();
    // register many subscribers for a single msg type
    for i in 0..64usize {
        matcher.register(100, i);
    }

    let payload = vec![0u8; 64];

    let mut group = c.benchmark_group("hotpath_notification_match");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("match_into_64subs", |b| {
        b.iter(|| {
            let mut out: smallvec::SmallVec<[usize; 8]> = smallvec::SmallVec::new();
            let n = matcher.match_into(&mut out, MessageType::new(100), black_box(&payload));
            black_box(n);
        })
    });

    group.finish();
}

/// Benchmark Arc-based fanout to measure improvement from zero-allocation design
fn bench_fanout_scaling(c: &mut Criterion) {
    // Precompute test data
    let route = Route::new("notice://realm/area/events");
    let payload = Bytes::from_static(b"test payload content here");

    let mut group = c.benchmark_group("hotpath_notification_fanout");
    group.sampling_mode(SamplingMode::Flat);

    // Test scaling with subscriber counts: 1, 10, 100, 1000
    for sub_count in [1, 10, 100, 1000] {
        group.throughput(Throughput::Elements(sub_count));

        // Old approach (per-subscriber clone) - DEPRECATED, kept for comparison only
        group.bench_with_input(
            BenchmarkId::new("old_clone_per_subscriber", sub_count),
            &sub_count,
            |b, &count| {
                b.iter(|| {
                    let route_ref = &route;
                    let payload_ref = &payload;
                    
                    // Simulate per-subscriber clone (OLD behavior)
                    let mut messages = Vec::with_capacity(count as usize);
                    for _ in 0..count {
                        let notify = NotifyMessage::new(
                            route_ref.clone(),
                            payload_ref.clone(),
                        );
                        messages.push(black_box(notify));
                    }
                    messages.len()
                })
            },
        );

        // New Arc-based approach (zero-allocation fanout)
        group.bench_with_input(
            BenchmarkId::new("arc_shared", sub_count),
            &sub_count,
            |b, &count| {
                b.iter(|| {
                    let route_ref = &route;
                    let payload_ref = &payload;
                    
                    // Create Arc once, share for all subscribers (NEW behavior)
                    let route_arc = Arc::new(route_ref.clone());
                    let payload_arc = Arc::new(payload_ref.clone());
                    
                    let mut messages = Vec::with_capacity(count as usize);
                    for _ in 0..count {
                        let notify = NotifyMessage::new_shared(
                            Arc::clone(&route_arc),
                            Arc::clone(&payload_arc),
                        );
                        messages.push(black_box(notify));
                    }
                    messages.len()
                })
            },
        );
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = config::criterion_config();
    targets = bench_matcher_lookup, bench_fanout_scaling
}
criterion_main!(benches);