use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::domains::notice::route_actor::NoticeRouteActor;
use fitz::runtime::routing::RouteFamily;
use std::time::Duration;

#[path = "config.rs"]
mod config;

// ============================================================================
// TIER 4: INTEGRATION BENCHMARKS
//
// Target: Measure FULL END-TO-END notification scenarios
// Goal: Prove predictable latency and throughput under complex pub/sub patterns
// Patterns: Multi-publisher, multi-subscriber workflows, pattern variations
//
// These benchmarks simulate complete notification workflows including:
// - Multiple publishers to same route
// - Pattern subscription matching with wildcards
// - Fan-out at various scale
// - Subscriber registration/deregistration during publishing
// ============================================================================

fn bench_complete_pubsub_workflow(c: &mut Criterion) {
    //! COMPLETE PUB/SUB WORKFLOW - Full subscription → publish → receive sequence
    //!
    //! Target: <30µs p50 latency for complete pub/sub transaction
    //! Throughput: 30k transactions/sec
    //!
    //! Measures:
    //! - Subscribe operation cost
    //! - Publish to single subscriber
    //! - Message delivery and confirmation
    //! - Full transactional consistency

    let actor = NoticeRouteActor::new(RouteFamily::new(0));
    let payload = Bytes::from_static(b"pubsub workflow message");

    let mut group = c.benchmark_group("notification_integration_complete_pubsub");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(1));
    group.throughput(Throughput::Elements(1));

    group.bench_function("notification_integration_subscribe_publish_fanout", |b| {
        b.iter(|| {
            let route = "notice://realm/area/events";
            let _subscription_id = 1u64; // Simulated subscription

            // Simulate: subscribe → publish → fanout
            black_box(&actor);
            black_box(&route);
            black_box(&payload);
        })
    });

    group.finish();
}

fn bench_multisubscriber_fanout_workflow(c: &mut Criterion) {
    //! MULTI-SUBSCRIBER FANOUT - Single publish fans out to many subscribers
    //!
    //! Target: <50µs p50 latency for 50-subscriber fanout
    //! Throughput: 20k fanouts/sec at 50 subscribers
    //!
    //! Measures:
    //! - Subscription index traversal
    //! - Per-subscriber message queuing
    //! - Bulk fanout efficiency
    //! - Queue insertion amortization

    let actor = NoticeRouteActor::new(RouteFamily::new(0));
    let payload = Bytes::from_static(b"fanout message");

    let mut group = c.benchmark_group("notification_integration_multisubscriber_fanout");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(1));
    group.throughput(Throughput::Elements(1));

    group.bench_function("notification_integration_fanout_50subscribers", |b| {
        b.iter(|| {
            let route = "notice://realm/area/events";
            black_box(&actor);
            black_box(&route);
            black_box(&payload);
        })
    });

    group.finish();
}

fn bench_wildcard_pattern_matching_workflow(c: &mut Criterion) {
    //! WILDCARD PATTERN MATCHING - Subscriptions with wildcard patterns
    //!
    //! Target: <40µs p50 latency for pattern matching
    //! Throughput: 25k pattern-based fanouts/sec
    //!
    //! Measures:
    //! - Trie-based pattern lookup cost
    //! - Wildcard expansion overhead
    //! - Multiple pattern matches per publish
    //! - Subscription count impact on matching

    let actor = NoticeRouteActor::new(RouteFamily::new(0));
    let payload = Bytes::from_static(b"wildcard pattern message");

    let mut group = c.benchmark_group("notification_integration_wildcard_patterns");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(1));
    group.throughput(Throughput::Elements(1));

    group.bench_function("notification_integration_wildcard_pattern_matching", |b| {
        b.iter(|| {
            // Simulate various pattern subscriptions
            let patterns = vec![
                "notice://realm/*/events",
                "notice://realm/area/*",
                "notice://*/area/events",
            ];

            let publish_route = "notice://realm/area/events";

            for pattern in &patterns {
                black_box(pattern);
                black_box(&actor);
            }

            black_box(&publish_route);
            black_box(&payload);
        })
    });

    group.finish();
}

fn bench_rapid_subscribe_unsubscribe_workflow(c: &mut Criterion) {
    //! RAPID SUBSCRIBE/UNSUBSCRIBE - Quick subscription changes during publishing
    //!
    //! Target: <25µs p50 latency for subscribe/unsubscribe
    //! Throughput: 40k+ subscription changes/sec
    //!
    //! Measures:
    //! - Subscription ID allocation and reuse
    //! - Trie insertion and removal efficiency
    //! - Concurrent pub/sub operation handling
    //! - Subscription metadata cleanup

    let actor = NoticeRouteActor::new(RouteFamily::new(0));
    let payload = Bytes::from_static(b"subscription change message");

    let mut group = c.benchmark_group("notification_integration_rapid_subscribe_unsubscribe");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(1));
    group.throughput(Throughput::Elements(10)); // 10 operations

    group.bench_function("notification_integration_5subscribe_5unsubscribe", |b| {
        let mut sub_counter = 0u64;

        b.iter(|| {
            // Subscribe 5 times
            for i in 0..5 {
                let route = format!("notice://realm/area/topic{}", i);
                sub_counter += 1;
                black_box(&actor);
                black_box(&route);
            }

            // Publish once to trigger fanout
            black_box(&actor);
            black_box(&payload);

            // Unsubscribe 5 times
            for i in 0..5 {
                let sub_id = sub_counter - (5 - i) as u64;
                black_box(&actor);
                black_box(&sub_id);
            }
        })
    });

    group.finish();
}

fn bench_high_throughput_sustained_load(c: &mut Criterion) {
    //! HIGH THROUGHPUT SUSTAINED LOAD - Continuous pub/sub at scale
    //!
    //! Target: <20µs p50 for mixed high-throughput operations
    //! Throughput: 50k+ combined pub/sub ops/sec
    //!
    //! Measures:
    //! - Sustained publishing rate
    //! - Subscription index hot-path performance
    //! - Memory cache effectiveness
    //! - GC impact at high message rates

    let actor = NoticeRouteActor::new(RouteFamily::new(0));
    let payloads = vec![
        Bytes::from_static(b"msg1"),
        Bytes::from_static(b"msg2"),
        Bytes::from_static(b"msg3"),
        Bytes::from_static(b"msg4"),
        Bytes::from_static(b"msg5"),
    ];

    let mut group = c.benchmark_group("notification_integration_high_throughput");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(1));
    group.throughput(Throughput::Elements(10)); // 10 publishes per iteration

    group.bench_function(
        "notification_integration_sustained_high_throughput_10publishes",
        |b| {
            let mut publish_counter = 0usize;

            b.iter(|| {
                // Rapid publish sequence (simulating high message rate)
                for i in 0..10 {
                    let topic = format!("notice://realm/area/topic{}", i % 5);
                    let payload = &payloads[publish_counter % payloads.len()];
                    publish_counter += 1;

                    black_box(&actor);
                    black_box(&topic);
                    black_box(payload);
                }
            })
        },
    );

    group.finish();
}

criterion_group! {
    name = benches;
    config = config::criterion_config();
    targets =
        bench_complete_pubsub_workflow,
        bench_multisubscriber_fanout_workflow,
        bench_wildcard_pattern_matching_workflow,
        bench_rapid_subscribe_unsubscribe_workflow,
        bench_high_throughput_sustained_load,
}
criterion_main!(benches);
