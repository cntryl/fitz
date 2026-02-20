use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::runtime::envelope::{Envelope, MessageId};
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use std::time::{Duration, Instant};

#[path = "criterion_config.rs"]
mod criterion_config;

// Simple message type for benchmarking
#[derive(Clone)]
#[allow(dead_code)]
struct TestMessage {
    value: u64,
}

fn bench_message_id_generation(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_envelope");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("messageid_new", |b| {
        b.iter(|| {
            // ONLY hot path - atomic counter increment
            let _id = black_box(MessageId::new());
        })
    });

    group.finish();
}

fn bench_envelope_creation(c: &mut Criterion) {
    // Pre-built pool OUTSIDE benchmark - rotating index avoids cloning same capture in loop
    let pairs: Vec<(RouteAddress, TestMessage)> = (0..4)
        .map(|i| {
            let dest = RouteAddress::new(
                RouteFamily::new(1),
                Route::new(format!("ftz://1/kv/acme/app/users{}", i)),
            );
            (
                dest,
                TestMessage {
                    value: 42 + i as u64,
                },
            )
        })
        .collect();

    let mut group = c.benchmark_group("hotpath_envelope");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("envelope_new", |b| {
        let mut idx = 0usize;
        b.iter(|| {
            let (d, m) = &pairs[idx % pairs.len()];
            idx += 1;
            let _envelope = Envelope::new(black_box(d.clone()), black_box(m.clone()));
        })
    });

    group.finish();
}

fn bench_envelope_from_route(c: &mut Criterion) {
    // Pre-built pool OUTSIDE benchmark
    let triples: Vec<(RouteAddress, RouteAddress, TestMessage)> = (0..4)
        .map(|i| {
            let src = RouteAddress::new(
                RouteFamily::new(1),
                Route::new(format!("ftz://1/rpc/acme/app/client{}", i)),
            );
            let dst = RouteAddress::new(
                RouteFamily::new(1),
                Route::new(format!("ftz://1/rpc/acme/app/server{}", i)),
            );
            (
                src,
                dst,
                TestMessage {
                    value: 100 + i as u64,
                },
            )
        })
        .collect();

    let mut group = c.benchmark_group("hotpath_envelope");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("envelope_from_route", |b| {
        let mut idx = 0usize;
        b.iter(|| {
            let (src, dst, m) = &triples[idx % triples.len()];
            idx += 1;
            let _envelope = Envelope::from_route(
                black_box(src.clone()),
                black_box(dst.clone()),
                black_box(m.clone()),
            );
        })
    });

    group.finish();
}

fn bench_envelope_with_deadline(c: &mut Criterion) {
    let deadline = Instant::now() + Duration::from_secs(30);
    let pairs: Vec<(RouteAddress, TestMessage)> = (0..4)
        .map(|i| {
            let dest = RouteAddress::new(
                RouteFamily::new(1),
                Route::new(format!("ftz://1/lease/acme/app/resource{}", i)),
            );
            (
                dest,
                TestMessage {
                    value: 200 + i as u64,
                },
            )
        })
        .collect();

    let mut group = c.benchmark_group("hotpath_envelope");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("envelope_with_deadline", |b| {
        let mut idx = 0usize;
        b.iter(|| {
            let (d, m) = &pairs[idx % pairs.len()];
            idx += 1;
            let _envelope = Envelope::new(black_box(d.clone()), black_box(m.clone()))
                .with_deadline(black_box(deadline));
        })
    });

    group.finish();
}

fn bench_envelope_with_causation(c: &mut Criterion) {
    let parent_id = MessageId::new();
    let pairs: Vec<(RouteAddress, TestMessage)> = (0..4)
        .map(|i| {
            let dest = RouteAddress::new(
                RouteFamily::new(1),
                Route::new(format!("ftz://1/notice/acme/app/events{}", i)),
            );
            (
                dest,
                TestMessage {
                    value: 300 + i as u64,
                },
            )
        })
        .collect();

    let mut group = c.benchmark_group("hotpath_envelope");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("envelope_with_causation", |b| {
        let mut idx = 0usize;
        b.iter(|| {
            let (d, m) = &pairs[idx % pairs.len()];
            idx += 1;
            let _envelope = Envelope::new(black_box(d.clone()), black_box(m.clone()))
                .with_causation(black_box(parent_id));
        })
    });

    group.finish();
}

fn bench_deadline_checking(c: &mut Criterion) {
    // Setup OUTSIDE benchmark
    let dest = RouteAddress::new(
        RouteFamily::new(1),
        Route::new("ftz://1/queue/acme/app/tasks".to_string()),
    );

    // Create envelopes with different deadline states
    let not_expired = Envelope::new(dest.clone(), TestMessage { value: 1 })
        .with_deadline(Instant::now() + Duration::from_secs(3600));

    let expired = Envelope::new(dest.clone(), TestMessage { value: 2 })
        .with_deadline(Instant::now() - Duration::from_secs(1));

    let no_deadline = Envelope::new(dest, TestMessage { value: 3 });

    let mut group = c.benchmark_group("hotpath_envelope");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("is_expired_not_expired", |b| {
        b.iter(|| {
            // ONLY hot path - deadline check (should be false)
            let _result = black_box(not_expired.is_expired());
        })
    });

    group.bench_function("is_expired_expired", |b| {
        b.iter(|| {
            // ONLY hot path - deadline check (should be true)
            let _result = black_box(expired.is_expired());
        })
    });

    group.bench_function("is_expired_no_deadline", |b| {
        b.iter(|| {
            // ONLY hot path - deadline check (None case)
            let _result = black_box(no_deadline.is_expired());
        })
    });

    group.finish();
}

fn bench_envelope_metadata_extraction(c: &mut Criterion) {
    // Setup OUTSIDE benchmark
    let source = RouteAddress::new(
        RouteFamily::new(1),
        Route::new("ftz://1/rpc/acme/app/client".to_string()),
    );
    let dest = RouteAddress::new(
        RouteFamily::new(1),
        Route::new("ftz://1/rpc/acme/app/server".to_string()),
    );
    let envelope = Envelope::from_route(source, dest, TestMessage { value: 500 })
        .with_deadline(Instant::now() + Duration::from_secs(30))
        .with_causation(MessageId::new());

    let mut group = c.benchmark_group("hotpath_envelope");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("metadata_extraction", |b| {
        b.iter(|| {
            // ONLY hot path - clone metadata without consuming envelope
            let _metadata = black_box(envelope.metadata());
        })
    });

    group.finish();
}

fn bench_type_erasure_overhead(c: &mut Criterion) {
    // Pre-built pool: small and large message variants
    let small_pool: Vec<(RouteAddress, TestMessage)> = (0..4)
        .map(|i| {
            let dest = RouteAddress::new(
                RouteFamily::new(1),
                Route::new(format!("ftz://1/stream/acme/app/logs{}", i)),
            );
            (
                dest,
                TestMessage {
                    value: 1 + i as u64,
                },
            )
        })
        .collect();
    let large_msg = (0..100).collect::<Vec<u64>>();
    let large_pool: Vec<(RouteAddress, Vec<u64>)> = (0..4)
        .map(|i| {
            let dest = RouteAddress::new(
                RouteFamily::new(1),
                Route::new(format!("ftz://1/stream/acme/app/logs_large{}", i)),
            );
            (dest, large_msg.clone())
        })
        .collect();

    let mut group = c.benchmark_group("hotpath_envelope");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("type_erasure_small_message", |b| {
        let mut idx = 0usize;
        b.iter(|| {
            let (d, m) = &small_pool[idx % small_pool.len()];
            idx += 1;
            let _envelope = Envelope::new(black_box(d.clone()), black_box(m.clone()));
        })
    });

    group.bench_function("type_erasure_large_message", |b| {
        let mut idx = 0usize;
        b.iter(|| {
            let (d, m) = &large_pool[idx % large_pool.len()];
            idx += 1;
            let _envelope = Envelope::new(black_box(d.clone()), black_box(m.clone()));
        })
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = criterion_config::criterion_config_for_tier1();
    targets =
        bench_message_id_generation,
        bench_envelope_creation,
        bench_envelope_from_route,
        bench_envelope_with_deadline,
        bench_envelope_with_causation,
        bench_deadline_checking,
        bench_envelope_metadata_extraction,
        bench_type_erasure_overhead
}
criterion_main!(benches);
