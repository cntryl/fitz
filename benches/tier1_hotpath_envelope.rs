use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::runtime::envelope::{Envelope, MessageId};
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use std::time::{Duration, Instant};

#[path = "criterion_config.rs"]
mod criterion_config;

#[derive(Clone)]
#[allow(dead_code)]
struct TestMessage {
    value: u64,
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

    group.bench_function("owning_new_struct_payload", |b| {
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

    group.bench_function("owning_from_route_struct_payload", |b| {
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

    group.bench_function("owning_new_with_deadline_struct_payload", |b| {
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

    group.bench_function("owning_new_with_causation_struct_payload", |b| {
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

fn bench_envelope_owning_heap_payload_sizes(c: &mut Criterion) {
    let small_vec_pool: Vec<(RouteAddress, Vec<u64>)> = (0..4)
        .map(|i| {
            let dest = RouteAddress::new(
                RouteFamily::new(1),
                Route::new(format!("ftz://1/stream/acme/app/logs{}", i)),
            );
            (dest, vec![1 + i as u64])
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

    group.bench_function("owning_new_vec_payload_1_u64", |b| {
        let mut idx = 0usize;
        b.iter(|| {
            let (d, m) = &small_vec_pool[idx % small_vec_pool.len()];
            idx += 1;
            let _envelope = Envelope::new(black_box(d.clone()), black_box(m.clone()));
        })
    });

    group.bench_function("owning_new_vec_payload_100_u64", |b| {
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
        bench_envelope_creation,
        bench_envelope_from_route,
        bench_envelope_with_deadline,
        bench_envelope_with_causation,
    bench_envelope_owning_heap_payload_sizes
}
criterion_main!(benches);
