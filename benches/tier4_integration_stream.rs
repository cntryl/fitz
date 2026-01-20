use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::benchkit::create_local_bench_stream_actor;
use std::time::Duration;

#[path = "config.rs"]
mod config;

// ============================================================================
// TIER 4: INTEGRATION BENCHMARKS
//
// Target: Measure FULL END-TO-END stream scenarios with realistic workloads
// Goal: Prove predictable latency and throughput under complex scenarios
//
// Current Status: Benchmarking actor creation + local storage initialization
// Note: These benchmarks currently measure setup overhead only. Full operation
//       benchmarks (append, read, commit) will be added once StreamActor API is finalized.
//
// Uses local disk-backed storage (midge) for realistic persistence characteristics
//
// These benchmarks measure:
// - Actor initialization cost
// - Local storage setup overhead
// - Multi-actor creation for partition scenarios
//
// TODO: Implement actual stream operations (append, read, commit) in benchmarks
//
// ============================================================================

fn bench_complete_append_read_workflow(c: &mut Criterion) {
    //! STREAM ACTOR SETUP - Actor creation + storage initialization
    //!
    //! Status: Measures baseline costs only (setup overhead)
    //! TODO: Add actual append/read/commit operations
    //!
    //! Currently measures:
    //! - Actor initialization cost
    //! - Local storage setup time
    //! - TempDir creation for isolated storage
    //!
    //! Future: Will measure
    //! - Append operation latency
    //! - Read-after-write consistency
    //! - Full transactional overhead

    let mut group = c.benchmark_group("stream_integration_append_read_workflow");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(1));
    group.throughput(Throughput::Elements(1));

    group.bench_function("stream_integration_append_then_read_immediate", |b| {
        b.iter_batched(
            || {
                let payload = Bytes::from_static(b"append read workflow");
                (
                    create_local_bench_stream_actor("bench", "integration", "append_read"),
                    payload,
                )
            },
            |((actor, _ctx, _temp_dir), payload)| {
                // Benchmark the actor creation + storage initialization overhead
                // Future: Add actual append/read operations when API is finalized
                let _ = black_box((actor, &payload));
            },
            criterion::BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn bench_batch_append_consumer_read(c: &mut Criterion) {
    //! BATCH STREAM SETUP - Multi-actor initialization
    //!
    //! Status: Measures baseline costs only (setup overhead)
    //! TODO: Add actual batch operations
    //!
    //! Currently measures:
    //! - Single actor creation + storage
    //! - Batched throughput target
    //!
    //! Future: Will measure
    //! - Batched append efficiency
    //! - Consumer offset tracking
    //! - Read consistency

    let mut group = c.benchmark_group("stream_integration_batch_append_consumer");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(1));
    group.throughput(Throughput::Elements(50)); // 50 events per batch

    group.bench_function("stream_integration_batch_50appends_consumer_read", |b| {
        b.iter_batched(
            || {
                let payload = Bytes::from_static(b"batch append consumer read");
                (
                    create_local_bench_stream_actor("bench", "integration", "batch"),
                    payload,
                )
            },
            |((actor, _ctx, _temp_dir), payload)| {
                // Benchmark the actor + storage overhead for batch operations
                // Future: Implement actual batch append/read operations
                let _ = black_box((actor, &payload));
            },
            criterion::BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn bench_multipartition_read_scan(c: &mut Criterion) {
    //! MULTI-PARTITION SETUP - Multiple actor initialization
    //!
    //! Status: Measures actor creation overhead only
    //! TODO: Add actual scan operations
    //!
    //! Currently measures:
    //! - Creating 4 isolated actor instances
    //! - 4x storage initialization overhead
    //! - Partition setup costs
    //!
    //! Future: Will measure
    //! - Cross-partition read scanning
    //! - Event ordering guarantees
    //! - Partition merging efficiency

    let mut group = c.benchmark_group("stream_integration_multipartition_scan");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(1));
    group.throughput(Throughput::Elements(100)); // 100 events across partitions

    group.bench_function("stream_integration_scan_4partitions_25events_each", |b| {
        b.iter_batched(
            || {
                let actors: Vec<_> = (0..4)
                    .map(|i| {
                        create_local_bench_stream_actor(
                            "bench",
                            "integration",
                            &format!("partition{}", i),
                        )
                    })
                    .collect();
                actors
            },
            |actors| {
                // Benchmark multi-partition actor creation + storage setup
                // Future: Implement actual read/scan operations across partitions
                let _ = black_box(actors);
            },
            criterion::BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn bench_consumer_offset_commit_workflow(c: &mut Criterion) {
    //! STREAM OFFSET COMMIT SETUP - Actor creation for offset operations
    //!
    //! Status: Measures baseline actor setup only
    //! TODO: Add actual offset commit operations
    //!
    //! Currently measures:
    //! - Actor initialization cost
    //! - Storage readiness for offset tracking
    //!
    //! Future: Will measure
    //! - Offset write durability
    //! - Consumer group state updates
    //! - Commit acknowledgment latency

    let mut group = c.benchmark_group("stream_integration_consumer_offset_commit");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(1));
    group.throughput(Throughput::Elements(1));

    group.bench_function("stream_integration_commit_consumer_offset", |b| {
        b.iter_batched(
            || create_local_bench_stream_actor("bench", "integration", "offset_commit"),
            |(actor, _ctx, _temp_dir)| {
                // Benchmark actor + storage overhead for offset operations
                // Future: Implement actual offset commit operations
                let _ = black_box(actor);
            },
            criterion::BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn bench_long_running_append_read_interleave(c: &mut Criterion) {
    //! LONG-RUNNING SETUP - Actor initialization and storage
    //!
    //! Status: Measures baseline overhead only
    //! TODO: Add actual interleaved operations
    //!
    //! Currently measures:
    //! - Single actor creation cost
    //! - Storage setup overhead
    //!
    //! Future: Will measure
    //! - Sustained mixed operation performance
    //! - Memory efficiency over long runs
    //! - Cache locality with extended sequences

    let mut group = c.benchmark_group("stream_integration_long_running_interleave");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(1));
    group.throughput(Throughput::Elements(20)); // 20 operations per iteration

    group.bench_function("stream_integration_20ops_mixed_append_read", |b| {
        b.iter_batched(
            || {
                let payload = Bytes::from_static(b"interleaved op");
                (
                    create_local_bench_stream_actor("bench", "integration", "long_running"),
                    payload,
                )
            },
            |((actor, _ctx, _temp_dir), payload)| {
                // Benchmark actor creation + storage overhead
                // Future: Implement actual mixed append/read operations
                let _ = black_box((actor, &payload));
            },
            criterion::BatchSize::SmallInput,
        )
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = config::criterion_config();
    targets =
        bench_complete_append_read_workflow,
        bench_batch_append_consumer_read,
        bench_multipartition_read_scan,
        bench_consumer_offset_commit_workflow,
        bench_long_running_append_read_interleave,
}
criterion_main!(benches);
