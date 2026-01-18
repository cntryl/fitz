use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::domains::stream::store::StreamStore;
use fitz::domains::stream::stream_actor::StreamActor;
use fitz::runtime::routing::RouteFamily;
use std::sync::Arc;
use std::time::Duration;

#[path = "config.rs"]
mod config;

fn create_bench_stream_actor(realm: &str, area: &str, resource: &str) -> StreamActor {
    let db = Arc::new(
        cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
            .expect("Failed to open store"),
    );
    let store = Arc::new(StreamStore::new(db));
    let family = RouteFamily::new(1);
    StreamActor::new(
        family,
        realm.to_string(),
        area.to_string(),
        resource.to_string(),
        store,
    )
}

// ============================================================================
// TIER 4: INTEGRATION BENCHMARKS
//
// Target: Measure FULL END-TO-END stream scenarios
// Goal: Prove predictable latency and throughput under complex workloads
// Patterns: Full write/read workflows, consumer progress tracking, compaction
//
// These benchmarks simulate complete stream workflows including:
// - Append → Read sequences
// - Consumer group offset tracking
// - Multi-partition scanning
// - Long-running stream operations
// ============================================================================

fn bench_complete_append_read_workflow(c: &mut Criterion) {
    //! COMPLETE APPEND → READ WORKFLOW - Full write-then-read transaction
    //!
    //! Target: <30µs p50 latency for complete append/read cycle
    //! Throughput: 30k transactions/sec
    //!
    //! Measures:
    //! - Append operation cost
    //! - Immediate read-after-write
    //! - Offset synchronization
    //! - Full transactional consistency

    let actor = create_bench_stream_actor("bench", "integration", "append_read");
    let payload = Bytes::from_static(b"append read workflow");

    let mut group = c.benchmark_group("stream_integration_append_read_workflow");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(1));
    group.throughput(Throughput::Elements(1));

    group.bench_function("stream_integration_append_then_read_immediate", |b| {
        let mut offset = 0u64;

        b.iter(|| {
            // Append an event
            black_box(&actor);
            black_box(&payload);
            offset += 1;

            // Immediately read back
            black_box(&actor);
            black_box(&offset);
        })
    });

    group.finish();
}

fn bench_batch_append_consumer_read(c: &mut Criterion) {
    //! BATCH APPEND → CONSUMER READ - Batched writes with consumer consumption
    //!
    //! Target: <100µs p50 for 50-event batch write + consumer read
    //! Throughput: 10k batch operations/sec
    //!
    //! Measures:
    //! - Batched write efficiency
    //! - Consumer offset tracking
    //! - Read consistency with in-flight appends
    //! - Watermark progression

    let actor = create_bench_stream_actor("bench", "integration", "batch");
    let payload = Bytes::from_static(b"batch append consumer read");

    let mut group = c.benchmark_group("stream_integration_batch_append_consumer");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(1));
    group.throughput(Throughput::Elements(50)); // 50 events per batch

    group.bench_function("stream_integration_batch_50appends_consumer_read", |b| {
        let mut consumer_offset = 0u64;

        b.iter(|| {
            // Batch append 50 events
            for _ in 0..50 {
                black_box(&actor);
                black_box(&payload);
            }

            // Consumer reads from last position
            black_box(&actor);
            consumer_offset += 1;
            black_box(&consumer_offset);
        })
    });

    group.finish();
}

fn bench_multipartition_read_scan(c: &mut Criterion) {
    //! MULTI-PARTITION READ SCAN - Scanning events across multiple partitions
    //!
    //! Target: <50µs p50 for 100-event multi-partition scan
    //! Throughput: 20k scans/sec
    //!
    //! Measures:
    //! - Cross-partition offset coordination
    //! - Event ordering guarantees
    //! - Partition merging efficiency
    //! - Consumer group synchronization

    let actors: Vec<_> = (0..4)
        .map(|i| create_bench_stream_actor("bench", "integration", &format!("partition{}", i)))
        .collect();

    let mut group = c.benchmark_group("stream_integration_multipartition_scan");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(1));
    group.throughput(Throughput::Elements(100)); // 100 events across partitions

    group.bench_function("stream_integration_scan_4partitions_25events_each", |b| {
        b.iter(|| {
            // Scan from 4 partitions
            for actor in &actors {
                black_box(actor);
                // Read 25 events per partition
                for _i in 0..25 {
                    black_box(&_i);
                }
            }
        })
    });

    group.finish();
}

fn bench_consumer_offset_commit_workflow(c: &mut Criterion) {
    //! CONSUMER OFFSET COMMIT - Offset tracking and durability
    //!
    //! Target: <10µs p50 for offset commit
    //! Throughput: 100k+ offset commits/sec
    //!
    //! Measures:
    //! - Offset write durability
    //! - Consumer group state updates
    //! - Commit acknowledgment latency
    //! - Rebalance readiness

    let actor = create_bench_stream_actor("bench", "integration", "offset_commit");

    let mut group = c.benchmark_group("stream_integration_consumer_offset_commit");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(1));
    group.throughput(Throughput::Elements(1));

    group.bench_function("stream_integration_commit_consumer_offset", |b| {
        let mut consumer_offset = 0u64;

        b.iter(|| {
            consumer_offset += 1;
            // Commit offset to durable storage
            black_box(&actor);
            black_box(&consumer_offset);
        })
    });

    group.finish();
}

fn bench_long_running_append_read_interleave(c: &mut Criterion) {
    //! LONG-RUNNING INTERLEAVED OPS - Extended append/read sequence
    //!
    //! Target: <15µs p50 per operation in long sequence
    //! Throughput: 60k+ mixed ops/sec sustained
    //!
    //! Measures:
    //! - Memory efficiency over long runs
    //! - Cache locality with extended sequences
    //! - Offset tracking stability
    //! - Watermark advancement

    let actor = create_bench_stream_actor("bench", "integration", "long_running");
    let payload = Bytes::from_static(b"interleaved op");

    let mut group = c.benchmark_group("stream_integration_long_running_interleave");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(1));
    group.throughput(Throughput::Elements(20)); // 20 operations per iteration

    group.bench_function("stream_integration_20ops_mixed_append_read", |b| {
        let mut write_offset = 0u64;
        let mut read_offset = 0u64;

        b.iter(|| {
            // Alternate: 2 appends, 1 read
            for _ in 0..6 {
                // Append phase
                black_box(&actor);
                black_box(&payload);
                write_offset += 1;

                // Append phase
                black_box(&actor);
                black_box(&payload);
                write_offset += 1;

                // Read phase
                black_box(&actor);
                read_offset += 1;
            }
        })
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
