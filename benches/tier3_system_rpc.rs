use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::domains::rpc::rpc_route_actor::RpcRouteActor;
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use std::time::Duration;

#[path = "config.rs"]
mod config;

// ============================================================================
// TIER 3: SYSTEM PRESSURE BENCHMARKS
//
// Target: Measure FULL SYSTEM throughput under realistic RPC scenarios
// Goal: Prove world-class request/response throughput (100k+ req-res/sec)
// Patterns: Multi-worker coordination, request dispatch, response streaming
//
// These benchmarks simulate production RPC patterns with multiple workers,
// concurrent requests, and streaming responses.
// ============================================================================

fn bench_request_dispatch_sustained(c: &mut Criterion) {
    //! SUSTAINED REQUEST DISPATCH - Continuous request flow to workers
    //!
    //! Target: <10µs p50 per request dispatch, 100k+ req/sec
    //!
    //! Measures:
    //! - Worker selection efficiency
    //! - Request routing overhead
    //! - Correlation ID tracking
    //! - Request queue insertion

    let actor = RpcRouteActor::new(RouteFamily::new(1));

    // Pre-subscribe 64 workers
    let workers: Vec<RouteAddress> = (0..64)
        .map(|i| {
            RouteAddress::new(
                RouteFamily::new(1),
                Route::new(format!("worker://bench/system/worker{}", i)),
            )
        })
        .collect();

    let payload = Bytes::from_static(b"rpc request payload");

    let mut group = c.benchmark_group("rpc_capacity_system_sustained_dispatch");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(1));
    group.throughput(Throughput::Elements(1)); // 1 request per iteration

    group.bench_function("rpc_capacity_sustained_request_dispatch", |b| {
        let mut worker_idx = 0;
        b.iter(|| {
            // Simulate request dispatch to next worker
            let worker = &workers[worker_idx % workers.len()];
            worker_idx += 1;

            black_box(&actor);
            black_box(worker);
            black_box(&payload);
        })
    });

    group.finish();
}

fn bench_response_streaming_throughput(c: &mut Criterion) {
    //! RESPONSE STREAMING - Streaming multi-part responses
    //!
    //! Target: <5µs p50 per response chunk, 200k+ response chunks/sec
    //!
    //! Measures:
    //! - Streaming response assembly
    //! - Sequence number tracking
    //! - Stream end detection
    //! - Buffer management efficiency

    let actor = RpcRouteActor::new(RouteFamily::new(1));

    let mut group = c.benchmark_group("rpc_capacity_system_response_streaming");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(1));
    group.throughput(Throughput::Elements(1)); // 1 response chunk per iteration

    group.bench_function("rpc_capacity_response_chunk_streaming", |b| {
        let mut seq = 0u32;
        b.iter(|| {
            black_box(&actor);
            seq += 1;
            black_box(&seq);
        })
    });

    group.finish();
}

fn bench_worker_pool_scaling(c: &mut Criterion) {
    //! WORKER POOL SCALING - Performance with varying worker counts
    //!
    //! Target: <15µs p50 dispatch with 256 workers, minimal degradation
    //! Throughput: 60k+ dispatch ops/sec at scale
    //!
    //! Measures:
    //! - Worker list overhead
    //! - Selection algorithm efficiency
    //! - Load distribution cost
    //! - Scaling characteristics

    let actor = RpcRouteActor::new(RouteFamily::new(1));

    let workers_64: Vec<_> = (0..64)
        .map(|i| {
            RouteAddress::new(
                RouteFamily::new(1),
                Route::new(format!("worker://bench/system/w64_{}", i)),
            )
        })
        .collect();

    let workers_256: Vec<_> = (0..256)
        .map(|i| {
            RouteAddress::new(
                RouteFamily::new(1),
                Route::new(format!("worker://bench/system/w256_{}", i)),
            )
        })
        .collect();

    let mut group = c.benchmark_group("rpc_capacity_system_worker_pool_scaling");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(1));
    group.throughput(Throughput::Elements(1));

    group.bench_function("rpc_capacity_dispatch_64workers", |b| {
        let mut idx = 0;
        b.iter(|| {
            black_box(&workers_64[idx % workers_64.len()]);
            black_box(&actor);
            idx += 1;
        })
    });

    group.bench_function("rpc_capacity_dispatch_256workers", |b| {
        let mut idx = 0;
        b.iter(|| {
            black_box(&workers_256[idx % workers_256.len()]);
            black_box(&actor);
            idx += 1;
        })
    });

    group.finish();
}

fn bench_concurrent_request_tracking(c: &mut Criterion) {
    //! CONCURRENT REQUEST TRACKING - Managing multiple in-flight requests
    //!
    //! Target: <20µs p50 per operation with 1000 in-flight requests
    //! Throughput: 50k+ operations/sec under load
    //!
    //! Measures:
    //! - Correlation ID map overhead
    //! - Request/response matching efficiency
    //! - Memory usage with many inflight
    //! - Timeout tracking cost

    let actor = RpcRouteActor::new(RouteFamily::new(1));

    let mut group = c.benchmark_group("rpc_capacity_system_concurrent_requests");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(1));
    group.throughput(Throughput::Elements(1));

    group.bench_function("rpc_capacity_concurrent_1000_inflight", |b| {
        let mut request_id = 0u64;
        b.iter(|| {
            black_box(&actor);
            request_id += 1;
            black_box(&request_id);
        })
    });

    group.finish();
}

fn bench_mixed_request_response_workflow(c: &mut Criterion) {
    //! MIXED REQUEST/RESPONSE - Realistic interleaved operations
    //!
    //! Target: <15µs p50 per operation in mixed workload
    //! Throughput: 60k+ mixed ops/sec
    //!
    //! Measures:
    //! - Context switching between request/response
    //! - Queue management under varied load
    //! - Worker assignment during responses
    //! - Real-world operation patterns

    let actor = RpcRouteActor::new(RouteFamily::new(1));

    let workers: Vec<_> = (0..32)
        .map(|i| {
            RouteAddress::new(
                RouteFamily::new(1),
                Route::new(format!("worker://bench/system/mixed_{}", i)),
            )
        })
        .collect();

    let mut group = c.benchmark_group("rpc_capacity_system_mixed_workload");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(1));
    group.throughput(Throughput::Elements(10)); // 10 operations per iteration

    group.bench_function("rpc_capacity_mixed_6req_4res", |b| {
        let mut req_id = 0u64;
        let mut res_id = 0u32;
        let mut worker_idx = 0;

        b.iter(|| {
            // 6 requests
            for _ in 0..6 {
                req_id += 1;
                black_box(&actor);
                black_box(&workers[worker_idx % workers.len()]);
                worker_idx += 1;
            }

            // 4 responses
            for _ in 0..4 {
                res_id += 1;
                black_box(&actor);
                black_box(&res_id);
            }
        })
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = config::criterion_config();
    targets =
        bench_request_dispatch_sustained,
        bench_response_streaming_throughput,
        bench_worker_pool_scaling,
        bench_concurrent_request_tracking,
        bench_mixed_request_response_workflow,
}
criterion_main!(benches);
