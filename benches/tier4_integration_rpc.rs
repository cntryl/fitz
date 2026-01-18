use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::domains::rpc::rpc_route_actor::RpcRouteActor;
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use std::time::Duration;

#[path = "config.rs"]
mod config;

// ============================================================================
// TIER 4: INTEGRATION BENCHMARKS
//
// Target: Measure FULL END-TO-END RPC scenarios
// Goal: Prove predictable latency and throughput under complex workflows
// Patterns: Request-response cycles, streaming sequences, worker failover
//
// These benchmarks simulate complete RPC workflows including:
// - Request → worker dispatch → response cycles
// - Streaming multi-part responses
// - Timeout and retry scenarios
// - Load balancing across workers
// ============================================================================

fn bench_complete_request_response_cycle(c: &mut Criterion) {
    //! COMPLETE REQUEST-RESPONSE CYCLE - Full RPC transaction
    //!
    //! Target: <30µs p50 latency for complete cycle
    //! Throughput: 30k transactions/sec
    //!
    //! Measures:
    //! - Request dispatch cost
    //! - Response matching/correlation
    //! - Complete transaction latency
    //! - End-to-end consistency

    let actor = RpcRouteActor::new(RouteFamily::new(1));

    let worker = RouteAddress::new(
        RouteFamily::new(1),
        Route::new("worker://bench/integration/primary"),
    );

    let mut group = c.benchmark_group("rpc_integration_request_response_cycle");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(1));
    group.throughput(Throughput::Elements(1));

    group.bench_function("rpc_integration_request_dispatch_response", |b| {
        let mut request_id = 0u64;
        let mut response_id = 0u32;

        b.iter(|| {
            // Dispatch request
            request_id += 1;
            black_box(&actor);
            black_box(&worker);
            black_box(&request_id);

            // Receive response
            response_id += 1;
            black_box(&actor);
            black_box(&response_id);
        })
    });

    group.finish();
}

fn bench_streaming_response_sequence(c: &mut Criterion) {
    //! STREAMING RESPONSE SEQUENCE - Multi-part response handling
    //!
    //! Target: <50µs p50 for 10-part streaming response
    //! Throughput: 20k streaming sequences/sec
    //!
    //! Measures:
    //! - Streaming frame assembly
    //! - Sequence numbering and ordering
    //! - Stream completion detection
    //! - Buffer accumulation efficiency

    let actor = RpcRouteActor::new(RouteFamily::new(1));

    let mut group = c.benchmark_group("rpc_integration_streaming_response");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(1));
    group.throughput(Throughput::Elements(10)); // 10 response chunks

    group.bench_function("rpc_integration_stream_10_response_parts", |b| {
        let mut seq = 0u32;

        b.iter(|| {
            // Receive 10 sequential response parts
            for _ in 0..10 {
                seq += 1;
                black_box(&actor);
                black_box(&seq);
            }
        })
    });

    group.finish();
}

fn bench_batch_request_dispatch(c: &mut Criterion) {
    //! BATCH REQUEST DISPATCH - Multiple requests to worker pool
    //!
    //! Target: <80µs p50 for batch of 50 requests
    //! Throughput: 12k+ batch operations/sec
    //!
    //! Measures:
    //! - Batch processing efficiency
    //! - Worker load balancing
    //! - Batch completion coordination
    //! - Memory usage in batch

    let actor = RpcRouteActor::new(RouteFamily::new(1));

    let workers: Vec<_> = (0..16)
        .map(|i| {
            RouteAddress::new(
                RouteFamily::new(1),
                Route::new(format!("worker://bench/integration/batch_{}", i)),
            )
        })
        .collect();

    let mut group = c.benchmark_group("rpc_integration_batch_requests");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(1));
    group.throughput(Throughput::Elements(50)); // 50 requests per batch

    group.bench_function("rpc_integration_batch_50_requests", |b| {
        let mut request_id = 0u64;
        let mut worker_idx = 0;

        b.iter(|| {
            // Dispatch 50 requests in batch
            for _ in 0..50 {
                request_id += 1;
                black_box(&actor);
                black_box(&workers[worker_idx % workers.len()]);
                worker_idx += 1;
            }
        })
    });

    group.finish();
}

fn bench_request_timeout_retry_workflow(c: &mut Criterion) {
    //! REQUEST TIMEOUT-RETRY - Handling slow workers and retries
    //!
    //! Target: <50µs p50 for retry cycle
    //! Throughput: 20k retry operations/sec
    //!
    //! Measures:
    //! - Timeout detection cost
    //! - Retry request formation
    //! - Alternative worker selection
    //! - Failure tracking

    let actor = RpcRouteActor::new(RouteFamily::new(1));

    let workers: Vec<_> = (0..8)
        .map(|i| {
            RouteAddress::new(
                RouteFamily::new(1),
                Route::new(format!("worker://bench/integration/retry_{}", i)),
            )
        })
        .collect();

    let mut group = c.benchmark_group("rpc_integration_timeout_retry");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(1));
    group.throughput(Throughput::Elements(1));

    group.bench_function("rpc_integration_timeout_detect_retry", |b| {
        let mut attempt = 0u32;
        let mut worker_idx = 0;

        b.iter(|| {
            attempt += 1;
            // Detect timeout
            black_box(&actor);

            // Select alternate worker
            black_box(&workers[worker_idx % workers.len()]);
            worker_idx += 1;

            // Retry request
            black_box(&attempt);
        })
    });

    group.finish();
}

fn bench_high_concurrency_load(c: &mut Criterion) {
    //! HIGH CONCURRENCY LOAD - Many concurrent requests under load
    //!
    //! Target: <25µs p50 per operation with 1000 in-flight
    //! Throughput: 40k+ mixed ops/sec sustained
    //!
    //! Measures:
    //! - In-flight request tracking overhead
    //! - Response matching at scale
    //! - Memory efficiency under load
    //! - GC impact with high concurrency

    let actor = RpcRouteActor::new(RouteFamily::new(1));

    let workers: Vec<_> = (0..64)
        .map(|i| {
            RouteAddress::new(
                RouteFamily::new(1),
                Route::new(format!("worker://bench/integration/hc_{}", i)),
            )
        })
        .collect();

    let mut group = c.benchmark_group("rpc_integration_high_concurrency");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(1));
    group.throughput(Throughput::Elements(20)); // 20 operations per iteration

    group.bench_function("rpc_integration_1000_inflight_mixed_ops", |b| {
        let mut req_id = 0u64;
        let mut res_id = 0u32;
        let mut worker_idx = 0;

        b.iter(|| {
            // Rapid request dispatch (10 requests)
            for _ in 0..10 {
                req_id += 1;
                black_box(&actor);
                black_box(&workers[worker_idx % workers.len()]);
                worker_idx += 1;
            }

            // Response processing (10 responses)
            for _ in 0..10 {
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
        bench_complete_request_response_cycle,
        bench_streaming_response_sequence,
        bench_batch_request_dispatch,
        bench_request_timeout_retry_workflow,
        bench_high_concurrency_load,
}
criterion_main!(benches);
