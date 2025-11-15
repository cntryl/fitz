//! Hotpath benchmarks for RPC service operations
//!
//! These benchmarks exercise the core RPC service primitives that are
//! performance-critical in typical request/response flows.

use criterion::{criterion_group, criterion_main, Criterion};
use fitz::core::rpc::RpcService;
use std::sync::{Arc, OnceLock};
use tokio::runtime::Runtime;

#[path = "../config.rs"]
mod config;

// ---------------------------------------------------------
// Shared runtime and services
// ---------------------------------------------------------
static RT: OnceLock<Runtime> = OnceLock::new();
fn rt() -> &'static Runtime {
    RT.get_or_init(|| Runtime::new().expect("runtime"))
}

fn rpc_service() -> RpcService {
    RpcService::new()
}

static TEST_REQUESTS: OnceLock<Vec<Vec<u8>>> = OnceLock::new();
fn test_requests() -> &'static [Vec<u8>] {
    TEST_REQUESTS.get_or_init(|| {
        vec![
            vec![b'r'; 64],        // 64B request
            vec![b'r'; 1024],      // 1KB request
            vec![b'r'; 64 * 1024], // 64KB request
        ]
    })
}

// ---------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------

fn bench_rpc_request(c: &mut Criterion) {
    let mut service = rpc_service();
    let requests = test_requests();
    let mut counter = 0;

    c.bench_function("rpc_request", |b| {
        b.iter(|| {
            let request = &requests[counter % requests.len()];
            counter += 1;
            rt().block_on(async {
                let inbox = service.allocate_inbox(1);
                // Correlation tracking as in handler flow
                service.register_request(
                    "corr-req".to_string(),
                    "rpc://test/bench/service1/method1".to_string(),
                    inbox.clone(),
                );
                criterion::black_box(inbox);
            });
        })
    });
}

fn bench_rpc_response(c: &mut Criterion) {
    let mut service = rpc_service();

    c.bench_function("rpc_response", |b| {
        b.iter_batched(
            || {
                // Setup: register an active request
                let inbox = service.allocate_inbox(1);
                service.register_request(
                    "corr-resp".to_string(),
                    "rpc://test/bench/response_service/method1".to_string(),
                    inbox,
                );
                "corr-resp".to_string()
            },
            |corr_id| {
                rt().block_on(async {
                    // Simulate looking up and deregistering a completed request
                    let result = service.deregister_request(&corr_id);
                    criterion::black_box(result);
                });
            },
            criterion::BatchSize::SmallInput,
        )
    });
}

fn bench_rpc_poll(c: &mut Criterion) {
    let mut service = rpc_service();

    c.bench_function("rpc_poll", |b| {
        b.iter(|| {
            rt().block_on(async {
                // Hot path-ish: count handler subscriptions for a route family
                let count = service.handler_count();
                criterion::black_box(count);
            });
        })
    });
}

fn bench_rpc_request_response_round_trip(c: &mut Criterion) {
    let mut service = rpc_service();
    let mut counter = 0;

    c.bench_function("rpc_request_response_round_trip", |b| {
        b.iter(|| {
            let request = &requests[counter % requests.len()];
            counter += 1;
            rt().block_on(async {
                // Simulate basic lifecycle using public APIs
                let inbox = service.allocate_inbox(1);
                service.register_request(
                    "corr-rt".to_string(),
                    "rpc://test/bench/round_trip_service/method1".to_string(),
                    inbox.clone(),
                );
                let can_publish = service.can_publish_to_inbox(&inbox, "corr-rt");
                let removed = service.deregister_request("corr-rt");
                criterion::black_box((can_publish, removed));
            });
        })
    });
}

fn bench_rpc_batch_request(c: &mut Criterion) {
    let mut service = rpc_service();

    c.bench_function("rpc_batch_request", |b| {
        b.iter(|| {
            // Tight loop over allocation and registration to stress map growth
            for i in 0..16 {
                let corr = format!("corr-batch-{}", i);
                let handler = "rpc://test/bench/batch_service/method".to_string();
                let inbox = service.allocate_inbox(1);
                service.register_request(corr, handler.clone(), inbox);
            }
            let count = service.handler_count() + service.inbox_count();
            criterion::black_box(count);
        })
    });
}

criterion_group!(
    name = hotpath_rpc;
    config = config::criterion_config();
    targets =
        bench_rpc_request,
        bench_rpc_response,
        bench_rpc_poll,
        bench_rpc_request_response_round_trip,
        bench_rpc_batch_request
);

criterion_main!(hotpath_rpc);
