//! Subsystem benchmarks for RPC domain operations
//!
//! These benchmarks test full RPC domain operations end-to-end,
//! including handler processing, request/response correlation, and domain logic.

use criterion::{criterion_group, criterion_main, Criterion};
use fitz::core::domain::{Domain, DomainContext, DomainResponse};
use fitz::core::rpc::{RpcDomain, RpcService};
use fitz::protocol::frame::{build_tlv, PooledFrame};
use fitz::protocol::tags::*;
use fitz::routing::RouteFamilyId;
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

static RPC_DOMAIN: OnceLock<Arc<RpcDomain>> = OnceLock::new();
fn rpc_domain() -> Arc<RpcDomain> {
    RPC_DOMAIN.get_or_init(|| {
        rt().block_on(async {
            Arc::new(RpcDomain::new().await)
        })
    })
}

// ---------------------------------------------------------
// Helper functions
// ---------------------------------------------------------

fn create_rpc_request_frame(service: &str, method: &str, correlation_id: &str, data: Option<&[u8]>) -> PooledFrame {
    let route = format!("rpc://{}/{}", service, method);
    let mut payload = Vec::new();
    build_tlv(TAG_ROUTE, route.as_bytes(), &mut payload);
    build_tlv(TAG_CORRELATION_ID, correlation_id.as_bytes(), &mut payload);
    if let Some(d) = data {
        build_tlv(TAG_BODY, d, &mut payload);
    }
    PooledFrame::from_vec(payload)
}

fn create_rpc_response_frame(service: &str, method: &str, correlation_id: &str, data: &[u8]) -> PooledFrame {
    let route = format!("rpc://{}/{}/response", service, method);
    let mut payload = Vec::new();
    build_tlv(TAG_ROUTE, route.as_bytes(), &mut payload);
    build_tlv(TAG_CORRELATION_ID, correlation_id.as_bytes(), &mut payload);
    build_tlv(TAG_BODY, data, &mut payload);
    PooledFrame::from_vec(payload)
}

// ---------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------

fn bench_rpc_request_small(c: &mut Criterion) {
    let domain = rpc_domain();
    let data = b"small request payload";
    let correlation_id = "req_123";
    let frame = create_rpc_request_frame("user_service", "get_profile", correlation_id, Some(data));

    c.bench_function("rpc_request_small", |b| {
        b.iter(|| {
            let ctx = DomainContext {
                route_family: RouteFamilyId::new(),
                route_str: "rpc://user_service/get_profile".to_string(),
                payload: frame.payload(),
            };

            rt().block_on(async {
                let result = domain.handle(ctx).await;
                criterion::black_box(result);
            });
        })
    });
}

fn bench_rpc_request_large(c: &mut Criterion) {
    let large_data = vec![b'x'; 64 * 1024]; // 64KB payload
    let domain = rpc_domain();
    let correlation_id = "req_large_123";
    let frame = create_rpc_request_frame("user_service", "process_data", correlation_id, Some(&large_data));

    c.bench_function("rpc_request_large", |b| {
        b.iter(|| {
            let ctx = DomainContext {
                route_family: RouteFamilyId::new(),
                route_str: "rpc://user_service/process_data".to_string(),
                payload: frame.payload(),
            };

            rt().block_on(async {
                let result = domain.handle(ctx).await;
                criterion::black_box(result);
            });
        })
    });
}

fn bench_rpc_response_small(c: &mut Criterion) {
    let domain = rpc_domain();
    let data = b"small response payload";
    let correlation_id = "resp_123";
    let frame = create_rpc_response_frame("user_service", "get_profile", correlation_id, data);

    c.bench_function("rpc_response_small", |b| {
        b.iter(|| {
            let ctx = DomainContext {
                route_family: RouteFamilyId::new(),
                route_str: "rpc://user_service/get_profile/response".to_string(),
                payload: frame.payload(),
            };

            rt().block_on(async {
                let result = domain.handle(ctx).await;
                criterion::black_box(result);
            });
        })
    });
}

fn bench_rpc_response_large(c: &mut Criterion) {
    let large_data = vec![b'y'; 64 * 1024]; // 64KB response
    let domain = rpc_domain();
    let correlation_id = "resp_large_123";
    let frame = create_rpc_response_frame("user_service", "process_data", correlation_id, &large_data);

    c.bench_function("rpc_response_large", |b| {
        b.iter(|| {
            let ctx = DomainContext {
                route_family: RouteFamilyId::new(),
                route_str: "rpc://user_service/process_data/response".to_string(),
                payload: frame.payload(),
            };

            rt().block_on(async {
                let result = domain.handle(ctx).await;
                criterion::black_box(result);
            });
        })
    });
}

fn bench_rpc_request_response_roundtrip(c: &mut Criterion) {
    let domain = rpc_domain();

    c.bench_function("rpc_request_response_roundtrip", |b| {
        b.iter_batched(
            || format!("roundtrip_{}", fastrand::u64(0..1000000)),
            |correlation_id| {
                rt().block_on(async {
                    // Send request
                    let req_data = b"request data";
                    let req_frame = create_rpc_request_frame("order_service", "place_order", &correlation_id, Some(req_data));
                    let req_ctx = DomainContext {
                        route_family: RouteFamilyId::new(),
                        route_str: "rpc://order_service/place_order".to_string(),
                        payload: req_frame.payload(),
                    };
                    let req_result = domain.handle(req_ctx).await;

                    // Send response
                    let resp_data = b"order placed successfully";
                    let resp_frame = create_rpc_response_frame("order_service", "place_order", &correlation_id, resp_data);
                    let resp_ctx = DomainContext {
                        route_family: RouteFamilyId::new(),
                        route_str: "rpc://order_service/place_order/response".to_string(),
                        payload: resp_frame.payload(),
                    };
                    let resp_result = domain.handle(resp_ctx).await;

                    criterion::black_box((req_result, resp_result));
                });
            },
            criterion::BatchSize::SmallInput,
        )
    });
}

fn bench_rpc_concurrent_requests(c: &mut Criterion) {
    let domain = rpc_domain();

    c.bench_function("rpc_concurrent_requests", |b| {
        b.iter(|| {
            rt().block_on(async {
                let mut handles = Vec::new();

                // Send 10 concurrent requests
                for i in 0..10 {
                    let correlation_id = format!("concurrent_{}", i);
                    let data = format!("request {}", i).into_bytes();
                    let frame = create_rpc_request_frame("calc_service", "add", &correlation_id, Some(&data));
                    let ctx = DomainContext {
                        route_family: RouteFamilyId::new(),
                        route_str: "rpc://calc_service/add".to_string(),
                        payload: frame.payload(),
                    };

                    let domain_clone = Arc::clone(&domain);
                    handles.push(tokio::spawn(async move {
                        domain_clone.handle(ctx).await
                    }));
                }

                for handle in handles {
                    let result = handle.await.unwrap();
                    criterion::black_box(result);
                }
            });
        })
    });
}

fn bench_rpc_batch_requests(c: &mut Criterion) {
    let domain = rpc_domain();

    c.bench_function("rpc_batch_requests", |b| {
        b.iter_batched(
            || {
                let mut batch_payload = Vec::new();
                build_tlv(TAG_ROUTE, b"rpc://batch_service/process_batch", &mut batch_payload);

                // Create batch of 5 requests
                let mut batch_body = Vec::new();
                for i in 0..5 {
                    let correlation_id = format!("batch_{}", i);
                    let data = format!("batch item {}", i).into_bytes();

                    build_tlv(TAG_CORRELATION_ID, correlation_id.as_bytes(), &mut batch_body);
                    build_tlv(TAG_BODY, &data, &mut batch_body);
                    batch_body.push(b'\n'); // Request separator
                }
                build_tlv(TAG_BODY, &batch_body, &mut batch_payload);

                PooledFrame::from_vec(batch_payload)
            },
            |frame| {
                let ctx = DomainContext {
                    route_family: RouteFamilyId::new(),
                    route_str: "rpc://batch_service/process_batch".to_string(),
                    payload: frame.payload(),
                };

                rt().block_on(async {
                    let result = domain.handle(ctx).await;
                    criterion::black_box(result);
                });
            },
            criterion::BatchSize::SmallInput,
        )
    });
}

fn bench_rpc_timeout_handling(c: &mut Criterion) {
    let domain = rpc_domain();

    c.bench_function("rpc_timeout_handling", |b| {
        b.iter_batched(
            || format!("timeout_{}", fastrand::u64(0..1000000)),
            |correlation_id| {
                rt().block_on(async {
                    // Send request with timeout
                    let req_data = b"request with timeout";
                    let req_frame = create_rpc_request_frame("slow_service", "slow_operation", &correlation_id, Some(req_data));
                    let mut req_payload = Vec::new();
                    build_tlv(TAG_ROUTE, b"rpc://slow_service/slow_operation", &mut req_payload);
                    build_tlv(TAG_CORRELATION_ID, correlation_id.as_bytes(), &mut req_payload);
                    build_tlv(TAG_TIMEOUT_MS, &5000u32.to_le_bytes(), &mut req_payload); // 5 second timeout
                    build_tlv(TAG_BODY, req_data, &mut req_payload);
                    let req_frame = PooledFrame::from_vec(req_payload);

                    let req_ctx = DomainContext {
                        route_family: RouteFamilyId::new(),
                        route_str: "rpc://slow_service/slow_operation".to_string(),
                        payload: req_frame.payload(),
                    };

                    let result = domain.handle(req_ctx).await;
                    criterion::black_box(result);
                });
            },
            criterion::BatchSize::SmallInput,
        )
    });
}

fn bench_rpc_error_response(c: &mut Criterion) {
    let domain = rpc_domain();

    c.bench_function("rpc_error_response", |b| {
        b.iter_batched(
            || format!("error_{}", fastrand::u64(0..1000000)),
            |correlation_id| {
                rt().block_on(async {
                    // Send request that will fail
                    let req_data = b"invalid request";
                    let req_frame = create_rpc_request_frame("error_service", "failing_method", &correlation_id, Some(req_data));
                    let req_ctx = DomainContext {
                        route_family: RouteFamilyId::new(),
                        route_str: "rpc://error_service/failing_method".to_string(),
                        payload: req_frame.payload(),
                    };
                    let _ = domain.handle(req_ctx).await;

                    // Send error response
                    let error_data = b"{\"error\": \"InvalidRequest\", \"message\": \"Bad request data\"}";
                    let error_frame = create_rpc_response_frame("error_service", "failing_method", &correlation_id, error_data);
                    let mut error_payload = Vec::new();
                    build_tlv(TAG_ROUTE, b"rpc://error_service/failing_method/response", &mut error_payload);
                    build_tlv(TAG_CORRELATION_ID, correlation_id.as_bytes(), &mut error_payload);
                    build_tlv(TAG_ERROR_CODE, &400u16.to_le_bytes(), &mut error_payload);
                    build_tlv(TAG_BODY, error_data, &mut error_payload);
                    let error_frame = PooledFrame::from_vec(error_payload);

                    let error_ctx = DomainContext {
                        route_family: RouteFamilyId::new(),
                        route_str: "rpc://error_service/failing_method/response".to_string(),
                        payload: error_frame.payload(),
                    };

                    let result = domain.handle(error_ctx).await;
                    criterion::black_box(result);
                });
            },
            criterion::BatchSize::SmallInput,
        )
    });
}

fn bench_rpc_service_discovery(c: &mut Criterion) {
    let domain = rpc_domain();

    c.bench_function("rpc_service_discovery", |b| {
        b.iter(|| {
            let route = "rpc://service_registry/discover";
            let mut payload = Vec::new();
            build_tlv(TAG_ROUTE, route.as_bytes(), &mut payload);
            build_tlv(TAG_SERVICE_NAME, b"user_service", &mut payload);
            let frame = PooledFrame::from_vec(payload);

            let ctx = DomainContext {
                route_family: RouteFamilyId::new(),
                route_str: route.to_string(),
                payload: frame.payload(),
            };

            rt().block_on(async {
                let result = domain.handle(ctx).await;
                criterion::black_box(result);
            });
        })
    });
}

fn bench_rpc_load_balancing(c: &mut Criterion) {
    let domain = rpc_domain();

    c.bench_function("rpc_load_balancing", |b| {
        b.iter(|| {
            rt().block_on(async {
                let mut handles = Vec::new();

                // Send requests to same service method - should be load balanced
                for i in 0..20 {
                    let correlation_id = format!("lb_{}", i);
                    let data = format!("load balanced request {}", i).into_bytes();
                    let frame = create_rpc_request_frame("load_balanced_service", "process", &correlation_id, Some(&data));
                    let ctx = DomainContext {
                        route_family: RouteFamilyId::new(),
                        route_str: "rpc://load_balanced_service/process".to_string(),
                        payload: frame.payload(),
                    };

                    let domain_clone = Arc::clone(&domain);
                    handles.push(tokio::spawn(async move {
                        domain_clone.handle(ctx).await
                    }));
                }

                for handle in handles {
                    let result = handle.await.unwrap();
                    criterion::black_box(result);
                }
            });
        })
    });
}

criterion_group!(
    name = rpc_subsystem;
    config = config::criterion_config();
    targets =
        bench_rpc_request_small,
        bench_rpc_request_large,
        bench_rpc_response_small,
        bench_rpc_response_large,
        bench_rpc_request_response_roundtrip,
        bench_rpc_concurrent_requests,
        bench_rpc_batch_requests,
        bench_rpc_timeout_handling,
        bench_rpc_error_response,
        bench_rpc_service_discovery,
        bench_rpc_load_balancing
);

criterion_main!(rpc_subsystem);