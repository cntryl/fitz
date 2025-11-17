//! Hotpath benchmarks for RPC domain handler->service layer
//!
//! Tests RPC inbox allocation, correlation ID management, request tracking,
//! and handler registration/matching performance.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use fitz::core::domain::{Domain, DomainContext};
use fitz::core::rpc::RpcDomain;
use fitz::protocol::route::Route;
use fitz::protocol::tags::{TAG_BODY, TAG_ID, TAG_ROUTE_REPLY};
use tokio::sync::mpsc;
use std::sync::Arc;

#[path = "../config.rs"]
mod config;

/// Build TLV payload for RPC request (client -> handler)
fn build_request_payload(corr_id: &str, reply_route: &str, body: &[u8]) -> Vec<u8> {
    let mut payload = Vec::new();

    // TAG_ROUTE_REPLY
    payload.push(TAG_ROUTE_REPLY);
    payload.push(reply_route.len() as u8);
    payload.extend_from_slice(reply_route.as_bytes());

    // TAG_ID (correlation id)
    payload.push(TAG_ID);
    payload.push(corr_id.len() as u8);
    payload.extend_from_slice(corr_id.as_bytes());

    // TAG_BODY
    payload.push(TAG_BODY);
    if body.len() <= 254 {
        payload.push(body.len() as u8);
        payload.extend_from_slice(body);
    } else {
        payload.push(255);
        payload.extend_from_slice(&(body.len() as u32).to_be_bytes());
        payload.extend_from_slice(body);
    }

    payload
}

/// Build TLV payload for RPC reply (handler -> client inbox)
fn build_reply_payload(corr_id: &str, body: &[u8]) -> Vec<u8> {
    let mut payload = Vec::new();

    // TAG_ID (correlation id)
    payload.push(TAG_ID);
    payload.push(corr_id.len() as u8);
    payload.extend_from_slice(corr_id.as_bytes());

    // TAG_BODY
    payload.push(TAG_BODY);
    if body.len() <= 254 {
        payload.push(body.len() as u8);
        payload.extend_from_slice(body);
    } else {
        payload.push(255);
        payload.extend_from_slice(&(body.len() as u32).to_be_bytes());
        payload.extend_from_slice(body);
    }

    payload
}

fn build_rpc_route(resource: &str) -> Route {
    let route_str = format!("rpc://realm1/area1/{}", resource);
    Route {
        scheme: fitz::protocol::route::Scheme::Rpc,
        realm: Some("realm1".to_string()),
        area: Some("area1".to_string()),
        resource: Some(resource.to_string()),
        operation: None,
        raw: route_str,
    }
}

fn build_inbox_route(inbox_route: &str) -> Route {
    Route {
        scheme: fitz::protocol::route::Scheme::Inbox,
        realm: None,
        area: None,
        resource: None,
        operation: None,
        raw: inbox_route.to_string(),
    }
}

/// Test inbox allocation performance
fn bench_inbox_allocation(c: &mut Criterion) {
    let mut group = c.benchmark_group("rpc_inbox_allocation");
    group.throughput(Throughput::Elements(1));

    for &count in &[100, 1000, 10000] {
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, &count| {
            b.iter(|| {
                // Arrange
                let domain = RpcDomain::new();
                let service = domain.get_service();

                // Act - allocate many inboxes
                for i in 0..count {
                    let mut svc = service.write();
                    let _inbox = svc.allocate_inbox(i as u32);
                }

                // Assert - implicit success
            });
        });
    }

    group.finish();
}

/// Test request tracking (registration/deregistration)
fn bench_request_tracking(c: &mut Criterion) {
    let mut group = c.benchmark_group("rpc_request_tracking");
    group.throughput(Throughput::Elements(1));

    for &count in &[100, 1000] {
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, &count| {
            b.iter(|| {
                // Arrange
                let domain = RpcDomain::new();
                let service = domain.get_service();

                // Act - register and deregister requests
                for i in 0..count {
                    let mut svc = service.write();
                    let corr_id = format!("corr-{}", i);
                    let handler_route = format!("rpc://realm/area/handler");
                    let reply_route = format!("rpc://realm/area/inbox-{}", i);
                    svc.register_request(corr_id.clone(), handler_route, reply_route);
                    
                    // Deregister half of them
                    if i % 2 == 0 {
                        let _old = svc.deregister_request(&corr_id);
                    }
                }

                // Assert - implicit success
            });
        });
    }

    group.finish();
}

/// Test concurrent inbox allocation across threads
fn bench_concurrent_inbox_allocation(c: &mut Criterion) {
    c.bench_function("rpc_concurrent_inbox_allocation", |b| {
        b.iter(|| {
            // Arrange
            let domain: Arc<RpcDomain> = Arc::clone(&Arc::new(RpcDomain::new()));

            // Act - 100 concurrent inbox allocations
            let handles: Vec<_> = (0..100)
                .map(|i| {
                    let domain = Arc::clone(&domain);
                    std::thread::spawn(move || {
                        let service = domain.get_service();
                        let mut svc = service.write();
                        let _inbox = svc.allocate_inbox(i);
                    })
                })
                .collect();

            for handle in handles {
                let _ = handle.join();
            }

            // Assert - implicit success
        });
    });
}

/// Correlation lookup scaling (authorization check across active set)
fn bench_correlation_lookup_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("rpc_correlation_lookup_scaling");
    for &count in &[100usize, 1000, 10_000] {
        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, &count| {
            // Arrange once per input
            let domain = RpcDomain::new();
            let service = domain.get_service();
            {
                let mut svc = service.write();
                for i in 0..count {
                    let corr_id = format!("corr-{}", i);
                    let handler_route = "rpc://realm1/area1/handler".to_string();
                    let reply_route = format!("inbox://{}_{}", 1, i);
                    svc.register_request(corr_id, handler_route, reply_route);
                }
            }
            b.iter(|| {
                let svc = service.read();
                for i in 0..count {
                    let reply_route = format!("inbox://{}_{}", 1, i);
                    let corr_id = format!("corr-{}", i);
                    let _ = svc.can_publish_to_inbox(&reply_route, &corr_id);
                }
            });
        });
    }
    group.finish();
}

/// Sequential full request->reply cycles via Domain::handle
fn bench_sequential_request_reply(c: &mut Criterion) {
    let mut group = c.benchmark_group("rpc_sequential_request_reply");
    for &count in &[100usize, 1000] {
        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, &count| {
            b.iter(|| {
                // Arrange per-iter: fresh domain/service
                let domain = RpcDomain::new();
                let service = domain.get_service();

                // Subscribe a handler to receive requests
                let (handler_tx, mut _handler_rx) = mpsc::channel::<(
                    String,
                    Option<String>,
                    Vec<u8>,
                    Option<String>,
                    Option<u32>,
                    bool,
                )>(1_024);
                {
                    let mut svc = service.write();
                    let _sub_id = svc.subscribe_handler(
                        0,
                        "rpc://realm1/area1/handler".to_string(),
                        42,
                        handler_tx,
                    );
                }

                // Allocate inbox for client and subscribe to it
                let client_channel = 7u32;
                let inbox_route = {
                    let mut svc = service.write();
                    let route = svc.allocate_inbox(client_channel);
                    // Subscribe client to its inbox
                    let (client_tx, mut _client_rx) = mpsc::channel::<(
                        String,
                        Option<String>,
                        Vec<u8>,
                        Option<String>,
                        Option<u32>,
                        bool,
                    )>(1_024);
                    let _ = svc.subscribe_inbox(0, route.clone(), client_channel, client_tx);
                    route
                };

                // Perform request/reply cycles
                for i in 0..count {
                    let corr = format!("corr-{}", i);
                    let req_payload = build_request_payload(&corr, &inbox_route, b"ping");
                    let ctx = DomainContext {
                        route: build_rpc_route("handler"),
                        route_str: "rpc://realm1/area1/handler".to_string(),
                        payload: req_payload,
                        channel_id: client_channel,
                        route_family: 0,
                        sender: None,
                    };
                    let _ = domain.handle(ctx);

                    // Simulate handler publishing reply
                    let reply_payload = build_reply_payload(&corr, b"pong");
                    let ctx_reply = DomainContext {
                        route: build_inbox_route(&inbox_route),
                        route_str: inbox_route.clone(),
                        payload: reply_payload,
                        channel_id: 42,
                        route_family: 0,
                        sender: None,
                    };
                    let _ = domain.handle(ctx_reply);
                }
            });
        });
    }
    group.finish();
}

/// Reply payload sizes (handler -> client inbox)
fn bench_reply_payload_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("rpc_reply_payload_sizes");
    for &size in &[64usize, 256, 1024, 4096, 16_384] {
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter(|| {
                let domain = RpcDomain::new();
                let service = domain.get_service();

                // Subscribe a handler (not used in this bench) and client inbox setup
                let (client_tx, mut _client_rx) = mpsc::channel::<(
                    String,
                    Option<String>,
                    Vec<u8>,
                    Option<String>,
                    Option<u32>,
                    bool,
                )>(1_024);
                let client_channel = 9u32;
                let inbox_route = {
                    let mut svc = service.write();
                    let route = svc.allocate_inbox(client_channel);
                    let _ = svc.subscribe_inbox(0, route.clone(), client_channel, client_tx);
                    // Also register an active request so replies are authorized
                    svc.register_request("corr-size".to_string(), "rpc://realm1/area1/handler".to_string(), route.clone());
                    route
                };

                let body = vec![0u8; size];
                let payload = build_reply_payload("corr-size", &body);
                let ctx_reply = DomainContext {
                    route: build_inbox_route(&inbox_route),
                    route_str: inbox_route,
                    payload,
                    channel_id: 100,
                    route_family: 0,
                    sender: None,
                };
                let _ = domain.handle(ctx_reply);
            });
        });
    }
    group.finish();
}

criterion_group!(
    name = hotpath_rpc_core;
    config = config::criterion_config();
    targets =
    bench_inbox_allocation,
    bench_request_tracking,
    bench_concurrent_inbox_allocation,
    bench_correlation_lookup_scaling,
    bench_sequential_request_reply,
    bench_reply_payload_sizes
);
criterion_main!(hotpath_rpc_core);
