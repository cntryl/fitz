//! Hotpath benchmarks for RPC domain handler->service layer
//!
//! Tests RPC inbox allocation, correlation ID management, request tracking,
//! and handler registration/matching performance.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use fitz::core::rpc::RpcDomain;
use std::sync::Arc;

#[path = "../config.rs"]
mod config;

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

/// Test authorization checking (can publish to inbox)
fn bench_authorization_check(c: &mut Criterion) {
    c.bench_function("rpc_authorization_check", |b| {
        // Arrange
        let domain = RpcDomain::new();
        let service = domain.get_service();
        
        // Register 100 active requests
        {
            let mut svc = service.write();
            for i in 0..100 {
                let corr_id = format!("corr-{}", i);
                let handler_route = format!("rpc://realm/area/handler");
                let reply_route = format!("rpc://realm/area/inbox-{}", i);
                svc.register_request(corr_id, handler_route, reply_route);
            }
        }

        b.iter(|| {
            // Act - check authorization for 100 requests
            let svc = service.read();
            for i in 0..100 {
                let reply_route = format!("rpc://realm/area/inbox-{}", i);
                let corr_id = format!("corr-{}", i);
                let _can_publish = svc.can_publish_to_inbox(&reply_route, &corr_id);
            }

            // Assert - implicit success
        });
    });
}

criterion_group!(
    name = hotpath_rpc_core;
    config = config::criterion_config();
    targets =
        bench_inbox_allocation,
        bench_request_tracking,
        bench_concurrent_inbox_allocation,
        bench_authorization_check
);
criterion_main!(hotpath_rpc_core);
