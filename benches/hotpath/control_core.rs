//! Hotpath benchmarks for Control domain handler->service layer
//!
//! Tests control command processing, heartbeat handling, configuration updates,
//! and notice service integration.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use fitz::core::control::ControlDomain;
use fitz::core::domain::{Domain, DomainContext};
use fitz::protocol::route::Route;
use fitz::protocol::tags::TAG_BODY;
use std::sync::Arc;

#[path = "../config.rs"]
mod config;

/// Build TLV payload for control operations
fn build_control_payload(body: &[u8]) -> Vec<u8> {
    let mut payload = Vec::new();
    
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

/// Build route for control operation
fn build_route(operation: &str) -> Route {
    let route_str = format!("control://system/{}", operation);
    Route {
        scheme: fitz::protocol::route::Scheme::Control,
        realm: Some("system".to_string()),
        area: None,
        resource: Some(operation.to_string()),
        operation: None,
        raw: route_str.clone(),
    }
}

/// Sequential heartbeat operations
fn bench_sequential_heartbeat(c: &mut Criterion) {
    let domain = ControlDomain::new();
    
    let mut group = c.benchmark_group("control_sequential_heartbeat");
    
    for count in [100, 1000, 10000] {
        group.throughput(Throughput::Elements(count));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, &count| {
            b.iter(|| {
                // Arrange & Act
                for _ in 0..count {
                    let body = b"heartbeat";
                    let payload = build_control_payload(body);
                    let route = build_route("heartbeat");
                    
                    let ctx = DomainContext {
                        route,
                        route_str: "control://system/heartbeat".to_string(),
                        payload,
                        channel_id: 1,
                        route_family: 0,
                        sender: None,
                    };
                    
                    let _response = domain.handle(ctx);
                }
                
                // Assert - implicit success
            });
        });
    }
    
    group.finish();
}

/// Configuration update operations
fn bench_config_updates(c: &mut Criterion) {
    let domain = ControlDomain::new();
    
    c.bench_function("control_config_updates", |b| {
        b.iter(|| {
            // Arrange & Act
            for i in 0..100 {
                let body = format!("{{\"key\":\"value{}\"}}", i).into_bytes();
                let payload = build_control_payload(&body);
                let route = build_route("config");
                
                let ctx = DomainContext {
                    route,
                    route_str: "control://system/config".to_string(),
                    payload,
                    channel_id: 1,
                    route_family: 0,
                    sender: None,
                };
                
                let _response = domain.handle(ctx);
            }
            
            // Assert - implicit success
        });
    });
}

/// Concurrent control commands from multiple channels
fn bench_concurrent_commands(c: &mut Criterion) {
    c.bench_function("control_concurrent_commands", |b| {
        b.iter(|| {
            // Arrange
            let domain: Arc<ControlDomain> = Arc::clone(&Arc::new(ControlDomain::new()));

            // Act - 10 concurrent channels sending commands
            let handles: Vec<_> = (0..10)
                .map(|channel_id| {
                    let domain = Arc::clone(&domain);
                    std::thread::spawn(move || {
                        for i in 0..10 {
                            let body = format!("command-{}-{}", channel_id, i).into_bytes();
                            let payload = build_control_payload(&body);
                            let route = build_route("command");
                            let ctx = DomainContext {
                                route,
                                route_str: "control://system/command".to_string(),
                                payload,
                                channel_id: channel_id as u32,
                                route_family: 0,
                                sender: None,
                            };
                            domain.handle(ctx);
                        }
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

/// Command payload sizes
fn bench_command_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("control_command_sizes");
    
    for &size in &[64, 256, 1024, 4096] {
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            let domain = ControlDomain::new();
            
            b.iter(|| {
                // Arrange
                let body = vec![0u8; size];
                let payload = build_control_payload(&body);
                let route = build_route("command");
                
                // Act
                let ctx = DomainContext {
                    route,
                    route_str: "control://system/command".to_string(),
                    payload,
                    channel_id: 1,
                    route_family: 0,
                    sender: None,
                };
                
                let _response = domain.handle(ctx);
                
                // Assert - implicit success
            });
        });
    }
    
    group.finish();
}

criterion_group!(
    name = hotpath_control_core;
    config = config::criterion_config();
    targets =
        bench_sequential_heartbeat,
        bench_config_updates,
        bench_concurrent_commands,
        bench_command_sizes
);
criterion_main!(hotpath_control_core);
