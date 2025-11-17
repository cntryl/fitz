//! Hotpath benchmarks for Lease domain
//!
//! Tests both microbenchmarks (token generation, UUID formatting) and
//! handler->service layer benchmarks (acquire/renew/surrender cycles, contention).

use base64::{engine::general_purpose, Engine as _};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use fitz::core::domain::{Domain, DomainContext};
use fitz::core::lease::LeaseDomain;
use fitz::protocol::route::Route;
use fitz::protocol::tags::{TAG_BODY, TAG_TOKEN};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::sync::Arc;
use uuid::Uuid;

type HmacSha256 = Hmac<Sha256>;
use std::time::{Duration, Instant};

#[path = "../config.rs"]
mod config;

fn bench_token_generation(c: &mut Criterion) {
    // Recreate minimal token generation hotpath (HMAC-SHA256 + base64) so
    // this bench can run without accessing service internals.
    let key = "lease://realm/area/resource";
    let id = "123e4567-e89b-12d3-a456-426614174000".to_string();
    let expiry = Instant::now() + Duration::from_secs(30);
    let secret = b"bench-secret-key-0123456789".to_vec();

    c.bench_function("lease_token_generation", |b| {
        b.iter(|| {
            let expiry_unix = (std::time::SystemTime::now()
                + expiry.saturating_duration_since(Instant::now()))
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();

            let mut mac = HmacSha256::new_from_slice(&secret).expect("HMAC key");
            mac.update(key.as_bytes());
            mac.update(b"|");
            mac.update(id.as_bytes());
            mac.update(b"|");

            // write expiry digits without alloc
            let mut buf = [0u8; 20];
            let mut t = expiry_unix;
            let mut len = 0;
            if t == 0 {
                buf[0] = b'0';
                len = 1;
            } else {
                while t > 0 {
                    buf[len] = b'0' + (t % 10) as u8;
                    t /= 10;
                    len += 1;
                }
                buf[..len].reverse();
            }
            mac.update(&buf[..len]);
            let token = general_purpose::STANDARD.encode(mac.finalize().into_bytes());
            std::hint::black_box(token);
        });
    });
}

fn bench_uuid_formatting(c: &mut Criterion) {
    c.bench_function("lease_uuid_formatting", |b| {
        b.iter(|| {
            let _ = Uuid::new_v4().to_string();
        });
    });
}

fn bench_lease_entry_state_transitions(c: &mut Criterion) {
    struct LocalLeaseEntry {
        id: String,
        token: String,
        expiry: Instant,
    }

    impl LocalLeaseEntry {
        fn free() -> Self {
            Self {
                id: String::new(),
                token: String::new(),
                expiry: Instant::now(),
            }
        }

        #[inline]
        fn is_active(&self, now: Instant) -> bool {
            !self.id.is_empty() && now < self.expiry
        }
    }

    let mut entry = LocalLeaseEntry::free();
    let now = Instant::now();

    c.bench_function("lease_entry_state_transitions", |b| {
        b.iter(|| {
            // simulate acquire
            entry.id = "id".to_string();
            entry.token = "token".to_string();
            entry.expiry = now + Duration::from_secs(30);

            // check active
            let _active = entry.is_active(now);

            // simulate expire/reset
            entry = LocalLeaseEntry::free();
        });
    });
}

// --- Handler->Service Layer Benchmarks ---

/// Build TLV payload for lease acquire
fn build_acquire_payload(ttl_secs: u64) -> Vec<u8> {
    let mut payload = Vec::new();
    
    // TAG_BODY (TTL as bytes)
    payload.push(TAG_BODY);
    payload.push(8);
    payload.extend_from_slice(&ttl_secs.to_be_bytes());
    
    payload
}

/// Build TLV payload for lease renew
#[allow(dead_code)]
fn build_renew_payload(token: &str) -> Vec<u8> {
    let mut payload = Vec::new();
    
    // TAG_TOKEN
    payload.push(TAG_TOKEN);
    payload.push(token.len() as u8);
    payload.extend_from_slice(token.as_bytes());
    
    payload
}

/// Build route for lease operation
fn build_route(operation: &str) -> Route {
    let route_str = format!("lease://realm1/area1/{}", operation);
    Route {
        scheme: fitz::protocol::route::Scheme::Lease,
        realm: Some("realm1".to_string()),
        area: Some("area1".to_string()),
        resource: Some(operation.to_string()),
        operation: None,
        raw: route_str.clone(),
    }
}

/// Sequential lease acquire/renew/surrender cycles
fn bench_sequential_lease_cycles(c: &mut Criterion) {
    let domain = LeaseDomain::new();
    
    let mut group = c.benchmark_group("lease_sequential_cycles");
    
    for count in [100, 1000] {
        group.throughput(Throughput::Elements(count));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, &count| {
            b.iter(|| {
                // Arrange & Act
                for i in 0..count {
                    // Acquire
                    let payload = build_acquire_payload(30);
                    let route = build_route(&format!("resource-{}", i));
                    let ctx = DomainContext {
                        route: route.clone(),
                        route_str: format!("lease://realm1/area1/resource-{}", i),
                        payload,
                        channel_id: 1,
                        route_family: 0,
                        sender: None,
                    };
                    let _response = domain.handle(ctx);
                    
                    // Note: In a real benchmark we'd extract the token and renew/surrender,
                    // but for throughput testing this measures the acquire hotpath
                }
                
                // Assert - implicit success
            });
        });
    }
    
    group.finish();
}

/// Concurrent lease contention on same resource
fn bench_concurrent_lease_contention(c: &mut Criterion) {
    c.bench_function("lease_concurrent_contention", |b| {
        b.iter(|| {
            // Arrange
            let domain: Arc<LeaseDomain> = Arc::clone(&Arc::new(LeaseDomain::new()));

            // Act - 10 concurrent attempts to acquire the same lease
            let handles: Vec<_> = (0..10)
                .map(|i| {
                    let domain = Arc::clone(&domain);
                    std::thread::spawn(move || {
                        let payload = build_acquire_payload(30);
                        let route = build_route("contested-resource");
                        let ctx = DomainContext {
                            route,
                            route_str: "lease://realm1/area1/contested-resource".to_string(),
                            payload,
                            channel_id: i as u32,
                            route_family: 0,
                            sender: None,
                        };
                        domain.handle(ctx)
                    })
                })
                .collect();

            for handle in handles {
                let _ = handle.join();
            }

            // Assert - implicit success (only one should succeed)
        });
    });
}

/// Multi-tenant lease isolation
fn bench_multitenant_leases(c: &mut Criterion) {
    c.bench_function("lease_multitenant_isolation", |b| {
        b.iter(|| {
            // Arrange
            let domain: Arc<LeaseDomain> = Arc::clone(&Arc::new(LeaseDomain::new()));

            // Act - 10 tenants each acquiring their own leases
            let handles: Vec<_> = (0..10)
                .map(|tenant_id| {
                    let domain = Arc::clone(&domain);
                    std::thread::spawn(move || {
                        for i in 0..10 {
                            let payload = build_acquire_payload(30);
                            let route = build_route(&format!("resource-{}", i));
                            let ctx = DomainContext {
                                route,
                                route_str: format!("lease://realm1/area1/resource-{}", i),
                                payload,
                                channel_id: tenant_id as u32,
                                route_family: tenant_id as u32, // Different route families
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

criterion_group!(
    name = hotpath_lease_core;
    config = config::criterion_config();
    targets =
        // Microbenchmarks
        bench_token_generation,
        bench_uuid_formatting,
        bench_lease_entry_state_transitions,
        // Handler->Service benchmarks
        bench_sequential_lease_cycles,
        bench_concurrent_lease_contention,
        bench_multitenant_leases
);

criterion_main!(hotpath_lease_core);
