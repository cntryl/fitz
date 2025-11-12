use criterion::{criterion_group, criterion_main, Criterion};
use fitz::core::domain::{Domain, DomainResponse};
use fitz::core::lease::service::LeaseService;
use fitz::core::lease::LeaseDomain;
use fitz::protocol::frame::{build_tlv, find_tlv};
use fitz::protocol::tags::*;
use std::sync::{Arc, OnceLock};
use tokio::runtime::Runtime;

#[path = "../config.rs"]
mod config;

// Shared Tokio runtime
static RT: OnceLock<Runtime> = OnceLock::new();
fn rt() -> &'static Runtime {
    RT.get_or_init(|| Runtime::new().expect("runtime"))
}

// Helper to build a DomainContext for a given raw route and payload (used by handler benches)
fn make_request(raw: &str, payload: Vec<u8>) -> fitz::core::domain::DomainContext {
    fitz::core::domain::DomainContext {
        route: fitz::protocol::route::Route {
            scheme: fitz::protocol::route::Scheme::Lease,
            realm: None,
            area: None,
            resource: None,
            operation: None,
            raw: raw.to_string(),
        },
        route_str: raw.to_string(),
        payload,
        channel_id: 1,
        route_family: 0, // benchmarks use default route family
    }
}

// Shared LeaseService instance (no expirer for benches)
fn shared_service() -> Arc<LeaseService> {
    rt().block_on(async { LeaseService::new_no_expirer() })
}

// Shared LeaseDomain instance (use Arc so benches can clone cheaply)
fn shared_domain() -> Arc<LeaseDomain> {
    // Ensure background expirer is disabled for microbench (quiescent service)
    std::env::set_var("FITZ_LEASE_SPAWN_EXPIRER", "0");
    Arc::new(LeaseDomain::new())
}

/// Benchmark: Acquire a lease (no contention)
fn bench_acquire_uncontended(c: &mut Criterion) {
    let svc = shared_service();
    let mut counter = 0u64;
    c.bench_function("lease_acquire_uncontended", |b| {
        b.iter(|| {
            counter = counter.wrapping_add(1);
            let key = format!("lease://hotpath/bench/{}", counter % 64);
            let svc = svc.clone();
            rt().block_on(async move {
                let grant = svc.acquire(&key, 3).await.unwrap();
                // release immediately to avoid leaving the lease active and
                // blocking future iterations that reuse the same key
                let _ = svc.surrender(&key, &grant.id, &grant.token).await;
                grant
            })
        });
    });
}

/// Benchmark: Extend active lease
fn bench_renew(c: &mut Criterion) {
    let svc = shared_service();
    let mut counter = 0u64;
    c.bench_function("lease_extend", |b| {
        b.iter(|| {
            counter = counter.wrapping_add(1);
            let key = format!("lease://hotpath/bench/{}", counter % 64);
            let svc = svc.clone();
            rt().block_on(async move {
                let grant = svc.acquire(&key, 3).await.unwrap();
                let res = svc.renew(&key, &grant.id, &grant.token, 2).await;
                // clean up so the next iteration is uncontended
                let _ = svc.surrender(&key, &grant.id, &grant.token).await;
                res
            })
        });
    });
}

/// Benchmark: Release lease (no waiters)
fn bench_release(c: &mut Criterion) {
    let svc = shared_service();
    let mut counter = 0u64;
    c.bench_function("lease_release_no_waiters", |b| {
        b.iter(|| {
            counter = counter.wrapping_add(1);
            let key = format!("lease://hotpath/bench/{}", counter % 64);
            let svc = svc.clone();
            rt().block_on(async move {
                let grant = svc.acquire(&key, 3).await.unwrap();
                svc.surrender(&key, &grant.id, &grant.token).await
            })
        });
    });
}

/// Benchmark: Acquire  Release cycle
fn bench_cycle(c: &mut Criterion) {
    let svc = shared_service();
    let mut counter = 0u64;
    c.bench_function("lease_acquire_release_cycle", |b| {
        b.iter(|| {
            counter = counter.wrapping_add(1);
            let key = format!("lease://hotpath/bench/{}", counter % 64);
            let svc = svc.clone();
            rt().block_on(async move {
                let grant = svc.acquire(&key, 3).await.unwrap();
                svc.surrender(&key, &grant.id, &grant.token).await
            })
        });
    });
}

/// --- handler-level benches (via LeaseDomain)
/// Benchmark: Acquire + immediate release through the handler (no contention)
fn bench_acquire_release_handler(c: &mut Criterion) {
    let domain = shared_domain();
    let mut counter = 0u64;
    c.bench_function("handler_acquire_release_uncontended", |b| {
        b.iter(|| {
            counter = counter.wrapping_add(1);
            let key = format!("lease://hotpath/handler/{}", counter % 64);
            let svc = domain.clone();
            rt().block_on(async move {
                // Acquire payload
                let mut acq_payload = Vec::new();
                build_tlv(TAG_LEASE, &3u32.to_be_bytes(), &mut acq_payload);
                let req = make_request(&key, acq_payload);
                let resp = (&*svc).handle(req).await;

                // Parse response to extract id and token
                if let DomainResponse::Frame(frame) = resp {
                    let id = find_tlv(frame.as_ref(), TAG_ID)
                        .map(|b| String::from_utf8_lossy(b).into_owned());
                    let token = find_tlv(frame.as_ref(), TAG_DELIVERY_TOKEN)
                        .map(|b| String::from_utf8_lossy(b).into_owned());
                    if let (Some(id), Some(token)) = (id, token) {
                        // Build release payload
                        let mut rel_payload = Vec::new();
                        build_tlv(TAG_ID, id.as_bytes(), &mut rel_payload);
                        build_tlv(TAG_DELIVERY_TOKEN, token.as_bytes(), &mut rel_payload);
                        let rel_req = make_request(&key, rel_payload);
                        let _ = (&*svc).handle(rel_req).await;
                    }
                }
            })
        });
    });
}

/// Benchmark: Extend via handler
fn bench_renew_handler(c: &mut Criterion) {
    let domain = shared_domain();
    let mut counter = 0u64;
    c.bench_function("handler_extend", |b| {
        b.iter(|| {
            counter = counter.wrapping_add(1);
            let key = format!("lease://hotpath/handler/{}", counter % 64);
            let svc = domain.clone();
            rt().block_on(async move {
                // Acquire
                let mut acq_payload = Vec::new();
                build_tlv(TAG_LEASE, &3u32.to_be_bytes(), &mut acq_payload);
                let req = make_request(&key, acq_payload);
                let resp = (&*svc).handle(req).await;

                if let DomainResponse::Frame(frame) = resp {
                    let id = find_tlv(frame.as_ref(), TAG_ID)
                        .map(|b| String::from_utf8_lossy(b).into_owned());
                    let token = find_tlv(frame.as_ref(), TAG_DELIVERY_TOKEN)
                        .map(|b| String::from_utf8_lossy(b).into_owned());
                    if let (Some(id), Some(token)) = (id, token) {
                        // Extend payload
                        let mut ext_payload = Vec::new();
                        build_tlv(TAG_ID, id.as_bytes(), &mut ext_payload);
                        build_tlv(TAG_DELIVERY_TOKEN, token.as_bytes(), &mut ext_payload);
                        build_tlv(TAG_LEASE, &5u32.to_be_bytes(), &mut ext_payload);
                        let ext_req = make_request(&key, ext_payload);
                        let _ = (&*svc).handle(ext_req).await;

                        // Release to keep next iteration clean
                        let mut rel_payload = Vec::new();
                        build_tlv(TAG_ID, id.as_bytes(), &mut rel_payload);
                        build_tlv(TAG_DELIVERY_TOKEN, token.as_bytes(), &mut rel_payload);
                        let rel_req = make_request(&key, rel_payload);
                        let _ = (&*svc).handle(rel_req).await;
                    }
                }
            })
        });
    });
}

criterion_group! {
    name = hotpath_lease;
    config = config::criterion_config();
    targets =
        bench_acquire_uncontended,
        bench_renew,
        bench_release,
        bench_cycle,
        bench_acquire_release_handler,
        bench_renew_handler,
}

criterion_main!(hotpath_lease);
