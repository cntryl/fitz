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

// ---------------------------------------------------------
// Shared Runtime
// ---------------------------------------------------------
static RT: OnceLock<Runtime> = OnceLock::new();
fn rt() -> &'static Runtime {
    RT.get_or_init(|| Runtime::new().expect("runtime"))
}

// ---------------------------------------------------------
// Shared LeaseService (no expirer for microbench)
// ---------------------------------------------------------
static SVC: OnceLock<Arc<LeaseService>> = OnceLock::new();
fn shared_service() -> Arc<LeaseService> {
    SVC.get_or_init(|| {
        std::env::set_var("FITZ_LEASE_SPAWN_EXPIRER", "0");
        rt().block_on(async { LeaseService::new_no_expirer() })
    })
    .clone()
}

// ---------------------------------------------------------
// Shared LeaseDomain
// ---------------------------------------------------------
static DOMAIN: OnceLock<Arc<LeaseDomain>> = OnceLock::new();
fn shared_domain() -> Arc<LeaseDomain> {
    DOMAIN
        .get_or_init(|| {
            std::env::set_var("FITZ_LEASE_SPAWN_EXPIRER", "0");
            Arc::new(LeaseDomain::new())
        })
        .clone()
}

// ---------------------------------------------------------
// Key helper (256-key pool)
// ---------------------------------------------------------
#[inline]
fn key(i: u64) -> String {
    format!("lease://bench/hot/{}", i & 0xFF)
}

// ---------------------------------------------------------
// DomainContext builder for handler benches
// ---------------------------------------------------------
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
        route_family: 0,
    }
}

// ---------------------------------------------------------
// Base: acquire (uncontended)
// ---------------------------------------------------------
fn bench_acquire_uncontended(c: &mut Criterion) {
    let svc = shared_service();
    let mut counter = 0u64;

    c.bench_function("lease_acquire_uncontended", |b| {
        b.iter(|| {
            counter = counter.wrapping_add(1);
            let k = key(counter);
            let svc = svc.clone();

            rt().block_on(async move {
                let grant = svc.acquire(0, &k, 3).await.unwrap();
                let _ = svc.surrender(0, &k, &grant.id, &grant.token).await;
                grant
            })
        });
    });
}

// ---------------------------------------------------------
// Base: renew
// ---------------------------------------------------------
fn bench_renew(c: &mut Criterion) {
    let svc = shared_service();
    let mut counter = 0u64;

    c.bench_function("lease_extend", |b| {
        b.iter(|| {
            counter = counter.wrapping_add(1);
            let k = key(counter);
            let svc = svc.clone();

            rt().block_on(async move {
                let grant = svc.acquire(0, &k, 3).await.unwrap();
                let _ = svc.renew(0, &k, &grant.id, &grant.token, 2).await;
                let _ = svc.surrender(0, &k, &grant.id, &grant.token).await;
                grant
            })
        });
    });
}

// ---------------------------------------------------------
// Base: surrender only
// ---------------------------------------------------------
fn bench_surrender(c: &mut Criterion) {
    let svc = shared_service();
    let mut counter = 0u64;

    c.bench_function("lease_surrender_no_waiters", |b| {
        b.iter(|| {
            counter = counter.wrapping_add(1);
            let k = key(counter);
            let svc = svc.clone();

            rt().block_on(async move {
                let grant = svc.acquire(0, &k, 3).await.unwrap();
                svc.surrender(0, &k, &grant.id, &grant.token).await
            })
        });
    });
}

// ---------------------------------------------------------
// Base: acquire + surrender cycle
// ---------------------------------------------------------
fn bench_cycle(c: &mut Criterion) {
    let svc = shared_service();
    let mut counter = 0u64;

    c.bench_function("lease_cycle", |b| {
        b.iter(|| {
            counter = counter.wrapping_add(1);
            let k = key(counter);
            let svc = svc.clone();

            rt().block_on(async move {
                let grant = svc.acquire(0, &k, 3).await.unwrap();
                svc.surrender(0, &k, &grant.id, &grant.token).await
            })
        });
    });
}

// ---------------------------------------------------------
// Handler benches: Acquire + Surrender
// ---------------------------------------------------------
fn bench_acquire_surrender_handler(c: &mut Criterion) {
    let domain = shared_domain();
    let mut counter = 0u64;

    c.bench_function("handler_acquire_surrender", |b| {
        b.iter(|| {
            counter = counter.wrapping_add(1);
            let route = key(counter);
            let domain = domain.clone();

            rt().block_on(async move {
                // acquire payload
                let mut acq = Vec::with_capacity(16);
                build_tlv(TAG_LEASE, &3u32.to_be_bytes(), &mut acq);

                let req = make_request(&route, acq);
                let resp = domain.handle(req).await;

                if let DomainResponse::Frame(fr) = resp {
                    let id = find_tlv(fr.as_ref(), TAG_ID)
                        .map(|b| String::from_utf8_lossy(b).into_owned());
                    let token = find_tlv(fr.as_ref(), TAG_DELIVERY_TOKEN)
                        .map(|b| String::from_utf8_lossy(b).into_owned());

                    if let (Some(id), Some(token)) = (id, token) {
                        // surrender
                        let mut rel = Vec::with_capacity(32);
                        build_tlv(TAG_ID, id.as_bytes(), &mut rel);
                        build_tlv(TAG_DELIVERY_TOKEN, token.as_bytes(), &mut rel);

                        let rel_req = make_request(&route, rel);
                        let _ = domain.handle(rel_req).await;
                    }
                }
            })
        });
    });
}

// ---------------------------------------------------------
// Handler benches: renew
// ---------------------------------------------------------
fn bench_renew_handler(c: &mut Criterion) {
    let domain = shared_domain();
    let mut counter = 0u64;

    c.bench_function("handler_extend", |b| {
        b.iter(|| {
            counter = counter.wrapping_add(1);
            let route = key(counter);
            let domain = domain.clone();

            rt().block_on(async move {
                // acquire
                let mut acq = Vec::with_capacity(16);
                build_tlv(TAG_LEASE, &3u32.to_be_bytes(), &mut acq);
                let resp = domain.handle(make_request(&route, acq)).await;

                if let DomainResponse::Frame(fr) = resp {
                    let id = find_tlv(fr.as_ref(), TAG_ID)
                        .map(|b| String::from_utf8_lossy(b).into_owned());
                    let token = find_tlv(fr.as_ref(), TAG_DELIVERY_TOKEN)
                        .map(|b| String::from_utf8_lossy(b).into_owned());

                    if let (Some(id), Some(token)) = (id, token) {
                        // renew
                        let mut ext = Vec::with_capacity(32);
                        build_tlv(TAG_ID, id.as_bytes(), &mut ext);
                        build_tlv(TAG_DELIVERY_TOKEN, token.as_bytes(), &mut ext);
                        build_tlv(TAG_LEASE, &5u32.to_be_bytes(), &mut ext);

                        let _ = domain.handle(make_request(&route, ext)).await;

                        // release
                        let mut rel = Vec::with_capacity(32);
                        build_tlv(TAG_ID, id.as_bytes(), &mut rel);
                        build_tlv(TAG_DELIVERY_TOKEN, token.as_bytes(), &mut rel);
                        let _ = domain.handle(make_request(&route, rel)).await;
                    }
                }
            })
        });
    });
}

// ---------------------------------------------------------
// Multi-tenant: 5 route families
// ---------------------------------------------------------
fn bench_acquire_multi_tenant_5(c: &mut Criterion) {
    let svc = shared_service();
    let mut counter = 0u64;

    c.bench_function("lease_acquire_multi_tenant_5rf", |b| {
        b.iter(|| {
            counter = counter.wrapping_add(1);
            let rf = (counter % 5) as u32;
            let k = key(counter);
            let svc = svc.clone();

            rt().block_on(async move {
                let grant = svc.acquire(rf, &k, 3).await.unwrap();
                let _ = svc.surrender(rf, &k, &grant.id, &grant.token).await;
                grant
            })
        });
    });
}

fn bench_renew_multi_tenant_5(c: &mut Criterion) {
    let svc = shared_service();
    let mut counter = 0u64;

    c.bench_function("lease_renew_multi_tenant_5rf", |b| {
        b.iter(|| {
            counter = counter.wrapping_add(1);
            let rf = (counter % 5) as u32;
            let k = key(counter);
            let svc = svc.clone();

            rt().block_on(async move {
                let grant = svc.acquire(rf, &k, 3).await.unwrap();
                let _ = svc.renew(rf, &k, &grant.id, &grant.token, 2).await;
                let _ = svc.surrender(rf, &k, &grant.id, &grant.token).await;
                grant
            })
        });
    });
}

fn bench_cycle_multi_tenant_5(c: &mut Criterion) {
    let svc = shared_service();
    let mut counter = 0u64;

    c.bench_function("lease_cycle_multi_tenant_5rf", |b| {
        b.iter(|| {
            counter = counter.wrapping_add(1);
            let rf = (counter % 5) as u32;
            let k = key(counter);
            let svc = svc.clone();

            rt().block_on(async move {
                let grant = svc.acquire(rf, &k, 3).await.unwrap();
                svc.surrender(rf, &k, &grant.id, &grant.token).await
            })
        });
    });
}

// ---------------------------------------------------------
// Multi-tenant: 10 route families
// ---------------------------------------------------------
fn bench_acquire_multi_tenant_10(c: &mut Criterion) {
    let svc = shared_service();
    let mut counter = 0u64;

    c.bench_function("lease_acquire_multi_tenant_10rf", |b| {
        b.iter(|| {
            counter = counter.wrapping_add(1);
            let rf = (counter % 10) as u32;
            let k = key(counter);
            let svc = svc.clone();

            rt().block_on(async move {
                let grant = svc.acquire(rf, &k, 3).await.unwrap();
                let _ = svc.surrender(rf, &k, &grant.id, &grant.token).await;
                grant
            })
        });
    });
}

fn bench_renew_multi_tenant_10(c: &mut Criterion) {
    let svc = shared_service();
    let mut counter = 0u64;

    c.bench_function("lease_renew_multi_tenant_10rf", |b| {
        b.iter(|| {
            counter = counter.wrapping_add(1);
            let rf = (counter % 10) as u32;
            let k = key(counter);
            let svc = svc.clone();

            rt().block_on(async move {
                let grant = svc.acquire(rf, &k, 3).await.unwrap();
                let _ = svc.renew(rf, &k, &grant.id, &grant.token, 2).await;
                let _ = svc.surrender(rf, &k, &grant.id, &grant.token).await;
                grant
            })
        });
    });
}

fn bench_cycle_multi_tenant_10(c: &mut Criterion) {
    let svc = shared_service();
    let mut counter = 0u64;

    c.bench_function("lease_cycle_multi_tenant_10rf", |b| {
        b.iter(|| {
            counter = counter.wrapping_add(1);
            let rf = (counter % 10) as u32;
            let k = key(counter);
            let svc = svc.clone();

            rt().block_on(async move {
                let grant = svc.acquire(rf, &k, 3).await.unwrap();
                svc.surrender(rf, &k, &grant.id, &grant.token).await
            })
        });
    });
}

// ---------------------------------------------------------
// Multi-tenant: 100 route families
// ---------------------------------------------------------
fn bench_acquire_multi_tenant_100(c: &mut Criterion) {
    let svc = shared_service();
    let mut counter = 0u64;

    c.bench_function("lease_acquire_multi_tenant_100rf", |b| {
        b.iter(|| {
            counter = counter.wrapping_add(1);
            let rf = (counter % 100) as u32;
            let k = key(counter);
            let svc = svc.clone();

            rt().block_on(async move {
                let grant = svc.acquire(rf, &k, 3).await.unwrap();
                let _ = svc.surrender(rf, &k, &grant.id, &grant.token).await;
                grant
            })
        });
    });
}

fn bench_renew_multi_tenant_100(c: &mut Criterion) {
    let svc = shared_service();
    let mut counter = 0u64;

    c.bench_function("lease_renew_multi_tenant_100rf", |b| {
        b.iter(|| {
            counter = counter.wrapping_add(1);
            let rf = (counter % 100) as u32;
            let k = key(counter);
            let svc = svc.clone();

            rt().block_on(async move {
                let grant = svc.acquire(rf, &k, 3).await.unwrap();
                let _ = svc.renew(rf, &k, &grant.id, &grant.token, 2).await;
                let _ = svc.surrender(rf, &k, &grant.id, &grant.token).await;
                grant
            })
        });
    });
}

fn bench_cycle_multi_tenant_100(c: &mut Criterion) {
    let svc = shared_service();
    let mut counter = 0u64;

    c.bench_function("lease_cycle_multi_tenant_100rf", |b| {
        b.iter(|| {
            counter = counter.wrapping_add(1);
            let rf = (counter % 100) as u32;
            let k = key(counter);
            let svc = svc.clone();

            rt().block_on(async move {
                let grant = svc.acquire(rf, &k, 3).await.unwrap();
                svc.surrender(rf, &k, &grant.id, &grant.token).await
            })
        });
    });
}

// ---------------------------------------------------------
// Criterion group + main
// ---------------------------------------------------------
criterion_group! {
    name = hotpath_lease;
    config = config::criterion_config();
    targets =
        bench_acquire_uncontended,
        bench_renew,
        bench_surrender,
        bench_cycle,
        bench_acquire_surrender_handler,
        bench_renew_handler,
        bench_acquire_multi_tenant_5,
        bench_renew_multi_tenant_5,
        bench_cycle_multi_tenant_5,
        bench_acquire_multi_tenant_10,
        bench_renew_multi_tenant_10,
        bench_cycle_multi_tenant_10,
        bench_acquire_multi_tenant_100,
        bench_renew_multi_tenant_100,
        bench_cycle_multi_tenant_100,
}

criterion_main!(hotpath_lease);
