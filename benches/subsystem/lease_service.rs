//! Subsystem Bench: LeaseService
//!
//! This file mirrors existing `benches/hotpath/lease.rs` but is conceptually a
//! subsystem benchmark exercising the full `LeaseService` behavior (no E2E
//! routing, but real DashMap, RwLock, HMAC etc.).

use criterion::{criterion_group, criterion_main, Criterion};
use fitz::core::lease::LeaseService;
use fitz::routing::DEFAULT_RF;
use std::env;
use std::sync::{Arc, OnceLock};
use std::time::Instant;
use tokio::runtime::Runtime;

#[path = "../config.rs"]
mod config;

static RUNTIME: OnceLock<Runtime> = OnceLock::new();
fn rt() -> &'static Runtime {
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime")
    })
}

static SERVICE: OnceLock<Arc<LeaseService>> = OnceLock::new();
fn service() -> Arc<LeaseService> {
    SERVICE
        .get_or_init(|| {
            env::set_var("FITZ_LEASE_SPAWN_EXPIRER", "0");
            rt().block_on(async { LeaseService::new_no_expirer() })
        })
        .clone()
}

const MAX_ITERS: u64 = 5_000;

fn bench_acquire(c: &mut Criterion) {
    let svc = service();
    let rf = DEFAULT_RF;

    c.bench_function("lease_acquire_subsystem", |b| {
        b.iter_custom(|_| {
            let start = Instant::now();
            rt().block_on(async {
                for i in 0..MAX_ITERS {
                    let key = format!("lease://bench/area/key_{:04}", i % 1024);
                    let _ = svc.acquire(rf, &key, 30).await;
                }
            });
            start.elapsed()
        })
    });
}

criterion_group!(
    name = subsystem_lease_service;
    config = config::criterion_config();
    targets = bench_acquire
);
criterion_main!(subsystem_lease_service);
