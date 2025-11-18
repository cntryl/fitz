//! Hotpath benchmarks for permission checking operations
//!
//! Permission checks are called on every message and are critical for performance.
//! These benchmarks focus on the core authorization logic including route parsing,
//! grant matching, and permission validation.

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use fitz::authz::mock_jwks::Claims;
use fitz::authz::permissions::{check_route_authorization, has_permission, install_claim_grants};
use std::sync::OnceLock;

#[path = "../config.rs"]
mod config;

// ---------------------------------------------------------
// Shared test data
// ---------------------------------------------------------
static TEST_TENANT: &str = "acme";
static TEST_ROUTES: OnceLock<Vec<String>> = OnceLock::new();
static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

fn test_routes() -> &'static [String] {
    TEST_ROUTES.get_or_init(|| {
        vec![
            "stream://acme/orders/created".to_string(),
            "stream://acme/orders/updated".to_string(),
            "queue://acme/orders/pending".to_string(),
            "queue://acme/orders/processed".to_string(),
            "notice://acme/alerts/security".to_string(),
            "rpc://acme/auth/validate".to_string(),
            "kv://acme/config/settings".to_string(),
            "control://acme/metrics".to_string(),
            "inbox://acme/rpc/reply/123".to_string(),
        ]
    })
}

static CLAIMS: OnceLock<Claims> = OnceLock::new();
fn test_claims() -> &'static Claims {
    CLAIMS.get_or_init(|| {
        Claims {
            sub: "user123".to_string(),
            aud: Some("fitz".to_string()),
            exp: Some(2000000000), // far future
            perms: Some(vec![
                "read:stream://acme/orders/*".to_string(),
                "write:queue://acme/orders/*".to_string(),
                "read:notice://acme/alerts/*".to_string(),
                "write:rpc://acme/auth/*".to_string(),
                "*::kv://acme/config/*".to_string(),
            ]),
            scope: None,
            roles: None,
        }
    })
}

fn test_runtime() -> &'static tokio::runtime::Runtime {
    RUNTIME.get_or_init(|| tokio::runtime::Runtime::new().expect("runtime"))
}

// Initialize test tenant with claims (once per process)
fn init_test_tenant() {
    static INIT: OnceLock<()> = OnceLock::new();
    INIT.get_or_init(|| {
        let rt = test_runtime();
        rt.block_on(async {
            install_claim_grants(TEST_TENANT, test_claims()).await;
        });
    });
}

// ---------------------------------------------------------
// Micro-benchmarks: individual authorization paths
// ---------------------------------------------------------

fn bench_check_route_authorization_allowed(c: &mut Criterion) {
    init_test_tenant();

    c.bench_function("check_route_authorization_allowed", |b| {
        b.iter(|| {
            let result = check_route_authorization(TEST_TENANT, "stream://acme/orders/created");
            criterion::black_box(result);
        })
    });
}

fn bench_check_route_authorization_denied(c: &mut Criterion) {
    init_test_tenant();

    c.bench_function("check_route_authorization_denied", |b| {
        b.iter(|| {
            let result = check_route_authorization(TEST_TENANT, "stream://other/orders/created");
            criterion::black_box(result);
        })
    });
}

fn bench_has_permission_wrapper(c: &mut Criterion) {
    init_test_tenant();

    c.bench_function("has_permission_wrapper", |b| {
        b.iter(|| {
            let result = has_permission(TEST_TENANT, "queue://acme/orders/pending");
            criterion::black_box(result);
        })
    });
}

fn bench_check_route_authorization_control(c: &mut Criterion) {
    // System route: allowed via fast path, no grants lookup
    c.bench_function("check_route_authorization_control", |b| {
        b.iter(|| {
            let result = check_route_authorization(TEST_TENANT, "control://acme/metrics");
            criterion::black_box(result);
        })
    });
}

fn bench_check_route_authorization_inbox(c: &mut Criterion) {
    // System route: allowed via fast path, no grants lookup
    c.bench_function("check_route_authorization_inbox", |b| {
        b.iter(|| {
            let result = check_route_authorization(TEST_TENANT, "inbox://acme/rpc/reply/123");
            criterion::black_box(result);
        })
    });
}

fn bench_check_route_authorization_bare_route(c: &mut Criterion) {
    // Dev/test bare route: allowed without parsing or grant checking
    c.bench_function("check_route_authorization_bare_route", |b| {
        b.iter(|| {
            let result = check_route_authorization(TEST_TENANT, "ntc/alerts/security");
            criterion::black_box(result);
        })
    });
}

fn bench_check_route_authorization_rpc_reply(c: &mut Criterion) {
    // Dev/test bare route: allowed without parsing or grant checking
    c.bench_function("check_route_authorization_rpc_reply", |b| {
        b.iter(|| {
            let result = check_route_authorization(TEST_TENANT, "rpc/reply/123");
            criterion::black_box(result);
        })
    });
}

// ---------------------------------------------------------
// Macro-benchmarks: authz setup and mixed workloads
// ---------------------------------------------------------

fn bench_install_claim_grants(c: &mut Criterion) {
    c.bench_function("install_claim_grants", |b| {
        b.iter_batched(
            || Claims {
                sub: "bench_user".to_string(),
                aud: Some("fitz".to_string()),
                exp: Some(2000000000),
                perms: Some(vec![
                    "read:stream://bench/*".to_string(),
                    "write:queue://bench/*".to_string(),
                ]),
                scope: None,
                roles: None,
            },
            |claims| {
                let rt = test_runtime();
                rt.block_on(async {
                    install_claim_grants("bench_tenant", &claims).await;
                });
            },
            BatchSize::SmallInput,
        )
    });
}

fn bench_multiple_route_checks(c: &mut Criterion) {
    init_test_tenant();
    let routes = test_routes();

    c.bench_function("multiple_route_checks", |b| {
        b.iter(|| {
            for route in routes {
                let result = check_route_authorization(TEST_TENANT, route);
                criterion::black_box(result);
            }
        })
    });
}

criterion_group!(
    name = permissions_hotpath;
    config = config::criterion_config();
    targets =
        bench_check_route_authorization_allowed,
        bench_check_route_authorization_denied,
        bench_has_permission_wrapper,
        bench_check_route_authorization_control,
        bench_check_route_authorization_inbox,
        bench_check_route_authorization_bare_route,
        bench_check_route_authorization_rpc_reply,
        bench_install_claim_grants,
        bench_multiple_route_checks
);

criterion_main!(permissions_hotpath);
