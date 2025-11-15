//! Hotpath benchmarks for route parsing and matching operations
//!
//! Route operations are called on every message and are critical for performance.
//! These benchmarks focus on the core route parsing and trie matching logic.

use criterion::{criterion_group, criterion_main, Criterion};
use fitz::protocol::route::parse_route;
use fitz::routing::{RouteFamilyId, RouteTable};
use std::sync::OnceLock;

#[path = "../config.rs"]
mod config;

// ---------------------------------------------------------
// Shared test data
// ---------------------------------------------------------
static TEST_ROUTES: OnceLock<Vec<String>> = OnceLock::new();
fn test_routes() -> &'static [String] {
    TEST_ROUTES.get_or_init(|| {
        vec![
            "queue://tenant1/orders/pending".to_string(),
            "queue://tenant1/orders/processed".to_string(),
            "queue://tenant1/inventory/updates".to_string(),
            "stream://tenant1/events/user_actions".to_string(),
            "stream://tenant1/events/system_events".to_string(),
            "notice://tenant1/alerts/security".to_string(),
            "rpc://tenant1/auth/user/validate".to_string(),
            "lease://tenant1/locks/database".to_string(),
        ]
    })
}

static ROUTE_TABLE: OnceLock<RouteTable> = OnceLock::new();
fn route_table() -> &'static RouteTable {
    ROUTE_TABLE.get_or_init(|| {
        let mut table = RouteTable::new();
        let rf = RouteFamilyId::new();

        // Subscribe to various patterns
        for route in test_routes() {
            table.subscribe(rf, route, 1).unwrap();
        }

        // Add some wildcard subscriptions
        table.subscribe(rf, "queue://tenant1/orders/*", 2).unwrap();
        table.subscribe(rf, "stream://tenant1/events/*", 3).unwrap();
        table.subscribe(rf, "notice://tenant1/*", 4).unwrap();

        table
    })
}

// ---------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------

fn bench_route_parse_simple(c: &mut Criterion) {
    c.bench_function("route_parse_simple", |b| {
        b.iter(|| {
            let route = parse_route("queue://tenant1/orders/pending").unwrap();
            criterion::black_box(route);
        })
    });
}

fn bench_route_parse_complex(c: &mut Criterion) {
    c.bench_function("route_parse_complex", |b| {
        b.iter(|| {
            let route = parse_route("rpc://tenant1/auth/user/validate/operation").unwrap();
            criterion::black_box(route);
        })
    });
}

fn bench_route_parse_wildcard(c: &mut Criterion) {
    c.bench_function("route_parse_wildcard", |b| {
        b.iter(|| {
            let route = parse_route("queue://tenant1/orders/*").unwrap();
            criterion::black_box(route);
        })
    });
}

fn bench_route_table_subscribe(c: &mut Criterion) {
    c.bench_function("route_table_subscribe", |b| {
        b.iter_batched(
            || RouteTable::new(),
            |mut table| {
                let rf = RouteFamilyId::new();
                table.subscribe(rf, "queue://tenant1/test/route", 1).unwrap();
            },
            criterion::BatchSize::SmallInput,
        )
    });
}

fn bench_route_table_match_exact(c: &mut Criterion) {
    let table = route_table();
    let rf = RouteFamilyId::new();

    c.bench_function("route_table_match_exact", |b| {
        b.iter(|| {
            let matches = table.matches(rf, "queue://tenant1/orders/pending");
            criterion::black_box(matches);
        })
    });
}

fn bench_route_table_match_wildcard(c: &mut Criterion) {
    let table = route_table();
    let rf = RouteFamilyId::new();

    c.bench_function("route_table_match_wildcard", |b| {
        b.iter(|| {
            let matches = table.matches(rf, "queue://tenant1/orders/new_order");
            criterion::black_box(matches);
        })
    });
}

fn bench_route_table_match_multiple(c: &mut Criterion) {
    let table = route_table();
    let rf = RouteFamilyId::new();

    c.bench_function("route_table_match_multiple", |b| {
        b.iter(|| {
            let matches = table.matches(rf, "notice://tenant1/alerts/security");
            criterion::black_box(matches);
        })
    });
}

fn bench_route_table_no_match(c: &mut Criterion) {
    let table = route_table();
    let rf = RouteFamilyId::new();

    c.bench_function("route_table_no_match", |b| {
        b.iter(|| {
            let matches = table.matches(rf, "unknown://route");
            criterion::black_box(matches);
        })
    });
}

fn bench_route_family_creation(c: &mut Criterion) {
    c.bench_function("route_family_creation", |b| {
        b.iter(|| {
            let rf = RouteFamilyId::new();
            criterion::black_box(rf);
        })
    });
}

criterion_group!(
    name = route_hotpath;
    config = config::criterion_config();
    targets =
        bench_route_parse_simple,
        bench_route_parse_complex,
        bench_route_parse_wildcard,
        bench_route_table_subscribe,
        bench_route_table_match_exact,
        bench_route_table_match_wildcard,
        bench_route_table_match_multiple,
        bench_route_table_no_match,
        bench_route_family_creation
);

criterion_main!(route_hotpath);