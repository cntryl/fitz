//! Hotpath benchmarks for route parsing and matching operations
//!
//! Route operations are called on every message and are critical for performance.
//! These benchmarks focus on the core route parsing and trie matching logic.

use criterion::{criterion_group, criterion_main, Criterion};
use fitz::protocol::route::parse_route;
use fitz::routing::{RouteTable, DEFAULT_RF};
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
        let rf = DEFAULT_RF;

        // Subscribe to various patterns
        use tokio::sync::mpsc;

        // Subscribe to various patterns
        for (idx, route) in test_routes().iter().enumerate() {
            let (tx, _rx) = mpsc::channel(1);
            let sub = fitz::routing::RtSubscription {
                id: idx as u64 + 1,
                route_pattern: route.clone(),
                channel_id: 1,
                sender: tx,
            };
            table.insert(rf, sub);
        }

        // Add some wildcard subscriptions
        let (tx1, _rx1) = mpsc::channel(1);
        table.insert(rf, fitz::routing::RtSubscription {
            id: 10,
            route_pattern: "queue://tenant1/orders/*".to_string(),
            channel_id: 2,
            sender: tx1,
        });

        let (tx2, _rx2) = mpsc::channel(1);
        table.insert(rf, fitz::routing::RtSubscription {
            id: 11,
            route_pattern: "stream://tenant1/events/*".to_string(),
            channel_id: 3,
            sender: tx2,
        });

        let (tx3, _rx3) = mpsc::channel(1);
        table.insert(rf, fitz::routing::RtSubscription {
            id: 12,
            route_pattern: "notice://tenant1/*".to_string(),
            channel_id: 4,
            sender: tx3,
        });

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
                use tokio::sync::mpsc;
                let rf = DEFAULT_RF;
                let (tx, _rx) = mpsc::channel(1);
                let sub = fitz::routing::RtSubscription {
                    id: 1,
                    route_pattern: "queue://tenant1/test/route".to_string(),
                    channel_id: 1,
                    sender: tx,
                };
                table.insert(rf, sub);
            },
            criterion::BatchSize::SmallInput,
        )
    });
}

fn bench_route_table_match_exact(c: &mut Criterion) {
    let table = route_table();
    let rf = DEFAULT_RF;

    c.bench_function("route_table_match_exact", |b| {
        b.iter(|| {
            let matches = table.matching_subscribers(rf, "queue://tenant1/orders/pending");
            criterion::black_box(matches);
        })
    });
}

fn bench_route_table_match_wildcard(c: &mut Criterion) {
    let table = route_table();
    let rf = DEFAULT_RF;

    c.bench_function("route_table_match_wildcard", |b| {
        b.iter(|| {
            let matches = table.matching_subscribers(rf, "queue://tenant1/orders/new_order");
            criterion::black_box(matches);
        })
    });
}

fn bench_route_table_match_multiple(c: &mut Criterion) {
    let table = route_table();
    let rf = DEFAULT_RF;

    c.bench_function("route_table_match_multiple", |b| {
        b.iter(|| {
            let matches = table.matching_subscribers(rf, "notice://tenant1/alerts/security");
            criterion::black_box(matches);
        })
    });
}

fn bench_route_table_no_match(c: &mut Criterion) {
    let table = route_table();
    let rf = DEFAULT_RF;

    c.bench_function("route_table_no_match", |b| {
        b.iter(|| {
            let matches = table.matching_subscribers(rf, "unknown://route");
            criterion::black_box(matches);
        })
    });
}

fn bench_route_family_creation(c: &mut Criterion) {
    c.bench_function("route_family_creation", |b| {
        b.iter(|| {
            let rf: fitz::routing::RouteFamilyId = 42;
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