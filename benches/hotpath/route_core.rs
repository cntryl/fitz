//! Comprehensive Route + RouteTable benchmarks
//!
//! Goals:
//! - Micro: parsing + realm matching
//! - Hotpath: matching behavior under scale + wildcard density
//! - Fanout: subscriber clone/lists
//! - Churn: insert/remove behavior
//! - RF (route family) sharding
//!
//! All lookup-only benches use Arc<RouteTable>.
//! All mutation benches construct RouteTable per iteration.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use fitz::protocol::route::{parse_route, realm_matches, Route};
use fitz::routing::{RouteTable, RtSubscription, DEFAULT_RF};

use std::sync::{Arc, OnceLock};

#[path = "../config.rs"]
mod config;

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

static LARGE_ROUTES: OnceLock<Vec<String>> = OnceLock::new();

fn large_routes(n: usize) -> &'static [String] {
    LARGE_ROUTES.get_or_init(|| {
        let total = 200_000;
        (0..total)
            .map(|i| format!("queue://realm{}/orders/{}", i % 1000, i))
            .collect()
    })[0..n]
        .as_ref()
}

fn make_subscription(id: u64, pattern: String, channel_id: u32) -> RtSubscription {
    let (tx, _rx) = tokio::sync::mpsc::channel::<(String, Option<String>, Vec<u8>, Option<String>, Option<u32>, bool)>(1);
    RtSubscription {
        id,
        route_pattern: pattern,
        channel_id,
        sender: tx,
    }
}

// -----------------------------------------------------------------------------
// Micro: parsing + realm match
// -----------------------------------------------------------------------------

fn bench_route_parse_simple(c: &mut Criterion) {
    let route = "kv://realm1/area1/resource1";

    c.bench_function("route_parse_simple", |b| {
        b.iter(|| {
            black_box(parse_route(route).unwrap());
        });
    });
}

fn bench_route_parse_with_operation(c: &mut Criterion) {
    let route = "lease://realm1/area1/resource1/acquire";

    c.bench_function("route_parse_with_operation", |b| {
        b.iter(|| {
            black_box(parse_route(route).unwrap());
        });
    });
}

fn bench_realm_match(c: &mut Criterion) {
    let route: Route = parse_route("stream://realm-a/area/res/op").unwrap();
    let jwt_realm = "realm-a";

    c.bench_function("route_realm_match", |b| {
        b.iter(|| {
            black_box(realm_matches(&route, jwt_realm));
        });
    });
}

// -----------------------------------------------------------------------------
// Matching benchmarks — Arc<RouteTable>, read-only
// -----------------------------------------------------------------------------

fn bench_route_table_match_exact_scale(c: &mut Criterion) {
    let mut group = c.benchmark_group("route_table_match_exact_scale");
    let sizes = [1_000usize, 8_000, 32_000, 128_000];

    for &size in &sizes {
        let mut table = RouteTable::new();
        let rf = DEFAULT_RF;

        for (idx, route) in large_routes(size).iter().enumerate() {
            let sub = make_subscription(idx as u64 + 1, route.clone(), 1);
            table.insert(rf, sub);
        }

        let table = Arc::new(table);
        let probe = format!("queue://realm{}/orders/{}", (size / 2) % 1000, size / 2);

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, {
            let table = Arc::clone(&table);
            move |b, _| {
                b.iter(|| {
                    let matches = table.matching_subscribers(rf, &probe);
                    black_box(matches.collect::<Vec<_>>());
                });
            }
        });
    }

    group.finish();
}

fn bench_route_table_match_wildcards_scale(c: &mut Criterion) {
    let mut group = c.benchmark_group("route_table_match_wildcards_scale");
    let sizes = [1_000usize, 8_000, 32_000, 64_000];

    for &size in &sizes {
        let mut table = RouteTable::new();
        let rf = DEFAULT_RF;

        for i in 0..size {
            let pat = match i % 10 {
                0..=5 => format!("queue://realm{}/orders/{}", i % 1000, i),
                6..=8 => format!("queue://realm{}/orders/*", i % 1000),
                _ => "queue://*/orders/*".to_string(),
            };

            table.insert(rf, make_subscription(i as u64 + 1, pat, (i % 16) as u32));
        }

        let table = Arc::new(table);
        let probe_hit = "queue://realm50/orders/123".to_string();
        let probe_miss = "queue://realm9999/other/999".to_string();

        // hit
        group.bench_with_input(BenchmarkId::new("hit", size), &size, {
            let table = Arc::clone(&table);
            let probe = probe_hit.clone();
            move |b, _| {
                b.iter(|| {
                    black_box(table.matching_subscribers(rf, &probe).collect::<Vec<_>>());
                });
            }
        });

        // miss
        group.bench_with_input(BenchmarkId::new("miss", size), &size, {
            let table = Arc::clone(&table);
            let probe = probe_miss.clone();
            move |b, _| {
                b.iter(|| {
                    black_box(table.matching_subscribers(rf, &probe).collect::<Vec<_>>());
                });
            }
        });
    }

    group.finish();
}

fn bench_route_table_fanout_clone_cost(c: &mut Criterion) {
    let mut table = RouteTable::new();
    let rf = DEFAULT_RF;

    for i in 0..2_000usize {
        table.insert(
            rf,
            make_subscription(
                i as u64 + 1,
                "queue://realm-a/app/alerts/*".to_string(),
                (i % 8) as u32,
            ),
        );
    }

    let table = Arc::new(table);
    let probe = "queue://realm-a/app/alerts/critical".to_string();

    c.bench_function("route_table_fanout_2k_clone_cost", {
        let table = Arc::clone(&table);
            move |b| {
                b.iter(|| {
                    black_box(table.matching_subscribers(rf, &probe).collect::<Vec<_>>());
                });
            }
    });
}

fn bench_route_table_multi_rf_sharding(c: &mut Criterion) {
    let mut table = RouteTable::new();

    for rf in 0u32..256u32 {
        for i in 0..100usize {
            let pat = format!("queue://realm{}/area{}/*", rf % 16, i % 10);
            let id = ((rf as u64) << 32) | i as u64;
            table.insert(rf, make_subscription(id, pat, (i % 8) as u32));
        }
    }

    let table = Arc::new(table);
    let probe_rf = 13u32;
    let probe = "queue://realm13/area3/resource";

    c.bench_function("route_table_multi_rf_shard_lookup", {
        let table = Arc::clone(&table);
        move |b| {
            b.iter(|| {
                black_box(table.matching_subscribers(probe_rf, probe).collect::<Vec<_>>());
            });
        }
    });
}

// -----------------------------------------------------------------------------
// Mutation benches — build table inside iter
// -----------------------------------------------------------------------------

fn bench_route_table_insert_scale(c: &mut Criterion) {
    let mut group = c.benchmark_group("route_table_insert_scale");
    let sizes = [1_000usize, 10_000, 50_000];
    let rf = DEFAULT_RF;

    for &size in &sizes {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, move |b, _| {
            b.iter(|| {
                let mut table = RouteTable::new();
                for i in 0..size {
                    let pat = format!("queue://realm{}/orders/{}", i % 1000, i);
                    table.insert(rf, make_subscription(i as u64 + 1, pat, (i % 8) as u32));
                }
                black_box(table);
            });
        });
    }

    group.finish();
}

fn bench_route_table_remove_scale(c: &mut Criterion) {
    let mut group = c.benchmark_group("route_table_remove_scale");
    let sizes = [1_000usize, 10_000, 50_000];
    let rf = DEFAULT_RF;

    for &size in &sizes {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, move |b, _| {
            b.iter(|| {
                let mut table = RouteTable::new();

                for i in 0..size {
                    let pat = format!("queue://realm{}/orders/{}", i % 1000, i);
                    table.insert(rf, make_subscription(i as u64 + 1, pat, (i % 8) as u32));
                }

                for i in 0..size {
                    let _ = table.remove(rf, i as u64 + 1);
                }

                black_box(table);
            });
        });
    }

    group.finish();
}

fn bench_route_table_insert_remove_churn(c: &mut Criterion) {
    let rf = DEFAULT_RF;

    c.bench_function("route_table_insert_remove_churn_steady", move |b| {
        b.iter(|| {
            let mut table = RouteTable::new();

            // warm state
            for i in 0..2_000u64 {
                let pat = format!("queue://realm{}/tmp/{}", i % 100, i);
                table.insert(rf, make_subscription(i + 1, pat, (i % 4) as u32));
            }

            // churn cycles
            for cycle in 0..10 {
                let base = cycle * 100;

                // +100
                for i in 0..100u64 {
                    let id = 10_000 + base + i;
                    let pat = format!("queue://realm{}/tmp/{}", id % 100, id);
                    table.insert(rf, make_subscription(id, pat, (id % 4) as u32));
                }

                // -100
                for i in 0..100u64 {
                    let _ = table.remove(rf, 1 + ((base + i) % 2_000));
                }
            }

            black_box(table);
        });
    });
}

// -----------------------------------------------------------------------------
// Criterion wiring
// -----------------------------------------------------------------------------

criterion_group!(
    name = hotpath_route_core;
    config = config::criterion_config();
    targets =
        bench_route_parse_simple,
        bench_route_parse_with_operation,
        bench_realm_match,
        bench_route_table_match_exact_scale,
        bench_route_table_match_wildcards_scale,
        bench_route_table_fanout_clone_cost,
        bench_route_table_multi_rf_sharding,
        bench_route_table_insert_scale,
        bench_route_table_remove_scale,
        bench_route_table_insert_remove_churn,
);

criterion_main!(hotpath_route_core);
