// Moved from benches/hotpath/route.rs — subsystem bench for RouteTable
use criterion::{criterion_group, criterion_main, Criterion};
use fitz::routing::RouteTable;
use fitz::routing::DEFAULT_RF;
use std::sync::OnceLock;

#[path = "../config.rs"]
mod config;

// The route_table bench uses real route table prefix matching and wildcard
// tests; keeping it as a subsystem-level test makes it clearer.

static TEST_ROUTES: OnceLock<Vec<String>> = OnceLock::new();
fn test_routes() -> &'static [String] {
    TEST_ROUTES.get_or_init(|| {
        vec![
            "queue://tenant1/orders/pending".to_string(),
            "queue://tenant1/orders/processed".to_string(),
        ]
    })
}

fn bench_route_table_match_exact(c: &mut Criterion) {
    let mut table = RouteTable::new();
    let rf = DEFAULT_RF;
    for (idx, route) in test_routes().iter().enumerate() {
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let sub = fitz::routing::RtSubscription {
            id: idx as u64 + 1,
            route_pattern: route.clone(),
            channel_id: 1,
            sender: tx,
        };
        table.insert(rf, sub);
    }

    c.bench_function("route_table_match_exact", |b| {
        b.iter(|| {
            let matches = table.matching_subscribers(rf, "queue://tenant1/orders/pending");
            criterion::black_box(matches);
        })
    });
}

criterion_group!(
    name = subsystem_route_table;
    config = config::criterion_config();
    targets = bench_route_table_match_exact
);
criterion_main!(subsystem_route_table);
