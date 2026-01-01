use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use fitz::runtime::routing::{Route, RouteFamily};
use fitz::runtime::subscriptions::{SubscriptionId, SubscriptionIndex};

#[path = "config.rs"]
mod config;

fn make_subscriptions_with_patterns(pattern_count: usize) -> SubscriptionIndex {
    let mut index = SubscriptionIndex::new();
    let family = RouteFamily::new(1);

    for i in 0..pattern_count {
        let pattern_str = match i % 4 {
            0 => "notify://realm/orders/create".to_string(),
            1 => "notify://realm/orders/*".to_string(),
            2 => "notify://realm/**/created".to_string(),
            _ => "notify://realm/items/*/action".to_string(),
        };
        let pattern = Route::new(pattern_str);
        index.insert(family, &pattern, SubscriptionId(i as u64));
    }

    index
}

/// Build index with fanout shape: many subscriptions, few matches
fn make_index_fanout_sparse(sub_count: usize) -> (SubscriptionIndex, Route, RouteFamily) {
    let mut index = SubscriptionIndex::new();
    let family = RouteFamily::new(1);

    // Insert many non-overlapping patterns
    for i in 0..sub_count {
        let pattern = Route::new(format!("notify://realm/orders/item{}/action", i));
        index.insert(family, &pattern, SubscriptionId(i as u64));
    }

    // Route that matches only one
    let route = Route::new("notify://realm/orders/item0/action".to_string());
    (index, route, family)
}

/// Build index with fanout shape: many subscriptions, many matches
fn make_index_fanout_dense(sub_count: usize) -> (SubscriptionIndex, Route, RouteFamily) {
    let mut index = SubscriptionIndex::new();
    let family = RouteFamily::new(1);

    // Insert patterns that all match the same route via **
    for i in 0..sub_count {
        let pattern = Route::new("notify://realm/**/action".to_string());
        index.insert(family, &pattern, SubscriptionId(i as u64));
    }

    let route = Route::new("notify://realm/orders/items/action".to_string());
    (index, route, family)
}

/// Build index with varying trie depth
fn make_index_with_depth(
    depth: usize,
    sub_count: usize,
) -> (SubscriptionIndex, Route, RouteFamily) {
    let mut index = SubscriptionIndex::new();
    let family = RouteFamily::new(1);

    // Create patterns with controlled depth
    for i in 0..sub_count {
        let mut path = vec!["notify://realm".to_string()];
        for d in 0..depth {
            path.push(format!("seg{}", d));
        }
        path.push("action".to_string());
        let pattern = Route::new(path.join("/"));
        index.insert(family, &pattern, SubscriptionId(i as u64));
    }

    // Route that matches all
    let mut route_path = vec!["notify://realm".to_string()];
    for d in 0..depth {
        route_path.push(format!("seg{}", d));
    }
    route_path.push("action".to_string());
    let route = Route::new(route_path.join("/"));
    (index, route, family)
}

fn bench_insert_single_pattern(c: &mut Criterion) {
    let family = RouteFamily::new(1);
    let pattern = Route::new("notify://realm/orders/create".to_string());

    let mut group = c.benchmark_group("subscriptions/insert");
    group.throughput(Throughput::Elements(1));
    group.bench_function("exact_pattern", |b| {
        b.iter_batched(
            SubscriptionIndex::new,
            |mut index| {
                index.insert(family, black_box(&pattern), SubscriptionId(1));
            },
            criterion::BatchSize::SmallInput,
        )
    });
    group.finish();
}

fn bench_insert_with_single_star(c: &mut Criterion) {
    let family = RouteFamily::new(1);
    let pattern = Route::new("notify://realm/orders/*".to_string());

    let mut group = c.benchmark_group("subscriptions/insert");
    group.bench_function("single_star_pattern", |b| {
        b.iter_batched(
            SubscriptionIndex::new,
            |mut index| {
                index.insert(family, black_box(&pattern), SubscriptionId(1));
            },
            criterion::BatchSize::SmallInput,
        )
    });
    group.finish();
}

fn bench_insert_with_double_star(c: &mut Criterion) {
    let family = RouteFamily::new(1);
    let pattern = Route::new("notify://realm/**/created".to_string());

    let mut group = c.benchmark_group("subscriptions/insert");
    group.bench_function("double_star_pattern", |b| {
        b.iter_batched(
            SubscriptionIndex::new,
            |mut index| {
                index.insert(family, black_box(&pattern), SubscriptionId(1));
            },
            criterion::BatchSize::SmallInput,
        )
    });
    group.finish();
}

fn bench_match_exact_pattern(c: &mut Criterion) {
    let mut group = c.benchmark_group("subscriptions/match");
    group.bench_function("exact", |b| {
        let index = make_subscriptions_with_patterns(100);
        let family = RouteFamily::new(1);
        let route = Route::new("notify://realm/orders/create".to_string());
        b.iter(|| {
            black_box(index.match_all(family, black_box(&route)));
        })
    });
    group.finish();
}

fn bench_match_single_star(c: &mut Criterion) {
    let mut group = c.benchmark_group("subscriptions/match");
    group.bench_function("single_star", |b| {
        let index = make_subscriptions_with_patterns(100);
        let family = RouteFamily::new(1);
        let route = Route::new("notify://realm/orders/create".to_string());
        b.iter(|| {
            black_box(index.match_all(family, black_box(&route)));
        })
    });
    group.finish();
}

fn bench_match_double_star(c: &mut Criterion) {
    let mut group = c.benchmark_group("subscriptions/match");
    group.bench_function("double_star", |b| {
        let index = make_subscriptions_with_patterns(100);
        let family = RouteFamily::new(1);
        let route = Route::new("notify://realm/orders/created".to_string());
        b.iter(|| {
            black_box(index.match_all(family, black_box(&route)));
        })
    });
    group.finish();
}

fn bench_match_fanout_sparse_100(c: &mut Criterion) {
    let mut group = c.benchmark_group("subscriptions/match/fanout");
    group.bench_function("10k_subs_1_match", |b| {
        let (index, route, family) = make_index_fanout_sparse(10000);
        b.iter(|| {
            black_box(index.match_all(family, black_box(&route)));
        })
    });
    group.finish();
}

fn bench_match_fanout_dense_100(c: &mut Criterion) {
    let mut group = c.benchmark_group("subscriptions/match/fanout");
    group.bench_function("10k_subs_10k_matches", |b| {
        let (index, route, family) = make_index_fanout_dense(10000);
        b.iter(|| {
            black_box(index.match_all(family, black_box(&route)));
        })
    });
    group.finish();
}

fn bench_match_depth_3(c: &mut Criterion) {
    let mut group = c.benchmark_group("subscriptions/match/depth");
    group.bench_function("depth_3", |b| {
        let (index, route, family) = make_index_with_depth(3, 1000);
        b.iter(|| {
            black_box(index.match_all(family, black_box(&route)));
        })
    });
    group.finish();
}

fn bench_match_depth_5(c: &mut Criterion) {
    let mut group = c.benchmark_group("subscriptions/match/depth");
    group.bench_function("depth_5", |b| {
        let (index, route, family) = make_index_with_depth(5, 1000);
        b.iter(|| {
            black_box(index.match_all(family, black_box(&route)));
        })
    });
    group.finish();
}

fn bench_match_depth_10(c: &mut Criterion) {
    let mut group = c.benchmark_group("subscriptions/match/depth");
    group.bench_function("depth_10", |b| {
        let (index, route, family) = make_index_with_depth(10, 1000);
        b.iter(|| {
            black_box(index.match_all(family, black_box(&route)));
        })
    });
    group.finish();
}

fn bench_remove_subscription(c: &mut Criterion) {
    let family = RouteFamily::new(1);
    let pattern = Route::new("notify://realm/orders/*".to_string());

    let mut group = c.benchmark_group("subscriptions/remove");
    group.throughput(Throughput::Elements(1));
    group.bench_function("remove_from_index", |b| {
        b.iter_batched(
            || {
                let mut index = SubscriptionIndex::new();
                index.insert(family, &pattern, SubscriptionId(1));
                index
            },
            |mut index| {
                index.remove(family, black_box(&pattern), SubscriptionId(1));
            },
            criterion::BatchSize::SmallInput,
        )
    });
    group.finish();
}

fn bench_mixed_insert_remove_match(c: &mut Criterion) {
    let family = RouteFamily::new(1);
    let routes = vec![
        Route::new("notify://realm/orders/create".to_string()),
        Route::new("notify://realm/items/remove/action".to_string()),
    ];

    let mut group = c.benchmark_group("subscriptions/mixed");
    group.throughput(Throughput::Elements(100));
    group.bench_function("insert_100_match_2", |b| {
        b.iter_batched(
            SubscriptionIndex::new,
            |mut index| {
                // Insert various patterns
                for i in 0..100 {
                    let pattern = match i % 4 {
                        0 => Route::new("notify://realm/orders/create".to_string()),
                        1 => Route::new("notify://realm/orders/*".to_string()),
                        2 => Route::new("notify://realm/**/created".to_string()),
                        _ => Route::new("notify://realm/items/*/action".to_string()),
                    };
                    index.insert(family, &pattern, SubscriptionId(i as u64));
                }

                // Match against pre-built routes
                for route in &routes {
                    black_box(index.match_all(family, black_box(route)));
                }
            },
            criterion::BatchSize::LargeInput,
        )
    });
    group.finish();
}

criterion_group! {
    name = benches;
    config = config::criterion_config();
    targets =
        bench_insert_single_pattern,
        bench_insert_with_single_star,
        bench_insert_with_double_star,
        bench_match_exact_pattern,
        bench_match_single_star,
        bench_match_double_star,
        bench_match_fanout_sparse_100,
        bench_match_fanout_dense_100,
        bench_match_depth_3,
        bench_match_depth_5,
        bench_match_depth_10,
        bench_remove_subscription,
        bench_mixed_insert_remove_match
}
criterion_main!(benches);
