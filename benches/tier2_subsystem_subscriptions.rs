use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use fitz::transport::routing::{Route, RouteFamily};
use fitz::transport::subscriptions::{SubscriptionId, SubscriptionIndex};

#[path = "config.rs"]
mod config;

fn make_subscription_index_with_patterns(pattern_count: usize) -> SubscriptionIndex {
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

fn bench_insert_single_pattern(c: &mut Criterion) {
    let family = RouteFamily::new(1);
    let pattern = Route::new("notify://realm/orders/create".to_string());

    let mut group = c.benchmark_group("subscription_index_insert");
    group.throughput(Throughput::Elements(1));
    group.bench_function("insert_exact_pattern", |b| {
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

    let mut group = c.benchmark_group("subscription_index_insert");
    group.bench_function("insert_single_star_pattern", |b| {
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

    let mut group = c.benchmark_group("subscription_index_insert");
    group.bench_function("insert_double_star_pattern", |b| {
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
    let family = RouteFamily::new(1);
    let route = Route::new("notify://realm/orders/create".to_string());

    let mut group = c.benchmark_group("subscription_index_match");
    group.throughput(Throughput::Elements(1));
    group.bench_function("match_exact_pattern", |b| {
        let index = make_subscription_index_with_patterns(100);
        b.iter(|| {
            index.match_all(family, black_box(&route));
        })
    });
    group.finish();
}

fn bench_match_against_single_star(c: &mut Criterion) {
    let family = RouteFamily::new(1);
    let route = Route::new("notify://realm/orders/create".to_string());

    let mut group = c.benchmark_group("subscription_index_match");
    group.bench_function("match_against_single_star_pattern", |b| {
        let index = make_subscription_index_with_patterns(100);
        b.iter(|| {
            index.match_all(family, black_box(&route));
        })
    });
    group.finish();
}

fn bench_match_against_double_star(c: &mut Criterion) {
    let family = RouteFamily::new(1);
    let route = Route::new("notify://realm/orders/created".to_string());

    let mut group = c.benchmark_group("subscription_index_match");
    group.bench_function("match_against_double_star_pattern", |b| {
        let index = make_subscription_index_with_patterns(100);
        b.iter(|| {
            index.match_all(family, black_box(&route));
        })
    });
    group.finish();
}

fn bench_match_many_subscriptions(c: &mut Criterion) {
    let family = RouteFamily::new(1);
    let route = Route::new("notify://realm/orders/create".to_string());

    let mut group = c.benchmark_group("subscription_index_match");
    group.throughput(Throughput::Elements(1000));
    group.bench_function("match_with_1000_patterns", |b| {
        let index = make_subscription_index_with_patterns(1000);
        b.iter(|| {
            index.match_all(family, black_box(&route));
        })
    });
    group.finish();
}

fn bench_remove_subscription(c: &mut Criterion) {
    let family = RouteFamily::new(1);
    let pattern = Route::new("notify://realm/orders/*".to_string());

    let mut group = c.benchmark_group("subscription_index_remove");
    group.throughput(Throughput::Elements(1));
    group.bench_function("remove_subscription", |b| {
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

    let mut group = c.benchmark_group("subscription_index_mixed");
    group.throughput(Throughput::Elements(100));
    group.bench_function("mixed_operations_100_patterns", |b| {
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

                // Match against routes
                let routes = vec![
                    Route::new("notify://realm/orders/create".to_string()),
                    Route::new("notify://realm/items/remove/action".to_string()),
                ];

                for route in &routes {
                    index.match_all(family, black_box(route));
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
        bench_match_against_single_star,
        bench_match_against_double_star,
        bench_match_many_subscriptions,
        bench_remove_subscription,
        bench_mixed_insert_remove_match
}
criterion_main!(benches);
