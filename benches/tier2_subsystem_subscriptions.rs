#![allow(deprecated)]
#[path = "tier2_stress.rs"]
mod tier2_stress;

use cntryl_stress::{stress_main, stress_test, StressContext};
use fitz::runtime::routing::{Route, RouteFamily};
use fitz::runtime::subscriptions::{SubscriptionId, SubscriptionIndex};
use std::hint::black_box;

const SINGLE_PATTERN_BATCH_SIZE: usize = 512;
const REMOVE_BATCH_SIZE: usize = 512;
const MATCH_REPEAT_COUNT: usize = 4_096;

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

fn make_subscription_batch(count: usize, id_offset: u64) -> Vec<(Route, SubscriptionId)> {
    (0..count)
        .map(|i| {
            let pattern = match i % 4 {
                0 => Route::new(format!("notify://realm/orders/create/{i}")),
                1 => Route::new(format!("notify://realm/orders/*/{i}")),
                2 => Route::new(format!("notify://realm/**/created/{i}")),
                _ => Route::new(format!("notify://realm/items/*/action/{i}")),
            };
            (pattern, SubscriptionId(id_offset + i as u64))
        })
        .collect()
}

fn make_dense_match_batch(count: usize, id_offset: u64) -> Vec<(Route, SubscriptionId)> {
    (0..count)
        .map(|i| {
            let pattern = match i % 4 {
                0 => Route::new("notify://realm/orders/items/action"),
                1 => Route::new("notify://realm/orders/items/*"),
                2 => Route::new("notify://realm/orders/**"),
                _ => Route::new("notify://realm/**/action"),
            };
            (pattern, SubscriptionId(id_offset + i as u64))
        })
        .collect()
}

/// Build index with fanout shape: many subscriptions, few matches
fn make_index_fanout_sparse(sub_count: usize) -> (SubscriptionIndex, Route, RouteFamily) {
    let mut index = SubscriptionIndex::new();
    let family = RouteFamily::new(1);

    // Insert many non-overlapping patterns
    for i in 0..sub_count {
        let pattern = Route::new(format!("notify://realm/orders/item{i}/action"));
        index.insert(family, &pattern, SubscriptionId(i as u64));
    }

    // Route that matches only one
    let route = Route::new("notify://realm/orders/item0/action");
    (index, route, family)
}

/// Build index with fanout shape: many subscriptions, many matches
fn make_index_fanout_dense(sub_count: usize) -> (SubscriptionIndex, Route, RouteFamily) {
    let mut index = SubscriptionIndex::new();
    let family = RouteFamily::new(1);

    // Insert patterns that all match the same route via **
    for i in 0..sub_count {
        let pattern = Route::new("notify://realm/**/action");
        index.insert(family, &pattern, SubscriptionId(i as u64));
    }

    let route = Route::new("notify://realm/orders/items/action");
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
            path.push(format!("seg{d}"));
        }
        path.push("action".to_string());
        let pattern = Route::new(path.join("/"));
        index.insert(family, &pattern, SubscriptionId(i as u64));
    }

    // Route that matches all
    let mut route_path = vec!["notify://realm".to_string()];
    for d in 0..depth {
        route_path.push(format!("seg{d}"));
    }
    route_path.push("action".to_string());
    let route = Route::new(route_path.join("/"));
    (index, route, family)
}

fn insert_single_pattern(ctx: &mut StressContext, pattern: &Route) {
    let family = RouteFamily::new(1);
    let indexes = (0..SINGLE_PATTERN_BATCH_SIZE)
        .map(|_| SubscriptionIndex::new())
        .collect::<Vec<_>>();

    tier2_stress::measure_once(ctx, SINGLE_PATTERN_BATCH_SIZE as u64, || {
        for mut index in indexes {
            index.insert(family, black_box(pattern), SubscriptionId(1));
        }
    });
}

fn match_repeated(
    ctx: &mut StressContext,
    index: &SubscriptionIndex,
    family: RouteFamily,
    route: &Route,
) {
    tier2_stress::measure_iterations(ctx, MATCH_REPEAT_COUNT as u64, || {
        for _ in 0..MATCH_REPEAT_COUNT {
            black_box(index.match_all(family, black_box(route)));
        }
    });
}

#[stress_test(tier = 2, name = "exact_pattern")]
fn should_insert_exact_pattern(ctx: &mut StressContext) {
    insert_single_pattern(ctx, &Route::new("notify://realm/orders/create"));
}

#[stress_test(tier = 2, name = "single_star_pattern")]
fn should_insert_single_star_pattern(ctx: &mut StressContext) {
    insert_single_pattern(ctx, &Route::new("notify://realm/orders/*"));
}

#[stress_test(tier = 2, name = "double_star_pattern")]
fn should_insert_double_star_pattern(ctx: &mut StressContext) {
    insert_single_pattern(ctx, &Route::new("notify://realm/**/created"));
}

#[stress_test(tier = 2, name = "exact")]
fn should_match_exact(ctx: &mut StressContext) {
    let index = make_subscriptions_with_patterns(100);
    let family = RouteFamily::new(1);
    let route = Route::new("notify://realm/orders/create");
    match_repeated(ctx, &index, family, &route);
}

#[stress_test(tier = 2, name = "single_star")]
fn should_match_single_star(ctx: &mut StressContext) {
    let index = make_subscriptions_with_patterns(100);
    let family = RouteFamily::new(1);
    let route = Route::new("notify://realm/orders/create");
    match_repeated(ctx, &index, family, &route);
}

#[stress_test(tier = 2, name = "double_star")]
fn should_match_double_star(ctx: &mut StressContext) {
    let index = make_subscriptions_with_patterns(100);
    let family = RouteFamily::new(1);
    let route = Route::new("notify://realm/orders/created");
    match_repeated(ctx, &index, family, &route);
}

#[stress_test(tier = 2, name = "10k_subs_1_match")]
fn should_match_10k_subs_1_match(ctx: &mut StressContext) {
    let (index, route, family) = make_index_fanout_sparse(10000);
    match_repeated(ctx, &index, family, &route);
}

#[stress_test(tier = 2, name = "10k_subs_10k_matches")]
fn should_match_10k_subs_10k_matches(ctx: &mut StressContext) {
    let (index, route, family) = make_index_fanout_dense(10000);
    tier2_stress::measure_iterations(ctx, 1, || {
        black_box(index.match_all_with_capacity(family, black_box(&route), 10_000));
    });
}

fn match_depth(ctx: &mut StressContext, depth: usize) {
    let (index, route, family) = make_index_with_depth(depth, 1000);
    match_repeated(ctx, &index, family, &route);
}

#[stress_test(tier = 2, name = "depth_3")]
fn should_match_depth_3(ctx: &mut StressContext) {
    match_depth(ctx, 3);
}

#[stress_test(tier = 2, name = "depth_5")]
fn should_match_depth_5(ctx: &mut StressContext) {
    match_depth(ctx, 5);
}

#[stress_test(tier = 2, name = "depth_10")]
fn should_match_depth_10(ctx: &mut StressContext) {
    match_depth(ctx, 10);
}

#[stress_test(tier = 2, name = "remove_from_index")]
fn should_remove_from_index(ctx: &mut StressContext) {
    let family = RouteFamily::new(1);
    let pattern = Route::new("notify://realm/orders/*");
    let indexes = (0..REMOVE_BATCH_SIZE)
        .map(|_| {
            let mut index = SubscriptionIndex::new();
            index.insert(family, &pattern, SubscriptionId(1));
            index
        })
        .collect::<Vec<_>>();

    tier2_stress::measure_once(ctx, REMOVE_BATCH_SIZE as u64, || {
        for mut index in indexes {
            index.remove(family, black_box(&pattern), SubscriptionId(1));
        }
    });
}

#[stress_test(tier = 2, name = "insert_100_match_2")]
fn should_insert_100_match_2(ctx: &mut StressContext) {
    let family = RouteFamily::new(1);
    let routes = vec![
        Route::new("notify://realm/orders/create"),
        Route::new("notify://realm/items/remove/action"),
    ];
    let mut index = SubscriptionIndex::new();
    let batch = (0_u64..100)
        .map(|i| {
            let pattern = match i % 4 {
                0 => Route::new("notify://realm/orders/create"),
                1 => Route::new("notify://realm/orders/*"),
                2 => Route::new("notify://realm/**/created"),
                _ => Route::new("notify://realm/items/*/action"),
            };
            (pattern, SubscriptionId(i))
        })
        .collect::<Vec<_>>();

    tier2_stress::measure_once(ctx, 100, || {
        index.insert_batch(family, &batch);
        for route in &routes {
            black_box(index.match_all(family, black_box(route)));
        }
    });
}

#[stress_test(tier = 2, name = "replace_100_patterns")]
fn should_replace_100_patterns(ctx: &mut StressContext) {
    let family = RouteFamily::new(1);
    let old_batch = make_subscription_batch(100, 0);
    let new_batch = make_subscription_batch(100, 10_000);
    let mut index = SubscriptionIndex::new();
    index.insert_batch(family, &old_batch);

    tier2_stress::measure_once(ctx, 200, || {
        for (pattern, subscription_id) in &old_batch {
            index.remove(family, black_box(pattern), *subscription_id);
        }
        index.insert_batch(family, black_box(&new_batch));
    });
}

#[stress_test(tier = 2, name = "replace_100_patterns_then_dense_match")]
fn should_replace_100_patterns_then_dense_match(ctx: &mut StressContext) {
    let family = RouteFamily::new(1);
    let old_batch = make_dense_match_batch(100, 0);
    let new_batch = make_dense_match_batch(100, 10_000);
    let route = Route::new("notify://realm/orders/items/action");
    let mut index = SubscriptionIndex::new();
    index.insert_batch(family, &old_batch);

    tier2_stress::measure_once(ctx, 100, || {
        for (pattern, subscription_id) in &old_batch {
            index.remove(family, black_box(pattern), *subscription_id);
        }
        index.insert_batch(family, black_box(&new_batch));
        black_box(index.match_all_with_capacity(family, black_box(&route), 100));
    });
}

stress_main!();
