use criterion::{black_box, criterion_group, criterion_main, Criterion};
use fitz::domains::notification::matcher::Pattern;
use fitz::transport::routing::Route;

#[path = "config.rs"]
mod config;

/// Exact literal route match (best case)
fn bench_exact_match(c: &mut Criterion) {
    // Arrange: Pre-construct pattern and route
    let pattern = Pattern::new("notify://acme/orders/create");
    let route = Route::new("notify://acme/orders/create".to_string());

    let mut group = c.benchmark_group("hotpath_matcher_exact");
    group.sampling_mode(criterion::SamplingMode::Flat);
    group.bench_function("exact_literal_match", |b| {
        b.iter(|| {
            // Act: measure only the match operation
            pattern.matches(black_box(&route))
        })
    });
    group.finish();
}

/// Single-level wildcard match
fn bench_single_wildcard(c: &mut Criterion) {
    // Arrange: Pre-construct pattern and route
    let pattern = Pattern::new("notify://acme/orders/*");
    let route_create = Route::new("notify://acme/orders/create".to_string());
    let route_update = Route::new("notify://acme/orders/update".to_string());

    let mut group = c.benchmark_group("hotpath_matcher_single_star");
    group.sampling_mode(criterion::SamplingMode::Flat);
    group.bench_function("single_star_match", |b| {
        b.iter(|| {
            // Act: measure match with single * wildcard
            let _ = pattern.matches(black_box(&route_create));
            let _ = pattern.matches(black_box(&route_update));
        })
    });
    group.finish();
}

/// Multi-level wildcard at end
fn bench_double_star_end(c: &mut Criterion) {
    // Arrange: Pre-construct pattern and routes with varying depths
    let pattern = Pattern::new("notify://acme/orders/**");
    let route_direct = Route::new("notify://acme/orders".to_string());
    let route_level1 = Route::new("notify://acme/orders/create".to_string());
    let route_level3 = Route::new("notify://acme/orders/items/history/view".to_string());

    let mut group = c.benchmark_group("hotpath_matcher_double_star_end");
    group.sampling_mode(criterion::SamplingMode::Flat);
    group.bench_function("double_star_at_end", |b| {
        b.iter(|| {
            // Act: measure match with ** at end (varies depth)
            let _ = pattern.matches(black_box(&route_direct));
            let _ = pattern.matches(black_box(&route_level1));
            let _ = pattern.matches(black_box(&route_level3));
        })
    });
    group.finish();
}

/// Multi-level wildcard in middle
fn bench_double_star_middle(c: &mut Criterion) {
    // Arrange: Pre-construct pattern and routes
    let pattern = Pattern::new("notify://acme/**/created");
    let route_direct = Route::new("notify://acme/created".to_string());
    let route_level2 = Route::new("notify://acme/orders/created".to_string());
    let route_level4 = Route::new("notify://acme/orders/items/history/created".to_string());

    let mut group = c.benchmark_group("hotpath_matcher_double_star_middle");
    group.sampling_mode(criterion::SamplingMode::Flat);
    group.bench_function("double_star_in_middle", |b| {
        b.iter(|| {
            // Act: measure match with ** in middle
            let _ = pattern.matches(black_box(&route_direct));
            let _ = pattern.matches(black_box(&route_level2));
            let _ = pattern.matches(black_box(&route_level4));
        })
    });
    group.finish();
}

/// Negative match that fails late (worst case for backtracking)
fn bench_negative_match_late_fail(c: &mut Criterion) {
    // Arrange: Pattern that will fail only at the final segment
    // Pattern expects "*/items/history/created" but route has different final segment
    let pattern = Pattern::new("notify://acme/*/items/history/created");
    let route_fail = Route::new("notify://acme/orders/items/history/updated".to_string());

    let mut group = c.benchmark_group("hotpath_matcher_negative_late");
    group.sampling_mode(criterion::SamplingMode::Flat);
    group.bench_function("negative_match_late_fail", |b| {
        b.iter(|| {
            // Act: measure negative match (fails at last segment)
            let _ = pattern.matches(black_box(&route_fail));
        })
    });
    group.finish();
}

criterion_group! {
    name = benches;
    config = config::criterion_config();
    targets = bench_exact_match, bench_single_wildcard, bench_double_star_end, bench_double_star_middle, bench_negative_match_late_fail
}
criterion_main!(benches);
