use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::runtime::matcher::Pattern;
use fitz::runtime::routing::Route;

#[path = "criterion_config.rs"]
mod criterion_config;

/// Exact literal route match (best case)
fn bench_exact_match(c: &mut Criterion) {
    let pattern = Pattern::new("notify://acme/orders/create");
    let route = Route::new("notify://acme/orders/create");

    let mut group = c.benchmark_group("hotpath_matcher");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));
    group.bench_function("exact_literal_match", |b| {
        b.iter(|| pattern.matches(black_box(&route)))
    });
    group.finish();
}

/// Single-level wildcard match
fn bench_single_wildcard(c: &mut Criterion) {
    let pattern = Pattern::new("notify://acme/orders/*");
    let route_create = Route::new("notify://acme/orders/create");
    let route_update = Route::new("notify://acme/orders/update");

    let mut group = c.benchmark_group("hotpath_matcher");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(2));
    group.bench_function("single_star_match", |b| {
        b.iter(|| {
            let _ = pattern.matches(black_box(&route_create));
            let _ = pattern.matches(black_box(&route_update));
        })
    });
    group.finish();
}

/// Multi-level wildcard at end (depth knee)
fn bench_double_star_end(c: &mut Criterion) {
    let pattern = Pattern::new("notify://acme/orders/**");
    let route_direct = Route::new("notify://acme/orders");
    let route_level1 = Route::new("notify://acme/orders/create");
    let route_level3 = Route::new("notify://acme/orders/items/history/view");

    let mut group = c.benchmark_group("hotpath_matcher");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(3));
    group.bench_function("double_star_at_end", |b| {
        b.iter(|| {
            let _ = pattern.matches(black_box(&route_direct));
            let _ = pattern.matches(black_box(&route_level1));
            let _ = pattern.matches(black_box(&route_level3));
        })
    });
    group.finish();
}

/// Multi-level wildcard in middle
fn bench_double_star_middle(c: &mut Criterion) {
    let pattern = Pattern::new("notify://acme/**/created");
    let route_direct = Route::new("notify://acme/created");
    let route_level2 = Route::new("notify://acme/orders/created");
    let route_level4 = Route::new("notify://acme/orders/items/history/created");

    let mut group = c.benchmark_group("hotpath_matcher");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(3));
    group.bench_function("double_star_in_middle", |b| {
        b.iter(|| {
            let _ = pattern.matches(black_box(&route_direct));
            let _ = pattern.matches(black_box(&route_level2));
            let _ = pattern.matches(black_box(&route_level4));
        })
    });
    group.finish();
}

/// Negative match that fails late (worst case for backtracking)
fn bench_negative_match_late_fail(c: &mut Criterion) {
    let pattern = Pattern::new("notify://acme/*/items/history/created");
    let route_fail = Route::new("notify://acme/orders/items/history/updated");

    let mut group = c.benchmark_group("hotpath_matcher");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));
    group.bench_function("negative_match_late_fail", |b| {
        b.iter(|| pattern.matches(black_box(&route_fail)))
    });
    group.finish();
}

/// Find the depth knee: increasing route depth
fn bench_depth_knee(c: &mut Criterion) {
    let pattern = Pattern::new("notify://acme/orders/**");

    // Varying route depths
    let route_depth1 = Route::new("notify://acme/orders/x");
    let route_depth3 = Route::new("notify://acme/orders/a/b/c");
    let route_depth5 = Route::new("notify://acme/orders/a/b/c/d/e");
    let route_depth10 = Route::new("notify://acme/orders/a/b/c/d/e/f/g/h/i/j");

    let mut group = c.benchmark_group("hotpath_matcher");
    group.sampling_mode(SamplingMode::Flat);

    group.throughput(Throughput::Elements(1));
    group.bench_function("depth_1", |b| {
        b.iter(|| pattern.matches(black_box(&route_depth1)))
    });
    group.throughput(Throughput::Elements(1));
    group.bench_function("depth_3", |b| {
        b.iter(|| pattern.matches(black_box(&route_depth3)))
    });
    group.throughput(Throughput::Elements(1));
    group.bench_function("depth_5", |b| {
        b.iter(|| pattern.matches(black_box(&route_depth5)))
    });
    group.throughput(Throughput::Elements(1));
    group.bench_function("depth_10", |b| {
        b.iter(|| pattern.matches(black_box(&route_depth10)))
    });

    group.finish();
}

/// Find the pattern complexity knee: multiple wildcards
fn bench_pattern_complexity_knee(c: &mut Criterion) {
    let route = Route::new("notify://acme/orders/items/history/created");

    let pattern_literals = Pattern::new("notify://acme/orders/items/history/created");
    let pattern_one_star = Pattern::new("notify://acme/orders/*/history/created");
    let pattern_two_stars = Pattern::new("notify://acme/*/items/*/created");
    let pattern_three_stars = Pattern::new("notify://acme/*/*/*/*");
    let pattern_double_star = Pattern::new("notify://acme/**/created");

    let mut group = c.benchmark_group("hotpath_matcher");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("all_literals", |b| {
        b.iter(|| pattern_literals.matches(black_box(&route)))
    });
    group.bench_function("one_star", |b| {
        b.iter(|| pattern_one_star.matches(black_box(&route)))
    });
    group.bench_function("two_stars", |b| {
        b.iter(|| pattern_two_stars.matches(black_box(&route)))
    });
    group.bench_function("three_stars", |b| {
        b.iter(|| pattern_three_stars.matches(black_box(&route)))
    });
    group.bench_function("double_star", |b| {
        b.iter(|| pattern_double_star.matches(black_box(&route)))
    });

    group.finish();
}

/// Find the backtracking knee: ** with increasing alternatives
fn bench_backtracking_knee(c: &mut Criterion) {
    // Pattern with ** that must backtrack through alternatives
    let pattern = Pattern::new("notify://acme/**/items/created");

    // Routes with varying depths before "items"
    let route_depth1 = Route::new("notify://acme/items/created");
    let route_depth2 = Route::new("notify://acme/orders/items/created");
    let route_depth3 = Route::new("notify://acme/orders/details/items/created");
    let route_depth5 = Route::new("notify://acme/a/b/c/d/items/created");

    let mut group = c.benchmark_group("hotpath_matcher");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("backtrack_0_segments", |b| {
        b.iter(|| pattern.matches(black_box(&route_depth1)))
    });
    group.bench_function("backtrack_1_segment", |b| {
        b.iter(|| pattern.matches(black_box(&route_depth2)))
    });
    group.bench_function("backtrack_2_segments", |b| {
        b.iter(|| pattern.matches(black_box(&route_depth3)))
    });
    group.bench_function("backtrack_4_segments", |b| {
        b.iter(|| pattern.matches(black_box(&route_depth5)))
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = criterion_config::criterion_config_for_tier1();
    targets = bench_exact_match, bench_single_wildcard, bench_double_star_end, bench_double_star_middle,
              bench_negative_match_late_fail, bench_depth_knee, bench_pattern_complexity_knee, bench_backtracking_knee
}
criterion_main!(benches);
