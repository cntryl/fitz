#![allow(deprecated)]
use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};

#[path = "criterion_config.rs"]
mod criterion_config;

fn bench_route_parsing(c: &mut Criterion) {
    // Setup OUTSIDE benchmark - precompute test routes with varying depths
    let routes_2_segments = [
        "rpc://acme/auth".to_string(),
        "notify://prod/events".to_string(),
        "queue://staging/jobs".to_string(),
    ];

    let routes_3_segments = [
        "rpc://acme/auth/users".to_string(),
        "notify://prod/events/orders".to_string(),
        "queue://staging/jobs/worker".to_string(),
    ];

    let routes_4_segments = [
        "rpc://acme/auth/users/authenticate".to_string(),
        "notify://prod/events/orders/created".to_string(),
        "queue://staging/jobs/worker/process".to_string(),
    ];

    let routes_5_segments = [
        "rpc://acme/auth/users/session/create".to_string(),
        "notify://prod/events/orders/items/added".to_string(),
        "queue://staging/jobs/worker/task/execute".to_string(),
    ];

    let routes_6_segments = [
        "rpc://acme/auth/users/session/token/refresh".to_string(),
        "notify://prod/events/orders/items/status/changed".to_string(),
        "queue://staging/jobs/worker/task/result/complete".to_string(),
    ];

    let mut group = c.benchmark_group("hotpath_routing");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    // Benchmark route parsing at different depths
    group.bench_function("route_new_2_segments", |b| {
        let mut idx = 0;
        b.iter(|| {
            // ONLY hot path - route creation from &str
            let route_str = &routes_2_segments[idx % routes_2_segments.len()];
            let _route = Route::new(black_box(route_str));
            idx += 1;
        })
    });

    group.bench_function("route_new_3_segments", |b| {
        let mut idx = 0;
        b.iter(|| {
            let route_str = &routes_3_segments[idx % routes_3_segments.len()];
            let _route = Route::new(black_box(route_str));
            idx += 1;
        })
    });

    group.bench_function("route_new_4_segments", |b| {
        let mut idx = 0;
        b.iter(|| {
            let route_str = &routes_4_segments[idx % routes_4_segments.len()];
            let _route = Route::new(black_box(route_str));
            idx += 1;
        })
    });

    group.bench_function("route_new_5_segments", |b| {
        let mut idx = 0;
        b.iter(|| {
            let route_str = &routes_5_segments[idx % routes_5_segments.len()];
            let _route = Route::new(black_box(route_str));
            idx += 1;
        })
    });

    group.bench_function("route_new_6_segments", |b| {
        let mut idx = 0;
        b.iter(|| {
            let route_str = &routes_6_segments[idx % routes_6_segments.len()];
            let _route = Route::new(black_box(route_str));
            idx += 1;
        })
    });

    group.finish();
}

fn bench_route_address_creation(c: &mut Criterion) {
    // Setup OUTSIDE benchmark
    let family = RouteFamily::new(1);
    let route = Route::new("rpc://acme/auth/users/authenticate");

    let mut group = c.benchmark_group("hotpath_routing");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("route_address_new", |b| {
        b.iter(|| {
            // ONLY hot path - RouteAddress construction
            let _address = RouteAddress::new(black_box(family), black_box(route.clone()));
        })
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = criterion_config::criterion_config_for_tier1();
    targets =
        bench_route_parsing,
        bench_route_address_creation,
}
criterion_main!(benches);
