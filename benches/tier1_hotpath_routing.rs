use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};

#[path = "config.rs"]
mod config;

fn bench_route_family_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_routing");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("route_family_new_u64", |b| {
        b.iter(|| {
            // ONLY hot path - RouteFamily creation from u64
            let _family = black_box(RouteFamily::new(black_box(1)));
        })
    });

    group.bench_function("route_family_from_u32", |b| {
        b.iter(|| {
            // ONLY hot path - RouteFamily creation from u32
            let _family = black_box(RouteFamily::from_u32(black_box(1)));
        })
    });

    group.finish();
}

fn bench_route_parsing(c: &mut Criterion) {
    // Setup OUTSIDE benchmark - precompute test routes with varying depths
    let routes_2_segments = [
        "rpc://acme/auth".to_string(),
        "notify://prod/events".to_string(),
        "queue://staging/jobs".to_string(),
    ];

    let _routes_3_segments = [
        "rpc://acme/auth/users".to_string(),
        "notify://prod/events/orders".to_string(),
        "queue://staging/jobs/worker".to_string(),
    ];

    let routes_4_segments = [
        "rpc://acme/auth/users/authenticate".to_string(),
        "notify://prod/events/orders/created".to_string(),
        "queue://staging/jobs/worker/process".to_string(),
    ];

    let _routes_5_segments = [
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
            // ONLY hot path - route creation with string allocation
            let route_str = &routes_2_segments[idx % routes_2_segments.len()];
            let _route = Route::new(black_box(route_str.clone()));
            idx += 1;
        })
    });

    group.bench_function("route_new_4_segments", |b| {
        let mut idx = 0;
        b.iter(|| {
            let route_str = &routes_4_segments[idx % routes_4_segments.len()];
            let _route = Route::new(black_box(route_str.clone()));
            idx += 1;
        })
    });

    group.bench_function("route_new_6_segments", |b| {
        let mut idx = 0;
        b.iter(|| {
            let route_str = &routes_6_segments[idx % routes_6_segments.len()];
            let _route = Route::new(black_box(route_str.clone()));
            idx += 1;
        })
    });

    group.finish();
}

fn bench_route_address_creation(c: &mut Criterion) {
    // Setup OUTSIDE benchmark
    let family = RouteFamily::new(1);
    let route = Route::new("rpc://acme/auth/users/authenticate".to_string());

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

fn bench_route_access(c: &mut Criterion) {
    // Setup OUTSIDE benchmark
    let route = Route::new("queue://prod/jobs/worker/process/task".to_string());

    let mut group = c.benchmark_group("hotpath_routing");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("route_as_str", |b| {
        b.iter(|| {
            // ONLY hot path - string slice access
            let _str = black_box(route.as_str());
        })
    });

    group.finish();
}

fn bench_route_address_access(c: &mut Criterion) {
    // Setup OUTSIDE benchmark
    let family = RouteFamily::new(1);
    let route = Route::new("stream://acme/analytics/events/append".to_string());
    let address = RouteAddress::new(family, route);

    let mut group = c.benchmark_group("hotpath_routing");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("route_address_family_access", |b| {
        b.iter(|| {
            // ONLY hot path - family getter
            let _fam = black_box(address.family());
        })
    });

    group.bench_function("route_address_route_access", |b| {
        b.iter(|| {
            // ONLY hot path - route getter
            let _rt = black_box(address.route());
        })
    });

    group.finish();
}

fn bench_route_clone_overhead(c: &mut Criterion) {
    // Setup OUTSIDE benchmark
    let short_route = Route::new("rpc://acme/auth".to_string());
    let long_route = Route::new("rpc://acme/very/deep/nested/organizational/structure/authentication/service/endpoint/handler".to_string());

    let mut group = c.benchmark_group("hotpath_routing");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("route_clone_short", |b| {
        b.iter(|| {
            // ONLY hot path - string clone for short route
            let _cloned = black_box(short_route.clone());
        })
    });

    group.bench_function("route_clone_long", |b| {
        b.iter(|| {
            // ONLY hot path - string clone for long route
            let _cloned = black_box(long_route.clone());
        })
    });

    group.finish();
}

fn bench_route_address_clone_overhead(c: &mut Criterion) {
    // Setup OUTSIDE benchmark
    let family = RouteFamily::new(1);
    let route = Route::new("lease://acme/locks/db/migration/acquire".to_string());
    let address = RouteAddress::new(family, route);

    let mut group = c.benchmark_group("hotpath_routing");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("route_address_clone", |b| {
        b.iter(|| {
            // ONLY hot path - RouteAddress clone (includes string clone)
            let _cloned = black_box(address.clone());
        })
    });

    group.finish();
}

fn bench_full_address_construction_from_string(c: &mut Criterion) {
    // Setup OUTSIDE benchmark - precompute route strings
    let route_strings = [
        "rpc://acme/auth/users/authenticate",
        "notify://prod/events/orders/created",
        "queue://staging/jobs/worker/process",
        "stream://acme/analytics/events/append",
        "lease://acme/locks/db/migration/acquire",
    ];

    let mut group = c.benchmark_group("hotpath_routing");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("full_address_from_string", |b| {
        let mut idx = 0;
        b.iter(|| {
            // ONLY hot path - full pipeline: family + route + address creation
            let family = RouteFamily::new(black_box(1));
            let route_str = black_box(&route_strings[idx % route_strings.len()]);
            let route = Route::new(route_str.to_string());
            let _address = RouteAddress::new(family, route);
            idx += 1;
        })
    });

    group.finish();
}

fn bench_route_equality(c: &mut Criterion) {
    // Setup OUTSIDE benchmark
    let route1 = Route::new("rpc://acme/auth/users/authenticate".to_string());
    let route2 = Route::new("rpc://acme/auth/users/authenticate".to_string());
    let route3 = Route::new("rpc://acme/auth/users/authorize".to_string());

    let mut group = c.benchmark_group("hotpath_routing");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("route_equality_same", |b| {
        b.iter(|| {
            // ONLY hot path - string equality check (equal routes)
            let _equal = black_box(route1 == route2);
        })
    });

    group.bench_function("route_equality_different", |b| {
        b.iter(|| {
            // ONLY hot path - string equality check (different routes)
            let _equal = black_box(route1 == route3);
        })
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = config::criterion_config();
    targets =
        bench_route_family_creation,
        bench_route_parsing,
        bench_route_address_creation,
        bench_route_access,
        bench_route_address_access,
        bench_route_clone_overhead,
        bench_route_address_clone_overhead,
        bench_full_address_construction_from_string,
        bench_route_equality
}
criterion_main!(benches);
