use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};

#[path = "config.rs"]
mod config;

fn bench_route_creation(c: &mut Criterion) {
    // Pre-compute static route
    let route_str = "/service/method";
    let route = Route::new(route_str.to_string());

    let mut group = c.benchmark_group("subsystem_router");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("create_route_address", |b| {
        let r = route.clone();
        b.iter(|| {
            let _family = RouteFamily::new(black_box(1));
            let _addr = RouteAddress::new(_family, black_box(r.clone()));
        })
    });

    group.finish();
}

fn bench_route_family_isolation(c: &mut Criterion) {
    // Pre-compute routes to avoid string allocation in hot path
    let route = Route::new("/service/method".to_string());
    let family1 = RouteFamily::new(1);
    let family2 = RouteFamily::new(2);

    let mut group = c.benchmark_group("subsystem_router_isolation");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("same_route_different_families", |b| {
        let r = route.clone();
        let f1 = family1;
        let f2 = family2;
        b.iter(|| {
            let addr1 = RouteAddress::new(black_box(f1), black_box(r.clone()));
            let addr2 = RouteAddress::new(black_box(f2), black_box(r.clone()));
            black_box((addr1, addr2));
        })
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = config::criterion_config();
    targets = bench_route_creation, bench_route_family_isolation
}
criterion_main!(benches);
