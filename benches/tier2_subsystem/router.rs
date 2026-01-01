use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::transport::routing::{Route, RouteAddress, RouteFamily};

#[path = "../config.rs"]
mod config;

fn test_address(family: u64, route: &str) -> RouteAddress {
    RouteAddress::new(RouteFamily::new(family), Route::new(route.to_string()))
}

fn bench_route_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_router");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("create_route_address", |b| {
        b.iter(|| {
            let _addr = test_address(black_box(1), black_box("/service/method"));
        })
    });

    group.finish();
}

fn bench_route_family_isolation(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_router_isolation");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("same_route_different_families", |b| {
        b.iter(|| {
            let addr1 = test_address(black_box(1), black_box("/service/method"));
            let addr2 = test_address(black_box(2), black_box("/service/method"));
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
