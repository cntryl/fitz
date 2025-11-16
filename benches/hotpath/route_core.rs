//! Hotpath microbenchmarks for routing core primitives.
//!
//! Focused on:
//! - route parsing
//! - realm matching

use criterion::{criterion_group, criterion_main, Criterion};
use fitz::protocol::route::{parse_route, realm_matches, Route};

#[path = "../config.rs"]
mod config;

fn bench_route_parse_simple(c: &mut Criterion) {
    let route = "kv://realm1/area1/resource1";

    c.bench_function("route_parse_simple", |b| {
        b.iter(|| {
            let _ = parse_route(route).unwrap();
        });
    });
}

fn bench_route_parse_with_operation(c: &mut Criterion) {
    let route = "lease://realm1/area1/resource1/acquire";

    c.bench_function("route_parse_with_operation", |b| {
        b.iter(|| {
            let _ = parse_route(route).unwrap();
        });
    });
}

fn bench_realm_match(c: &mut Criterion) {
    let route: Route = parse_route("stream://tenant-a/area/resource/op").unwrap();
    let jwt_realm = "tenant-a";

    c.bench_function("route_realm_match", |b| {
        b.iter(|| {
            let _ = realm_matches(&route, jwt_realm);
        });
    });
}

criterion_group!(
    name = hotpath_route_core;
    config = config::criterion_config();
    targets =
        bench_route_parse_simple,
        bench_route_parse_with_operation,
        bench_realm_match,
);

criterion_main!(hotpath_route_core);
