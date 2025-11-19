//! Hotpath benchmarks for route segment parsing.
//!
//! Measures ONLY the pure parsing path:
//!   - find "://"
//!   - extract realm / area / resource
//!
//! No route struct creation, no interning,
//! no engine, no domain, no validation beyond segmentation.

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use fitz::core::parsing::parse_route_segments;

#[path = "../config.rs"]
mod config;

// -----------------------------------------------------------------------------
// Baseline: simplest routes you actually use
// -----------------------------------------------------------------------------

fn bench_parse_segments_hot(c: &mut Criterion) {
    let mut group = c.benchmark_group("route_hot_parse_segments");

    let test_cases = [
        "notice://realm/area/resource",
        "lease://realmA/db/primary",
        "queue://prod/api/jobs",
        "rpc://alpha/beta/gamma",
        "kv://proj/env/key",
    ];

    for &route in &test_cases {
        group.bench_function(BenchmarkId::new("parse", route), |b| {
            b.iter(|| {
                let _ = parse_route_segments(black_box(route)).unwrap();
            });
        });
    }

    group.finish();
}

// -----------------------------------------------------------------------------
// Stress cases: longer strings, more nested route strings
// -----------------------------------------------------------------------------

fn bench_parse_segments_large(c: &mut Criterion) {
    let mut group = c.benchmark_group("route_hot_parse_large");

    let long_realm = "r".repeat(64);
    let long_area = "a".repeat(64);
    let long_resource = "x".repeat(128);

    let long_route = format!("notice://{}/{}/{}", long_realm, long_area, long_resource);

    group.bench_function("parse_large_segments", |b| {
        b.iter(|| {
            let _ = parse_route_segments(black_box(&long_route)).unwrap();
        })
    });

    group.finish();
}

// -----------------------------------------------------------------------------
// Registration
// -----------------------------------------------------------------------------

criterion_group!(
    name = hotpath_parsing;
    config = config::criterion_config();
    targets =
        bench_parse_segments_hot,
        bench_parse_segments_large
);

criterion_main!(hotpath_parsing);
