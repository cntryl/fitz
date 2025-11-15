//! Hotpath benchmarks for notice routing and validation
//!
//! These benchmarks exercise notice-specific route validation paths,
//! which are hit on every publish/subscribe.

use criterion::{criterion_group, criterion_main, Criterion};
use fitz::protocol::route::{parse_route, validate_notice_publish, validate_notice_subscription};

#[path = "../config.rs"]
mod config;

fn bench_notice_validate_publish(c: &mut Criterion) {
    c.bench_function("notice_validate_publish", |b| {
        b.iter(|| {
            let route = parse_route("notice://tenant1/area1/resource1/op1").unwrap();
            let result = validate_notice_publish(&route);
            criterion::black_box(result.is_ok());
        })
    });
}

fn bench_notice_validate_subscription_exact(c: &mut Criterion) {
    c.bench_function("notice_validate_subscription_exact", |b| {
        b.iter(|| {
            let result = validate_notice_subscription("notice://tenant1/area1/resource1/op1");
            criterion::black_box(result.is_ok());
        })
    });
}

fn bench_notice_validate_subscription_wildcards(c: &mut Criterion) {
    c.bench_function("notice_validate_subscription_wildcards", |b| {
        b.iter(|| {
            let inputs = [
                "notice://tenant1/*",
                "notice://tenant1/area1/*",
                "notice://tenant1/area1/resource1/*",
            ];
            for inp in inputs {
                let result = validate_notice_subscription(inp);
                criterion::black_box(result.is_ok());
            }
        })
    });
}

criterion_group!(
    name = hotpath_notice;
    config = config::criterion_config();
    targets =
        bench_notice_validate_publish,
        bench_notice_validate_subscription_exact,
        bench_notice_validate_subscription_wildcards
);

criterion_main!(hotpath_notice);
