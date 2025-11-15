//! Hotpath benchmarks for notice service operations
//!
//! These benchmarks test the core notice service primitives that are performance-critical:
//! publish operations on the NoticeService directly.

use criterion::{criterion_group, criterion_main, Criterion};
use fitz::core::notice::NoticeService;
use std::sync::OnceLock;
use tokio::sync::mpsc;

#[path = "../config.rs"]
mod config;

// ---------------------------------------------------------
// Shared services
// ---------------------------------------------------------
fn notice_service() -> NoticeService {
    NoticeService::new()
}

static TEST_PAYLOADS: OnceLock<Vec<Vec<u8>>> = OnceLock::new();
fn test_payloads() -> &'static [Vec<u8>] {
    TEST_PAYLOADS.get_or_init(|| {
        vec![
            vec![b'p'; 64],        // 64B payload
            vec![b'p'; 1024],      // 1KB payload
            vec![b'p'; 64 * 1024], // 64KB payload
        ]
    })
}

// ---------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------

fn bench_notice_publish_no_subscribers(c: &mut Criterion) {
    let payloads = test_payloads();
    let mut counter = 0;

    c.bench_function("notice_publish_no_subscribers", |b| {
        b.iter(|| {
            let mut service = notice_service();
            let payload = &payloads[counter % payloads.len()];
            counter += 1;
            let result = service.publish(1, "bench.topic", Some("msg-123"), payload);
            criterion::black_box(result);
        })
    });
}

fn bench_notice_publish_with_subscriber(c: &mut Criterion) {
    let payloads = test_payloads();
    let mut counter = 0;

    c.bench_function("notice_publish_with_subscriber", |b| {
        b.iter(|| {
            let mut service = notice_service();
            let payload = &payloads[counter % payloads.len()];
            counter += 1;

            // Set up one subscriber for this iteration
            let (tx, _rx) = mpsc::channel(100);
            let _sub_id = service.subscribe(1, "bench.topic".to_string(), 1, tx);

            let result = service.publish(1, "bench.topic", Some("msg-123"), payload);
            criterion::black_box(result);
        })
    });
}

fn bench_notice_subscribe(c: &mut Criterion) {
    c.bench_function("notice_subscribe", |b| {
        b.iter(|| {
            let mut service = notice_service();
            let (tx, _rx) = mpsc::channel(100);
            let result = service.subscribe(1, "bench.topic".to_string(), 1, tx);
            criterion::black_box(result);
        })
    });
}

criterion_group!(
    name = hotpath_notice;
    config = config::criterion_config();
    targets =
        bench_notice_publish_no_subscribers,
        bench_notice_publish_with_subscriber,
        bench_notice_subscribe
);

criterion_main!(hotpath_notice);
