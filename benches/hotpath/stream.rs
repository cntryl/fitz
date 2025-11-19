//! Hotpath benchmarks for Stream domain.
//!
//! Measures ONLY the internal logic of the Stream service:
//!   - Append: add events to streams
//!   - Read: retrieve events from streams
//!   - Read area: retrieve events across area
//!
//! Zero frame parsing, zero engine, zero outbound delivery.
//! This is the true "business logic" bench.

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use fitz::core::stream::{StreamService, types::StreamEvent};
use fitz::storage::midge_adapter;
use parking_lot::RwLock;
use std::sync::Arc;

#[path = "../config.rs"]
mod config;

// -----------------------------------------------------------------------------
// Benchmarks
// -----------------------------------------------------------------------------

fn bench_hot_append(c: &mut Criterion) {
    let mut group = c.benchmark_group("stream_hot_append");
    group.bench_function("append", |b| {
        b.iter_batched(
            || {
                let kv_store = midge_adapter::create_memory_store().unwrap();
                let svc = Arc::new(RwLock::new(StreamService::new(kv_store)));
                svc
            },
            |svc| {
                let service = svc.write();
                let event = StreamEvent {
                    sequence: 0,
                    resource: "resource1".to_string(),
                    area_seq: None,
                    body: b"test event body".to_vec(),
                    metadata: Some(b"metadata".to_vec()),
                    created_at: 1234567890,
                    is_end: false,
                };
                let txn_id = service.begin_append(0, "realm", "area", "resource1").unwrap();
                service.append_event(txn_id, 0, event).unwrap();
                service.commit_append(txn_id, 0).unwrap();
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

fn bench_hot_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("stream_hot_read");
    group.bench_function("read", |b| {
        b.iter_batched(
            || {
                let kv_store = midge_adapter::create_memory_store().unwrap();
                let svc = Arc::new(RwLock::new(StreamService::new(kv_store)));
                // Pre-populate
                {
                    let service = svc.write();
                    for i in 0..10 {
                        let event = StreamEvent {
                            sequence: i as u64,
                            resource: "resource1".to_string(),
                            area_seq: None,
                            body: format!("test event body {}", i).into_bytes(),
                            metadata: Some(format!("metadata {}", i).into_bytes()),
                            created_at: 1234567890 + i as u64,
                            is_end: false,
                        };
                        let txn_id = service.begin_append(0, "realm", "area", "resource1").unwrap();
                        service.append_event(txn_id, 0, event).unwrap();
                        service.commit_append(txn_id, 0).unwrap();
                    }
                }
                svc
            },
            |svc| {
                let service = svc.read();
                let _events = service.read(0, "realm", "area", "resource1", 0, 10).unwrap();
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

fn bench_hot_read_area(c: &mut Criterion) {
    let mut group = c.benchmark_group("stream_hot_read_area");
    group.bench_function("read_area", |b| {
        b.iter_batched(
            || {
                let kv_store = midge_adapter::create_memory_store().unwrap();
                let svc = Arc::new(RwLock::new(StreamService::new(kv_store)));
                // Pre-populate
                {
                    let service = svc.write();
                    for res in 0..3 {
                        for i in 0..5 {
                            let event = StreamEvent {
                                sequence: i as u64,
                                resource: format!("resource{}", res),
                                area_seq: None,
                                body: format!("test event body {} {}", res, i).into_bytes(),
                                metadata: Some(format!("metadata {} {}", res, i).into_bytes()),
                                created_at: 1234567890 + (res * 10 + i) as u64,
                                is_end: false,
                            };
                            let txn_id = service.begin_append(0, "realm", "area", &format!("resource{}", res)).unwrap();
                            service.append_event(txn_id, 0, event).unwrap();
                            service.commit_append(txn_id, 0).unwrap();
                        }
                    }
                }
                svc
            },
            |svc| {
                let service = svc.read();
                let _events = service.read_area(0, "realm", "area", 0, 10).unwrap();
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

criterion_group!(
    name = hotpath_stream;
    config = config::criterion_config();
    targets =
        bench_hot_append,
        bench_hot_read,
        bench_hot_read_area
);
criterion_main!(hotpath_stream);