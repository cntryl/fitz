//! Subsystem benchmarks for stream domain operations
//!
//! These benchmarks test full stream domain operations end-to-end,
//! including handler processing, transaction semantics, and domain logic.

use criterion::{criterion_group, criterion_main, Criterion};
use fitz::core::domain::DomainContext;
use fitz::core::stream::StreamDomain;
use fitz::protocol::frame::{build_tlv, PooledFrame};
use fitz::protocol::tags::*;
use fitz::routing::RouteFamilyId;
use fitz::protocol::route::parse_route;
use std::sync::{Arc, OnceLock};
use tokio::runtime::Runtime;

#[path = "../config.rs"]
mod config;

// ---------------------------------------------------------
// Shared runtime and services
// ---------------------------------------------------------
static RT: OnceLock<Runtime> = OnceLock::new();
fn rt() -> &'static Runtime {
    RT.get_or_init(|| Runtime::new().expect("runtime"))
}

static STREAM_DOMAIN: OnceLock<Arc<StreamDomain>> = OnceLock::new();
fn stream_domain() -> Arc<StreamDomain> {
    STREAM_DOMAIN
        .get_or_init(|| {
            rt().block_on(async {
                let store = fitz::storage::midge_adapter::create_memory_store()
                    .expect("create memory store");
                Arc::new(StreamDomain::new(store))
            })
        })
        .clone()
}

// ---------------------------------------------------------
// Helper functions
// ---------------------------------------------------------

fn create_stream_frame(operation: &str, realm: &str, stream: &str, partition: u32, offset: Option<u64>, data: Option<&[u8]>) -> PooledFrame {
    let mut payload = Vec::new();
    if let Some(off) = offset {
        build_tlv(TAG_SEQ, &off.to_be_bytes(), &mut payload);
    }
    if let Some(d) = data {
        build_tlv(TAG_BODY, d, &mut payload);
    }
    PooledFrame::from_vec(payload)
}

fn create_stream_batch_frame(realm: &str, stream: &str, partition: u32, records: &[(&[u8], Option<u64>)]) -> PooledFrame {
    let mut payload = Vec::new();
    // Encode records as individual TAG_BODY entries (simple append semantics)
    for (data, _offset) in records {
        build_tlv(TAG_BODY, data, &mut payload);
    }
    PooledFrame::from_vec(payload)
}

// ---------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------

fn bench_stream_append_small(c: &mut Criterion) {
    let domain = stream_domain();
    let data = b"small record data";
    let frame = create_stream_frame("append", "test", "events", 0, None, Some(data));

    c.bench_function("stream_append_small", |b| {
        b.iter(|| {
            let route_str = "stream://test/events/0/append".to_string();
            let route = parse_route(&route_str).expect("parse route");
            let ctx = DomainContext {
                route,
                route_str,
                payload: frame.payload(),
                channel_id: 1,
                route_family: RouteFamilyId::new(),
                sender: None,
            };

            rt().block_on(async {
                let result = domain.handle(ctx).await;
                criterion::black_box(result);
            });
        })
    });
}

fn bench_stream_append_large(c: &mut Criterion) {
    let large_data = vec![b'x'; 64 * 1024]; // 64KB record
    let domain = stream_domain();
    let frame = create_stream_frame("append", "test", "events", 0, None, Some(&large_data));

    c.bench_function("stream_append_large", |b| {
        b.iter(|| {
            let route_str = "stream://test/events/0/append".to_string();
            let route = parse_route(&route_str).expect("parse route");
            let ctx = DomainContext {
                route,
                route_str,
                payload: frame.payload(),
                channel_id: 1,
                route_family: RouteFamilyId::new(),
                sender: None,
            };

            rt().block_on(async {
                let result = domain.handle(ctx).await;
                criterion::black_box(result);
            });
        })
    });
}

fn bench_stream_read_single(c: &mut Criterion) {
    let domain = stream_domain();

    // Setup: append some records first
    for i in 0..10 {
        let data = format!("record {}", i).into_bytes();
        let frame = create_stream_frame("append", "test", "events", 0, None, Some(&data));
        let route_str = "stream://test/events/0/append".to_string();
        let route = parse_route(&route_str).expect("parse route");
        let ctx = DomainContext {
            route,
            route_str,
            payload: frame.payload(),
            channel_id: 1,
            route_family: RouteFamilyId::new(),
            sender: None,
        };
        rt().block_on(async {
            let _ = domain.handle(ctx).await;
        });
    }

    c.bench_function("stream_read_single", |b| {
        b.iter(|| {
            let frame = create_stream_frame("read", "test", "events", 0, Some(5), None);
            let route_str = "stream://test/events/0/read".to_string();
            let route = parse_route(&route_str).expect("parse route");
            let ctx = DomainContext {
                route,
                route_str,
                payload: frame.payload(),
                channel_id: 1,
                route_family: RouteFamilyId::new(),
                sender: None,
            };

            rt().block_on(async {
                let result = domain.handle(ctx).await;
                criterion::black_box(result);
            });
        })
    });
}

fn bench_stream_read_range(c: &mut Criterion) {
    let domain = stream_domain();

    // Setup: append many records
    for i in 0..100 {
        let data = format!("record {}", i).into_bytes();
        let frame = create_stream_frame("append", "test", "events", 0, None, Some(&data));
        let route_str = "stream://test/events/0/append".to_string();
        let route = parse_route(&route_str).expect("parse route");
        let ctx = DomainContext {
            route,
            route_str,
            payload: frame.payload(),
            channel_id: 1,
            route_family: RouteFamilyId::new(),
            sender: None,
        };
        rt().block_on(async {
            let _ = domain.handle(ctx).await;
        });
    }

    c.bench_function("stream_read_range", |b| {
        b.iter(|| {
            let route = "stream://test/events/0/read_range";
            let mut payload = Vec::new();
            build_tlv(TAG_ROUTE, route.as_bytes(), &mut payload);
            build_tlv(TAG_OFFSET, &10u64.to_le_bytes(), &mut payload);
            build_tlv(TAG_LIMIT, &20u32.to_le_bytes(), &mut payload);
            let frame = PooledFrame::from_vec(payload);

            let route_str = route.to_string();
            let parsed = parse_route(&route_str).expect("parse route");
            let ctx = DomainContext {
                route: parsed,
                route_str,
                payload: frame.payload(),
                channel_id: 1,
                route_family: RouteFamilyId::new(),
                sender: None,
            };

            rt().block_on(async {
                let result = domain.handle(ctx).await;
                criterion::black_box(result);
            });
        })
    });
}

fn bench_stream_commit_offset(c: &mut Criterion) {
    let domain = stream_domain();

    c.bench_function("stream_commit_offset", |b| {
        b.iter(|| {
            let frame = create_stream_frame("commit", "test", "events", 0, Some(42), None);
            let route_str = "stream://test/events/0/commit".to_string();
            let route = parse_route(&route_str).expect("parse route");
            let ctx = DomainContext {
                route,
                route_str,
                payload: frame.payload(),
                channel_id: 1,
                route_family: RouteFamilyId::new(),
                sender: None,
            };

            rt().block_on(async {
                let result = domain.handle(ctx).await;
                criterion::black_box(result);
            });
        })
    });
}

fn bench_stream_append_batch(c: &mut Criterion) {
    let domain = stream_domain();

    c.bench_function("stream_append_batch", |b| {
        b.iter_batched(
            || {
                vec![
                    (b"record 1", None),
                    (b"record 2", None),
                    (b"record 3", None),
                    (b"record 4", None),
                    (b"record 5", None),
                ]
            },
            |records| {
                let frame = create_stream_batch_frame("test", "events", 0, &records);
                let route_str = "stream://test/events/0/append_batch".to_string();
                let route = parse_route(&route_str).expect("parse route");
                let ctx = DomainContext {
                    route,
                    route_str,
                    payload: frame.payload(),
                    channel_id: 1,
                    route_family: RouteFamilyId::new(),
                    sender: None,
                };

                rt().block_on(async {
                    let result = domain.handle(ctx).await;
                    criterion::black_box(result);
                });
            },
            criterion::BatchSize::SmallInput,
        )
    });
}

fn bench_stream_multi_partition_append(c: &mut Criterion) {
    let domain = stream_domain();

    c.bench_function("stream_multi_partition_append", |b| {
        b.iter(|| {
            rt().block_on(async {
                let mut handles = Vec::new();

                // Append to multiple partitions concurrently
                for partition in 0..4 {
                    let data = format!("partition {} data", partition).into_bytes();
                    let frame = create_stream_frame("append", "test", "events", partition, None, Some(&data));
                    let route_str = format!("stream://test/events/{}/append", partition);
                    let route = parse_route(&route_str).expect("parse route");
                    let ctx = DomainContext {
                        route,
                        route_str,
                        payload: frame.payload(),
                        channel_id: 1,
                        route_family: RouteFamilyId::new(),
                        sender: None,
                    };

                    let domain_clone = Arc::clone(&domain);
                    handles.push(tokio::spawn(async move {
                        domain_clone.handle(ctx).await
                    }));
                }

                for handle in handles {
                    let result = handle.await.unwrap();
                    criterion::black_box(result);
                }
            });
        })
    });
}

fn bench_stream_transaction_append(c: &mut Criterion) {
    let domain = stream_domain();

    c.bench_function("stream_transaction_append", |b| {
        b.iter_batched(
            || {
                // Start transaction
                let start_frame = create_stream_frame("begin_transaction", "test", "events", 0, None, None);
                let start_ctx = DomainContext {
                    route_family: RouteFamilyId::new(),
                    route_str: "stream://test/events/0/begin_transaction".to_string(),
                    payload: start_frame.payload(),
                };
                let txn_id = rt().block_on(async {
                    match domain.handle(start_ctx).await {
                        DomainResponse::Ok(data) => {
                            // Extract transaction ID from response
                            String::from_utf8_lossy(&data).to_string()
                        }
                        _ => "txn_123".to_string(),
                    }
                });
                txn_id
            },
            |txn_id| {
                rt().block_on(async {
                    // Append within transaction
                    let data = b"transactional record";
                    let append_frame = create_stream_frame("append_txn", "test", "events", 0, None, Some(data));
                    let mut append_payload = Vec::new();
                    build_tlv(TAG_ROUTE, b"stream://test/events/0/append_txn", &mut append_payload);
                    build_tlv(TAG_TRANSACTION_ID, txn_id.as_bytes(), &mut append_payload);
                    build_tlv(TAG_BODY, data, &mut append_payload);
                    let append_frame = PooledFrame::from_vec(append_payload);

                    let append_ctx = DomainContext {
                        route_family: RouteFamilyId::new(),
                        route_str: "stream://test/events/0/append_txn".to_string(),
                        payload: append_frame.payload(),
                    };

                    let append_result = domain.handle(append_ctx).await;

                    // Commit transaction
                    let commit_frame = create_stream_frame("commit_transaction", "test", "events", 0, None, None);
                    let mut commit_payload = Vec::new();
                    build_tlv(TAG_ROUTE, b"stream://test/events/0/commit_transaction", &mut commit_payload);
                    build_tlv(TAG_TRANSACTION_ID, txn_id.as_bytes(), &mut commit_payload);
                    let commit_frame = PooledFrame::from_vec(commit_payload);

                    let commit_ctx = DomainContext {
                        route_family: RouteFamilyId::new(),
                        route_str: "stream://test/events/0/commit_transaction".to_string(),
                        payload: commit_frame.payload(),
                    };

                    let commit_result = domain.handle(commit_ctx).await;

                    criterion::black_box((append_result, commit_result));
                });
            },
            criterion::BatchSize::SmallInput,
        )
    });
}

criterion_group!(
    name = stream_subsystem;
    config = config::criterion_config();
    targets =
        bench_stream_append_small,
        bench_stream_append_large,
        bench_stream_read_single,
        bench_stream_read_range,
        bench_stream_commit_offset,
        bench_stream_append_batch,
        bench_stream_multi_partition_append,
        bench_stream_transaction_append
);

criterion_main!(stream_subsystem);