/// Tier 2: Stream Domain Subsystem Coordination Benchmarks
/// 
/// Tests multi-actor, leasing, watermark advancement, and merging.
/// GOALS:
/// - Stress actor coordination
/// - Validate lease renewal overhead
/// - Measure watermark advancement cost
/// - Test multi-way merge efficiency
/// - Validate sustained ingest throughput
///
/// CHARACTERISTICS:
/// - Multiple StreamActors
/// - Real lease renewal (small lease sizes force renewal)
/// - Real watermark tracking
/// - Single-node but coordinated
/// - Still deterministic and reproducible
///
/// Each bench:
/// - Setup outside hot path
/// - Deterministic test data
/// - Measures coordination overhead
/// - Reports tail latency (p95, p99)

use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::domains::stream::stream_actor::StreamActor;
use fitz::domains::stream::store::StreamStore;
use fitz::prelude::Actor;
use fitz::runtime::actor::Context;
use fitz::runtime::router::Router;
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use std::sync::Arc;
use std::sync::Mutex;

#[path = "config.rs"]
mod config;

/// Create multiple StreamActors in the same area
fn setup_multi_stream_actors(
    realm: &str,
    area: &str,
    resource_count: usize,
) -> (Vec<StreamActor>, Vec<Context<StreamActor>>, Arc<StreamStore>) {
    let router = Arc::new(Router::new());
    let family = RouteFamily::new(1);
    let store = Arc::new(StreamStore::open(Default::default()).expect("Failed to open store"));

    let mut actors = Vec::new();
    let mut contexts = Vec::new();

    for i in 0..resource_count {
        let resource_name = format!("resource-{}", i);
        let addr = RouteAddress::new(
            family,
            Route::new(format!("stream://{}/{}/{}/append", realm, area, resource_name)),
        );

        let actor = StreamActor::new(
            family,
            realm.to_string(),
            area.to_string(),
            resource_name,
            store.clone(),
        );
        let ctx = Context::new(addr, router.clone());

        actors.push(actor);
        contexts.push(ctx);
    }

    (actors, contexts, store)
}

/// BENCH 8: Append with lease renewal
/// Small lease size forces frequent renewal during continuous appends
fn bench_append_with_lease_renewal(c: &mut Criterion) {
    let (actors, mut contexts, _store) = setup_multi_stream_actors("bench-realm", "bench-area", 1);
    let mut actor = actors.into_iter().next().unwrap();
    let mut ctx = contexts.into_iter().next().unwrap();

    // Precompute payloads
    let payloads = make_event_payloads(1000, 256);

    let mut group = c.benchmark_group("tier2_stream_append_lease_renewal");
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(2));
    group.throughput(Throughput::Elements(1));

    // Test with artificially small lease (32 offsets) to force renewal
    let mut offset = 0u64;
    group.bench_function("append_with_lease_renewal_32", |b| {
        b.iter(|| {
            let idx = offset as usize % payloads.len();
            let payload = black_box(&payloads[idx]);
            
            // Each append checks lease, renews if exhausted
            let _result = actor.receive(
                fitz::domains::stream::protocol::StreamMessage::Append {
                    session_id: 0,
                    expected_offset: offset,
                    data: Bytes::from(payload.clone()),
                },
                &mut ctx,
            );
            
            offset += 1;
        })
    });

    group.finish();
}

/// BENCH 9: Concurrent writes to multiple resources in same area
fn bench_concurrent_resource_writes(c: &mut Criterion) {
    let resource_counts = [2usize, 4, 8];

    let mut group = c.benchmark_group("tier2_stream_concurrent_writes");
    group.sampling_mode(SamplingMode::Flat);

    for &resource_count in &resource_counts {
        let (actors, mut contexts, _store) =
            setup_multi_stream_actors("bench-realm", "bench-area", resource_count);

        // Precompute payloads per resource
        let payloads_per_resource: Vec<Vec<Vec<u8>>> = (0..resource_count)
            .map(|_| make_event_payloads(500, 256))
            .collect();

        group.throughput(Throughput::Elements(resource_count as u64));
        let name = format!("concurrent_{}_resources", resource_count);

        let mut offsets = vec![0u64; resource_count];
        group.bench_function(&name, |b| {
            b.iter(|| {
                // Round-robin append to each resource
                for res_idx in 0..resource_count {
                    let idx = offsets[res_idx] as usize % payloads_per_resource[res_idx].len();
                    let payload = black_box(&payloads_per_resource[res_idx][idx]);

                    let _result = actors[res_idx].receive(
                        fitz::domains::stream::protocol::StreamMessage::Append {
                            session_id: res_idx as u64,
                            expected_offset: offsets[res_idx],
                            data: Bytes::from(payload.clone()),
                        },
                        &mut contexts[res_idx],
                    );

                    offsets[res_idx] += 1;
                }
            })
        });
    }

    group.finish();
}

/// BENCH 10: Area watermark advancement with out-of-order commits
fn bench_area_watermark_advancement(c: &mut Criterion) {
    // Setup multiple resources, append in specific patterns to test watermark
    let (actors, mut contexts, _store) = setup_multi_stream_actors("bench-realm", "bench-area", 4);

    let payloads = make_event_payloads(1000, 256);

    let mut group = c.benchmark_group("tier2_stream_area_watermark");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(4u64)); // 4 resources per iteration

    let mut offsets = vec![0u64; 4];
    group.bench_function("watermark_advancement_4resources", |b| {
        b.iter(|| {
            // Out-of-order append pattern to stress watermark logic
            // Resource 0, 2, 1, 3 (deliberately not sequential)
            for res_idx in &[0, 2, 1, 3] {
                let idx = offsets[*res_idx] as usize % payloads.len();
                let payload = black_box(&payloads[idx]);

                let _result = actors[*res_idx].receive(
                    fitz::domains::stream::protocol::StreamMessage::Append {
                        session_id: *res_idx as u64,
                        expected_offset: offsets[*res_idx],
                        data: Bytes::from(payload.clone()),
                    },
                    &mut contexts[*res_idx],
                );

                offsets[*res_idx] += 1;
            }
        })
    });

    group.finish();
}

/// BENCH 11: Realm watermark advancement (multiple areas, uneven progress)
fn bench_realm_watermark_advancement(c: &mut Criterion) {
    // Setup multiple areas, each with multiple resources
    // Areas advance at different rates to stress realm watermark
    
    let area_count = 4;
    let resource_per_area = 2;
    let total_resources = area_count * resource_per_area;

    let mut all_actors = Vec::new();
    let mut all_contexts = Vec::new();

    for area_idx in 0..area_count {
        let area_name = format!("area-{}", area_idx);
        let (actors, contexts, _store) =
            setup_multi_stream_actors("bench-realm", &area_name, resource_per_area);
        all_actors.push(actors);
        all_contexts.push(contexts);
    }

    let payloads = make_event_payloads(1000, 256);

    let mut group = c.benchmark_group("tier2_stream_realm_watermark");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(total_resources as u64));

    let mut offsets = vec![0u64; total_resources];
    group.bench_function("realm_watermark_advancement_4areas", |b| {
        b.iter(|| {
            // Stagger appends: area 0 fast, area 1 medium, area 2 slow, area 3 very slow
            // This tests min(area_watermarks) behavior
            let append_counts = [4, 3, 2, 1]; // uneven progress

            for (area_idx, &count) in append_counts.iter().enumerate() {
                for _ in 0..count {
                    // Pick a resource in this area (round-robin)
                    let res_in_area = (offsets[area_idx * resource_per_area] % resource_per_area as u64) as usize;
                    let global_res_idx = area_idx * resource_per_area + res_in_area;

                    let idx = offsets[global_res_idx] as usize % payloads.len();
                    let payload = black_box(&payloads[idx]);

                    let area_ctx_idx = area_idx;
                    let res_ctx_idx = res_in_area;

                    let _result = all_actors[area_ctx_idx][res_ctx_idx].receive(
                        fitz::domains::stream::protocol::StreamMessage::Append {
                            session_id: global_res_idx as u64,
                            expected_offset: offsets[global_res_idx],
                            data: Bytes::from(payload.clone()),
                        },
                        &mut all_contexts[area_ctx_idx][res_ctx_idx],
                    );

                    offsets[global_res_idx] += 1;
                }
            }
        })
    });

    group.finish();
}

/// BENCH 12: K-way merge of resource streams within an area
/// Simulates area-level read needing to merge K resource streams
fn bench_area_read_k_way_merge(c: &mut Criterion) {
    let k_values = [2usize, 4, 8, 16];

    let mut group = c.benchmark_group("tier2_stream_area_merge");
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(2));

    for &k in &k_values {
        // Pre-populate k resource streams
        let (actors, mut contexts, _store) = setup_multi_stream_actors("bench-realm", "bench-area", k);

        let payloads = make_event_payloads(10000, 256);

        // Pre-populate with event data
        for (res_idx, actor) in actors.iter().enumerate() {
            // Each resource gets 100 events
            for i in 0..100 {
                let idx = (res_idx * 100 + i) % payloads.len();
                let _result = actor.receive(
                    fitz::domains::stream::protocol::StreamMessage::Append {
                        session_id: res_idx as u64,
                        expected_offset: i as u64,
                        data: Bytes::from(payloads[idx].clone()),
                    },
                    &mut contexts[res_idx],
                );
            }
        }

        group.throughput(Throughput::Elements(k as u64));
        let name = format!("merge_{}_way", k);

        // Simulate merge operation: read from each stream sequentially
        // In reality this would interleave reads
        let mut read_offsets = vec![0u64; k];
        group.bench_function(&name, |b| {
            b.iter(|| {
                // One full round: read one event from each of k resources
                for res_idx in 0..k {
                    let _result = actors[res_idx].receive(
                        fitz::domains::stream::protocol::StreamMessage::Read {
                            session_id: res_idx as u64,
                            offset: black_box(read_offsets[res_idx]),
                            count: 1,
                        },
                        &mut contexts[res_idx],
                    );

                    read_offsets[res_idx] = (read_offsets[res_idx] + 1) % 100;
                }
            })
        });
    }

    group.finish();
}

/// BENCH 13: K-way merge at realm level (multiple areas)
fn bench_realm_read_k_way_merge(c: &mut Criterion) {
    let area_count = 4;
    let resource_per_area = 2;

    let mut all_actors = Vec::new();
    let mut all_contexts = Vec::new();

    for area_idx in 0..area_count {
        let area_name = format!("area-{}", area_idx);
        let (actors, contexts, _store) =
            setup_multi_stream_actors("bench-realm", &area_name, resource_per_area);
        all_actors.push(actors);
        all_contexts.push(contexts);
    }

    let payloads = make_event_payloads(10000, 256);

    // Pre-populate all resources
    for area_idx in 0..area_count {
        for res_idx in 0..resource_per_area {
            for i in 0..50 {
                let idx = (area_idx * resource_per_area * 50 + res_idx * 50 + i) % payloads.len();
                let _result = all_actors[area_idx][res_idx].receive(
                    fitz::domains::stream::protocol::StreamMessage::Append {
                        session_id: (area_idx * resource_per_area + res_idx) as u64,
                        expected_offset: i as u64,
                        data: Bytes::from(payloads[idx].clone()),
                    },
                    &mut all_contexts[area_idx][res_idx],
                );
            }
        }
    }

    let mut group = c.benchmark_group("tier2_stream_realm_merge");
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(2));

    let total_k = area_count * resource_per_area;
    group.throughput(Throughput::Elements(total_k as u64));

    let mut read_offsets = vec![0u64; total_k];
    group.bench_function("realm_merge_4areas_2res", |b| {
        b.iter(|| {
            // Realm-level merge: read one from each (area, resource) pair
            for area_idx in 0..area_count {
                for res_idx in 0..resource_per_area {
                    let global_idx = area_idx * resource_per_area + res_idx;

                    let _result = all_actors[area_idx][res_idx].receive(
                        fitz::domains::stream::protocol::StreamMessage::Read {
                            session_id: global_idx as u64,
                            offset: black_box(read_offsets[global_idx]),
                            count: 1,
                        },
                        &mut all_contexts[area_idx][res_idx],
                    );

                    read_offsets[global_idx] = (read_offsets[global_idx] + 1) % 50;
                }
            }
        })
    });

    group.finish();
}

/// BENCH 14: Sustained 10k event ingest via chunked sessions
/// Tests memory stability and throughput under realistic streaming workload
fn bench_streaming_ingest_10k(c: &mut Criterion) {
    let (actors, mut contexts, _store) = setup_multi_stream_actors("bench-realm", "bench-area", 2);
    
    // Precompute all 10k events
    let payloads = make_event_payloads(10000, 256);

    let mut group = c.benchmark_group("tier2_stream_ingest_10k");
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(3));

    // Chunk size: 100 events per commit
    let chunk_size = 100;
    group.throughput(Throughput::Elements(chunk_size as u64));

    let mut offsets = vec![0u64; 2];
    let mut global_event_idx = 0usize;

    group.bench_function("ingest_10k_chunks", |b| {
        b.iter(|| {
            // Commit chunk_size events
            for _ in 0..chunk_size {
                let res_idx = global_event_idx % 2;
                let idx = global_event_idx % payloads.len();
                let payload = black_box(&payloads[idx]);

                let _result = actors[res_idx].receive(
                    fitz::domains::stream::protocol::StreamMessage::Append {
                        session_id: res_idx as u64,
                        expected_offset: offsets[res_idx],
                        data: Bytes::from(payload.clone()),
                    },
                    &mut contexts[res_idx],
                );

                offsets[res_idx] += 1;
                global_event_idx += 1;
            }
        })
    });

    group.finish();
}

/// Helper: precompute deterministic event payloads
fn make_event_payloads(count: usize, size: usize) -> Vec<Vec<u8>> {
    (0..count)
        .map(|i| {
            let mut payload = vec![0u8; size];
            payload[..8.min(size)].copy_from_slice(&(i as u64).to_le_bytes()[..8.min(size)]);
            payload
        })
        .collect()
}

criterion_group! {
    name = benches;
    config = config::criterion_config();
    targets = 
        bench_append_with_lease_renewal,
        bench_concurrent_resource_writes,
        bench_area_watermark_advancement,
        bench_realm_watermark_advancement,
        bench_area_read_k_way_merge,
        bench_realm_read_k_way_merge,
        bench_streaming_ingest_10k
}
criterion_main!(benches);
