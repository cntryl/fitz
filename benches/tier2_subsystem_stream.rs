/// Tier 2: Stream Domain Subsystem Coordination Benchmarks
///
/// Tests multi-resource coordination, lease renewal, and watermark advancement.
///
/// GOALS:
/// - Measure lease renewal overhead
/// - Test concurrent resource write patterns
/// - Validate watermark advancement cost
/// - Measure sustained ingest throughput
///
/// ARCHITECTURE:
/// - Multiple StreamActors (one per resource)
/// - Proper BeginSession/CommitSession flow for ALL writes
/// - Tests measure coordination overhead vs pure write cost
///
/// Each bench:
/// - Setup outside hot path
/// - Deterministic test data
/// - Measures coordination overhead
/// - Uses proper session flow
use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::domains::stream::protocol::StreamMessage;
use fitz::domains::stream::store::StreamStore;
use fitz::domains::stream::stream_actor::StreamActor;
use fitz::prelude::Actor;
use fitz::runtime::actor::Context;
use fitz::runtime::router::Router;
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use std::sync::Arc;

#[path = "config.rs"]
mod config;

/// Precompute deterministic event payloads
fn create_bench_event_payloads(count: usize, size: usize) -> Vec<Vec<u8>> {
    (0..count)
        .map(|i| {
            let mut payload = vec![0u8; size];
            payload[..8.min(size)].copy_from_slice(&(i as u64).to_le_bytes()[..8.min(size)]);
            payload
        })
        .collect()
}

/// Create multiple StreamActors in the same area
fn setup_multi_stream_actors(
    realm: &str,
    area: &str,
    resource_count: usize,
) -> (
    Vec<StreamActor>,
    Vec<Context<StreamActor>>,
    Arc<StreamStore>,
) {
    let router = Arc::new(Router::new());
    let family = RouteFamily::new(1);
    let db = Arc::new(
        cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
            .expect("open db"),
    );
    let store = Arc::new(StreamStore::new(db));

    let mut actors = Vec::new();
    let mut contexts = Vec::new();

    for i in 0..resource_count {
        let resource_name = format!("resource-{}", i);
        let addr = RouteAddress::new(
            family,
            Route::new(format!(
                "stream://{}/{}/{}/append",
                realm, area, resource_name
            )),
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

//
// LEASE RENEWAL BENCHMARKS
//

/// BENCH 1: Commit across multiple resources (round-robin)
/// Measures: overhead of managing multiple StreamActors
fn bench_multi_resource_round_robin(c: &mut Criterion) {
    let resource_counts = [2usize, 4];

    for &resource_count in &resource_counts {
        let (mut actors, mut contexts, _store) =
            setup_multi_stream_actors("bench", "bench", resource_count);

        let payloads = create_bench_event_payloads(1000, 256);
        let batch_size = 10;

        let mut group = c.benchmark_group("tier2_stream_multi_resource");
        group.sampling_mode(SamplingMode::Flat);
        group.throughput(Throughput::Elements((resource_count * batch_size) as u64));

        let name = format!("round_robin_{}_resources", resource_count);

        let mut expected_offsets = vec![0u64; resource_count];
        let mut payload_offset = 0usize;

        group.bench_function(&name, |b| {
            b.iter(|| {
                // Round-robin: commit batch_size events to each resource
                for res_idx in 0..resource_count {
                    let route =
                        Route::new(format!("stream://bench/bench/resource-{}/append", res_idx));
                    let family_id = RouteFamily::new(1);

                    // BeginSession
                    actors[res_idx].receive(
                        StreamMessage::BeginSession {
                            family_id,
                            route: route.clone(),
                            expected_offset: expected_offsets[res_idx],
                            ingest_metadata: None,
                        },
                        &mut contexts[res_idx],
                    );

                    let session_id =
                        format!("bench-session-{}-{}", res_idx, expected_offsets[res_idx]);

                    // Append batch_size events
                    for _ in 0..batch_size {
                        let payload = black_box(&payloads[payload_offset % payloads.len()]);
                        actors[res_idx].receive(
                            StreamMessage::AppendToSession {
                                session_id: session_id.clone(),
                                body: Bytes::from(payload.clone()),
                                metadata: None,
                            },
                            &mut contexts[res_idx],
                        );
                        payload_offset += 1;
                    }

                    // CommitSession
                    actors[res_idx].receive(
                        StreamMessage::CommitSession { session_id },
                        &mut contexts[res_idx],
                    );

                    expected_offsets[res_idx] += batch_size as u64;
                }
            })
        });

        group.finish();
    }
}

//
// SUSTAINED INGEST BENCHMARKS
//

/// BENCH 2: Streaming ingest (10k events across 2 resources)
/// Measures: sustained throughput with chunked commits
fn bench_streaming_ingest_10k(c: &mut Criterion) {
    let (mut actors, mut contexts, _store) = setup_multi_stream_actors("bench", "bench", 2);

    let payloads = create_bench_event_payloads(10000, 256);
    let chunk_size = 100; // Events per commit

    let mut group = c.benchmark_group("tier2_stream_ingest");
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(5));
    group.throughput(Throughput::Elements((2 * chunk_size) as u64));

    let mut expected_offsets = [0u64; 2];
    let mut global_event_idx = 0usize;

    group.bench_function("ingest_10k_chunked", |b| {
        b.iter(|| {
            // Alternate between resources, commit chunk_size events to each
            for res_idx in 0..2 {
                let route = Route::new(format!("stream://bench/bench/resource-{}/append", res_idx));
                let family_id = RouteFamily::new(1);

                // BeginSession
                actors[res_idx].receive(
                    StreamMessage::BeginSession {
                        family_id,
                        route: route.clone(),
                        expected_offset: expected_offsets[res_idx],
                        ingest_metadata: None,
                    },
                    &mut contexts[res_idx],
                );

                let session_id = format!("bench-session-{}-{}", res_idx, expected_offsets[res_idx]);

                // Append chunk_size events
                for _ in 0..chunk_size {
                    let idx = global_event_idx % payloads.len();
                    let payload = black_box(&payloads[idx]);
                    actors[res_idx].receive(
                        StreamMessage::AppendToSession {
                            session_id: session_id.clone(),
                            body: Bytes::from(payload.clone()),
                            metadata: None,
                        },
                        &mut contexts[res_idx],
                    );
                    global_event_idx += 1;
                }

                // CommitSession
                actors[res_idx].receive(
                    StreamMessage::CommitSession { session_id },
                    &mut contexts[res_idx],
                );

                expected_offsets[res_idx] += chunk_size as u64;
            }
        })
    });

    group.finish();
}

/// BENCH 3: Multi-resource actor coordination (formerly "merge")
/// Measures: cost of coordinating K actor reads in round-robin
/// NOTE: This is NOT testing K-way merge algorithms (no BinaryHeap).
///       Area/realm indexes are pre-interleaved by writes - no merge needed.
///       This measures actor message dispatch overhead for multi-resource reads.
fn bench_multi_resource_actor_coordination(c: &mut Criterion) {
    let k_values = [2usize, 4];

    for &k in &k_values {
        let (mut actors, mut contexts, _store) = setup_multi_stream_actors("bench", "bench", k);

        let payloads = create_bench_event_payloads(1000, 256);

        // Pre-populate each resource with 100 committed events
        for (res_idx, actor) in actors.iter_mut().enumerate() {
            let route = Route::new(format!("stream://bench/bench/resource-{}/append", res_idx));
            let family_id = RouteFamily::new(1);
            let mut expected_offset = 0u64;

            for chunk_start in (0..100).step_by(10) {
                actor.receive(
                    StreamMessage::BeginSession {
                        family_id,
                        route: route.clone(),
                        expected_offset,
                        ingest_metadata: None,
                    },
                    &mut contexts[res_idx],
                );

                let session_id = format!("bench-session-{}-{}", res_idx, expected_offset);

                for i in 0..10 {
                    actor.receive(
                        StreamMessage::AppendToSession {
                            session_id: session_id.clone(),
                            body: Bytes::from(payloads[(chunk_start + i) % payloads.len()].clone()),
                            metadata: None,
                        },
                        &mut contexts[res_idx],
                    );
                }

                actor.receive(
                    StreamMessage::CommitSession { session_id },
                    &mut contexts[res_idx],
                );

                expected_offset += 10;
            }
        }

        let mut group = c.benchmark_group("tier2_stream_actor_coordination");
        group.sampling_mode(SamplingMode::Flat);
        group.throughput(Throughput::Elements((k * 1000) as u64));

        let name = format!("round_robin_{}_actors", k);
        let mut read_offsets = vec![0u64; k];

        group.bench_function(&name, |b| {
            b.iter(|| {
                // Coordinate K actor reads (round-robin actor message dispatch)
                for res_idx in 0..k {
                    let route =
                        Route::new(format!("stream://bench/bench/resource-{}/read", res_idx));
                    actors[res_idx].receive(
                        StreamMessage::Read {
                            family_id: RouteFamily::new(1),
                            route,
                            from_offset: black_box(read_offsets[res_idx]),
                            limit: 1000,
                            max_bytes: None,
                        },
                        &mut contexts[res_idx],
                    );
                    read_offsets[res_idx] = (read_offsets[res_idx] + 1000) % 100;
                }
            })
        });

        group.finish();
    }
}

criterion_group! {
    name = benches;
    config = config::criterion_config();
    targets = bench_multi_resource_round_robin, bench_streaming_ingest_10k, bench_multi_resource_actor_coordination
}
criterion_main!(benches);
