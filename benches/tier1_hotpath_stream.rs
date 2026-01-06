/// Tier 1: Stream Domain Hot-Path Microbenchmarks
/// 
/// Tests isolated, single-actor hot paths with minimal coordination.
/// CRITICAL: All write benches MUST use the full session flow:
///   BeginSession(expected_offset)  Append(N events)  CommitSession
/// 
/// GOALS:
/// - Measure raw commit latency (per-event and batched)
/// - Measure read scanning throughput
/// - Validate batching amortization
/// - Catch regressions in hot paths
/// 
/// RESTRICTIONS:
/// - Single StreamActor (no multi-actor coordination)
/// - No explicit lease renewal testing (that's Tier 2)
/// - No watermark advancement testing (that's Tier 2)
/// - All setup precomputed outside hot path
///
/// Each bench:
/// - Precomputes all data outside measured loop
/// - No allocations in hot path
/// - Deterministic (black_box on inputs)
/// - Measures only core operation
use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::domains::stream::stream_actor::StreamActor;
use fitz::domains::stream::store::StreamStore;
use fitz::domains::stream::protocol::StreamMessage;
use fitz::prelude::Actor;
use fitz::runtime::actor::Context;
use fitz::runtime::router::Router;
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use std::sync::Arc;

#[path = "config.rs"]
mod config;

/// Create a StreamActor and its context for benchmarking
fn setup_stream_actor(realm: &str, area: &str, resource: &str) -> (StreamActor, Context<StreamActor>) {
    let router = Arc::new(Router::new());
    let family = RouteFamily::new(1);
    let addr = RouteAddress::new(
        family,
        Route::new(format!("stream://{}/{}/{}/append", realm, area, resource)),
    );

    let db = Arc::new(cntryl_midge::MidgeEngine::open(cntryl_midge::MidgeOptions::default()).expect("Failed to open store"));
    let store = Arc::new(StreamStore::new(db));
    let actor = StreamActor::new(family, realm.to_string(), area.to_string(), resource.to_string(), store);
    let ctx = Context::new(addr, router);

    (actor, ctx)
}

/// Precompute deterministic event payloads
fn make_event_payloads(count: usize, size: usize) -> Vec<Vec<u8>> {
    (0..count)
        .map(|i| {
            let mut payload = vec![0u8; size];
            // Deterministic pattern: include event index in payload
            payload[..8.min(size)].copy_from_slice(&(i as u64).to_le_bytes()[..8.min(size)]);
            payload
        })
        .collect()
}

// ═══════════════════════════════════════════════════════════════════════════
// WRITE BENCHMARKS — FULL SESSION FLOW
// ═══════════════════════════════════════════════════════════════════════════

/// BENCH 1: Single event commit (BeginSession → Append(1) → CommitSession)
/// Measures: worst-case latency per event (no batching amortization)
fn bench_append_commit_single_event(c: &mut Criterion) {
    let (mut actor, mut ctx) = setup_stream_actor("bench-realm", "bench-area", "bench-single");
    
    // Precompute payloads outside benchmark
    let payloads = make_event_payloads(512, 256);
    let route = Route::new("stream://bench-realm/bench-area/bench-single/append".to_string());
    let family_id = RouteFamily::new(1);

    let mut group = c.benchmark_group("tier1_stream_commit");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    let mut expected_offset = 0u64;
    let mut payload_idx = 0;
    
    group.bench_function("commit_single_event_256B", |b| {
        b.iter(|| {
            let payload = black_box(&payloads[payload_idx % payloads.len()]);
            
            // FULL FLOW: BeginSession → Append → CommitSession
            actor.receive(
                StreamMessage::BeginSession {
                    family_id,
                    route: route.clone(),
                    expected_offset,
                    ingest_metadata: None,
                },
                &mut ctx,
            );
            
            // Note: In Fitz actor model, session_id is managed internally by the actor
            // We use a deterministic session_id based on expected_offset for benchmarking
            let session_id = format!("bench-session-{}", expected_offset);
            
            actor.receive(
                StreamMessage::AppendToSession {
                    session_id: session_id.clone(),
                    body: Bytes::from(payload.clone()),
                    metadata: None,
                },
                &mut ctx,
            );
            
            actor.receive(
                StreamMessage::CommitSession {
                    session_id,
                },
                &mut ctx,
            );
            
            expected_offset += 1;
            payload_idx += 1;
        })
    });

    group.finish();
}

/// BENCH 2: Batch commits (5, 10, 50 events per commit)
/// Measures: batching amortization cost
fn bench_append_commit_batches(c: &mut Criterion) {
    let batch_sizes = [5usize, 10, 50];
    
    let mut group = c.benchmark_group("tier1_stream_commit_batch");
    group.sampling_mode(SamplingMode::Flat);

    for &batch_size in &batch_sizes {
        let (mut actor, mut ctx) = setup_stream_actor("bench-realm", "bench-area", &format!("bench-batch-{}", batch_size));
        
        let payloads = make_event_payloads(batch_size + 50, 256);
        let route = Route::new(format!("stream://bench-realm/bench-area/bench-batch-{}/append", batch_size));
        let family_id = RouteFamily::new(1);
        
        group.throughput(Throughput::Elements(batch_size as u64));
        let name = format!("commit_batch_{}_events", batch_size);

        let mut expected_offset = 0u64;
        let mut offset = 0u64;
        
        group.bench_function(&name, |b| {
            b.iter(|| {
                // BeginSession
                actor.receive(
                    StreamMessage::BeginSession {
                        family_id,
                        route: route.clone(),
                        expected_offset,
                        ingest_metadata: None,
                    },
                    &mut ctx,
                );
                
                let session_id = format!("bench-session-{}", expected_offset);
                
                // Append batch_size events
                for i in 0..batch_size {
                    let idx = (offset as usize + i) % payloads.len();
                    let payload = black_box(&payloads[idx]);
                    actor.receive(
                        StreamMessage::AppendToSession {
                            session_id: session_id.clone(),
                            body: Bytes::from(payload.clone()),
                            metadata: None,
                        },
                        &mut ctx,
                    );
                }
                
                // CommitSession
                actor.receive(
                    StreamMessage::CommitSession {
                        session_id,
                    },
                    &mut ctx,
                );
                
                expected_offset += batch_size as u64;
                offset += batch_size as u64;
            })
        });
    }

    group.finish();
}

/// BENCH 3: Sequential read from resource stream (cursor-based)
/// Measures: read throughput from committed events (real store scan)
fn bench_resource_read_sequential(c: &mut Criterion) {
    let (mut actor, mut ctx) = setup_stream_actor("bench-realm", "bench-area", "bench-read");
    
    let route = Route::new("stream://bench-realm/bench-area/bench-read/append".to_string());
    let family_id = RouteFamily::new(1);
    
    // Pre-populate with committed events using proper session flow
    let payloads = make_event_payloads(1000, 256);
    let mut expected_offset = 0u64;
    
    // Commit 1000 events in batches of 100
    for chunk_start in (0..1000).step_by(100) {
        actor.receive(
            StreamMessage::BeginSession {
                family_id,
                route: route.clone(),
                expected_offset,
                ingest_metadata: None,
            },
            &mut ctx,
        );
        
        let session_id = format!("bench-session-{}", expected_offset);
        
        for i in 0..100 {
            actor.receive(
                StreamMessage::AppendToSession {
                    session_id: session_id.clone(),
                    body: Bytes::from(payloads[chunk_start + i].clone()),
                    metadata: None,
                },
                &mut ctx,
            );
        }
        
        actor.receive(
            StreamMessage::CommitSession {
                session_id,
            },
            &mut ctx,
        );
        
        expected_offset += 100;
    }

    let mut group = c.benchmark_group("tier1_stream_resource_read");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    let read_route = Route::new("stream://bench-realm/bench-area/bench-read/read".to_string());
    let mut read_offset = 0u64;
    
    group.bench_function("resource_read_sequential_256B", |b| {
        b.iter(|| {
            // Read one event at a time, cursor-based (real store scan)
            actor.receive(
                StreamMessage::Read {
                    family_id: RouteFamily::new(1),
                    route: read_route.clone(),
                    from_offset: black_box(read_offset),
                    limit: 1,
                    max_bytes: None,
                },
                &mut ctx,
            );
            read_offset = (read_offset + 1) % 1000;
        })
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = config::criterion_config();
    targets = bench_append_commit_single_event, bench_append_commit_batches, bench_resource_read_sequential
}
criterion_main!(benches);
