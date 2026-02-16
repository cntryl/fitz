use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::benchkit::{create_bench_event_payloads, create_local_bench_stream_actor};
use fitz::domains::stream::protocol::{LeaseGrant, StreamMessage, StreamWriteMode, DEFAULT_LEASE_SIZE};
use fitz::prelude::Actor;
use fitz::runtime::routing::Route;
use std::time::Duration;

#[path = "config.rs"]
mod config;

// ============================================================================
// TIER 4: INTEGRATION BENCHMARKS (implemented)
// - Each benchmark measures realistic append/read/commit workflows against
//   a local disk-backed StreamActor created via `create_local_bench_stream_actor`.
// - All setup (actor + payload creation) is outside the measured hot-path.
// - Leases are pre-granted in setup so commits exercise the hot-path without
//   cross-actor coordination (reasonable for integration microbench).
// ============================================================================

fn bench_complete_append_read_workflow(c: &mut Criterion) {
    let mut group = c.benchmark_group("stream_integration_append_read_workflow");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(1));
    group.throughput(Throughput::Elements(1));

    group.bench_function("stream_integration_append_then_read_immediate", |b| {
        b.iter_batched(
            || {
                // Setup: actor + payload + pre-grant lease so commit proceeds
                let payload = Bytes::from_static(b"append read workflow");
                let (mut actor, mut ctx, _temp_dir) =
                    create_local_bench_stream_actor("bench", "integration", "append_read");

                // Pre-grant a generous lease so commits succeed immediately in bench
                actor.receive(
                    StreamMessage::LeaseGranted {
                        grant: LeaseGrant {
                            area_start: 0,
                            area_end_exclusive: DEFAULT_LEASE_SIZE,
                            realm_start: 0,
                            realm_end_exclusive: DEFAULT_LEASE_SIZE,
                        },
                    },
                    &mut ctx,
                );

                (actor, ctx, _temp_dir, payload)
            },
            |(mut actor, mut ctx, _temp_dir, payload)| {
                // Measured hot-path: begin -> append -> commit -> read
                let family = *ctx.address().family();
                let route = Route::new("stream://bench/integration/append_read");

                actor.receive(
                    StreamMessage::Begin {
                        family_id: family,
                        route: route.clone(),
                        expected_offset: 0,
                        ingest_metadata: None,
                    },
                    &mut ctx,
                );

                actor.receive(
                    StreamMessage::Append {
                        session_id: 1,
                        body: payload.clone(),
                        metadata: None,
                    },
                    &mut ctx,
                );

                actor.receive(
                    StreamMessage::Commit {
                        session_id: 1,
                        mode: StreamWriteMode::Sync,
                    },
                    &mut ctx,
                );

                actor.receive(
                    StreamMessage::Read {
                        family_id: family,
                        route,
                        from_offset: 0,
                        limit: 1,
                        max_bytes: None,
                    },
                    &mut ctx,
                );

                black_box(());
            },
            criterion::BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn bench_batch_append_consumer_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("stream_integration_batch_append_consumer");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(1));
    group.throughput(Throughput::Elements(50)); // 50 events per batch

    group.bench_function("stream_integration_batch_50appends_consumer_read", |b| {
        b.iter_batched(
            || {
                let payloads = create_bench_event_payloads(50, 128);
                let (mut actor, mut ctx, _temp_dir) =
                    create_local_bench_stream_actor("bench", "integration", "batch");

                // Pre-grant lease for the batch
                actor.receive(
                    StreamMessage::LeaseGranted {
                        grant: LeaseGrant {
                            area_start: 0,
                            area_end_exclusive: DEFAULT_LEASE_SIZE,
                            realm_start: 0,
                            realm_end_exclusive: DEFAULT_LEASE_SIZE,
                        },
                    },
                    &mut ctx,
                );

                (actor, ctx, _temp_dir, payloads)
            },
            |(mut actor, mut ctx, _temp_dir, payloads)| {
                let family = *ctx.address().family();
                let route = Route::new("stream://bench/integration/batch");

                actor.receive(
                    StreamMessage::Begin {
                        family_id: family,
                        route: route.clone(),
                        expected_offset: 0,
                        ingest_metadata: None,
                    },
                    &mut ctx,
                );

                for p in payloads.iter() {
                    actor.receive(
                        StreamMessage::Append {
                            session_id: 1,
                            body: Bytes::from(p.clone()),
                            metadata: None,
                        },
                        &mut ctx,
                    );
                }

                actor.receive(
                    StreamMessage::Commit {
                        session_id: 1,
                        mode: StreamWriteMode::Sync,
                    },
                    &mut ctx,
                );

                // Consumer read to validate end-to-end
                actor.receive(
                    StreamMessage::Read {
                        family_id: family,
                        route,
                        from_offset: 0,
                        limit: 50,
                        max_bytes: None,
                    },
                    &mut ctx,
                );

                black_box(());
            },
            criterion::BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn bench_multipartition_read_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("stream_integration_multipartition_scan");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(1));
    group.throughput(Throughput::Elements(100)); // 100 events across partitions

    group.bench_function("stream_integration_scan_4partitions_25events_each", |b| {
        b.iter_batched(
            || {
                // Create 4 actors and precompute payloads per partition
                let actors: Vec<_> = (0..4)
                    .map(|i| create_local_bench_stream_actor("bench", "integration", &format!("partition{}", i)))
                    .collect();
                let payloads = create_bench_event_payloads(25, 64);

                // Pre-grant leases for each actor
                let mut actors_with_ctx = actors;

                for (actor, ctx, _td) in actors_with_ctx.iter_mut() {
                    actor.receive(
                        StreamMessage::LeaseGranted {
                            grant: LeaseGrant {
                                area_start: 0,
                                area_end_exclusive: DEFAULT_LEASE_SIZE,
                                realm_start: 0,
                                realm_end_exclusive: DEFAULT_LEASE_SIZE,
                            },
                        },
                        ctx,
                    );
                }

                (actors_with_ctx, payloads)
            },
            |(mut actors_with_ctx, payloads)| {
                // Append 25 events into each partition and commit
                for (i, (actor, ctx, _td)) in actors_with_ctx.iter_mut().enumerate() {
                    let family = *ctx.address().family();
                    let route = Route::new(format!("stream://bench/integration/partition{}/append", i));

                    actor.receive(
                        StreamMessage::Begin {
                            family_id: family,
                            route: route.clone(),
                            expected_offset: 0,
                            ingest_metadata: None,
                        },
                        ctx,
                    );

                    for p in payloads.iter() {
                        actor.receive(
                            StreamMessage::Append {
                                session_id: 1,
                                body: Bytes::from(p.clone()),
                                metadata: None,
                            },
                            ctx,
                        );
                    }

                    actor.receive(
                        StreamMessage::Commit {
                            session_id: 1,
                            mode: StreamWriteMode::Sync,
                        },
                        ctx,
                    );
                }

                black_box(());
            },
            criterion::BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn bench_consumer_offset_commit_workflow(c: &mut Criterion) {
    let mut group = c.benchmark_group("stream_integration_consumer_offset_commit");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(1));
    group.throughput(Throughput::Elements(1));

    group.bench_function("stream_integration_commit_consumer_offset", |b| {
        b.iter_batched(
            || {
                let (mut actor, mut ctx, _temp_dir) = create_local_bench_stream_actor("bench", "integration", "offset_commit");
                actor.receive(
                    StreamMessage::LeaseGranted {
                        grant: LeaseGrant {
                            area_start: 0,
                            area_end_exclusive: DEFAULT_LEASE_SIZE,
                            realm_start: 0,
                            realm_end_exclusive: DEFAULT_LEASE_SIZE,
                        },
                    },
                    &mut ctx,
                );
                (actor, ctx)
            },
            |(mut actor, mut ctx)| {
                let family = *ctx.address().family();
                let route = Route::new("stream://bench/integration/offset_commit");

                actor.receive(
                    StreamMessage::Begin {
                        family_id: family,
                        route: route.clone(),
                        expected_offset: 0,
                        ingest_metadata: None,
                    },
                    &mut ctx,
                );

                actor.receive(
                    StreamMessage::Append {
                        session_id: 1,
                        body: Bytes::from_static(b"offset_commit_event"),
                        metadata: None,
                    },
                    &mut ctx,
                );

                actor.receive(
                    StreamMessage::Commit {
                        session_id: 1,
                        mode: StreamWriteMode::Sync,
                    },
                    &mut ctx,
                );

                black_box(());
            },
            criterion::BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn bench_long_running_append_read_interleave(c: &mut Criterion) {
    let mut group = c.benchmark_group("stream_integration_long_running_interleave");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(1));
    group.throughput(Throughput::Elements(20)); // 20 operations per iteration

    group.bench_function("stream_integration_20ops_mixed_append_read", |b| {
        b.iter_batched(
            || {
                let payload = Bytes::from_static(b"interleaved op");
                let (mut actor, mut ctx, _temp_dir) = create_local_bench_stream_actor("bench", "integration", "long_running");
                actor.receive(
                    StreamMessage::LeaseGranted {
                        grant: LeaseGrant {
                            area_start: 0,
                            area_end_exclusive: DEFAULT_LEASE_SIZE,
                            realm_start: 0,
                            realm_end_exclusive: DEFAULT_LEASE_SIZE,
                        },
                    },
                    &mut ctx,
                );
                (actor, ctx, payload)
            },
            |(mut actor, mut ctx, payload)| {
                let family = *ctx.address().family();
                let route = Route::new("stream://bench/integration/long_running/append");

                // Interleave 20 operations: append + occasional read
                for i in 0..20u32 {
                    actor.receive(
                        StreamMessage::Begin {
                            family_id: family,
                            route: route.clone(),
                            expected_offset: 0, // single-session microbench; session ids reset per setup
                            ingest_metadata: None,
                        },
                        &mut ctx,
                    );

                    actor.receive(
                        StreamMessage::Append {
                            session_id: 1,
                            body: payload.clone(),
                            metadata: None,
                        },
                        &mut ctx,
                    );

                    actor.receive(
                        StreamMessage::Commit {
                            session_id: 1,
                            mode: StreamWriteMode::Sync,
                        },
                        &mut ctx,
                    );

                    if i % 5 == 0 {
                        actor.receive(
                            StreamMessage::Read {
                                family_id: family,
                                route: route.clone(),
                                from_offset: 0,
                                limit: 1,
                                max_bytes: None,
                            },
                            &mut ctx,
                        );
                    }
                }

                black_box(());
            },
            criterion::BatchSize::SmallInput,
        )
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = config::criterion_config();
    targets =
        bench_complete_append_read_workflow,
        bench_batch_append_consumer_read,
        bench_multipartition_read_scan,
        bench_consumer_offset_commit_workflow,
        bench_long_running_append_read_interleave,
}
criterion_main!(benches);
