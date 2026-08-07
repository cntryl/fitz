#![allow(dead_code)] // Stream targets select focused direct-layer rows.

use crate::tier4_stream_support::{
    measure_operations, tag_row, LayerKind, ReadScope, RowDimensions, StorageProfile,
};
use bytes::Bytes;
use cntryl_stress::StressContext;
use fitz::benchkit::{create_local_bench_store, create_write_heavy_bench_store};
use fitz::domains::stream::protocol::{StreamFilterClause, StreamFilterSet, StreamWriteMode};
use fitz::domains::stream::store::{BatchLimits, ReadResourceParams, StreamTTL};
use fitz::domains::stream::{StreamActor, StreamStore};
use fitz::runtime::routing::RouteFamily;
use std::sync::Arc;
use std::time::Instant;

const OWNER_SESSION_ID: u64 = 1;

pub(crate) struct DirectActorFixture {
    pub(crate) actor: StreamActor,
    store: Arc<StreamStore>,
    realm: String,
    _temp_dir: Option<tempfile::TempDir>,
}

fn fixture_from_store(
    store: Arc<StreamStore>,
    realm: &str,
    temp_dir: Option<tempfile::TempDir>,
) -> DirectActorFixture {
    DirectActorFixture {
        actor: new_direct_actor(store.clone(), realm),
        store,
        realm: realm.to_string(),
        _temp_dir: temp_dir,
    }
}

pub(crate) fn direct_actor(storage: StorageProfile, realm: &str) -> DirectActorFixture {
    let (store, temp_dir) = match storage {
        StorageProfile::Memory => (
            Arc::new(StreamStore::new(create_write_heavy_bench_store())),
            None,
        ),
        StorageProfile::LocalDisk => {
            let (engine, temp_dir) = create_local_bench_store();
            (Arc::new(StreamStore::new(engine)), Some(temp_dir))
        }
    };
    fixture_from_store(store, realm, temp_dir)
}

fn new_direct_actor(store: Arc<StreamStore>, realm: &str) -> StreamActor {
    StreamActor::new(
        RouteFamily::new(1),
        realm.to_string(),
        "orders".to_string(),
        "resource-0".to_string(),
        store,
    )
    .expect("create direct Stream actor")
}

impl DirectActorFixture {
    fn refresh_actor(&mut self) {
        self.actor = new_direct_actor(self.store.clone(), &self.realm);
    }

    fn next_resource_offset(&self) -> u64 {
        self.actor
            .metadata()
            .expect("read direct Stream metadata after conflict")
            .metadata
            .last_resource_offset
            .map_or(0, |offset| offset.saturating_add(1))
    }
}

pub(crate) fn seed_direct_actor(actor: &mut StreamActor, payload: &Bytes, event_count: usize) {
    actor
        .begin_append_session(OWNER_SESSION_ID, 1, None)
        .expect("begin direct Stream session");
    for offset in 0..event_count {
        actor
            .append_to_session_with_discriminator_for_owner(
                OWNER_SESSION_ID,
                1,
                u64::try_from(offset).expect("offset should fit u64"),
                payload.clone(),
                None,
                None,
            )
            .expect("append direct Stream event");
    }
    actor
        .commit_session_for_owner(OWNER_SESSION_ID, 1, StreamWriteMode::Sync)
        .expect("commit direct Stream history");
}

fn direct_write_dimensions(
    storage: StorageProfile,
    payload_size: usize,
    write_mode: StreamWriteMode,
    scenario: &'static str,
    history_depth: usize,
) -> RowDimensions<'static> {
    RowDimensions {
        scenario,
        storage_profile: storage,
        layer: LayerKind::Direct,
        write_mode: match write_mode {
            StreamWriteMode::Buffered => "buffered",
            StreamWriteMode::Sync => "sync",
            StreamWriteMode::CloudStrict => "cloud_strict",
        },
        write_operation: "begin_append_commit",
        payload_size,
        history_depth,
        read_limit: 0,
        read_scope: ReadScope::None,
        route_count: 1,
        filter_match_count: "not_filtered",
        client_count: 1,
        workload_mix: "write_only",
        completed_unit: "write_lifecycle",
        gate_class: "characterization",
    }
}

pub(crate) fn measure_direct_write(
    ctx: &mut StressContext,
    storage: StorageProfile,
    payload_size: usize,
    write_mode: StreamWriteMode,
    scenario: &'static str,
    measurement: &'static str,
) {
    measure_direct_write_after_history(
        ctx,
        storage,
        payload_size,
        write_mode,
        scenario,
        measurement,
        0,
    );
}

pub(crate) fn measure_direct_write_after_history(
    ctx: &mut StressContext,
    storage: StorageProfile,
    payload_size: usize,
    write_mode: StreamWriteMode,
    scenario: &'static str,
    measurement: &'static str,
    history_depth: usize,
) {
    tag_row(
        ctx,
        &direct_write_dimensions(storage, payload_size, write_mode, scenario, history_depth),
    );
    let mut fixture = direct_actor(storage, &format!("direct-write-{}", storage.label()));
    let payload = Bytes::from(vec![0xC3; payload_size]);
    let mut stream_session_id = 1_u64;
    let mut next_offset = 0_u64;
    for batch_start in (0..history_depth).step_by(1_000) {
        let batch_len = (history_depth - batch_start).min(1_000);
        fixture
            .actor
            .begin_append_session(OWNER_SESSION_ID, stream_session_id, None)
            .expect("begin direct Stream history session");
        for event_index in 0..batch_len {
            fixture
                .actor
                .append_to_session_with_discriminator_for_owner(
                    OWNER_SESSION_ID,
                    stream_session_id,
                    next_offset.saturating_add(
                        u64::try_from(event_index).expect("event index should fit u64"),
                    ),
                    payload.clone(),
                    None,
                    None,
                )
                .expect("append direct Stream history");
        }
        fixture
            .actor
            .commit_session_for_owner(OWNER_SESSION_ID, stream_session_id, write_mode)
            .expect("commit direct Stream history");
        stream_session_id = stream_session_id.saturating_add(1);
        next_offset = next_offset.saturating_add(u64::try_from(batch_len).unwrap_or(u64::MAX));
    }
    if storage == StorageProfile::Memory {
        // Memory-mode Midge intentionally does not flush: a Stream history is
        // therefore unbounded for the duration of a fixed-duration benchmark.
        // Recreate the ephemeral fixture only when its explicit memory budget
        // rejects a write so the row measures successful write lifecycles without
        // silently turning the storage contract into an unbounded allocation.
        ctx.metadata("memory_store_reset", "on_memory_budget_stall");
    } else {
        ctx.metadata("stream_storage_layout", "d4_immutable_fragments");
    }

    measure_operations(ctx, measurement, 1, |latencies| {
        let mut started = Instant::now();
        let mut commit_attempts = 0_u32;
        let mut stage_session = true;
        loop {
            if stage_session {
                fixture
                    .actor
                    .begin_append_session(OWNER_SESSION_ID, stream_session_id, None)
                    .expect("begin direct Stream write");
                fixture
                    .actor
                    .append_to_session_with_discriminator_for_owner(
                        OWNER_SESSION_ID,
                        stream_session_id,
                        next_offset,
                        payload.clone(),
                        None,
                        None,
                    )
                    .expect("append direct Stream write");
            }
            match fixture.actor.commit_session_for_owner(
                OWNER_SESSION_ID,
                stream_session_id,
                write_mode,
            ) {
                Ok(_) => break,
                // An optimistic conflict leaves this actor's cached frontier stale.
                // Recreate it on the same store, then restage against the refreshed
                // frontier before retrying the logical lifecycle.
                Err(error) if error.contains("concurrency conflict") => {
                    commit_attempts += 1;
                    assert!(
                        commit_attempts < 1_000,
                        "direct Stream commit retry limit exceeded"
                    );
                    fixture.refresh_actor();
                    next_offset = fixture.next_resource_offset();
                    stage_session = true;
                    std::thread::yield_now();
                }
                Err(error)
                    if storage == StorageProfile::Memory
                        && error.contains("Memory budget exceeded") =>
                {
                    // In-memory Midge retains every committed Stream record and
                    // deliberately makes flush a no-op. Start a fresh ephemeral
                    // store before retrying this logical operation; exclude the
                    // recovery/setup time from that operation's latency sample.
                    fixture = direct_actor(storage, &format!("direct-write-{}", storage.label()));
                    stream_session_id = 1;
                    next_offset = 0;
                    stage_session = true;
                    started = Instant::now();
                }
                Err(error) => panic!("commit direct Stream write: {error}"),
            }
        }
        latencies.push(started.elapsed());
        stream_session_id = stream_session_id.saturating_add(1);
        next_offset = next_offset.saturating_add(1);
    });
}

fn append_single_event(
    fixture: &mut DirectActorFixture,
    payload: &Bytes,
    discriminator: Option<&str>,
    session_id: u64,
    expected_offset: u64,
) {
    fixture
        .actor
        .begin_append_session(OWNER_SESSION_ID, session_id, None)
        .expect("begin Stream shape session");
    fixture
        .actor
        .append_to_session_with_discriminator_for_owner(
            OWNER_SESSION_ID,
            session_id,
            expected_offset,
            payload.clone(),
            None,
            discriminator.map(Into::into),
        )
        .expect("append Stream shape event");
    fixture
        .actor
        .commit_session_for_owner(OWNER_SESSION_ID, session_id, StreamWriteMode::Sync)
        .expect("commit Stream shape event");
}

pub(crate) fn measure_memtable_rotation_write(ctx: &mut StressContext) {
    tag_row(
        ctx,
        &direct_write_dimensions(
            StorageProfile::LocalDisk,
            4 * 1_024,
            StreamWriteMode::Sync,
            "repeated_memtable_rotation",
            0,
        ),
    );
    ctx.parameter("memtable_size_bytes", 128 * 1_024);
    let temp_dir = tempfile::tempdir().expect("create rotating Stream benchmark directory");
    let engine = Arc::new(
        cntryl_midge::Engine::open(
            cntryl_midge::OpenOptions::local(temp_dir.path())
                .with_memtable_size_limit(128 * 1_024)
                .build()
                .expect("build rotating Stream benchmark options"),
        )
        .expect("open rotating Stream benchmark engine"),
    );
    engine
        .create_column_family("cf_1")
        .expect("create rotating Stream benchmark family");
    let mut fixture = fixture_from_store(
        Arc::new(StreamStore::new(engine)),
        "direct-memtable-rotation",
        Some(temp_dir),
    );
    let payload = Bytes::from(vec![0x6D; 4 * 1_024]);
    let mut session_id = 1_u64;
    let mut offset = 0_u64;
    measure_operations(ctx, "memtable_rotation_append_4k", 1, |latencies| {
        let started = Instant::now();
        append_single_event(&mut fixture, &payload, None, session_id, offset);
        latencies.push(started.elapsed());
        session_id = session_id.saturating_add(1);
        offset = offset.saturating_add(1);
    });
}

pub(crate) fn measure_ttl_churn(ctx: &mut StressContext) {
    tag_row(
        ctx,
        &direct_write_dimensions(
            StorageProfile::Memory,
            64,
            StreamWriteMode::Sync,
            "ttl_churn",
            0,
        ),
    );
    ctx.parameter("ttl_seconds", 0);
    ctx.parameter("maintenance_after_each_commit", true);
    let engine = create_write_heavy_bench_store();
    let store = Arc::new(StreamStore::with_config(
        engine,
        BatchLimits::default(),
        StreamTTL::with_seconds(0),
    ));
    let mut fixture = fixture_from_store(store.clone(), "direct-ttl-churn", None);
    let payload = Bytes::from_static(b"ttl-churn-event");
    let mut session_id = 1_u64;
    let mut offset = 0_u64;
    measure_operations(ctx, "ttl_churn_commit_and_maintain", 1, |latencies| {
        let started = Instant::now();
        append_single_event(&mut fixture, &payload, None, session_id, offset);
        store.run_maintenance(1).expect("run TTL churn maintenance");
        latencies.push(started.elapsed());
        session_id = session_id.saturating_add(1);
        offset = offset.saturating_add(1);
    });
}

pub(crate) fn measure_filtered_locator_read(ctx: &mut StressContext, sparse: bool) {
    let selectivity = if sparse { "1_of_16" } else { "all" };
    let dimensions = RowDimensions {
        scenario: "locator_density_read",
        storage_profile: StorageProfile::Memory,
        layer: LayerKind::Direct,
        write_mode: "not_applicable",
        write_operation: "none",
        payload_size: 64,
        history_depth: 512,
        read_limit: 512,
        read_scope: ReadScope::Realm,
        route_count: 1,
        filter_match_count: selectivity,
        client_count: 1,
        workload_mix: "read_only",
        completed_unit: "read_request",
        gate_class: "characterization",
    };
    tag_row(ctx, &dimensions);
    let mut fixture = direct_actor(StorageProfile::Memory, "direct-locator-density");
    let payload = Bytes::from(vec![0x4C; 64]);
    fixture
        .actor
        .begin_append_session(OWNER_SESSION_ID, 1, None)
        .expect("begin locator history");
    for offset in 0..512_u64 {
        let discriminator = if !sparse || offset % 16 == 0 {
            "match"
        } else {
            "skip"
        };
        fixture
            .actor
            .append_to_session_with_discriminator_for_owner(
                OWNER_SESSION_ID,
                1,
                offset,
                payload.clone(),
                None,
                Some(discriminator.into()),
            )
            .expect("append locator history");
    }
    fixture
        .actor
        .commit_session_for_owner(OWNER_SESSION_ID, 1, StreamWriteMode::Sync)
        .expect("commit locator history");
    let filter = StreamFilterSet {
        clauses: vec![StreamFilterClause::Equals("match".to_string())],
    };
    let measurement = if sparse {
        "sparse_locator_read"
    } else {
        "dense_locator_read"
    };
    measure_operations(ctx, measurement, 1, |latencies| {
        let started = Instant::now();
        let records = fixture
            .store
            .read_realm_with_filter(1, &fixture.realm, 0, 512, None, Some(&filter))
            .expect("read locator density")
            .0;
        let event_count = records
            .iter()
            .filter(|item| matches!(item, fitz::domains::stream::StreamReadItem::Event(_)))
            .count();
        assert_eq!(event_count, if sparse { 32 } else { 512 });
        latencies.push(started.elapsed());
    });
}

pub(crate) fn measure_compaction_replay(ctx: &mut StressContext, compacted: bool) {
    let scenario = if compacted {
        "replay_after_compaction"
    } else {
        "replay_before_compaction"
    };
    let dimensions = RowDimensions {
        scenario,
        storage_profile: StorageProfile::Memory,
        layer: LayerKind::Direct,
        write_mode: "not_applicable",
        write_operation: "none",
        payload_size: 64,
        history_depth: 64,
        read_limit: 64,
        read_scope: ReadScope::Resource,
        route_count: 1,
        filter_match_count: "unfiltered",
        client_count: 1,
        workload_mix: "read_only",
        completed_unit: "read_request",
        gate_class: "characterization",
    };
    tag_row(ctx, &dimensions);
    let mut fixture = direct_actor(StorageProfile::Memory, scenario);
    let payload = Bytes::from(vec![0x52; 64]);
    for offset in 0..64_u64 {
        append_single_event(
            &mut fixture,
            &payload,
            None,
            offset.saturating_add(1),
            offset,
        );
    }
    if compacted {
        while fixture
            .store
            .run_maintenance(1)
            .expect("compact replay fixture")
            .buckets_compacted
            > 0
        {}
    }
    measure_operations(ctx, scenario, 1, |latencies| {
        let started = Instant::now();
        let records = fixture
            .store
            .read_resource(&ReadResourceParams {
                family: 1,
                realm: &fixture.realm,
                area: "orders",
                resource: "resource-0",
                from_offset: 0,
                limit: 64,
                max_bytes: None,
            })
            .expect("read compaction replay fixture")
            .0;
        assert_eq!(records.len(), 64);
        latencies.push(started.elapsed());
    });
}
