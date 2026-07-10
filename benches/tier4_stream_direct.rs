#![allow(dead_code)] // Stream targets select focused direct-layer rows.

use crate::tier4_stream_support::{
    measure_operations, tag_row, LayerKind, ReadScope, RowDimensions, StorageProfile,
};
use bytes::Bytes;
use cntryl_stress::StressContext;
use fitz::benchkit::create_write_heavy_bench_store;
use fitz::domains::stream::protocol::StreamWriteMode;
use fitz::domains::stream::{StreamActor, StreamStore};
use fitz::runtime::routing::RouteFamily;
use std::sync::Arc;
use std::time::Instant;

const OWNER_SESSION_ID: u64 = 1;

pub(crate) struct DirectActorFixture {
    pub(crate) actor: StreamActor,
    _temp_dir: Option<tempfile::TempDir>,
}

pub(crate) fn direct_actor(storage: StorageProfile, realm: &str) -> DirectActorFixture {
    match storage {
        StorageProfile::Memory => {
            let store = Arc::new(StreamStore::new(create_write_heavy_bench_store()));
            let actor = StreamActor::new(
                RouteFamily::new(1),
                realm.to_string(),
                "orders".to_string(),
                "resource-0".to_string(),
                store,
            )
            .expect("create write-heavy memory Stream actor");
            DirectActorFixture {
                actor,
                _temp_dir: None,
            }
        }
        StorageProfile::LocalDisk => {
            let (actor, temp_dir) =
                fitz::benchkit::create_local_bench_stream_actor(realm, "orders", "resource-0");
            DirectActorFixture {
                actor,
                _temp_dir: Some(temp_dir),
            }
        }
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

pub(crate) fn measure_direct_write(
    ctx: &mut StressContext,
    storage: StorageProfile,
    payload_size: usize,
    write_mode: StreamWriteMode,
    scenario: &'static str,
    measurement: &'static str,
) {
    let mode_label = match write_mode {
        StreamWriteMode::Buffered => "buffered",
        StreamWriteMode::Sync => "sync",
        StreamWriteMode::CloudStrict => "cloud_strict",
    };
    tag_row(
        ctx,
        &RowDimensions {
            scenario,
            storage_profile: storage,
            layer: LayerKind::Direct,
            write_mode: mode_label,
            write_operation: "begin_append_commit",
            payload_size,
            history_depth: 0,
            read_limit: 0,
            read_scope: ReadScope::None,
            route_count: 1,
            filter_match_count: "not_filtered",
            client_count: 1,
            workload_mix: "write_only",
            completed_unit: "write_lifecycle",
            gate_class: "characterization",
        },
    );
    let mut fixture = direct_actor(storage, &format!("direct-write-{}", storage.label()));
    let payload = Bytes::from(vec![0xC3; payload_size]);
    let mut stream_session_id = 1_u64;
    let mut next_offset = 0_u64;

    measure_operations(ctx, measurement, 1, |latencies| {
        let started = Instant::now();
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
        let mut commit_attempts = 0_u32;
        loop {
            match fixture.actor.commit_session_for_owner(
                OWNER_SESSION_ID,
                stream_session_id,
                write_mode,
            ) {
                Ok(_) => break,
                // Local Midge commits are optimistic. A transient conflict leaves the
                // staged session intact, so retrying the exact same commit is safe.
                Err(error) if error.contains("concurrency conflict") => {
                    commit_attempts += 1;
                    assert!(
                        commit_attempts < 1_000,
                        "direct Stream commit retry limit exceeded"
                    );
                    std::thread::yield_now();
                }
                Err(error) => panic!("commit direct Stream write: {error}"),
            }
        }
        latencies.push(started.elapsed());
        stream_session_id = stream_session_id.saturating_add(1);
        next_offset = next_offset.saturating_add(1);
    });
}
