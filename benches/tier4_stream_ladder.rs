#[path = "tier4_stream_direct.rs"]
pub(crate) mod tier4_stream_direct;
#[path = "tier4_stream_support.rs"]
pub(crate) mod tier4_stream_support;
#[path = "tier4_stream_transport.rs"]
pub(crate) mod tier4_stream_transport;
#[path = "tier4_support.rs"]
pub(crate) mod tier4_support;

use crate::tier4_stream_direct::{direct_actor, measure_direct_write, seed_direct_actor};
use crate::tier4_stream_support::{
    measure_operations, tag_row, tlv_field, LayerKind, MutableAppendFrame, MutableCommitFrame,
    ReadScope, RowDimensions, StorageProfile, TransportKind, CANONICAL_HISTORY_DEPTH,
    CANONICAL_PAYLOAD_SIZE, CANONICAL_READ_LIMIT, STREAM_SYNC_COMMIT_MODE,
};
use crate::tier4_stream_transport::{measure_exact_replay, measure_write_lifecycle};
use bytes::Bytes;
use cntryl_stress::{stress, StressContext};
use fitz::benchkit::{
    build_stream_append, build_stream_begin, build_stream_commit, build_stream_read_with_limit,
    count_stream_read_records_from_payload, create_local_bench_stream_sink,
    create_write_heavy_bench_stream_sink, parse_stream_session_id, register_session_queue_sink,
    route_frame,
};
use fitz::domains::stream::protocol::StreamWriteMode;
use fitz::protocol::frame::ChannelId;
use fitz::runtime::router::{MailboxSink, Router};
use fitz::runtime::routing::{RouteAddress, RouteFamily};
use std::sync::Arc;
use std::time::{Duration, Instant};

const OWNER_SESSION_ID: u64 = 1;

fn read_dimensions(storage: StorageProfile, layer: LayerKind) -> RowDimensions<'static> {
    RowDimensions {
        scenario: "cost_ladder_exact_replay",
        storage_profile: storage,
        layer,
        write_mode: "not_applicable",
        write_operation: "none",
        payload_size: CANONICAL_PAYLOAD_SIZE,
        history_depth: CANONICAL_HISTORY_DEPTH,
        read_limit: CANONICAL_READ_LIMIT,
        read_scope: ReadScope::Resource,
        route_count: 1,
        filter_match_count: "unfiltered",
        client_count: 1,
        workload_mix: "read_only",
        completed_unit: "read_request",
        gate_class: "characterization",
    }
}

fn write_dimensions(storage: StorageProfile, layer: LayerKind) -> RowDimensions<'static> {
    RowDimensions {
        scenario: "cost_ladder_sync_write_lifecycle",
        storage_profile: storage,
        layer,
        write_mode: "sync",
        write_operation: "begin_append_commit",
        payload_size: CANONICAL_PAYLOAD_SIZE,
        history_depth: 0,
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

fn measure_direct_read(
    ctx: &mut StressContext,
    storage: StorageProfile,
    measurement: &'static str,
) {
    tag_row(ctx, &read_dimensions(storage, LayerKind::Direct));
    let mut fixture = direct_actor(storage, &format!("ladder-direct-read-{}", storage.label()));
    let payload = Bytes::from(vec![0x5A; CANONICAL_PAYLOAD_SIZE]);
    seed_direct_actor(&mut fixture.actor, &payload, CANONICAL_HISTORY_DEPTH);

    measure_operations(ctx, measurement, 1, |latencies| {
        let started = Instant::now();
        let response = fixture
            .actor
            .read(0, CANONICAL_READ_LIMIT as u64, None)
            .expect("direct Stream read");
        latencies.push(started.elapsed());
        assert_eq!(
            response.items.len(),
            CANONICAL_READ_LIMIT,
            "unexpected direct Stream read count"
        );
    });
}

struct EncodedFixture {
    router: Arc<Router>,
    family: RouteFamily,
    source: RouteAddress,
    inbox: Arc<fitz::benchkit::FrameQueueSink>,
    _temp_dir: Option<tempfile::TempDir>,
}

fn encoded_fixture(storage: StorageProfile) -> EncodedFixture {
    let family = RouteFamily::new(1);
    let router = Arc::new(Router::new());
    let (sink, temp_dir) = match storage {
        StorageProfile::Memory => (create_write_heavy_bench_stream_sink(router.clone()), None),
        StorageProfile::LocalDisk => {
            let (sink, temp_dir) = create_local_bench_stream_sink(router.clone());
            (sink, Some(temp_dir))
        }
    };
    router.register_domain_pattern("stream", sink as Arc<dyn MailboxSink>);
    let (source, inbox) = register_session_queue_sink(&router, family, OWNER_SESSION_ID);
    EncodedFixture {
        router,
        family,
        source,
        inbox,
        _temp_dir: temp_dir,
    }
}

fn encoded_request(fixture: &EncodedFixture, route: &str, frame: &[u8]) -> Bytes {
    let (message_type, payload) = tlv_field(frame);
    route_frame(
        &fixture.router,
        &fixture.source,
        route,
        OWNER_SESSION_ID,
        ChannelId::Pub,
        message_type,
        payload,
        fixture.family,
    )
    .expect("route encoded Stream frame");
    let response = fixture
        .inbox
        .drain_after_count(1, Duration::from_secs(5))
        .last()
        .map(|response| response.payload.clone())
        .expect("encoded Stream response");
    if response.first().copied() != Some(0) {
        let error = fitz::protocol::error_codes::decode_error_body(&response).map_or_else(
            |_| "encoded Stream request failed".to_string(),
            |(_, message)| message,
        );
        panic!("{error}");
    }
    response
}

fn seed_encoded(fixture: &EncodedFixture, route: &str, payload: &[u8]) {
    let begin_response = encoded_request(fixture, route, &build_stream_begin(route));
    let session_id = parse_stream_session_id(&begin_response).expect("encoded Stream session id");
    for offset in 0..CANONICAL_HISTORY_DEPTH {
        let frame = build_stream_append(session_id, offset as u64, payload);
        encoded_request(fixture, route, &frame);
    }
    encoded_request(
        fixture,
        route,
        &build_stream_commit(session_id, STREAM_SYNC_COMMIT_MODE),
    );
}

fn measure_encoded_read(
    ctx: &mut StressContext,
    storage: StorageProfile,
    measurement: &'static str,
) {
    tag_row(ctx, &read_dimensions(storage, LayerKind::Encoded));
    let fixture = encoded_fixture(storage);
    let route = format!(
        "stream://ladder-encoded-{}/orders/resource-0",
        storage.label()
    );
    let payload = vec![0x5A; CANONICAL_PAYLOAD_SIZE];
    seed_encoded(&fixture, &route, &payload);
    let read_frame = build_stream_read_with_limit(&route, 0, CANONICAL_READ_LIMIT as u64);

    measure_operations(ctx, measurement, 1, |latencies| {
        let started = Instant::now();
        let response = encoded_request(&fixture, &route, &read_frame);
        latencies.push(started.elapsed());
        let count =
            count_stream_read_records_from_payload(&response).expect("encoded Stream read count");
        assert_eq!(count, CANONICAL_READ_LIMIT);
    });
}

fn measure_encoded_write(
    ctx: &mut StressContext,
    storage: StorageProfile,
    measurement: &'static str,
) {
    tag_row(ctx, &write_dimensions(storage, LayerKind::Encoded));
    let fixture = encoded_fixture(storage);
    let route = format!("stream://ladder-encoded-{}/write/resource", storage.label());
    let begin_frame = build_stream_begin(&route);
    let payload = vec![0xC3; CANONICAL_PAYLOAD_SIZE];
    let mut append_frame = MutableAppendFrame::new(0, 0, &payload);
    let mut commit_frame = MutableCommitFrame::new(0, STREAM_SYNC_COMMIT_MODE);
    let mut next_offset = 0_u64;

    measure_operations(ctx, measurement, 1, |latencies| {
        let started = Instant::now();
        let begin_response = encoded_request(&fixture, &route, &begin_frame);
        let session_id =
            parse_stream_session_id(&begin_response).expect("encoded Stream session id");
        append_frame.set_session_id(session_id);
        append_frame.set_expected_offset(next_offset);
        encoded_request(&fixture, &route, append_frame.as_slice());
        commit_frame.set_session_id(session_id);
        encoded_request(&fixture, &route, commit_frame.as_slice());
        latencies.push(started.elapsed());
        next_offset = next_offset.saturating_add(1);
    });
}

macro_rules! characterization_row {
    ($name:ident, $ctx:ident, $body:block) => {
        #[stress(tier = 4)]
        fn $name($ctx: &mut StressContext) $body
    };
}

characterization_row!(should_characterize_memory_direct_exact_replay, ctx, {
    measure_direct_read(ctx, StorageProfile::Memory, "memory_direct_exact_replay");
});
characterization_row!(should_characterize_disk_direct_exact_replay, ctx, {
    measure_direct_read(ctx, StorageProfile::LocalDisk, "disk_direct_exact_replay");
});
characterization_row!(should_characterize_memory_encoded_exact_replay, ctx, {
    measure_encoded_read(ctx, StorageProfile::Memory, "memory_encoded_exact_replay");
});
characterization_row!(should_characterize_disk_encoded_exact_replay, ctx, {
    measure_encoded_read(ctx, StorageProfile::LocalDisk, "disk_encoded_exact_replay");
});
characterization_row!(should_characterize_disk_tcp_exact_replay_ladder, ctx, {
    measure_exact_replay(
        ctx,
        read_dimensions(StorageProfile::LocalDisk, LayerKind::Tcp),
        TransportKind::Tcp,
        "disk_tcp_exact_replay_ladder",
    );
});
characterization_row!(should_characterize_disk_ws_exact_replay_ladder, ctx, {
    measure_exact_replay(
        ctx,
        read_dimensions(StorageProfile::LocalDisk, LayerKind::WebSocket),
        TransportKind::WebSocket,
        "disk_ws_exact_replay_ladder",
    );
});
characterization_row!(should_characterize_memory_direct_sync_write, ctx, {
    measure_direct_write(
        ctx,
        StorageProfile::Memory,
        CANONICAL_PAYLOAD_SIZE,
        StreamWriteMode::Sync,
        "cost_ladder_sync_write_lifecycle",
        "memory_direct_sync_write",
    );
});
characterization_row!(should_characterize_disk_direct_sync_write, ctx, {
    measure_direct_write(
        ctx,
        StorageProfile::LocalDisk,
        CANONICAL_PAYLOAD_SIZE,
        StreamWriteMode::Sync,
        "cost_ladder_sync_write_lifecycle",
        "disk_direct_sync_write",
    );
});
characterization_row!(should_characterize_memory_encoded_sync_write, ctx, {
    measure_encoded_write(ctx, StorageProfile::Memory, "memory_encoded_sync_write");
});
characterization_row!(should_characterize_disk_encoded_sync_write, ctx, {
    measure_encoded_write(ctx, StorageProfile::LocalDisk, "disk_encoded_sync_write");
});
characterization_row!(should_characterize_memory_tcp_sync_write_ladder, ctx, {
    measure_write_lifecycle(
        ctx,
        write_dimensions(StorageProfile::Memory, LayerKind::Tcp),
        TransportKind::Tcp,
        "memory_tcp_sync_write_ladder",
    );
});
characterization_row!(should_characterize_memory_ws_sync_write_ladder, ctx, {
    measure_write_lifecycle(
        ctx,
        write_dimensions(StorageProfile::Memory, LayerKind::WebSocket),
        TransportKind::WebSocket,
        "memory_ws_sync_write_ladder",
    );
});

cntryl_stress::stress_main!();
