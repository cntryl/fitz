#[path = "tier4_stream_support.rs"]
mod tier4_stream_support;
#[path = "tier4_stream_transport.rs"]
mod tier4_stream_transport;
#[path = "tier4_support.rs"]
mod tier4_support;

use crate::tier4_stream_support::{
    LayerKind, ReadScope, RowDimensions, StorageProfile, TransportKind, CANONICAL_HISTORY_DEPTH,
    CANONICAL_PAYLOAD_SIZE, CANONICAL_READ_LIMIT,
};
use crate::tier4_stream_transport::{
    measure_append_open_session, measure_exact_replay, measure_write_lifecycle,
};
use cntryl_stress::{stress, StressContext};

fn append_dimensions(transport: TransportKind) -> RowDimensions<'static> {
    RowDimensions {
        scenario: "append_open_session",
        storage_profile: StorageProfile::Memory,
        layer: LayerKind::from(transport),
        write_mode: "not_committed",
        write_operation: "append_open_session",
        payload_size: CANONICAL_PAYLOAD_SIZE,
        history_depth: 0,
        read_limit: 0,
        read_scope: ReadScope::None,
        route_count: 1,
        filter_match_count: "not_filtered",
        client_count: 1,
        workload_mix: "write_only",
        completed_unit: "append",
        gate_class: "regression_gate",
    }
}

fn write_dimensions(
    storage_profile: StorageProfile,
    transport: TransportKind,
) -> RowDimensions<'static> {
    RowDimensions {
        scenario: "sync_write_lifecycle",
        storage_profile,
        layer: LayerKind::from(transport),
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
        gate_class: if storage_profile == StorageProfile::Memory {
            "regression_gate"
        } else {
            "storage_characterization"
        },
    }
}

fn read_dimensions(transport: TransportKind) -> RowDimensions<'static> {
    RowDimensions {
        scenario: "exact_replay",
        storage_profile: StorageProfile::Memory,
        layer: LayerKind::from(transport),
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
        gate_class: "regression_gate",
    }
}

#[stress(tier = 4)]
fn should_measure_memory_tcp_append_open_session(ctx: &mut StressContext) {
    measure_append_open_session(
        ctx,
        append_dimensions(TransportKind::Tcp),
        TransportKind::Tcp,
        "memory_tcp_append_open_session",
    );
}

#[stress(tier = 4)]
fn should_measure_memory_ws_append_open_session(ctx: &mut StressContext) {
    measure_append_open_session(
        ctx,
        append_dimensions(TransportKind::WebSocket),
        TransportKind::WebSocket,
        "memory_ws_append_open_session",
    );
}

#[stress(tier = 4)]
fn should_measure_local_disk_tcp_sync_write_lifecycle(ctx: &mut StressContext) {
    measure_write_lifecycle(
        ctx,
        write_dimensions(StorageProfile::LocalDisk, TransportKind::Tcp),
        TransportKind::Tcp,
        "local_disk_tcp_sync_write_lifecycle",
    );
}

#[stress(tier = 4)]
fn should_measure_local_disk_ws_sync_write_lifecycle(ctx: &mut StressContext) {
    measure_write_lifecycle(
        ctx,
        write_dimensions(StorageProfile::LocalDisk, TransportKind::WebSocket),
        TransportKind::WebSocket,
        "local_disk_ws_sync_write_lifecycle",
    );
}

#[stress(tier = 4)]
fn should_measure_memory_tcp_sync_write_lifecycle(ctx: &mut StressContext) {
    measure_write_lifecycle(
        ctx,
        write_dimensions(StorageProfile::Memory, TransportKind::Tcp),
        TransportKind::Tcp,
        "memory_tcp_sync_write_lifecycle",
    );
}

#[stress(tier = 4)]
fn should_measure_memory_ws_sync_write_lifecycle(ctx: &mut StressContext) {
    measure_write_lifecycle(
        ctx,
        write_dimensions(StorageProfile::Memory, TransportKind::WebSocket),
        TransportKind::WebSocket,
        "memory_ws_sync_write_lifecycle",
    );
}

#[stress(tier = 4)]
fn should_measure_memory_tcp_exact_replay(ctx: &mut StressContext) {
    measure_exact_replay(
        ctx,
        read_dimensions(TransportKind::Tcp),
        TransportKind::Tcp,
        "memory_tcp_exact_replay",
    );
}

#[stress(tier = 4)]
fn should_measure_memory_ws_exact_replay(ctx: &mut StressContext) {
    measure_exact_replay(
        ctx,
        read_dimensions(TransportKind::WebSocket),
        TransportKind::WebSocket,
        "memory_ws_exact_replay",
    );
}

cntryl_stress::stress_main!();
