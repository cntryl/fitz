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
    measure_concurrent_exact_replay, measure_concurrent_write_lifecycle,
};
use cntryl_stress::{stress, StressContext};

fn read_dimensions(
    storage: StorageProfile,
    transport: TransportKind,
    client_count: usize,
) -> RowDimensions<'static> {
    RowDimensions {
        scenario: "concurrency_exact_replay",
        storage_profile: storage,
        layer: LayerKind::from(transport),
        write_mode: "not_applicable",
        write_operation: "none",
        payload_size: CANONICAL_PAYLOAD_SIZE,
        history_depth: CANONICAL_HISTORY_DEPTH,
        read_limit: CANONICAL_READ_LIMIT,
        read_scope: ReadScope::Resource,
        route_count: 1,
        filter_match_count: "unfiltered",
        client_count,
        workload_mix: "read_only",
        completed_unit: "read_request",
        gate_class: "characterization",
    }
}

fn write_dimensions(transport: TransportKind, client_count: usize) -> RowDimensions<'static> {
    RowDimensions {
        scenario: "concurrency_sync_write_lifecycle",
        storage_profile: StorageProfile::LocalDisk,
        layer: LayerKind::from(transport),
        write_mode: "sync",
        write_operation: "begin_append_commit",
        payload_size: CANONICAL_PAYLOAD_SIZE,
        history_depth: 0,
        read_limit: 0,
        read_scope: ReadScope::None,
        route_count: client_count,
        filter_match_count: "not_filtered",
        client_count,
        workload_mix: "write_only",
        completed_unit: "write_lifecycle",
        gate_class: "characterization",
    }
}

macro_rules! read_row {
    ($name:ident, $measurement:literal, $storage:expr, $transport:expr, $clients:expr) => {
        #[stress(tier = 4)]
        fn $name(ctx: &mut StressContext) {
            measure_concurrent_exact_replay(
                ctx,
                read_dimensions($storage, $transport, $clients),
                $transport,
                $measurement,
            );
        }
    };
}

macro_rules! write_row {
    ($name:ident, $measurement:literal, $transport:expr, $clients:expr) => {
        #[stress(tier = 4)]
        fn $name(ctx: &mut StressContext) {
            measure_concurrent_write_lifecycle(
                ctx,
                write_dimensions($transport, $clients),
                $transport,
                $measurement,
            );
        }
    };
}

macro_rules! read_sweep {
    ($storage:expr, $transport:expr, $(($name:ident, $measurement:literal, $clients:expr)),+ $(,)?) => {
        $(read_row!($name, $measurement, $storage, $transport, $clients);)+
    };
}

read_sweep!(
    StorageProfile::Memory,
    TransportKind::Tcp,
    (
        should_characterize_memory_tcp_exact_replay_1_client,
        "memory_tcp_exact_replay_1_client",
        1
    ),
    (
        should_characterize_memory_tcp_exact_replay_2_clients,
        "memory_tcp_exact_replay_2_clients",
        2
    ),
    (
        should_characterize_memory_tcp_exact_replay_4_clients,
        "memory_tcp_exact_replay_4_clients",
        4
    ),
    (
        should_characterize_memory_tcp_exact_replay_8_clients,
        "memory_tcp_exact_replay_8_clients",
        8
    ),
    (
        should_characterize_memory_tcp_exact_replay_16_clients,
        "memory_tcp_exact_replay_16_clients",
        16
    ),
    (
        should_characterize_memory_tcp_exact_replay_32_clients,
        "memory_tcp_exact_replay_32_clients",
        32
    ),
);
read_sweep!(
    StorageProfile::Memory,
    TransportKind::WebSocket,
    (
        should_characterize_memory_ws_exact_replay_1_client,
        "memory_ws_exact_replay_1_client",
        1
    ),
    (
        should_characterize_memory_ws_exact_replay_2_clients,
        "memory_ws_exact_replay_2_clients",
        2
    ),
    (
        should_characterize_memory_ws_exact_replay_4_clients,
        "memory_ws_exact_replay_4_clients",
        4
    ),
    (
        should_characterize_memory_ws_exact_replay_8_clients,
        "memory_ws_exact_replay_8_clients",
        8
    ),
    (
        should_characterize_memory_ws_exact_replay_16_clients,
        "memory_ws_exact_replay_16_clients",
        16
    ),
    (
        should_characterize_memory_ws_exact_replay_32_clients,
        "memory_ws_exact_replay_32_clients",
        32
    ),
);
read_sweep!(
    StorageProfile::LocalDisk,
    TransportKind::Tcp,
    (
        should_characterize_disk_tcp_exact_replay_1_client,
        "disk_tcp_exact_replay_1_client",
        1
    ),
    (
        should_characterize_disk_tcp_exact_replay_2_clients,
        "disk_tcp_exact_replay_2_clients",
        2
    ),
    (
        should_characterize_disk_tcp_exact_replay_4_clients,
        "disk_tcp_exact_replay_4_clients",
        4
    ),
    (
        should_characterize_disk_tcp_exact_replay_8_clients,
        "disk_tcp_exact_replay_8_clients",
        8
    ),
    (
        should_characterize_disk_tcp_exact_replay_16_clients,
        "disk_tcp_exact_replay_16_clients",
        16
    ),
    (
        should_characterize_disk_tcp_exact_replay_32_clients,
        "disk_tcp_exact_replay_32_clients",
        32
    ),
);
read_sweep!(
    StorageProfile::LocalDisk,
    TransportKind::WebSocket,
    (
        should_characterize_disk_ws_exact_replay_1_client,
        "disk_ws_exact_replay_1_client",
        1
    ),
    (
        should_characterize_disk_ws_exact_replay_2_clients,
        "disk_ws_exact_replay_2_clients",
        2
    ),
    (
        should_characterize_disk_ws_exact_replay_4_clients,
        "disk_ws_exact_replay_4_clients",
        4
    ),
    (
        should_characterize_disk_ws_exact_replay_8_clients,
        "disk_ws_exact_replay_8_clients",
        8
    ),
    (
        should_characterize_disk_ws_exact_replay_16_clients,
        "disk_ws_exact_replay_16_clients",
        16
    ),
    (
        should_characterize_disk_ws_exact_replay_32_clients,
        "disk_ws_exact_replay_32_clients",
        32
    ),
);

write_row!(
    should_characterize_disk_tcp_sync_write_1_client,
    "disk_tcp_sync_write_1_client",
    TransportKind::Tcp,
    1
);
write_row!(
    should_characterize_disk_tcp_sync_write_2_clients,
    "disk_tcp_sync_write_2_clients",
    TransportKind::Tcp,
    2
);
write_row!(
    should_characterize_disk_tcp_sync_write_4_clients,
    "disk_tcp_sync_write_4_clients",
    TransportKind::Tcp,
    4
);
write_row!(
    should_characterize_disk_tcp_sync_write_8_clients,
    "disk_tcp_sync_write_8_clients",
    TransportKind::Tcp,
    8
);
write_row!(
    should_characterize_disk_tcp_sync_write_16_clients,
    "disk_tcp_sync_write_16_clients",
    TransportKind::Tcp,
    16
);
write_row!(
    should_characterize_disk_tcp_sync_write_32_clients,
    "disk_tcp_sync_write_32_clients",
    TransportKind::Tcp,
    32
);
write_row!(
    should_characterize_disk_ws_sync_write_1_client,
    "disk_ws_sync_write_1_client",
    TransportKind::WebSocket,
    1
);
write_row!(
    should_characterize_disk_ws_sync_write_2_clients,
    "disk_ws_sync_write_2_clients",
    TransportKind::WebSocket,
    2
);
write_row!(
    should_characterize_disk_ws_sync_write_4_clients,
    "disk_ws_sync_write_4_clients",
    TransportKind::WebSocket,
    4
);
write_row!(
    should_characterize_disk_ws_sync_write_8_clients,
    "disk_ws_sync_write_8_clients",
    TransportKind::WebSocket,
    8
);
write_row!(
    should_characterize_disk_ws_sync_write_16_clients,
    "disk_ws_sync_write_16_clients",
    TransportKind::WebSocket,
    16
);
write_row!(
    should_characterize_disk_ws_sync_write_32_clients,
    "disk_ws_sync_write_32_clients",
    TransportKind::WebSocket,
    32
);

cntryl_stress::stress_main!();
