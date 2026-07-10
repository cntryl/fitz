#[path = "tier4_schedule_support.rs"]
mod tier4_schedule_support;
#[path = "tier4_support.rs"]
mod tier4_support;

use crate::tier4_schedule_support::{
    dimensions, measure_concurrent_creates, CANONICAL_PAYLOAD_SIZE,
};
use crate::tier4_support::{LayerKind, StorageProfile, TransportKind};
use cntryl_stress::{stress, StressContext};

const CANONICAL_CLIENT_COUNT: usize = 8;

fn measure_concurrency(
    ctx: &mut StressContext,
    storage: StorageProfile,
    transport: TransportKind,
    measurement: &'static str,
) {
    let layer = match transport {
        TransportKind::Tcp => LayerKind::TcpMultiClient,
        TransportKind::WebSocket => LayerKind::WebSocketMultiClient,
    };
    let write_mode = match storage {
        StorageProfile::Memory => "best_effort",
        StorageProfile::LocalDisk => "buffered",
    };
    measure_concurrent_creates(
        ctx,
        dimensions(
            "concurrent_schedule_create",
            storage,
            layer,
            write_mode,
            CANONICAL_PAYLOAD_SIZE,
            CANONICAL_CLIENT_COUNT,
            CANONICAL_CLIENT_COUNT,
            "concurrent_create",
            "schedule_create",
            "characterization",
        ),
        transport,
        measurement,
    );
}

#[stress(tier = 4)]
fn should_characterize_memory_tcp_concurrent_creates(ctx: &mut StressContext) {
    measure_concurrency(
        ctx,
        StorageProfile::Memory,
        TransportKind::Tcp,
        "memory_tcp_8_client_concurrent_schedule_create",
    );
}

#[stress(tier = 4)]
fn should_characterize_memory_ws_concurrent_creates(ctx: &mut StressContext) {
    measure_concurrency(
        ctx,
        StorageProfile::Memory,
        TransportKind::WebSocket,
        "memory_ws_8_client_concurrent_schedule_create",
    );
}

#[stress(tier = 4)]
fn should_characterize_local_disk_tcp_concurrent_creates(ctx: &mut StressContext) {
    measure_concurrency(
        ctx,
        StorageProfile::LocalDisk,
        TransportKind::Tcp,
        "local_disk_tcp_8_client_concurrent_schedule_create",
    );
}

#[stress(tier = 4)]
fn should_characterize_local_disk_ws_concurrent_creates(ctx: &mut StressContext) {
    measure_concurrency(
        ctx,
        StorageProfile::LocalDisk,
        TransportKind::WebSocket,
        "local_disk_ws_8_client_concurrent_schedule_create",
    );
}

cntryl_stress::stress_main!();
