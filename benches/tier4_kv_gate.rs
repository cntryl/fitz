#[path = "tier4_kv_support.rs"]
mod tier4_kv_support;
#[path = "tier4_support.rs"]
mod tier4_support;
use crate::tier4_kv_support::{measure_direct, measure_encoded, measure_transport};
use crate::tier4_support::{StorageProfile, TransportKind};
use cntryl_stress::{stress, StressContext};

#[stress(tier = 4)]
fn should_measure_memory_direct_sync_commit(ctx: &mut StressContext) {
    measure_direct(
        ctx,
        StorageProfile::Memory,
        true,
        "memory_direct_sync_commit",
    );
}
#[stress(tier = 4)]
fn should_measure_memory_encoded_sync_commit(ctx: &mut StressContext) {
    measure_encoded(
        ctx,
        StorageProfile::Memory,
        true,
        "memory_encoded_sync_commit",
    );
}
#[stress(tier = 4)]
fn should_measure_local_disk_tcp_sync_commit(ctx: &mut StressContext) {
    measure_transport(
        ctx,
        StorageProfile::LocalDisk,
        TransportKind::Tcp,
        "local_disk_tcp_sync_commit",
    );
}
#[stress(tier = 4)]
fn should_measure_local_disk_websocket_sync_commit(ctx: &mut StressContext) {
    measure_transport(
        ctx,
        StorageProfile::LocalDisk,
        TransportKind::WebSocket,
        "local_disk_websocket_sync_commit",
    );
}
#[stress(tier = 4)]
fn should_measure_memory_tcp_sync_commit(ctx: &mut StressContext) {
    measure_transport(
        ctx,
        StorageProfile::Memory,
        TransportKind::Tcp,
        "memory_tcp_sync_commit",
    );
}
#[stress(tier = 4)]
fn should_measure_memory_websocket_sync_commit(ctx: &mut StressContext) {
    measure_transport(
        ctx,
        StorageProfile::Memory,
        TransportKind::WebSocket,
        "memory_websocket_sync_commit",
    );
}

cntryl_stress::stress_main!();
