#[path = "tier4_queue_support.rs"]
mod tier4_queue_support;
#[path = "tier4_support.rs"]
mod tier4_support;

use crate::tier4_queue_support::{
    dimensions, measure_direct_lifecycle, measure_encoded_lifecycle, measure_transport_lifecycle,
    QueueWriteMode, CANONICAL_PAYLOAD_SIZE,
};
use crate::tier4_support::{LayerKind, StorageProfile, TransportKind};
use cntryl_stress::{stress, StressContext};

#[stress(tier = 4)]
fn should_measure_memory_direct_sync_lifecycle(ctx: &mut StressContext) {
    measure_direct_lifecycle(
        ctx,
        dimensions(
            "enqueue_reserve_ack_lifecycle",
            StorageProfile::Memory,
            LayerKind::Direct,
            "sync",
            CANONICAL_PAYLOAD_SIZE,
            1,
            "enqueue_reserve_ack",
            "queue_lifecycle",
            "regression_gate",
        ),
        QueueWriteMode::Sync,
        "memory_direct_sync_queue_lifecycle",
    );
}

#[stress(tier = 4)]
fn should_measure_local_disk_encoded_sync_lifecycle(ctx: &mut StressContext) {
    measure_encoded_lifecycle(
        ctx,
        dimensions(
            "enqueue_reserve_ack_lifecycle",
            StorageProfile::LocalDisk,
            LayerKind::Encoded,
            "sync",
            CANONICAL_PAYLOAD_SIZE,
            1,
            "enqueue_reserve_ack",
            "queue_lifecycle",
            "regression_gate",
        ),
        QueueWriteMode::Sync,
        "local_disk_encoded_sync_queue_lifecycle",
    );
}

#[stress(tier = 4)]
fn should_measure_memory_tcp_queue_lifecycle(ctx: &mut StressContext) {
    measure_transport_lifecycle(
        ctx,
        dimensions(
            "enqueue_reserve_ack_lifecycle",
            StorageProfile::Memory,
            LayerKind::Tcp,
            "best_effort",
            CANONICAL_PAYLOAD_SIZE,
            1,
            "enqueue_reserve_ack",
            "queue_lifecycle",
            "regression_gate",
        ),
        TransportKind::Tcp,
        "memory_tcp_queue_lifecycle",
    );
}

#[stress(tier = 4)]
fn should_measure_memory_ws_queue_lifecycle(ctx: &mut StressContext) {
    measure_transport_lifecycle(
        ctx,
        dimensions(
            "enqueue_reserve_ack_lifecycle",
            StorageProfile::Memory,
            LayerKind::WebSocket,
            "best_effort",
            CANONICAL_PAYLOAD_SIZE,
            1,
            "enqueue_reserve_ack",
            "queue_lifecycle",
            "regression_gate",
        ),
        TransportKind::WebSocket,
        "memory_ws_queue_lifecycle",
    );
}

cntryl_stress::stress_main!();
