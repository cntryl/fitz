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

fn direct_dimensions(
    storage: StorageProfile,
    layer: LayerKind,
    write_mode: QueueWriteMode,
    payload_size: usize,
) -> crate::tier4_support::Tier4Dimensions<'static> {
    dimensions(
        "enqueue_reserve_ack_shape",
        storage,
        layer,
        write_mode.label(),
        payload_size,
        1,
        "enqueue_reserve_ack",
        "queue_lifecycle",
        "characterization",
    )
}

#[stress(tier = 4)]
fn should_characterize_local_disk_direct_sync_lifecycle(ctx: &mut StressContext) {
    measure_direct_lifecycle(
        ctx,
        direct_dimensions(
            StorageProfile::LocalDisk,
            LayerKind::Direct,
            QueueWriteMode::Sync,
            CANONICAL_PAYLOAD_SIZE,
        ),
        QueueWriteMode::Sync,
        "local_disk_direct_sync_queue_lifecycle",
    );
}

#[stress(tier = 4)]
fn should_characterize_memory_encoded_sync_lifecycle(ctx: &mut StressContext) {
    measure_encoded_lifecycle(
        ctx,
        direct_dimensions(
            StorageProfile::Memory,
            LayerKind::Encoded,
            QueueWriteMode::Sync,
            CANONICAL_PAYLOAD_SIZE,
        ),
        QueueWriteMode::Sync,
        "memory_encoded_sync_queue_lifecycle",
    );
}

#[stress(tier = 4)]
fn should_characterize_local_disk_direct_buffered_lifecycle(ctx: &mut StressContext) {
    measure_direct_lifecycle(
        ctx,
        direct_dimensions(
            StorageProfile::LocalDisk,
            LayerKind::Direct,
            QueueWriteMode::Buffered,
            CANONICAL_PAYLOAD_SIZE,
        ),
        QueueWriteMode::Buffered,
        "local_disk_direct_buffered_queue_lifecycle",
    );
}

#[stress(tier = 4)]
fn should_characterize_local_disk_encoded_buffered_lifecycle(ctx: &mut StressContext) {
    measure_encoded_lifecycle(
        ctx,
        direct_dimensions(
            StorageProfile::LocalDisk,
            LayerKind::Encoded,
            QueueWriteMode::Buffered,
            CANONICAL_PAYLOAD_SIZE,
        ),
        QueueWriteMode::Buffered,
        "local_disk_encoded_buffered_queue_lifecycle",
    );
}

macro_rules! payload_row {
    ($name:ident, $measurement:literal, $payload_size:expr) => {
        #[stress(tier = 4)]
        fn $name(ctx: &mut StressContext) {
            measure_direct_lifecycle(
                ctx,
                direct_dimensions(
                    StorageProfile::Memory,
                    LayerKind::Direct,
                    QueueWriteMode::Sync,
                    $payload_size,
                ),
                QueueWriteMode::Sync,
                $measurement,
            );
        }
    };
}

payload_row!(
    should_characterize_memory_direct_sync_64b,
    "memory_direct_sync_queue_lifecycle_64b",
    64
);
payload_row!(
    should_characterize_memory_direct_sync_16k,
    "memory_direct_sync_queue_lifecycle_16k",
    16 * 1_024
);

fn measure_local_disk_transport(
    ctx: &mut StressContext,
    transport: TransportKind,
    measurement: &'static str,
) {
    measure_transport_lifecycle(
        ctx,
        dimensions(
            "enqueue_reserve_ack_shape",
            StorageProfile::LocalDisk,
            LayerKind::from(transport),
            "best_effort",
            CANONICAL_PAYLOAD_SIZE,
            1,
            "enqueue_reserve_ack",
            "queue_lifecycle",
            "characterization",
        ),
        transport,
        measurement,
    );
}

#[stress(tier = 4)]
fn should_characterize_local_disk_tcp_queue_lifecycle(ctx: &mut StressContext) {
    measure_local_disk_transport(ctx, TransportKind::Tcp, "local_disk_tcp_queue_lifecycle");
}

#[stress(tier = 4)]
fn should_characterize_local_disk_ws_queue_lifecycle(ctx: &mut StressContext) {
    measure_local_disk_transport(
        ctx,
        TransportKind::WebSocket,
        "local_disk_ws_queue_lifecycle",
    );
}

cntryl_stress::stress_main!();
