#[path = "tier4_queue_support.rs"]
mod tier4_queue_support;
#[path = "tier4_support.rs"]
mod tier4_support;

use crate::tier4_queue_support::{
    dimensions, measure_direct_lifecycle, measure_encoded_lifecycle, measure_transport_lifecycle,
    CANONICAL_PAYLOAD_SIZE,
};
use crate::tier4_support::{LayerKind, StorageProfile, TransportKind};
use cntryl_stress::{stress, StressContext};

fn direct_dimensions(
    storage: StorageProfile,
    layer: LayerKind,
    payload_size: usize,
) -> crate::tier4_support::Tier4Dimensions<'static> {
    dimensions(
        "enqueue_reserve_ack_shape",
        storage,
        layer,
        "best_effort",
        payload_size,
        1,
        "enqueue_reserve_ack",
        "queue_lifecycle",
        "characterization",
    )
}

#[stress(tier = 4)]
fn should_characterize_local_disk_direct_best_effort_lifecycle(ctx: &mut StressContext) {
    measure_direct_lifecycle(
        ctx,
        direct_dimensions(
            StorageProfile::LocalDisk,
            LayerKind::Direct,
            CANONICAL_PAYLOAD_SIZE,
        ),
        "local_disk_direct_best_effort_queue_lifecycle",
    );
}

#[stress(tier = 4)]
fn should_characterize_memory_encoded_best_effort_lifecycle(ctx: &mut StressContext) {
    measure_encoded_lifecycle(
        ctx,
        direct_dimensions(
            StorageProfile::Memory,
            LayerKind::Encoded,
            CANONICAL_PAYLOAD_SIZE,
        ),
        "memory_encoded_best_effort_queue_lifecycle",
    );
}

#[stress(tier = 4)]
fn should_characterize_local_disk_encoded_best_effort_lifecycle(ctx: &mut StressContext) {
    measure_encoded_lifecycle(
        ctx,
        direct_dimensions(
            StorageProfile::LocalDisk,
            LayerKind::Encoded,
            CANONICAL_PAYLOAD_SIZE,
        ),
        "local_disk_encoded_best_effort_queue_lifecycle",
    );
}

macro_rules! payload_row {
    ($name:ident, $measurement:literal, $payload_size:expr) => {
        #[stress(tier = 4)]
        fn $name(ctx: &mut StressContext) {
            measure_direct_lifecycle(
                ctx,
                direct_dimensions(StorageProfile::Memory, LayerKind::Direct, $payload_size),
                $measurement,
            );
        }
    };
}

payload_row!(
    should_characterize_memory_direct_best_effort_64b,
    "memory_direct_best_effort_queue_lifecycle_64b",
    64
);
payload_row!(
    should_characterize_memory_direct_best_effort_16k,
    "memory_direct_best_effort_queue_lifecycle_16k",
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
