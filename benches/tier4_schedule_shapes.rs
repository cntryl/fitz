#[path = "tier4_schedule_support.rs"]
mod tier4_schedule_support;
#[path = "tier4_support.rs"]
mod tier4_support;

use crate::tier4_schedule_support::{
    dimensions, measure_batch_create, measure_direct_lifecycle, measure_encoded_lifecycle,
    measure_transport_lifecycle, ScheduleWriteMode, CANONICAL_PAYLOAD_SIZE, CREATE_BATCH_WIDTH,
};
use crate::tier4_support::{LayerKind, StorageProfile, TransportKind};
use cntryl_stress::{stress, StressContext};

fn actor_dimensions(
    storage: StorageProfile,
    layer: LayerKind,
    write_mode: ScheduleWriteMode,
    payload_size: usize,
) -> crate::tier4_support::Tier4Dimensions<'static> {
    dimensions(
        "create_fire_ack_shape",
        storage,
        layer,
        write_mode.label(),
        payload_size,
        1,
        1,
        "create_fire_ack",
        "schedule_lifecycle",
        "characterization",
    )
}

#[stress(tier = 4)]
fn should_characterize_local_disk_direct_sync_lifecycle(ctx: &mut StressContext) {
    measure_direct_lifecycle(
        ctx,
        actor_dimensions(
            StorageProfile::LocalDisk,
            LayerKind::Direct,
            ScheduleWriteMode::Sync,
            CANONICAL_PAYLOAD_SIZE,
        ),
        ScheduleWriteMode::Sync,
        "local_disk_direct_sync_schedule_lifecycle",
    );
}

#[stress(tier = 4)]
fn should_characterize_memory_encoded_sync_lifecycle(ctx: &mut StressContext) {
    measure_encoded_lifecycle(
        ctx,
        actor_dimensions(
            StorageProfile::Memory,
            LayerKind::Encoded,
            ScheduleWriteMode::Sync,
            CANONICAL_PAYLOAD_SIZE,
        ),
        ScheduleWriteMode::Sync,
        "memory_encoded_sync_schedule_lifecycle",
    );
}

#[stress(tier = 4)]
fn should_characterize_local_disk_direct_buffered_lifecycle(ctx: &mut StressContext) {
    measure_direct_lifecycle(
        ctx,
        actor_dimensions(
            StorageProfile::LocalDisk,
            LayerKind::Direct,
            ScheduleWriteMode::Buffered,
            CANONICAL_PAYLOAD_SIZE,
        ),
        ScheduleWriteMode::Buffered,
        "local_disk_direct_buffered_schedule_lifecycle",
    );
}

#[stress(tier = 4)]
fn should_characterize_local_disk_encoded_buffered_lifecycle(ctx: &mut StressContext) {
    measure_encoded_lifecycle(
        ctx,
        actor_dimensions(
            StorageProfile::LocalDisk,
            LayerKind::Encoded,
            ScheduleWriteMode::Buffered,
            CANONICAL_PAYLOAD_SIZE,
        ),
        ScheduleWriteMode::Buffered,
        "local_disk_encoded_buffered_schedule_lifecycle",
    );
}

macro_rules! payload_row {
    ($name:ident, $measurement:literal, $payload_size:expr) => {
        #[stress(tier = 4)]
        fn $name(ctx: &mut StressContext) {
            measure_direct_lifecycle(
                ctx,
                actor_dimensions(
                    StorageProfile::Memory,
                    LayerKind::Direct,
                    ScheduleWriteMode::Sync,
                    $payload_size,
                ),
                ScheduleWriteMode::Sync,
                $measurement,
            );
        }
    };
}

payload_row!(
    should_characterize_memory_direct_sync_64b,
    "memory_direct_sync_schedule_lifecycle_64b",
    64
);
payload_row!(
    should_characterize_memory_direct_sync_16k,
    "memory_direct_sync_schedule_lifecycle_16k",
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
            "create_fire_ack_shape",
            StorageProfile::LocalDisk,
            LayerKind::from(transport),
            "buffered",
            CANONICAL_PAYLOAD_SIZE,
            1,
            2,
            "create_fire_ack_delivery_confirmed",
            "schedule_lifecycle",
            "characterization",
        ),
        transport,
        measurement,
    );
}

#[stress(tier = 4)]
fn should_characterize_local_disk_tcp_delivery_confirmed_lifecycle(ctx: &mut StressContext) {
    measure_local_disk_transport(
        ctx,
        TransportKind::Tcp,
        "local_disk_tcp_delivery_confirmed_schedule_lifecycle",
    );
}

#[stress(tier = 4)]
fn should_characterize_local_disk_ws_delivery_confirmed_lifecycle(ctx: &mut StressContext) {
    measure_local_disk_transport(
        ctx,
        TransportKind::WebSocket,
        "local_disk_ws_delivery_confirmed_schedule_lifecycle",
    );
}

fn measure_batch(
    ctx: &mut StressContext,
    storage: StorageProfile,
    transport: TransportKind,
    measurement: &'static str,
) {
    let write_mode = match storage {
        StorageProfile::Memory => "best_effort",
        StorageProfile::LocalDisk => "buffered",
    };
    measure_batch_create(
        ctx,
        dimensions(
            "batch_create",
            storage,
            LayerKind::from(transport),
            write_mode,
            CANONICAL_PAYLOAD_SIZE,
            CREATE_BATCH_WIDTH,
            1,
            "batch_create",
            "schedule_create",
            "characterization",
        ),
        transport,
        measurement,
    );
}

#[stress(tier = 4)]
fn should_characterize_memory_ws_batch_create(ctx: &mut StressContext) {
    measure_batch(
        ctx,
        StorageProfile::Memory,
        TransportKind::WebSocket,
        "memory_ws_batch_schedule_create_32",
    );
}

#[stress(tier = 4)]
fn should_characterize_local_disk_tcp_batch_create(ctx: &mut StressContext) {
    measure_batch(
        ctx,
        StorageProfile::LocalDisk,
        TransportKind::Tcp,
        "local_disk_tcp_batch_schedule_create_32",
    );
}

cntryl_stress::stress_main!();
