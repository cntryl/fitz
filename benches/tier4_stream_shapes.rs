#![allow(dead_code)] // This target selects focused subsets of shared Stream helpers.

#[path = "tier4_stream_direct.rs"]
mod tier4_stream_direct;
#[path = "tier4_stream_support.rs"]
mod tier4_stream_support;
#[path = "tier4_stream_transport.rs"]
mod tier4_stream_transport;
#[path = "tier4_support.rs"]
mod tier4_support;

use crate::tier4_stream_direct::{
    measure_compaction_replay, measure_direct_write, measure_direct_write_after_history,
    measure_filtered_locator_read, measure_memtable_rotation_write, measure_ttl_churn,
};
use crate::tier4_stream_support::{
    measure_operations, tag_row, LayerKind, ReadScope, RowDimensions, StorageProfile,
    TransportKind, CANONICAL_PAYLOAD_SIZE, STREAM_SYNC_COMMIT_MODE,
};
use crate::tier4_stream_transport::{
    delivery_confirmed_commit, read_pages, request_read_pages, seed_scope, subscribe,
    with_transport_client, with_transport_clients, WriteLifecycleState,
};
use cntryl_stress::{stress, StressContext};
use fitz::domains::stream::protocol::StreamWriteMode;
use std::time::Instant;

fn read_dimensions(
    scenario: &'static str,
    scope: ReadScope,
    history_depth: usize,
    read_limit: usize,
    route_count: usize,
) -> RowDimensions<'static> {
    RowDimensions {
        scenario,
        storage_profile: StorageProfile::LocalDisk,
        layer: LayerKind::Tcp,
        write_mode: "not_applicable",
        write_operation: "none",
        payload_size: CANONICAL_PAYLOAD_SIZE,
        history_depth,
        read_limit,
        read_scope: scope,
        route_count,
        filter_match_count: "unfiltered",
        client_count: 1,
        workload_mix: "read_only",
        completed_unit: "read_request",
        gate_class: "characterization",
    }
}

fn measure_read_shape(
    ctx: &mut StressContext,
    dimensions: RowDimensions<'static>,
    measurement: &'static str,
) {
    tag_row(ctx, &dimensions);
    with_transport_client(
        StorageProfile::LocalDisk,
        TransportKind::Tcp,
        |runtime, _server, client| {
            let realm = format!(
                "tier4-shape-{}-{}-{}",
                dimensions.read_scope.label(),
                dimensions.history_depth,
                dimensions.route_count
            );
            let payload = vec![0x5A; dimensions.payload_size];
            runtime.block_on(seed_scope(
                client,
                &realm,
                dimensions.read_scope,
                dimensions.history_depth,
                dimensions.route_count,
                &payload,
            ));
            let expected = dimensions.read_limit.min(dimensions.history_depth);
            let pages = read_pages(&dimensions.read_scope.route(&realm), expected);
            ctx.parameter(
                "wire_page_limit",
                crate::tier4_stream_support::WIRE_READ_PAGE_LIMIT,
            );
            ctx.parameter("wire_page_count", pages.len());
            runtime.block_on(request_read_pages(client, &pages));

            measure_operations(ctx, measurement, 1, |latencies| {
                let started = Instant::now();
                runtime.block_on(request_read_pages(client, &pages));
                latencies.push(started.elapsed());
            });
        },
    );
}

macro_rules! history_row {
    ($name:ident, $measurement:literal, $scope:expr, $depth:expr, $routes:expr) => {
        #[stress(tier = 4)]
        fn $name(ctx: &mut StressContext) {
            measure_read_shape(
                ctx,
                read_dimensions("history_depth_sweep", $scope, $depth, 100, $routes),
                $measurement,
            );
        }
    };
}

history_row!(
    should_characterize_exact_read_depth_100,
    "exact_read_depth_100",
    ReadScope::Resource,
    100,
    1
);
history_row!(
    should_characterize_exact_read_depth_1000,
    "exact_read_depth_1000",
    ReadScope::Resource,
    1_000,
    1
);
history_row!(
    should_characterize_exact_read_depth_10000,
    "exact_read_depth_10000",
    ReadScope::Resource,
    10_000,
    1
);
history_row!(
    should_characterize_area_read_depth_100,
    "area_read_depth_100",
    ReadScope::Area,
    100,
    4
);
history_row!(
    should_characterize_area_read_depth_1000,
    "area_read_depth_1000",
    ReadScope::Area,
    1_000,
    4
);
history_row!(
    should_characterize_area_read_depth_10000,
    "area_read_depth_10000",
    ReadScope::Area,
    10_000,
    4
);
history_row!(
    should_characterize_realm_read_depth_100,
    "realm_read_depth_100",
    ReadScope::Realm,
    100,
    4
);
history_row!(
    should_characterize_realm_read_depth_1000,
    "realm_read_depth_1000",
    ReadScope::Realm,
    1_000,
    4
);
history_row!(
    should_characterize_realm_read_depth_10000,
    "realm_read_depth_10000",
    ReadScope::Realm,
    10_000,
    4
);

macro_rules! route_row {
    ($name:ident, $measurement:literal, $scope:expr, $routes:expr) => {
        #[stress(tier = 4)]
        fn $name(ctx: &mut StressContext) {
            measure_read_shape(
                ctx,
                read_dimensions("route_count_sweep", $scope, 64, 64, $routes),
                $measurement,
            );
        }
    };
}

route_row!(
    should_characterize_area_read_1_route,
    "area_read_1_route",
    ReadScope::Area,
    1
);
route_row!(
    should_characterize_area_read_4_routes,
    "area_read_4_routes",
    ReadScope::Area,
    4
);
route_row!(
    should_characterize_area_read_16_routes,
    "area_read_16_routes",
    ReadScope::Area,
    16
);
route_row!(
    should_characterize_area_read_64_routes,
    "area_read_64_routes",
    ReadScope::Area,
    64
);
route_row!(
    should_characterize_realm_read_1_route,
    "realm_read_1_route",
    ReadScope::Realm,
    1
);
route_row!(
    should_characterize_realm_read_4_routes,
    "realm_read_4_routes",
    ReadScope::Realm,
    4
);
route_row!(
    should_characterize_realm_read_16_routes,
    "realm_read_16_routes",
    ReadScope::Realm,
    16
);
route_row!(
    should_characterize_realm_read_64_routes,
    "realm_read_64_routes",
    ReadScope::Realm,
    64
);

macro_rules! payload_row {
    ($name:ident, $measurement:literal, $storage:expr, $size:expr, $mode:expr, $scenario:literal) => {
        #[stress(tier = 4)]
        fn $name(ctx: &mut StressContext) {
            measure_direct_write(ctx, $storage, $size, $mode, $scenario, $measurement);
        }
    };
}

payload_row!(
    should_characterize_memory_append_64b,
    "memory_append_64b",
    StorageProfile::Memory,
    64,
    StreamWriteMode::Sync,
    "payload_size_memory_write"
);

#[stress(tier = 4)]
fn should_characterize_hot_resource_append_after_100000_events(ctx: &mut StressContext) {
    measure_direct_write_after_history(
        ctx,
        StorageProfile::LocalDisk,
        64,
        StreamWriteMode::Sync,
        "hot_resource_append",
        "hot_resource_append_depth_100000",
        100_000,
    );
}

#[stress(tier = 4)]
fn should_characterize_repeated_memtable_rotation(ctx: &mut StressContext) {
    measure_memtable_rotation_write(ctx);
}

#[stress(tier = 4)]
fn should_characterize_ttl_churn(ctx: &mut StressContext) {
    measure_ttl_churn(ctx);
}

#[stress(tier = 4)]
fn should_characterize_sparse_locator_read(ctx: &mut StressContext) {
    measure_filtered_locator_read(ctx, true);
}

#[stress(tier = 4)]
fn should_characterize_dense_locator_read(ctx: &mut StressContext) {
    measure_filtered_locator_read(ctx, false);
}

#[stress(tier = 4)]
fn should_characterize_replay_before_compaction(ctx: &mut StressContext) {
    measure_compaction_replay(ctx, false);
}

#[stress(tier = 4)]
fn should_characterize_replay_after_compaction(ctx: &mut StressContext) {
    measure_compaction_replay(ctx, true);
}
payload_row!(
    should_characterize_memory_append_1k,
    "memory_append_1k",
    StorageProfile::Memory,
    1_024,
    StreamWriteMode::Sync,
    "payload_size_memory_write"
);
payload_row!(
    should_characterize_memory_append_15k,
    "memory_append_15k",
    StorageProfile::Memory,
    15 * 1_024,
    StreamWriteMode::Sync,
    "payload_size_memory_write"
);
payload_row!(
    should_characterize_memory_append_16k,
    "memory_append_16k",
    StorageProfile::Memory,
    16 * 1_024,
    StreamWriteMode::Sync,
    "payload_size_memory_write"
);
payload_row!(
    should_characterize_memory_append_17k,
    "memory_append_17k",
    StorageProfile::Memory,
    17 * 1_024,
    StreamWriteMode::Sync,
    "payload_size_memory_write"
);
payload_row!(
    should_characterize_memory_append_256k,
    "memory_append_256k",
    StorageProfile::Memory,
    256 * 1_024,
    StreamWriteMode::Sync,
    "payload_size_memory_write"
);
payload_row!(
    should_characterize_disk_sync_write_64b,
    "disk_sync_write_64b",
    StorageProfile::LocalDisk,
    64,
    StreamWriteMode::Sync,
    "payload_size_sync_write"
);
payload_row!(
    should_characterize_disk_sync_write_1k,
    "disk_sync_write_1k",
    StorageProfile::LocalDisk,
    1_024,
    StreamWriteMode::Sync,
    "payload_size_sync_write"
);
payload_row!(
    should_characterize_disk_sync_write_15k,
    "disk_sync_write_15k",
    StorageProfile::LocalDisk,
    15 * 1_024,
    StreamWriteMode::Sync,
    "payload_size_sync_write"
);
payload_row!(
    should_characterize_disk_sync_write_16k,
    "disk_sync_write_16k",
    StorageProfile::LocalDisk,
    16 * 1_024,
    StreamWriteMode::Sync,
    "payload_size_sync_write"
);
payload_row!(
    should_characterize_disk_sync_write_17k,
    "disk_sync_write_17k",
    StorageProfile::LocalDisk,
    17 * 1_024,
    StreamWriteMode::Sync,
    "payload_size_sync_write"
);
payload_row!(
    should_characterize_disk_sync_write_256k,
    "disk_sync_write_256k",
    StorageProfile::LocalDisk,
    256 * 1_024,
    StreamWriteMode::Sync,
    "payload_size_sync_write"
);

payload_row!(
    should_characterize_disk_buffered_commit,
    "disk_buffered_commit",
    StorageProfile::LocalDisk,
    CANONICAL_PAYLOAD_SIZE,
    StreamWriteMode::Buffered,
    "commit_mode_comparison"
);
payload_row!(
    should_characterize_disk_sync_commit,
    "disk_sync_commit",
    StorageProfile::LocalDisk,
    CANONICAL_PAYLOAD_SIZE,
    StreamWriteMode::Sync,
    "commit_mode_comparison"
);

fn delivery_dimensions(transport: TransportKind) -> RowDimensions<'static> {
    RowDimensions {
        scenario: "delivery_confirmed_subscribe_notify",
        storage_profile: StorageProfile::LocalDisk,
        layer: LayerKind::from(transport),
        write_mode: "sync",
        write_operation: "begin_append_commit",
        payload_size: CANONICAL_PAYLOAD_SIZE,
        history_depth: 0,
        read_limit: 0,
        read_scope: ReadScope::None,
        route_count: 1,
        filter_match_count: "not_filtered",
        client_count: 2,
        workload_mix: "write_and_delivery",
        completed_unit: "confirmed_delivery",
        gate_class: "characterization",
    }
}

fn measure_delivery_confirmed(
    ctx: &mut StressContext,
    transport: TransportKind,
    measurement: &'static str,
) {
    tag_row(ctx, &delivery_dimensions(transport));
    with_transport_clients(
        StorageProfile::LocalDisk,
        transport,
        2,
        |runtime, _server, clients| {
            let (subscriber_slice, writer_slice) = clients.split_at_mut(1);
            let subscriber = &mut subscriber_slice[0];
            let writer = &mut writer_slice[0];
            let route = format!("stream://tier4-delivery/{}/resource", transport.label());
            runtime.block_on(subscribe(subscriber, &route));
            let payload = vec![0xD4; CANONICAL_PAYLOAD_SIZE];
            let mut lifecycle = WriteLifecycleState::new(&route, &payload);

            measure_operations(ctx, measurement, 1, |latencies| {
                let started = Instant::now();
                runtime.block_on(delivery_confirmed_commit(
                    writer,
                    subscriber,
                    &mut lifecycle,
                    &route,
                ));
                latencies.push(started.elapsed());
            });
        },
    );
}

#[stress(tier = 4)]
fn should_characterize_tcp_delivery_confirmed_notify(ctx: &mut StressContext) {
    measure_delivery_confirmed(ctx, TransportKind::Tcp, "tcp_delivery_confirmed_notify");
}

#[stress(tier = 4)]
fn should_characterize_ws_delivery_confirmed_notify(ctx: &mut StressContext) {
    measure_delivery_confirmed(
        ctx,
        TransportKind::WebSocket,
        "ws_delivery_confirmed_notify",
    );
}

const _: u8 = STREAM_SYNC_COMMIT_MODE;

cntryl_stress::stress_main!();
