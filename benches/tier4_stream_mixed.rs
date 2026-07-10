#[path = "tier4_stream_support.rs"]
mod tier4_stream_support;
#[path = "tier4_stream_transport.rs"]
mod tier4_stream_transport;
#[path = "tier4_support.rs"]
mod tier4_support;

use crate::tier4_stream_support::{
    measure_operations, tag_row, LayerKind, ReadScope, RowDimensions, StorageProfile,
    TransportKind, CANONICAL_HISTORY_DEPTH, CANONICAL_PAYLOAD_SIZE, CANONICAL_READ_LIMIT,
    MIXED_OPERATIONS_PER_CLIENT, MIXED_READS_PER_CLIENT, MIXED_WRITES_PER_CLIENT,
};
use crate::tier4_stream_transport::{
    read_pages, request_read_pages, seed_scope, with_transport_clients, OpenAppendState,
};
use cntryl_stress::{stress, StressContext};
use futures_util::future::join_all;
use std::time::Instant;

fn dimensions(transport: TransportKind, client_count: usize, gate: bool) -> RowDimensions<'static> {
    RowDimensions {
        scenario: "mixed_80_read_20_write",
        storage_profile: StorageProfile::Memory,
        layer: LayerKind::from(transport),
        write_mode: "not_committed",
        write_operation: "append_open_session",
        payload_size: CANONICAL_PAYLOAD_SIZE,
        history_depth: CANONICAL_HISTORY_DEPTH,
        read_limit: CANONICAL_READ_LIMIT,
        read_scope: ReadScope::Resource,
        route_count: 1,
        filter_match_count: "unfiltered",
        client_count,
        workload_mix: "80_read_20_write",
        completed_unit: "logical_operation",
        gate_class: if gate {
            "regression_gate"
        } else {
            "characterization"
        },
    }
}

fn measure_mixed(
    ctx: &mut StressContext,
    transport: TransportKind,
    client_count: usize,
    gate: bool,
    measurement: &'static str,
) {
    let dimensions = dimensions(transport, client_count, gate);
    tag_row(ctx, &dimensions);
    with_transport_clients(
        StorageProfile::Memory,
        transport,
        client_count,
        |runtime, _server, clients| {
            let realm = format!("tier4-mixed-{}-{client_count}", transport.label());
            let payload = vec![0x5A; CANONICAL_PAYLOAD_SIZE];
            runtime.block_on(seed_scope(
                &mut clients[0],
                &realm,
                ReadScope::Resource,
                CANONICAL_HISTORY_DEPTH,
                1,
                &payload,
            ));
            let pages = read_pages(&ReadScope::Resource.route(&realm), CANONICAL_READ_LIMIT);
            ctx.parameter(
                "wire_page_limit",
                crate::tier4_stream_support::WIRE_READ_PAGE_LIMIT,
            );
            ctx.parameter("wire_page_count", pages.len());
            let mut append_states = clients
                .iter_mut()
                .enumerate()
                .map(|(index, client)| {
                    runtime.block_on(OpenAppendState::prepare(
                        client,
                        &format!("stream://{realm}/mixed-write/resource-{index}"),
                        &payload,
                    ))
                })
                .collect::<Vec<_>>();

            let operations_per_iteration = client_count * MIXED_OPERATIONS_PER_CLIENT;
            measure_operations(
                ctx,
                measurement,
                u64::try_from(operations_per_iteration)
                    .expect("mixed operation count should fit u64"),
                |latencies| {
                    let per_client = runtime.block_on(async {
                        join_all(clients.iter_mut().zip(append_states.iter_mut()).map(
                            |(client, append_state)| {
                                let pages = &pages;
                                async move {
                                    let mut observations =
                                        Vec::with_capacity(MIXED_OPERATIONS_PER_CLIENT);
                                    for operation in 0..MIXED_OPERATIONS_PER_CLIENT {
                                        let started = Instant::now();
                                        if operation == 4 || operation == 9 {
                                            append_state.append(client).await;
                                        } else {
                                            request_read_pages(client, pages).await;
                                        }
                                        observations.push(started.elapsed());
                                    }
                                    observations
                                }
                            },
                        ))
                        .await
                    });
                    latencies.extend(per_client.into_iter().flatten());
                },
            );
        },
    );
}

#[stress(tier = 4)]
fn should_measure_memory_tcp_8_client_mixed_throughput(ctx: &mut StressContext) {
    measure_mixed(
        ctx,
        TransportKind::Tcp,
        8,
        true,
        "memory_tcp_8_client_mixed_throughput",
    );
}

#[stress(tier = 4)]
fn should_measure_memory_ws_8_client_mixed_throughput(ctx: &mut StressContext) {
    measure_mixed(
        ctx,
        TransportKind::WebSocket,
        8,
        true,
        "memory_ws_8_client_mixed_throughput",
    );
}

macro_rules! mixed_row {
    ($name:ident, $measurement:literal, $transport:expr, $clients:expr) => {
        #[stress(tier = 4)]
        fn $name(ctx: &mut StressContext) {
            measure_mixed(ctx, $transport, $clients, false, $measurement);
        }
    };
}

mixed_row!(
    should_characterize_memory_tcp_1_client_mixed,
    "memory_tcp_1_client_mixed",
    TransportKind::Tcp,
    1
);
mixed_row!(
    should_characterize_memory_tcp_2_client_mixed,
    "memory_tcp_2_client_mixed",
    TransportKind::Tcp,
    2
);
mixed_row!(
    should_characterize_memory_tcp_4_client_mixed,
    "memory_tcp_4_client_mixed",
    TransportKind::Tcp,
    4
);
mixed_row!(
    should_characterize_memory_tcp_16_client_mixed,
    "memory_tcp_16_client_mixed",
    TransportKind::Tcp,
    16
);
mixed_row!(
    should_characterize_memory_tcp_32_client_mixed,
    "memory_tcp_32_client_mixed",
    TransportKind::Tcp,
    32
);
mixed_row!(
    should_characterize_memory_ws_1_client_mixed,
    "memory_ws_1_client_mixed",
    TransportKind::WebSocket,
    1
);
mixed_row!(
    should_characterize_memory_ws_2_client_mixed,
    "memory_ws_2_client_mixed",
    TransportKind::WebSocket,
    2
);
mixed_row!(
    should_characterize_memory_ws_4_client_mixed,
    "memory_ws_4_client_mixed",
    TransportKind::WebSocket,
    4
);
mixed_row!(
    should_characterize_memory_ws_16_client_mixed,
    "memory_ws_16_client_mixed",
    TransportKind::WebSocket,
    16
);
mixed_row!(
    should_characterize_memory_ws_32_client_mixed,
    "memory_ws_32_client_mixed",
    TransportKind::WebSocket,
    32
);

const _: () = {
    assert!(MIXED_READS_PER_CLIENT == 8);
    assert!(MIXED_WRITES_PER_CLIENT == 2);
};

cntryl_stress::stress_main!();
