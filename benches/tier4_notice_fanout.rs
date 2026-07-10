#[path = "tier4_notice_support.rs"]
mod tier4_notice_support;
#[path = "tier4_support.rs"]
mod tier4_support;

use crate::tier4_notice_support::{
    complete_network_publish, subscribe_network_client, with_notice_clients, InProcessNoticeFixture,
};
use crate::tier4_support::{
    measure_operations, tag_dimensions, LayerKind, StorageProfile, Tier4Dimensions, TransportKind,
};
use cntryl_stress::{stress, stress_main, StressContext};
use fitz::benchkit::{build_notice_publish, build_notice_subscribe};

const FANOUT_PATTERN: &str = "notice://tier4/fanout/orders/*";
const FANOUT_ROUTE: &str = "notice://tier4/fanout/orders/create";
const FANOUT_PAYLOAD_SIZE: usize = 1_024;

fn dimensions(layer: LayerKind, subscriber_count: usize) -> Tier4Dimensions<'static> {
    Tier4Dimensions {
        domain: "notice",
        scenario: "delivery_confirmed_fanout",
        storage_profile: StorageProfile::Memory,
        layer,
        write_mode: "not_applicable",
        payload_size: FANOUT_PAYLOAD_SIZE,
        history_depth: 0,
        read_limit: 0,
        read_scope: "none",
        route_count: 1,
        filter_selectivity: "all_matching_single_star",
        client_count: subscriber_count + 1,
        workload_mix: "one_publish_many_deliveries",
        completed_unit: "fanout_publish",
        gate_class: "characterization",
    }
}

fn tag_fanout(ctx: &mut StressContext, layer: LayerKind, subscriber_count: usize) {
    tag_dimensions(ctx, &dimensions(layer, subscriber_count));
    ctx.parameter("publisher_count", 1);
    ctx.parameter("subscriber_count", subscriber_count);
    ctx.parameter("deliveries_per_logical_operation", subscriber_count);
    ctx.parameter(
        "completion_mode",
        "publisher_ack_and_all_subscribers_drained",
    );
}

fn measure_in_process_fanout(
    ctx: &mut StressContext,
    layer: LayerKind,
    subscriber_count: usize,
    measurement: &'static str,
) {
    tag_fanout(ctx, layer, subscriber_count);
    let payload = vec![0xBC; FANOUT_PAYLOAD_SIZE];
    let fixture =
        InProcessNoticeFixture::new(FANOUT_PATTERN, FANOUT_ROUTE, &payload, subscriber_count);
    measure_operations(ctx, measurement, 1, |latencies| {
        let latency = match layer {
            LayerKind::Direct => fixture.complete_direct_publish(),
            LayerKind::Encoded => fixture.complete_encoded_publish(),
            _ => unreachable!("in-process Notice fanout layer"),
        };
        latencies.push(latency);
    });
    fixture.stop();
}

fn measure_network_fanout(
    ctx: &mut StressContext,
    transport: TransportKind,
    subscriber_count: usize,
    measurement: &'static str,
) {
    let layer = match transport {
        TransportKind::Tcp => LayerKind::TcpMultiClient,
        TransportKind::WebSocket => LayerKind::WebSocketMultiClient,
    };
    tag_fanout(ctx, layer, subscriber_count);
    let payload = vec![0xBC; FANOUT_PAYLOAD_SIZE];
    let subscribe_frame = build_notice_subscribe(FANOUT_PATTERN);
    let publish_frame = build_notice_publish(FANOUT_ROUTE, &payload);

    with_notice_clients(transport, subscriber_count + 1, |runtime, clients| {
        let mut subscription_ids = Vec::with_capacity(subscriber_count);
        for subscriber in &mut clients[..subscriber_count] {
            subscription_ids
                .push(runtime.block_on(subscribe_network_client(subscriber, &subscribe_frame)));
        }
        let (subscribers, publisher_slice) = clients.split_at_mut(subscriber_count);
        let publisher = &mut publisher_slice[0];
        measure_operations(ctx, measurement, 1, |latencies| {
            latencies.push(runtime.block_on(complete_network_publish(
                publisher,
                subscribers,
                &publish_frame,
                &subscription_ids,
                FANOUT_ROUTE,
                &payload,
            )));
        });
    });
}

#[stress(tier = 4)]
fn should_characterize_direct_16_subscriber_fanout(ctx: &mut StressContext) {
    measure_in_process_fanout(ctx, LayerKind::Direct, 16, "direct_16_subscriber_fanout");
}

#[stress(tier = 4)]
fn should_characterize_encoded_16_subscriber_fanout(ctx: &mut StressContext) {
    measure_in_process_fanout(ctx, LayerKind::Encoded, 16, "encoded_16_subscriber_fanout");
}

macro_rules! network_fanout_row {
    ($name:ident, $transport:expr, $subscribers:expr, $measurement:literal) => {
        #[stress(tier = 4)]
        fn $name(ctx: &mut StressContext) {
            measure_network_fanout(ctx, $transport, $subscribers, $measurement);
        }
    };
}

network_fanout_row!(
    should_characterize_tcp_16_subscriber_fanout,
    TransportKind::Tcp,
    16,
    "tcp_16_subscriber_fanout"
);
network_fanout_row!(
    should_characterize_tcp_64_subscriber_fanout,
    TransportKind::Tcp,
    64,
    "tcp_64_subscriber_fanout"
);
network_fanout_row!(
    should_characterize_ws_16_subscriber_fanout,
    TransportKind::WebSocket,
    16,
    "ws_16_subscriber_fanout"
);
network_fanout_row!(
    should_characterize_ws_64_subscriber_fanout,
    TransportKind::WebSocket,
    64,
    "ws_64_subscriber_fanout"
);

stress_main!();
