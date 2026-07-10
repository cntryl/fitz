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

const NOTICE_ROUTE: &str = "notice://tier4/publish/events";
const NOTICE_PAYLOAD_SIZE: usize = 1_024;

fn dimensions(layer: LayerKind) -> Tier4Dimensions<'static> {
    Tier4Dimensions {
        domain: "notice",
        scenario: "delivery_confirmed_publish",
        storage_profile: StorageProfile::Memory,
        layer,
        write_mode: "not_applicable",
        payload_size: NOTICE_PAYLOAD_SIZE,
        history_depth: 0,
        read_limit: 0,
        read_scope: "none",
        route_count: 1,
        filter_selectivity: "all_matching",
        client_count: 2,
        workload_mix: "publish_and_delivery",
        completed_unit: "confirmed_publish",
        gate_class: "regression_gate",
    }
}

fn tag_publish(ctx: &mut StressContext, layer: LayerKind) {
    tag_dimensions(ctx, &dimensions(layer));
    ctx.parameter("publisher_count", 1);
    ctx.parameter("subscriber_count", 1);
    ctx.parameter("completion_mode", "publisher_ack_and_subscriber_delivery");
}

#[stress(tier = 4)]
fn should_measure_direct_delivery_confirmed_publish(ctx: &mut StressContext) {
    tag_publish(ctx, LayerKind::Direct);
    let payload = vec![0xA5; NOTICE_PAYLOAD_SIZE];
    let fixture = InProcessNoticeFixture::new(NOTICE_ROUTE, NOTICE_ROUTE, &payload, 1);
    measure_operations(ctx, "direct_delivery_confirmed_publish", 1, |latencies| {
        latencies.push(fixture.complete_direct_publish());
    });
    fixture.stop();
}

#[stress(tier = 4)]
fn should_measure_encoded_delivery_confirmed_publish(ctx: &mut StressContext) {
    tag_publish(ctx, LayerKind::Encoded);
    let payload = vec![0xA5; NOTICE_PAYLOAD_SIZE];
    let fixture = InProcessNoticeFixture::new(NOTICE_ROUTE, NOTICE_ROUTE, &payload, 1);
    measure_operations(ctx, "encoded_delivery_confirmed_publish", 1, |latencies| {
        latencies.push(fixture.complete_encoded_publish());
    });
    fixture.stop();
}

fn measure_network_publish(
    ctx: &mut StressContext,
    transport: TransportKind,
    measurement: &'static str,
) {
    tag_publish(ctx, LayerKind::from(transport));
    let payload = vec![0xA5; NOTICE_PAYLOAD_SIZE];
    let subscribe_frame = build_notice_subscribe(NOTICE_ROUTE);
    let publish_frame = build_notice_publish(NOTICE_ROUTE, &payload);

    with_notice_clients(transport, 2, |runtime, clients| {
        let subscription_id =
            runtime.block_on(subscribe_network_client(&mut clients[0], &subscribe_frame));
        let (subscribers, publisher_slice) = clients.split_at_mut(1);
        let publisher = &mut publisher_slice[0];
        measure_operations(ctx, measurement, 1, |latencies| {
            latencies.push(runtime.block_on(complete_network_publish(
                publisher,
                subscribers,
                &publish_frame,
                &[subscription_id],
                NOTICE_ROUTE,
                &payload,
            )));
        });
    });
}

#[stress(tier = 4)]
fn should_measure_tcp_delivery_confirmed_publish(ctx: &mut StressContext) {
    measure_network_publish(ctx, TransportKind::Tcp, "tcp_delivery_confirmed_publish");
}

#[stress(tier = 4)]
fn should_measure_ws_delivery_confirmed_publish(ctx: &mut StressContext) {
    measure_network_publish(
        ctx,
        TransportKind::WebSocket,
        "ws_delivery_confirmed_publish",
    );
}

stress_main!();
