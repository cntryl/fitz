#[path = "tier4_notice_support.rs"]
mod tier4_notice_support;
#[path = "tier4_support.rs"]
mod tier4_support;

use crate::tier4_notice_support::{
    complete_network_control_lifecycle, with_notice_clients, InProcessNoticeControlFixture,
    MutableNoticeUnsubscribeFrame,
};
use crate::tier4_support::{
    measure_operations, tag_dimensions, LayerKind, StorageProfile, Tier4Dimensions, TransportKind,
};
use cntryl_stress::{stress, stress_main, StressContext};
use fitz::benchkit::build_notice_subscribe;

const CONTROL_PATTERN: &str = "notice://tier4/control/events/*";

fn dimensions(layer: LayerKind) -> Tier4Dimensions<'static> {
    Tier4Dimensions {
        domain: "notice",
        scenario: "subscribe_unsubscribe_lifecycle",
        storage_profile: StorageProfile::Memory,
        layer,
        write_mode: "not_applicable",
        payload_size: 0,
        history_depth: 0,
        read_limit: 0,
        read_scope: "none",
        route_count: 1,
        filter_selectivity: "single_star_pattern",
        client_count: 1,
        workload_mix: "control_only",
        completed_unit: "subscription_lifecycle",
        gate_class: "characterization",
    }
}

fn tag_control(ctx: &mut StressContext, layer: LayerKind) {
    tag_dimensions(ctx, &dimensions(layer));
    ctx.parameter("subscriber_count", 1);
    ctx.parameter("control_operations_per_lifecycle", 2);
    ctx.parameter("completion_mode", "subscribe_ack_then_unsubscribe_ack");
}

#[stress(tier = 4)]
fn should_characterize_direct_subscription_lifecycle(ctx: &mut StressContext) {
    tag_control(ctx, LayerKind::Direct);
    let mut fixture = InProcessNoticeControlFixture::new(CONTROL_PATTERN);
    measure_operations(ctx, "direct_subscription_lifecycle", 1, |latencies| {
        latencies.push(fixture.complete_direct_lifecycle());
    });
    fixture.stop();
}

#[stress(tier = 4)]
fn should_characterize_encoded_subscription_lifecycle(ctx: &mut StressContext) {
    tag_control(ctx, LayerKind::Encoded);
    let mut fixture = InProcessNoticeControlFixture::new(CONTROL_PATTERN);
    measure_operations(ctx, "encoded_subscription_lifecycle", 1, |latencies| {
        latencies.push(fixture.complete_encoded_lifecycle());
    });
    fixture.stop();
}

fn measure_network_control(
    ctx: &mut StressContext,
    transport: TransportKind,
    measurement: &'static str,
) {
    tag_control(ctx, LayerKind::from(transport));
    let subscribe_frame = build_notice_subscribe(CONTROL_PATTERN);
    let mut unsubscribe_frame = MutableNoticeUnsubscribeFrame::new();
    with_notice_clients(transport, 1, |runtime, clients| {
        let client = &mut clients[0];
        measure_operations(ctx, measurement, 1, |latencies| {
            latencies.push(runtime.block_on(complete_network_control_lifecycle(
                client,
                &subscribe_frame,
                &mut unsubscribe_frame,
            )));
        });
    });
}

#[stress(tier = 4)]
fn should_characterize_tcp_subscription_lifecycle(ctx: &mut StressContext) {
    measure_network_control(ctx, TransportKind::Tcp, "tcp_subscription_lifecycle");
}

#[stress(tier = 4)]
fn should_characterize_ws_subscription_lifecycle(ctx: &mut StressContext) {
    measure_network_control(ctx, TransportKind::WebSocket, "ws_subscription_lifecycle");
}

stress_main!();
