#[path = "tier4_lease_support.rs"]
mod tier4_lease_support;
#[path = "tier4_support.rs"]
mod tier4_support;

use crate::tier4_lease_support::measure_transport;
use crate::tier4_support::TransportKind;
use cntryl_stress::{stress, StressContext};

#[stress(tier = 4)]
fn should_measure_tcp_acquire_release_lifecycle(ctx: &mut StressContext) {
    measure_transport(ctx, TransportKind::Tcp, "tcp_acquire_release_lifecycle");
}

#[stress(tier = 4)]
fn should_measure_websocket_acquire_release_lifecycle(ctx: &mut StressContext) {
    measure_transport(
        ctx,
        TransportKind::WebSocket,
        "websocket_acquire_release_lifecycle",
    );
}

cntryl_stress::stress_main!();
