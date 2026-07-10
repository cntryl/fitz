#[path = "tier4_lease_support.rs"]
mod tier4_lease_support;
#[path = "tier4_support.rs"]
mod tier4_support;

use crate::tier4_lease_support::measure_contention;
use crate::tier4_support::TransportKind;
use cntryl_stress::{stress, StressContext};

#[stress(tier = 4)]
fn should_characterize_tcp_same_route_ownership_contention(ctx: &mut StressContext) {
    measure_contention(
        ctx,
        TransportKind::Tcp,
        "tcp_same_route_ownership_contention",
    );
}

#[stress(tier = 4)]
fn should_characterize_websocket_same_route_ownership_contention(ctx: &mut StressContext) {
    measure_contention(
        ctx,
        TransportKind::WebSocket,
        "websocket_same_route_ownership_contention",
    );
}

cntryl_stress::stress_main!();
