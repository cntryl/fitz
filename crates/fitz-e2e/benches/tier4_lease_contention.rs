#[path = "tier4_lease_support.rs"]
mod tier4_lease_support;
#[path = "tier4_lease_sweeps_support.rs"]
mod tier4_lease_sweeps_support;
use fitz_core::testkit::operations as tier4_support;

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

#[stress(tier = 4)]
fn should_measure_lease_sweep_with_no_additional_leases(ctx: &mut StressContext) {
    crate::tier4_lease_sweeps_support::measure_sweeps(ctx, 0);
}

#[stress(tier = 4)]
fn should_measure_lease_sweep_with_1024_additional_leases(ctx: &mut StressContext) {
    crate::tier4_lease_sweeps_support::measure_sweeps(ctx, 1_024);
}

#[stress(tier = 4)]
fn should_measure_lease_sweep_with_8192_additional_leases(ctx: &mut StressContext) {
    crate::tier4_lease_sweeps_support::measure_sweeps(ctx, 8_192);
}

#[stress(tier = 4)]
fn should_measure_tcp_query_with_no_additional_leases(ctx: &mut StressContext) {
    crate::tier4_lease_sweeps_support::measure_tcp_query(ctx, 0);
}

#[stress(tier = 4)]
fn should_measure_tcp_query_with_1024_additional_leases(ctx: &mut StressContext) {
    crate::tier4_lease_sweeps_support::measure_tcp_query(ctx, 1_024);
}

#[stress(tier = 4)]
fn should_measure_tcp_query_with_8192_additional_leases(ctx: &mut StressContext) {
    crate::tier4_lease_sweeps_support::measure_tcp_query(ctx, 8_192);
}

cntryl_stress::stress_main!();
