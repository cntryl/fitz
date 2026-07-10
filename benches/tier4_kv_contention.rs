#[path = "tier4_kv_support.rs"]
mod tier4_kv_support;
#[path = "tier4_support.rs"]
mod tier4_support;
use crate::tier4_kv_support::measure_contention;
use crate::tier4_support::TransportKind;
use cntryl_stress::{stress, StressContext};

#[stress(tier = 4)]
fn should_characterize_tcp_transaction_contention(ctx: &mut StressContext) {
    measure_contention(ctx, TransportKind::Tcp, "tcp_transaction_contention");
}

#[stress(tier = 4)]
fn should_characterize_websocket_transaction_contention(ctx: &mut StressContext) {
    measure_contention(
        ctx,
        TransportKind::WebSocket,
        "websocket_transaction_contention",
    );
}

cntryl_stress::stress_main!();
