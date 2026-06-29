use super::*;

#[cfg_attr(feature = "bench-no-snapshot", allow(dead_code))]
pub(in crate::domains::rpc::sink) fn rpc_admin_snapshot_due(
    snapshot_dirty: bool,
    force: bool,
    now_elapsed_us: u64,
    last_snapshot_elapsed_us: u64,
) -> bool {
    snapshot_dirty
        && (force
            || now_elapsed_us.saturating_sub(last_snapshot_elapsed_us)
                >= RPC_ADMIN_SNAPSHOT_INTERVAL_US)
}
