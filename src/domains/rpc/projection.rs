use crate::api::admin::read_model::AdminReadModel;
use crate::api::admin::{RpcPendingRequest, RpcWorker};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Combined snapshot for the RPC admin projection.
pub struct RpcAdminState {
    pub workers: Vec<RpcWorker>,
    pub pending: Vec<RpcPendingRequest>,
}

/// Admin projection for the RPC domain.
///
/// Tracks dirty state and rebuilds the admin read model snapshot on demand.
/// Projection failure must never affect domain correctness.
pub struct RpcAdminProjection {
    read_model: Arc<AdminReadModel>,
    dirty: AtomicBool,
}

impl RpcAdminProjection {
    pub fn new(read_model: Arc<AdminReadModel>) -> Self {
        Self {
            read_model,
            dirty: AtomicBool::new(false),
        }
    }

    pub fn mark_dirty(&self) {
        self.dirty.store(true, Ordering::Relaxed);
    }

    pub fn refresh_if_dirty<F>(&self, build_state: F)
    where
        F: FnOnce() -> RpcAdminState,
    {
        if self.dirty.swap(false, Ordering::AcqRel) {
            let state = build_state();
            self.read_model.replace_rpc_workers(state.workers);
            self.read_model.replace_rpc_pending(state.pending);
        }
    }
}
