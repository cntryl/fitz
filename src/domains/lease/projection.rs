use crate::api::admin::read_model::AdminReadModel;
use crate::api::admin::LeaseInfo;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Admin projection for the lease domain.
///
/// Tracks dirty state and rebuilds the admin read model snapshot on demand.
/// Projection failure must never affect domain correctness.
pub struct LeaseAdminProjection {
    read_model: Arc<AdminReadModel>,
    dirty: AtomicBool,
}

impl LeaseAdminProjection {
    pub fn new(read_model: Arc<AdminReadModel>) -> Self {
        Self {
            read_model,
            dirty: AtomicBool::new(false),
        }
    }

    pub fn mark_dirty(&self) {
        self.dirty.store(true, Ordering::Relaxed);
    }

    pub fn refresh_if_dirty<F>(&self, build_leases: F)
    where
        F: FnOnce() -> Vec<LeaseInfo>,
    {
        if self.dirty.swap(false, Ordering::AcqRel) {
            self.read_model.replace_leases(build_leases());
        }
    }
}
