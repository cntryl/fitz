use crate::api::admin::read_model::AdminReadModel;
use crate::api::admin::KvTransaction;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Admin projection for the KV domain.
///
/// Tracks dirty state and rebuilds the admin read model snapshot on demand.
/// Projection failure must never affect domain correctness.
pub struct KvAdminProjection {
    read_model: Arc<AdminReadModel>,
    dirty: AtomicBool,
}

impl KvAdminProjection {
    pub fn new(read_model: Arc<AdminReadModel>) -> Self {
        Self {
            read_model,
            dirty: AtomicBool::new(false),
        }
    }

    pub fn mark_dirty(&self) {
        self.dirty.store(true, Ordering::Relaxed);
    }

    pub fn refresh_if_dirty<F>(&self, build_transactions: F)
    where
        F: FnOnce() -> Vec<KvTransaction>,
    {
        if self.dirty.swap(false, Ordering::AcqRel) {
            self.read_model
                .replace_kv_transactions(build_transactions());
        }
    }
}
