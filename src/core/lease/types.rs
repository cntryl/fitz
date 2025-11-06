//! Lease domain types

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Instant;

use dashmap::DashMap;
use tokio::sync::{oneshot, RwLock};

pub(crate) type LeaseLock = Arc<RwLock<LeaseEntry>>;
pub(crate) type ResourceMap = DashMap<String, LeaseLock>;
pub(crate) type AreaMap = DashMap<String, Arc<ResourceMap>>;
pub(crate) type RealmMap = DashMap<String, Arc<AreaMap>>;

#[derive(Debug, Clone)]
pub struct LeaseGrant {
    pub id: String,
    pub body: Option<Vec<u8>>,
    pub token: String,
    pub ttl_secs: u32,
}

#[derive(Debug)]
pub(crate) struct Pending {
    pub(crate) requested_ttl: u32,
    pub(crate) responder: oneshot::Sender<Result<LeaseGrant, String>>,
}

#[derive(Debug)]
pub(crate) struct LeaseEntry {
    pub(crate) id: String,
    pub(crate) token: String,
    pub(crate) expiry: Instant,
    pub(crate) body: Option<Vec<u8>>,
    pub(crate) waiters: VecDeque<Pending>, // FIFO within the resource
}
impl LeaseEntry {
    pub(crate) fn free() -> Self {
        Self {
            id: String::new(),
            token: String::new(),
            expiry: Instant::now(),
            body: None,
            waiters: VecDeque::new(),
        }
    }
    #[inline]
    pub(crate) fn is_active(&self, now: Instant) -> bool {
        !self.id.is_empty() && now < self.expiry
    }
}
