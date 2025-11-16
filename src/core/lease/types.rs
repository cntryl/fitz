//! Lease domain types

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Instant;

use dashmap::DashMap;
use tokio::sync::{oneshot, RwLock};

use crate::protocol::route::Route;

pub(crate) type LeaseLock = Arc<RwLock<LeaseEntry>>;
pub(crate) type ResourceMap = DashMap<String, LeaseLock>;
pub(crate) type AreaMap = DashMap<String, Arc<ResourceMap>>;
pub(crate) type RealmMap = DashMap<String, Arc<AreaMap>>;

/// Lease operations following the route pattern: lease://{realm}/{area}/{resource}/{operation}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseOperation {
    /// Acquire - request a lease on a resource
    Acquire,
    /// Renew - extend an existing lease
    Renew,
    /// Surrender - voluntarily surrender a lease
    Surrender,
}

impl LeaseOperation {
    /// Determine operation from route
    pub fn from_route(route: &Route) -> Result<Self, String> {
        match route.operation.as_deref() {
            Some("acquire") => Ok(LeaseOperation::Acquire),
            Some("renew") => Ok(LeaseOperation::Renew),
            Some("surrender") => Ok(LeaseOperation::Surrender),
            None => {
                // Default to Acquire if no operation specified
                Ok(LeaseOperation::Acquire)
            }
            Some(op) => Err(format!("Unknown lease operation: {}", op)),
        }
    }
}

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
