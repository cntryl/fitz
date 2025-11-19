//! Lease domain types

use parking_lot::RwLock;
use std::sync::Arc;
use std::time::Instant;

use crate::protocol::route::Route;

pub(crate) type LeaseLock = Arc<RwLock<LeaseEntry>>;

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

// Pending waiters removed - sync model uses immediate return with error for busy leases

#[derive(Debug)]
pub(crate) struct LeaseEntry {
    pub(crate) id: String,
    pub(crate) token: String,
    pub(crate) expiry: Instant,
    pub(crate) body: Option<Vec<u8>>,
    // Sync model: no waiters queue. Busy leases return error immediately.
}
impl LeaseEntry {
    pub(crate) fn free() -> Self {
        Self {
            id: String::new(),
            token: String::new(),
            expiry: Instant::now(),
            body: None,
        }
    }
    #[inline]
    pub(crate) fn is_active(&self, now: Instant) -> bool {
        !self.id.is_empty() && now < self.expiry
    }
}
