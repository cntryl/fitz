//! LeaseMsg messages.

use crate::actor::ActorRef;
use std::time::Duration;

/// Messages for LeaseActor.
#[derive(Debug)]
pub enum LeaseMsg {
    /// Acquire a lease.
    Acquire { realm: String, area: String, resource: String, ttl: Duration },
}
