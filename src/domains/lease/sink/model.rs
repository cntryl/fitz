// Lease domain sink for ephemeral in-memory coordination on the current broker
// process.
//
// The boot path mirrors the current process lease state into the admin read
// model. Lease ownership, wait queues, and subscriptions are expected to
// vanish on broker restart, and disconnect cleanup removes any session-owned
// lease state immediately. Fencing tokens are process-local and must not be
// interpreted as durable or cross-node identifiers.

pub(super) use crate::domains::lease::LeaseMetrics;
pub(super) use crate::domains::subscription_state::{RoutedSubscription, RoutedSubscriptionSet};
pub(super) use crate::runtime::{
    ClientChannel, DeliveryError, Envelope, MailboxSink, ManagedActor, Router,
};
pub(super) use chrono::Utc;
pub(super) use parking_lot::Mutex;
pub(super) use std::collections::{HashMap, HashSet, VecDeque};
pub(super) use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
pub(super) use std::sync::Arc;
pub(super) use std::time::{Duration, Instant};

pub(super) const LEASE_MAX_WAIT_SECONDS: u32 = 30;
pub(super) const LEASE_MAX_QUEUE_DEPTH: usize = 100;
pub(super) const LEASE_ACTOR_REPLY_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Clone)]
pub(super) struct SinkLeaseState {
    pub(super) owner_id: String,
    pub(super) owner_session_id: u64,
    pub(super) fencing_token: u64,
    pub(super) expiry: Instant,
    pub(super) acquired_at: String,
    pub(super) renewals: usize,
}

#[derive(Clone)]
pub(super) struct PendingAcquire {
    pub(super) owner_session_id: u64,
    pub(super) owner_id: String,
    pub(super) reply_destination: crate::runtime::routing::RouteAddress,
    pub(super) reply_source: crate::runtime::routing::RouteAddress,
    pub(super) channel: ClientChannel,
    pub(super) route_family: crate::runtime::routing::RouteFamily,
    pub(super) queued_token: u64,
    pub(super) ttl_secs: u64,
    pub(super) expires_at: Instant,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct PendingAcquireRef {
    pub(super) key: crate::domains::lease::protocol::LeaseKey,
    pub(super) queued_token: u64,
}

pub(super) struct LeaseAcquireRequest {
    pub(super) key: crate::domains::lease::protocol::LeaseKey,
    pub(super) owner_session_id: u64,
    pub(super) owner_id: String,
    pub(super) ttl_secs: u64,
    pub(super) wait_seconds: u32,
    pub(super) reply_source: crate::runtime::routing::RouteAddress,
    pub(super) reply_destination: Option<crate::runtime::routing::RouteAddress>,
    pub(super) channel: ClientChannel,
    pub(super) route_family: crate::runtime::routing::RouteFamily,
}

pub(super) struct QueuedAcquireRequest {
    pub(super) current_owner: String,
    pub(super) owner_session_id: u64,
    pub(super) owner_id: String,
    pub(super) ttl_secs: u64,
    pub(super) wait_seconds: u32,
    pub(super) reply_source: crate::runtime::routing::RouteAddress,
    pub(super) reply_destination: Option<crate::runtime::routing::RouteAddress>,
    pub(super) channel: ClientChannel,
    pub(super) route_family: crate::runtime::routing::RouteFamily,
    pub(super) now: Instant,
}

/// Live lease coordination state for the current broker process only.
///
/// The state is intentionally single-broker and non-durable: disconnect cleanup
/// releases session-owned state, restart clears ownership and waiters, and
/// fencing tokens reset with the process.
pub(super) struct LeaseDomainCore {
    // All mutation is actor-serialized. Helpers that need multiple state locks
    // must acquire `pending_acquires` before `leases`; never invert that order.
    pub(super) leases: Mutex<HashMap<crate::domains::lease::protocol::LeaseKey, SinkLeaseState>>,
    pub(super) session_leases:
        Mutex<HashMap<u64, HashSet<crate::domains::lease::protocol::LeaseKey>>>,
    pub(super) pending_acquires:
        Mutex<HashMap<crate::domains::lease::protocol::LeaseKey, VecDeque<PendingAcquire>>>,
    pub(super) session_waiters: Mutex<HashMap<u64, HashSet<PendingAcquireRef>>>,
    /// Sessions disconnect cleanup has already run for; guards against a
    /// stale queued request recreating a lease/waiter/subscription. See
    /// `cleanup.rs`.
    pub(super) cleaned_up_sessions: Mutex<crate::runtime::CleanedUpSessions>,
    /// Process-local fencing token counter; resets on broker restart.
    pub(super) next_token: AtomicU64,
    pub(super) router: Arc<Router>,
    pub(super) families: Mutex<HashMap<u64, RoutedSubscriptionSet<LeaseSubscription>>>,
    pub(super) next_sub_id: AtomicU64,
    pub(super) admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
    pub(super) metrics: Option<LeaseMetrics>,
}

pub(super) struct LeaseDomainState {
    pub(super) core: LeaseDomainCore,
    pub(super) active: AtomicBool,
}

pub(super) struct LeaseDomainRuntime<'a> {
    pub(super) core: &'a LeaseDomainCore,
    pub(super) active: &'a AtomicBool,
}

#[derive(Default)]
pub(super) struct LeaseLiveCounts {
    pub(super) leases: usize,
    pub(super) subscriptions: usize,
}

pub(super) enum LeaseDomainCommand {
    Deliver(Envelope),
    CleanupSession(u64, crossbeam_channel::Sender<()>),
    ReadLiveCounts(crossbeam_channel::Sender<LeaseLiveCounts>),
    ReadWaiters(crossbeam_channel::Sender<Vec<crate::control::admin::LeaseWaiterInfo>>),
    SweepExpiredState,
    #[cfg(any(test, feature = "benchkit"))]
    ApplyAcquireForBench(
        LeaseAcquireRequest,
        crossbeam_channel::Sender<crate::domains::lease::protocol::LeaseResponse>,
    ),
    #[cfg(any(test, feature = "benchkit"))]
    ApplyReleaseForBench(
        crate::domains::lease::protocol::LeaseKey,
        String,
        u64,
        crossbeam_channel::Sender<crate::domains::lease::protocol::LeaseResponse>,
    ),
    #[cfg(test)]
    ApplyAcquireForTests(
        LeaseAcquireRequest,
        crossbeam_channel::Sender<crate::domains::lease::protocol::LeaseResponse>,
    ),
    #[cfg(test)]
    ApplyExtendForTests(
        crate::domains::lease::protocol::LeaseKey,
        String,
        u64,
        u64,
        crossbeam_channel::Sender<crate::domains::lease::protocol::LeaseResponse>,
    ),
    #[cfg(test)]
    ExpireLeaseForTests(
        crate::domains::lease::protocol::LeaseKey,
        crossbeam_channel::Sender<bool>,
    ),
    #[cfg(test)]
    ReadPendingWaiterCountForTests(
        crate::domains::lease::protocol::LeaseKey,
        crossbeam_channel::Sender<usize>,
    ),
    PanicForFailpoint,
    #[cfg(test)]
    BlockForTests(
        crossbeam_channel::Sender<()>,
        crossbeam_channel::Receiver<()>,
    ),
}

pub(super) struct LeaseDomainActor {
    pub(super) state: Arc<LeaseDomainState>,
}

/// Production mailbox adapter for Lease semantics.
pub struct LeaseDomainSink {
    pub(super) state: Arc<LeaseDomainState>,
    pub(super) actor: ManagedActor<LeaseDomainCommand>,
}

pub(super) struct LeaseSubscription {
    pub(super) route: crate::runtime::matcher::Pattern,
    pub(super) session_id: u64,
    pub(super) route_address: crate::runtime::routing::RouteAddress,
    pub(super) subscription_id: u64,
}

impl RoutedSubscription for LeaseSubscription {
    fn pattern(&self) -> &crate::runtime::matcher::Pattern {
        &self.route
    }

    fn session_id(&self) -> u64 {
        self.session_id
    }

    fn subscription_id(&self) -> u64 {
        self.subscription_id
    }
}
