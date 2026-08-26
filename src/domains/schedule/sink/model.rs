pub(super) use crate::domains::schedule::ScheduleMetrics;
pub(super) use crate::runtime::{DeliveryError, Envelope, MailboxSink, ManagedActor, Router};
pub(super) use parking_lot::Mutex;
pub(super) use std::collections::hash_map::Entry;
pub(super) use std::collections::{HashMap, HashSet, VecDeque};
pub(super) use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
pub(super) use std::sync::Arc;
pub(super) use std::time::Instant;

#[cfg_attr(feature = "bench-no-snapshot", allow(dead_code))]
pub(super) const SCHEDULE_ADMIN_SNAPSHOT_INTERVAL_US: u64 = 250_000;
pub(super) const EXECUTIONS_WINDOW_MS: u64 = 60_000;

pub(super) fn duration_millis(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

pub(super) fn now_epoch_ms() -> u64 {
    u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
}

pub(super) type PendingFireKey = (u64, String);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PendingFireState {
    Claimed,
    HandedOff,
    Acknowledged,
}

pub(super) type PendingFireStates = HashMap<PendingFireKey, PendingFireState>;

#[cfg_attr(feature = "bench-no-snapshot", allow(dead_code))]
pub(super) fn schedule_admin_snapshot_due(
    snapshot_dirty: bool,
    force: bool,
    now_elapsed_us: u64,
    last_snapshot_elapsed_us: u64,
) -> bool {
    snapshot_dirty
        && (force
            || now_elapsed_us.saturating_sub(last_snapshot_elapsed_us)
                >= SCHEDULE_ADMIN_SNAPSHOT_INTERVAL_US)
}

pub(super) struct ScheduleSubscription {
    pub(super) pattern: crate::runtime::matcher::Pattern,
    pub(super) session_id: u64,
    pub(super) subscription_id: u64,
    pub(super) subscriber: crate::runtime::routing::RouteAddress,
}

impl crate::domains::subscription_state::RoutedSubscription for ScheduleSubscription {
    fn pattern(&self) -> &crate::runtime::matcher::Pattern {
        &self.pattern
    }
    fn session_id(&self) -> u64 {
        self.session_id
    }
    fn subscription_id(&self) -> u64 {
        self.subscription_id
    }
}

pub(super) struct ScheduleSubscriptionSet {
    pub(super) subscriptions:
        crate::domains::subscription_state::RoutedSubscriptionSet<ScheduleSubscription>,
    pub(super) round_robin_cursors: HashMap<String, usize>,
}

impl ScheduleSubscriptionSet {
    pub(super) fn new() -> Self {
        Self {
            subscriptions: crate::domains::subscription_state::RoutedSubscriptionSet::new(),
            round_robin_cursors: HashMap::new(),
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.subscriptions.is_empty()
    }

    pub(super) fn subscription_count(&self) -> usize {
        self.subscriptions.subscription_count()
    }

    pub(super) fn find_existing_id(&self, session_id: u64, route: &str) -> Option<u64> {
        self.subscriptions.find_existing_id(session_id, route)
    }

    pub(super) fn insert(
        &mut self,
        family: crate::runtime::routing::RouteFamily,
        subscription: ScheduleSubscription,
    ) {
        self.subscriptions.insert(family, subscription);
    }

    pub(super) fn remove_session_route(
        &mut self,
        family: crate::runtime::routing::RouteFamily,
        session_id: u64,
        route: &str,
    ) -> usize {
        let removed = self
            .subscriptions
            .remove_session_pattern(family, session_id, route);
        if removed > 0 {
            self.prune_unmatched_cursors(family);
        }
        removed
    }

    pub(super) fn remove_session(
        &mut self,
        family: crate::runtime::routing::RouteFamily,
        session_id: u64,
    ) -> usize {
        let removed = self.subscriptions.remove_session(family, session_id);
        if removed > 0 {
            self.prune_unmatched_cursors(family);
        }
        removed
    }

    pub(super) fn matching_ids(
        &self,
        family: crate::runtime::routing::RouteFamily,
        route: &str,
    ) -> Vec<u64> {
        self.subscriptions.matching_ids(family, route)
    }

    fn prune_unmatched_cursors(&mut self, family: crate::runtime::routing::RouteFamily) {
        let subscriptions = &self.subscriptions;
        self.round_robin_cursors
            .retain(|route, _| !subscriptions.matching_ids(family, route).is_empty());
    }
}

pub(super) struct ScheduleDomainCore {
    pub(super) store: crate::storage::FitzStorageEngine,
    pub(super) actors: Mutex<
        HashMap<crate::runtime::routing::RouteFamily, crate::domains::schedule::ScheduleActor>,
    >,
    pub(super) sub_families:
        Mutex<HashMap<crate::runtime::routing::RouteFamily, ScheduleSubscriptionSet>>,
    /// Sessions disconnect cleanup has already run for; guards against a
    /// stale queued request recreating a subscription. See `cleanup.rs`.
    pub(super) cleaned_up_sessions: Mutex<super::cleanup::CleanedUpSessions>,
    pub(super) next_sub_id: AtomicU64,
    pub(super) router: Arc<Router>,
    #[cfg_attr(feature = "bench-no-snapshot", allow(dead_code))]
    pub(super) admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
    pub(super) snapshot_dirty: AtomicBool,
    #[cfg_attr(feature = "bench-no-snapshot", allow(dead_code))]
    pub(super) snapshot_syncing: AtomicBool,
    #[cfg_attr(feature = "bench-no-snapshot", allow(dead_code))]
    pub(super) last_snapshot_elapsed_us: AtomicU64,
    pub(super) snapshot_epoch: Instant,
    /// Total number of live publish handoffs that failed to route.
    pub(super) live_publish_failures: AtomicU64,
    /// Total number of pending-fire acknowledgement persistence failures.
    pub(super) ack_failures: AtomicU64,
    /// Pending fire claims already handed off to the live publish path in this
    /// broker process but still waiting for durable acknowledgement retry.
    pub(super) pending_ack_retries: Mutex<HashMap<u64, PendingFireStates>>,
    /// Rolling window of acknowledged handoff timestamps for executions-per-minute.
    pub(super) recent_acknowledgement_ms: Mutex<VecDeque<u64>>,
    /// Write options for schedule persistence.
    pub(super) write_options: cntryl_midge::WriteOptions,
    pub(super) metrics: Option<ScheduleMetrics>,
}

pub(super) struct ScheduleDomainState {
    pub(super) core: ScheduleDomainCore,
    pub(super) active: AtomicBool,
}

/// Runtime body methods intentionally share names with their sink wrapper methods:
/// the wrapper crosses the mailbox, while the runtime body performs the work.
pub(super) struct ScheduleDomainRuntime<'a> {
    pub(super) core: &'a ScheduleDomainCore,
    pub(super) active: &'a AtomicBool,
}

#[derive(Default)]
pub(super) struct ScheduleLiveCounts {
    pub(super) subscriptions: usize,
    pub(super) schedules: usize,
    pub(super) pending_fires: usize,
    pub(super) executions_per_minute: f64,
    pub(super) notify_failures: u64,
    pub(super) ack_failures: u64,
    pub(super) pending_ack_retries: usize,
    pub(super) oldest_pending_claim_age_seconds: u64,
    pub(super) overdue_normalizations: u64,
}

pub(super) enum ScheduleDomainCommand {
    Deliver(Envelope),
    CleanupSession(u64),
    ReadLiveCounts(crossbeam_channel::Sender<ScheduleLiveCounts>),
    ReadPendingClaims(
        crate::runtime::routing::RouteFamily,
        crossbeam_channel::Sender<Vec<crate::control::admin::SchedulePendingClaimInfo>>,
    ),
    RefreshAdminSnapshotIfDirty(crossbeam_channel::Sender<()>),
    ScanDueSchedules,
    PreloadPersistedFamilies(crossbeam_channel::Sender<Result<(), String>>),
    BenchPublishEvent(
        crate::runtime::DomainPublishEvent,
        crossbeam_channel::Sender<()>,
    ),
    ForceDueScanForTests(usize, crossbeam_channel::Sender<()>),
    #[cfg(test)]
    PanicForTests,
    #[cfg(test)]
    BlockForTests(
        crossbeam_channel::Sender<()>,
        crossbeam_channel::Receiver<()>,
    ),
}

pub(super) struct ScheduleDomainActor {
    pub(super) state: Arc<ScheduleDomainState>,
}

pub struct ScheduleDomainSink {
    pub(super) state: Arc<ScheduleDomainState>,
    pub(super) actor: ManagedActor<ScheduleDomainCommand>,
}
