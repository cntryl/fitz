use crate::domains::schedule::ScheduleMetrics;
use crate::runtime::{Envelope, Router};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::Arc;
use std::time::Instant;

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

pub(super) struct ScheduleFamilyState {
    pub(super) route_family: crate::runtime::routing::RouteFamily,
    pub(super) store: crate::domains::schedule::ScheduleStore,
    pub(super) actor: Option<crate::domains::schedule::ScheduleActor>,
    pub(super) subscriptions: ScheduleSubscriptionSet,
    /// Sessions disconnect cleanup has already run for; guards against a
    /// stale queued request recreating a subscription. See `cleanup.rs`.
    pub(super) cleaned_up_sessions: crate::runtime::CleanedUpSessions,
    pub(super) next_sub_id: Arc<AtomicU64>,
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
    pub(super) live_publish_failures: u64,
    /// Total number of pending-fire acknowledgement persistence failures.
    pub(super) ack_failures: u64,
    /// Pending fire claims already handed off to the live publish path in this
    /// broker process but still waiting for durable acknowledgement retry.
    pub(super) pending_ack_retries: HashMap<u64, PendingFireStates>,
    /// Rolling window of acknowledged handoff timestamps for executions-per-minute.
    pub(super) recent_acknowledgement_ms: VecDeque<u64>,
    /// Write options for schedule persistence.
    pub(super) write_policy: crate::domains::WritePolicy,
    pub(super) metrics: Option<ScheduleMetrics>,
}

/// Runtime body methods intentionally share names with their sink wrapper methods:
/// the wrapper crosses the mailbox, while the runtime body performs the work.
pub(super) struct ScheduleDomainRuntime<'a> {
    pub(super) core: &'a mut ScheduleFamilyState,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ScheduleRunNowOutcome {
    HandoffAccepted,
    NoLiveSubscriptions,
    NoHandoffAccepted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ScheduleRunNowResult {
    pub(crate) delivery_mode: crate::domains::schedule::ScheduleDeliveryMode,
    pub(crate) matched_subscriptions: usize,
    pub(crate) attempted_handoffs: usize,
    pub(crate) accepted_handoffs: usize,
    pub(crate) outcome: ScheduleRunNowOutcome,
}

impl ScheduleLiveCounts {
    pub(super) fn merge(mut self, other: &Self) -> Self {
        self.subscriptions = self.subscriptions.saturating_add(other.subscriptions);
        self.schedules = self.schedules.saturating_add(other.schedules);
        self.pending_fires = self.pending_fires.saturating_add(other.pending_fires);
        self.executions_per_minute += other.executions_per_minute;
        self.notify_failures = self.notify_failures.saturating_add(other.notify_failures);
        self.ack_failures = self.ack_failures.saturating_add(other.ack_failures);
        self.pending_ack_retries = self
            .pending_ack_retries
            .saturating_add(other.pending_ack_retries);
        self.oldest_pending_claim_age_seconds = self
            .oldest_pending_claim_age_seconds
            .max(other.oldest_pending_claim_age_seconds);
        self.overdue_normalizations = self
            .overdue_normalizations
            .saturating_add(other.overdue_normalizations);
        self
    }
}

pub(super) enum ScheduleDomainCommand {
    Deliver(Envelope),
    CleanupSession(u64, crossbeam_channel::Sender<()>),
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
    RunNow(
        String,
        Instant,
        crossbeam_channel::Sender<Result<Option<ScheduleRunNowResult>, String>>,
    ),
    PanicForFailpoint,
    #[cfg(test)]
    BlockForTests(
        crossbeam_channel::Sender<()>,
        crossbeam_channel::Receiver<()>,
    ),
    #[cfg(test)]
    InspectForTests(
        Box<dyn FnOnce(&mut ScheduleFamilyState) + Send>,
        crossbeam_channel::Sender<()>,
    ),
}

pub(crate) struct ScheduleDomain {
    pub(super) family_runtime: crate::runtime::FamilyActorPoolRuntime<ScheduleDomainCommand>,
    pub(super) route_families: Vec<crate::runtime::routing::RouteFamily>,
    pub(super) active: Arc<AtomicBool>,
    pub(super) config: ScheduleDomainConfig,
}

#[derive(Clone)]
pub(super) struct ScheduleDomainConfig {
    pub(super) store: crate::domains::schedule::ScheduleStore,
    pub(super) router: Arc<Router>,
    pub(super) admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
    pub(super) next_sub_id: Arc<AtomicU64>,
    pub(super) write_policy: crate::domains::WritePolicy,
    pub(super) metrics: Option<ScheduleMetrics>,
}
