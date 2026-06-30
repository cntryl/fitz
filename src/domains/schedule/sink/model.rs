pub(super) use crate::domains::schedule::ScheduleMetrics;
pub(super) use crate::runtime::{DeliveryError, Envelope, MailboxSink, Router};
pub(super) use parking_lot::Mutex;
pub(super) use std::collections::hash_map::Entry;
pub(super) use std::collections::{HashMap, HashSet, VecDeque};
pub(super) use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
pub(super) use std::sync::Arc;
pub(super) use std::time::Instant;

#[cfg_attr(feature = "bench-no-snapshot", allow(dead_code))]
pub(super) const SCHEDULE_ADMIN_SNAPSHOT_INTERVAL_US: u64 = 250_000;
pub(super) const SCHEDULE_PENDING_CLAIM_TTL_MS: u64 = 24 * 60 * 60 * 1000;
pub(super) const SCHEDULE_PENDING_CLAIM_CLEANUP_INTERVAL_MS: u64 = 60_000;

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
    pub(super) route: String,
    pub(super) session_id: u64,
    pub(super) subscription_id: u64,
    pub(super) subscriber: crate::runtime::routing::RouteAddress,
}

pub(super) struct ScheduleSubscriptionSet {
    pub(super) subscriptions: HashMap<u64, ScheduleSubscription>,
    pub(super) session_routes: HashMap<u64, HashMap<String, u64>>,
    pub(super) exact_routes: HashMap<String, Vec<u64>>,
}

impl ScheduleSubscriptionSet {
    pub(super) fn new() -> Self {
        Self {
            subscriptions: HashMap::new(),
            session_routes: HashMap::new(),
            exact_routes: HashMap::new(),
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.subscriptions.is_empty()
    }

    pub(super) fn subscription_count(&self) -> usize {
        self.subscriptions.len()
    }

    pub(super) fn find_existing_id(&self, session_id: u64, route: &str) -> Option<u64> {
        self.session_routes
            .get(&session_id)
            .and_then(|routes| routes.get(route).copied())
    }

    pub(super) fn insert(&mut self, subscription: ScheduleSubscription) {
        let subscription_id = subscription.subscription_id;
        let session_id = subscription.session_id;
        let route = subscription.route.clone();

        self.session_routes
            .entry(session_id)
            .or_default()
            .insert(route.clone(), subscription_id);
        self.exact_routes
            .entry(route)
            .or_default()
            .push(subscription_id);
        self.subscriptions.insert(subscription_id, subscription);
    }

    pub(super) fn remove_session_route(&mut self, session_id: u64, route: &str) -> usize {
        let Some(subscription_id) = self.find_existing_id(session_id, route) else {
            return 0;
        };

        usize::from(self.remove_subscription(subscription_id))
    }

    pub(super) fn remove_session(&mut self, session_id: u64) -> usize {
        let Some(routes) = self.session_routes.remove(&session_id) else {
            return 0;
        };

        let removed_ids: Vec<u64> = routes.into_values().collect();
        for subscription_id in &removed_ids {
            self.remove_subscription(*subscription_id);
        }

        removed_ids.len()
    }

    pub(super) fn for_each_route(
        &self,
        route: &str,
        mut visit: impl FnMut(&ScheduleSubscription),
    ) -> usize {
        let Some(subscription_ids) = self.exact_routes.get(route) else {
            return 0;
        };

        let mut matched = 0;
        for subscription_id in subscription_ids {
            if let Some(subscription) = self.subscriptions.get(subscription_id) {
                matched += 1;
                visit(subscription);
            }
        }

        matched
    }

    pub(super) fn remove_subscription(&mut self, subscription_id: u64) -> bool {
        let Some(subscription) = self.subscriptions.remove(&subscription_id) else {
            return false;
        };

        let session_routes_empty =
            if let Some(routes) = self.session_routes.get_mut(&subscription.session_id) {
                routes.remove(subscription.route.as_str());
                routes.is_empty()
            } else {
                false
            };
        if session_routes_empty {
            self.session_routes.remove(&subscription.session_id);
        }

        let route_entries_empty =
            if let Some(route_entries) = self.exact_routes.get_mut(subscription.route.as_str()) {
                route_entries.retain(|id| *id != subscription_id);
                route_entries.is_empty()
            } else {
                false
            };
        if route_entries_empty {
            self.exact_routes.remove(subscription.route.as_str());
        }

        true
    }
}

pub struct ScheduleDomainSink {
    pub(super) store: crate::storage::FitzStorageEngine,
    pub(super) actors: Mutex<
        HashMap<crate::runtime::routing::RouteFamily, crate::domains::schedule::ScheduleActor>,
    >,
    pub(super) sub_families: Mutex<HashMap<u64, ScheduleSubscriptionSet>>,
    pub(super) next_sub_id: AtomicU64,
    pub(super) router: Arc<Router>,
    #[cfg_attr(feature = "bench-no-snapshot", allow(dead_code))]
    pub(super) admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
    pub(super) active: AtomicBool,
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
    pub(super) pending_ack_retries: Mutex<HashMap<u64, HashSet<PendingFireKey>>>,
    /// Maximum age for a pending claimed fire before cleanup removes it.
    pub(super) pending_claim_ttl_ms: u64,
    /// Last monotonic cleanup sweep time, measured relative to `snapshot_epoch`.
    pub(super) last_pending_claim_cleanup_elapsed_ms: AtomicU64,
    /// Rolling window of acknowledged handoff timestamps for the legacy
    /// executions-per-minute metric.
    pub(super) recent_acknowledgement_ms: Mutex<VecDeque<u64>>,
    /// Write options for schedule persistence.
    pub(super) write_options: cntryl_midge::WriteOptions,
    pub(super) metrics: Option<ScheduleMetrics>,
}
