//! Notice domain sink and core state definitions.
//!
//! Notice subscriptions are broker-local in-memory state only. They are
//! session-scoped, cleaned up on disconnect, and are never replayed or
//! restored after broker restart.

use super::{
    cleanup::CleanedUpSessions, NoticeDeliveryJob, NoticeDomainCommand, NoticeMetrics,
    NoticeRouteStats, NoticeRouteStatsKey, NoticeSubscription, RoutedSubscriptionSet,
};
use crate::runtime::{ManagedActor, Router};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::Arc;

/// Live notice pub/sub state for the current broker process.
///
/// This core owns the authoritative in-memory subscription index used for
/// delivery and admin snapshots. State disappears on session cleanup or broker
/// restart and is never durably recovered or replayed.
pub(super) struct NoticeDomainCore {
    /// Actor-owned single-writer state. The mutex supports immutable facade
    /// methods; production mutation remains serialized by `NoticeDomainActor`.
    pub(super) families: Mutex<
        HashMap<crate::runtime::routing::RouteFamily, RoutedSubscriptionSet<NoticeSubscription>>,
    >,
    /// Actor-owned single-writer route telemetry guarded for facade reads.
    pub(super) route_stats: Mutex<HashMap<NoticeRouteStatsKey, NoticeRouteStats>>,
    pub(super) next_sub_id: AtomicU64,
    pub(super) router: Arc<Router>,
    pub(super) admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
    pub(super) admin_snapshot_dirty: AtomicBool,
    pub(super) metrics: Option<NoticeMetrics>,
    pub(super) active: AtomicBool,
    /// Sessions disconnect cleanup has already run for; guards against a
    /// stale queued request recreating a subscription. See `cleanup.rs`.
    pub(super) cleaned_up_sessions: Mutex<CleanedUpSessions>,
    /// One bounded, ordered delivery lane per route family prevents a blocked
    /// subscriber from stalling unrelated families on the Notice actor.
    pub(super) delivery_workers: Mutex<
        HashMap<crate::runtime::routing::RouteFamily, crossbeam_channel::Sender<NoticeDeliveryJob>>,
    >,
}

pub struct NoticeDomainSink {
    pub(super) core: Arc<NoticeDomainCore>,
    pub(super) actor: ManagedActor<NoticeDomainCommand>,
}
