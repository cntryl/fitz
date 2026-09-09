//! Family-owned Notice state and immutable observation snapshots.

use super::{
    NoticeDeliveryJob, NoticeDomainCommand, NoticeMetrics, NoticeRouteStats, NoticeRouteStatsKey,
    NoticeSubscription, RoutedSubscriptionSet,
};
use crate::runtime::routing::RouteFamily;
use crate::runtime::{CleanedUpSessions, Router};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::Arc;

#[derive(Clone)]
pub(super) struct NoticeDomainConfig {
    pub(super) next_sub_id: Arc<AtomicU64>,
    pub(super) router: Arc<Router>,
    pub(super) admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
    pub(super) metrics: Option<NoticeMetrics>,
    pub(super) active: Arc<AtomicBool>,
}

/// Mutable Notice state owned exclusively by one family worker.
pub(super) struct NoticeFamilyState {
    pub(super) family: RouteFamily,
    pub(super) families: HashMap<RouteFamily, RoutedSubscriptionSet<NoticeSubscription>>,
    pub(super) route_stats: HashMap<NoticeRouteStatsKey, NoticeRouteStats>,
    pub(super) next_sub_id: Arc<AtomicU64>,
    pub(super) router: Arc<Router>,
    pub(super) admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
    pub(super) metrics: Option<NoticeMetrics>,
    pub(super) active: Arc<AtomicBool>,
    pub(super) cleaned_up_sessions: CleanedUpSessions,
    pub(super) delivery_workers: HashMap<RouteFamily, crossbeam_channel::Sender<NoticeDeliveryJob>>,
}

impl NoticeFamilyState {
    pub(super) fn new(family: RouteFamily, config: &NoticeDomainConfig) -> Self {
        Self {
            family,
            families: HashMap::new(),
            route_stats: HashMap::with_capacity(64),
            next_sub_id: config.next_sub_id.clone(),
            router: config.router.clone(),
            admin_read_model: config.admin_read_model.clone(),
            metrics: config.metrics.clone(),
            active: config.active.clone(),
            cleaned_up_sessions: CleanedUpSessions::new(
                crate::domains::DOMAIN_ACTOR_MAILBOX_CAPACITY,
            ),
            delivery_workers: HashMap::new(),
        }
    }
}

pub(crate) struct NoticeDomain {
    pub(super) config: NoticeDomainConfig,
    pub(super) family_runtime: crate::runtime::FamilyActorPoolRuntime<NoticeDomainCommand>,
    pub(super) family_families: Vec<RouteFamily>,
}
