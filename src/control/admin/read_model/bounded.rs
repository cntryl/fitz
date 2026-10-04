//! Bounded family-selected copies of the shared administrative projections.
use super::{
    AdminReadModel, KvTransaction, LeaseInfo, NoticeRouteInfo, NoticeSubscription, QueueDeadLetter,
    QueueInflight, QueueInfo, RpcPendingRequest, RpcWorker, ScheduleInfo, SessionInfo, StreamInfo,
};

#[derive(Debug, Clone)]
pub(crate) struct AdminSnapshot {
    pub kv_transactions: Vec<KvTransaction>,
    pub streams: Vec<StreamInfo>,
    pub notice_subscriptions: Vec<NoticeSubscription>,
    pub notice_routes: Vec<NoticeRouteInfo>,
    pub queues: Vec<QueueInfo>,
    pub queue_inflight: Vec<QueueInflight>,
    pub queue_dead_letters: Vec<QueueDeadLetter>,
    pub rpc_workers: Vec<RpcWorker>,
    pub rpc_pending: Vec<RpcPendingRequest>,
    pub leases: Vec<LeaseInfo>,
    pub schedules: Vec<ScheduleInfo>,
    pub sessions: Vec<SessionInfo>,
    pub pending_fire_claims: usize,
    pub truncated: bool,
    pub limit_per_collection: usize,
}

fn copy_page<'a, T: Clone + 'a>(
    items: impl Iterator<Item = &'a T>,
    limit: usize,
    truncated: &mut bool,
) -> Vec<T> {
    let mut items = items;
    let page = items.by_ref().take(limit).cloned().collect();
    *truncated |= items.next().is_some();
    page
}

impl AdminReadModel {
    pub(crate) fn bounded_snapshot(&self, family: Option<u64>, limit: usize) -> AdminSnapshot {
        let mut truncated = false;
        let sessions = copy_page(self.sessions.read().scoped(family), limit, &mut truncated);
        let kv_transactions = copy_page(
            self.kv_transactions.read().scoped(family),
            limit,
            &mut truncated,
        );
        let streams = copy_page(self.streams.read().scoped(family), limit, &mut truncated);
        let notice_subscriptions = copy_page(
            self.notice_subscriptions.read().scoped(family),
            limit,
            &mut truncated,
        );
        let notice_routes = copy_page(
            self.notice_routes.read().scoped(family),
            limit,
            &mut truncated,
        );
        let queues = copy_page(self.queues.read().scoped(family), limit, &mut truncated);
        let queue_inflight = copy_page(
            self.queue_inflight.read().scoped(family),
            limit,
            &mut truncated,
        );
        let queue_dead_letters = copy_page(
            self.queue_dead_letters.read().scoped(family),
            limit,
            &mut truncated,
        );
        let rpc_workers = copy_page(
            self.rpc_workers.read().scoped(family),
            limit,
            &mut truncated,
        );
        let rpc_pending = copy_page(
            self.rpc_pending.read().scoped(family),
            limit,
            &mut truncated,
        );
        let (start, end) = family.map_or((0, u64::MAX), |value| (value, value));
        let leases = copy_page(
            self.leases
                .read()
                .range((start, String::new(), String::new(), String::new())..)
                .take_while(|(key, _)| key.0 <= end)
                .map(|(_, item)| item),
            limit,
            &mut truncated,
        );
        let schedules = copy_page(
            self.schedules
                .read()
                .range(
                    (
                        start,
                        String::new(),
                        String::new(),
                        String::new(),
                        String::new(),
                    )..,
                )
                .take_while(|(key, _)| key.0 <= end)
                .map(|(_, item)| item),
            limit,
            &mut truncated,
        );
        let pending_fire_claims = family.map_or_else(
            || self.schedule_pending_fire_counts.read().values().sum(),
            |family| {
                self.schedule_pending_fire_counts
                    .read()
                    .get(&family)
                    .copied()
                    .unwrap_or_default()
            },
        );
        AdminSnapshot {
            kv_transactions,
            streams,
            notice_subscriptions,
            notice_routes,
            queues,
            queue_inflight,
            queue_dead_letters,
            rpc_workers,
            rpc_pending,
            leases,
            schedules,
            sessions,
            pending_fire_claims,
            truncated,
            limit_per_collection: limit,
        }
    }

    pub(crate) fn bounded_sessions(
        &self,
        family: Option<u64>,
        limit: usize,
    ) -> (Vec<SessionInfo>, bool) {
        let mut truncated = false;
        let sessions = copy_page(self.sessions.read().scoped(family), limit, &mut truncated);
        (sessions, truncated)
    }
}

#[cfg(test)]
mod tests {
    use super::copy_page;
    use crate::control::admin::read_model::family_rows::{FamilyRow, FamilyRows, ResourceIdentity};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    struct Row {
        family: u64,
        clones: Arc<AtomicUsize>,
    }
    impl FamilyRow for Row {
        fn family(&self) -> u64 {
            self.family
        }
        fn inventory_identity(&self) -> Option<ResourceIdentity> {
            None
        }
    }
    impl Clone for Row {
        fn clone(&self) -> Self {
            assert_eq!(self.family, 1, "sibling family was copied");
            self.clones.fetch_add(1, Ordering::Relaxed);
            Self {
                family: self.family,
                clones: self.clones.clone(),
            }
        }
    }

    #[test]
    fn should_select_family_before_copying_a_bounded_page() {
        // Arrange
        let clones = Arc::new(AtomicUsize::new(0));
        let rows = (0..2000)
            .map(|_| Row {
                family: 2,
                clones: clones.clone(),
            })
            .chain((0..5).map(|_| Row {
                family: 1,
                clones: clones.clone(),
            }))
            .collect::<Vec<_>>();
        let rows = FamilyRows::from(rows);
        let mut truncated = false;

        // Act
        let page = copy_page(rows.scoped(Some(1)), 2, &mut truncated);

        // Assert
        assert_eq!(page.len(), 2);
        assert_eq!(clones.load(Ordering::Relaxed), 2);
        assert!(truncated);
    }
}
