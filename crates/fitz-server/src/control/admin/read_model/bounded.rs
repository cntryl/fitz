//! Bounded family-selected copies of the shared administrative projections.
use super::{
    AdminReadModel, KvTransaction, LeaseInfo, NoticeRouteInfo, NoticeSubscription, QueueDeadLetter,
    QueueInflight, QueueInfo, RpcPendingRequest, RpcWorker, ScheduleInfo, SessionInfo,
    StreamAreaWatermarkDetail, StreamInfo, StreamRealmWatermarkDetail,
};

#[derive(Debug, Clone, Default)]
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
    pub(crate) fn bounded_stream_realm_watermarks(
        &self,
        limit: usize,
    ) -> (Vec<StreamRealmWatermarkDetail>, bool) {
        let watermarks = self.stream.stream_realm_watermarks_rows();
        let mut remaining = limit;
        let mut truncated = watermarks.len() > limit;
        let mut bounded = Vec::with_capacity(limit.min(watermarks.len()));
        for detail in watermarks.values().take(limit) {
            let mut detail = detail.clone();
            if detail.family_watermarks.len() > remaining {
                detail.family_watermarks.truncate(remaining);
                truncated = true;
            }
            remaining = remaining.saturating_sub(detail.family_watermarks.len());
            bounded.push(detail);
        }
        (bounded, truncated)
    }

    pub(crate) fn bounded_snapshot(&self, family: Option<u64>, limit: usize) -> AdminSnapshot {
        let mut truncated = false;
        macro_rules! page {
            ($rows:expr) => {
                copy_page($rows, limit, &mut truncated)
            };
        }
        let sessions = page!(self.sessions.read().scoped(family));
        let kv_transactions = page!(self.kv.kv_transactions_rows().scoped(family));
        let streams = page!(self.stream.streams_rows().scoped(family));
        let notice_subscriptions = page!(self.notice.notice_subscriptions_rows().scoped(family));
        let notice_routes = page!(self.notice.notice_routes_rows().scoped(family));
        let queues = page!(self.queue.queues_rows().scoped(family));
        let queue_inflight = page!(self.queue.queue_inflight_rows().scoped(family));
        let queue_dead_letters = page!(self.queue.queue_dead_letters_rows().scoped(family));
        let rpc_workers = page!(self.rpc.rpc_workers_rows().scoped(family));
        let rpc_pending = page!(self.rpc.rpc_pending_rows().scoped(family));
        let (start, end) = family.map_or((0, u64::MAX), |value| (value, value));
        let leases = copy_page(
            self.lease
                .leases_rows()
                .range((start, String::new(), String::new(), String::new())..)
                .take_while(|(key, _)| key.0 <= end)
                .map(|(_, item)| item),
            limit,
            &mut truncated,
        );
        let schedules = copy_page(
            self.schedule
                .schedules_rows()
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
            || {
                let counts = self.schedule.schedule_pending_fire_counts_rows();
                truncated |= counts.len() > limit;
                counts.values().take(limit).sum()
            },
            |family| {
                self.schedule
                    .schedule_pending_fire_counts_rows()
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

    pub(crate) fn bounded_stream_area_watermarks(
        &self,
        limit: usize,
    ) -> (Vec<StreamAreaWatermarkDetail>, bool) {
        let watermarks = self.stream.stream_area_watermarks_rows();
        let mut remaining = limit;
        let mut truncated = watermarks.len() > limit;
        let mut bounded = Vec::with_capacity(limit.min(watermarks.len()));
        for detail in watermarks.values().take(limit) {
            let mut detail = detail.clone();
            if detail.family_watermarks.len() > remaining {
                detail.family_watermarks.truncate(remaining);
                truncated = true;
            }
            remaining = remaining.saturating_sub(detail.family_watermarks.len());
            bounded.push(detail);
        }
        (bounded, truncated)
    }
}

#[cfg(test)]
mod tests {
    use super::copy_page;
    use fitz_core::projection::{FamilyRow, FamilyRows, ResourceIdentity};
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
