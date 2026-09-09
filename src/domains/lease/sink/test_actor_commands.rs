use super::model::{LeaseAcquireRequest, LeaseDomain, LeaseDomainCommand, LeaseFamilyState};
use crate::domains::lease::protocol::{LeaseKey, LeaseResponse};
use std::sync::atomic::Ordering;
use std::time::Duration;

impl LeaseDomain {
    pub(super) fn is_active_for_tests(&self) -> bool {
        self.active.load(Ordering::Relaxed)
    }

    pub(super) fn watch_families_are_empty_for_tests(&self) -> bool {
        self.inspect_primary_family_for_tests(|state| state.families.is_empty())
    }

    pub(super) fn session_leases_contain_for_tests(&self, session_id: u64, key: &LeaseKey) -> bool {
        let key = key.clone();
        self.inspect_family_for_tests(key.family, move |state| {
            state
                .session_leases
                .get(&session_id)
                .is_some_and(|leases| leases.contains(&key))
        })
    }

    pub(super) fn pending_acquire_count_for_tests(&self, key: &LeaseKey) -> usize {
        let key = key.clone();
        self.inspect_family_for_tests(key.family, move |state| {
            state
                .pending_acquires
                .get(&key)
                .map_or(0, std::collections::VecDeque::len)
        })
    }

    fn inspect_primary_family_for_tests<T>(
        &self,
        inspect: impl FnOnce(&mut LeaseFamilyState) -> T + Send + 'static,
    ) -> T
    where
        T: Send + 'static,
    {
        self.inspect_family_for_tests(self.route_families[0], inspect)
    }

    fn inspect_family_for_tests<T>(
        &self,
        family: crate::runtime::routing::RouteFamily,
        inspect: impl FnOnce(&mut LeaseFamilyState) -> T + Send + 'static,
    ) -> T
    where
        T: Send + 'static,
    {
        let (result_tx, result_rx) = crossbeam_channel::bounded(1);
        let (done_tx, _done_rx) = crossbeam_channel::bounded(1);
        self.enqueue(
            family,
            crate::runtime::FamilyActorLane::Control,
            LeaseDomainCommand::InspectForTests(
                Box::new(move |state| {
                    let _ = result_tx.send(inspect(state));
                }),
                done_tx,
            ),
        )
        .expect("enqueue Lease family-state inspection");
        result_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("receive Lease family-state inspection")
    }

    pub(super) fn sweep_list_snapshots_for_tests(&self) {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        self.enqueue(
            self.route_families[0],
            crate::runtime::FamilyActorLane::Control,
            LeaseDomainCommand::SweepListSnapshotsForTests(reply_tx),
        )
        .expect("enqueue Lease list-snapshot sweep");
        reply_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("complete Lease list-snapshot sweep");
    }

    pub(super) fn acquire_for_tests(&self, request: LeaseAcquireRequest) -> LeaseResponse {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        if self
            .enqueue(
                request.route_family,
                crate::runtime::FamilyActorLane::Control,
                LeaseDomainCommand::ApplyAcquireForTests(request, reply_tx),
            )
            .is_err()
        {
            return LeaseResponse::Timeout;
        }

        reply_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap_or(LeaseResponse::Timeout)
    }

    pub(super) fn extend_for_tests(
        &self,
        key: &LeaseKey,
        owner_id: &str,
        fencing_token: u64,
        ttl_secs: u64,
    ) -> LeaseResponse {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        if self
            .enqueue(
                key.family,
                crate::runtime::FamilyActorLane::Control,
                LeaseDomainCommand::ApplyExtendForTests(
                    key.clone(),
                    owner_id.to_string(),
                    fencing_token,
                    ttl_secs,
                    reply_tx,
                ),
            )
            .is_err()
        {
            return LeaseResponse::Timeout;
        }

        reply_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap_or(LeaseResponse::Timeout)
    }

    pub(super) fn expire_lease_for_tests(&self, key: &LeaseKey) -> bool {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        if self
            .enqueue(
                key.family,
                crate::runtime::FamilyActorLane::Control,
                LeaseDomainCommand::ExpireLeaseForTests(key.clone(), reply_tx),
            )
            .is_err()
        {
            return false;
        }

        reply_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap_or(false)
    }

    pub(super) fn pending_waiter_count_for_tests(&self, key: &LeaseKey) -> usize {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        if self
            .enqueue(
                key.family,
                crate::runtime::FamilyActorLane::Control,
                LeaseDomainCommand::ReadPendingWaiterCountForTests(key.clone(), reply_tx),
            )
            .is_err()
        {
            return 0;
        }

        reply_rx.recv_timeout(Duration::from_secs(1)).unwrap_or(0)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn list_for_tests(
        &self,
        family_id: crate::runtime::routing::RouteFamily,
        pattern: crate::runtime::routing::Route,
        cursor: Option<crate::domains::lease::protocol::LeaseListCursor>,
        limit: Option<u32>,
        session_id: u64,
    ) -> LeaseResponse {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        if self
            .enqueue(
                family_id,
                crate::runtime::FamilyActorLane::Control,
                LeaseDomainCommand::ApplyListForTests(
                    family_id, pattern, cursor, limit, session_id, reply_tx,
                ),
            )
            .is_err()
        {
            return LeaseResponse::Timeout;
        }

        reply_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap_or(LeaseResponse::Timeout)
    }
}
