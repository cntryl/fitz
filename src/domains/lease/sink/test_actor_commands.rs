use super::model::{LeaseAcquireRequest, LeaseDomainCommand, LeaseDomainSink};
use crate::domains::lease::protocol::{LeaseKey, LeaseResponse};
use std::sync::atomic::Ordering;
use std::time::Duration;

impl LeaseDomainSink {
    pub(super) fn is_active_for_tests(&self) -> bool {
        self.state.active.load(Ordering::Relaxed)
    }

    pub(super) fn watch_families_are_empty_for_tests(&self) -> bool {
        self.state.core.families.lock().is_empty()
    }

    pub(super) fn session_leases_contain_for_tests(&self, session_id: u64, key: &LeaseKey) -> bool {
        self.state
            .core
            .session_leases
            .lock()
            .get(&session_id)
            .is_some_and(|leases| leases.contains(key))
    }

    pub(super) fn pending_acquire_count_for_tests(&self, key: &LeaseKey) -> usize {
        self.state
            .core
            .pending_acquires
            .lock()
            .get(key)
            .map_or(0, std::collections::VecDeque::len)
    }

    pub(super) fn acquire_for_tests(&self, request: LeaseAcquireRequest) -> LeaseResponse {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        if self
            .actor
            .try_send_high_priority(LeaseDomainCommand::ApplyAcquireForTests(request, reply_tx))
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
            .actor
            .try_send_high_priority(LeaseDomainCommand::ApplyExtendForTests(
                key.clone(),
                owner_id.to_string(),
                fencing_token,
                ttl_secs,
                reply_tx,
            ))
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
            .actor
            .try_send_high_priority(LeaseDomainCommand::ExpireLeaseForTests(
                key.clone(),
                reply_tx,
            ))
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
            .actor
            .try_send_high_priority(LeaseDomainCommand::ReadPendingWaiterCountForTests(
                key.clone(),
                reply_tx,
            ))
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
            .actor
            .try_send_high_priority(LeaseDomainCommand::ApplyListForTests(
                family_id, pattern, cursor, limit, session_id, reply_tx,
            ))
            .is_err()
        {
            return LeaseResponse::Timeout;
        }

        reply_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap_or(LeaseResponse::Timeout)
    }
}
