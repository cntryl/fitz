use super::model::{LeaseAcquireRequest, LeaseDomainCommand, LeaseDomainSink};
use crate::domains::lease::protocol::{LeaseKey, LeaseResponse};
use std::time::Duration;

impl LeaseDomainSink {
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
}
