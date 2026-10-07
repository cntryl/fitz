//! ACK mutation retries are limited to Midge's pre-WAL L0 admission rejection.

use super::recovery_store::{QueueTransaction, QueueTransactionMode};
use super::{QueueActor, QueueCommit};
use std::time::{Duration, Instant};

impl QueueActor {
    pub(super) fn commit_ack_with_admission_retry(
        &self,
        prepare: impl FnMut(&mut QueueTransaction) -> Result<(), String>,
    ) -> Result<(), String> {
        self.commit_ack_with_admission_budget(Duration::from_secs(30), prepare)
    }

    pub(super) fn commit_ack_with_admission_budget(
        &self,
        budget: Duration,
        mut prepare: impl FnMut(&mut QueueTransaction) -> Result<(), String>,
    ) -> Result<(), String> {
        let deadline = Instant::now() + budget;
        loop {
            let mut txn = self
                .persistence
                .store
                .begin(self.queue_key.family.id(), QueueTransactionMode::ReadWrite)
                .map_err(|error| format!("Failed to begin queue ACK transaction: {error}"))?;
            prepare(&mut txn)?;
            match Self::commit_transaction_with_pressure_wait(
                txn,
                self.persistence.write_options(),
                QueueCommit::Ack,
                deadline.saturating_duration_since(Instant::now()),
            ) {
                Ok(()) => return Ok(()),
                Err(error)
                    if error.is_l0_admission_rejection(self.queue_key.family.id())
                        && Instant::now() < deadline =>
                {
                    // The actor owns this queue's mutation plan. Midge rejected it
                    // before WAL submission, so restaging it cannot duplicate an ACK.
                    // Admission is checked again within the original wait budget.
                }
                Err(error) => return Err(error.to_string()),
            }
        }
    }
}
