//! Actor-owned mutation retries require Midge's pre-WAL L0 admission rejection.

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
        let mut txn = self.begin_mutation_transaction()?;
        prepare(&mut txn)?;
        self.commit_prepared_with_admission_budget(txn, QueueCommit::Ack, budget, prepare)
    }

    pub(super) fn commit_prepared_with_admission_retry(
        &self,
        txn: QueueTransaction,
        commit: QueueCommit,
        prepare: impl FnMut(&mut QueueTransaction) -> Result<(), String>,
    ) -> Result<(), String> {
        self.commit_prepared_with_admission_budget(txn, commit, Duration::from_secs(30), prepare)
    }

    fn begin_mutation_transaction(&self) -> Result<QueueTransaction, String> {
        self.persistence
            .store
            .begin(self.queue_key.family.id(), QueueTransactionMode::ReadWrite)
            .map_err(|error| format!("Failed to begin queue mutation transaction: {error}"))
    }

    fn commit_prepared_with_admission_budget(
        &self,
        mut txn: QueueTransaction,
        commit: QueueCommit,
        budget: Duration,
        mut prepare: impl FnMut(&mut QueueTransaction) -> Result<(), String>,
    ) -> Result<(), String> {
        let deadline = Instant::now() + budget;
        loop {
            match Self::commit_transaction_with_pressure_wait(
                txn,
                self.persistence.write_options(),
                commit,
                deadline.saturating_duration_since(Instant::now()),
            ) {
                Ok(()) => return Ok(()),
                Err(error)
                    if error.is_l0_admission_rejection(self.queue_key.family.id())
                        && Instant::now() < deadline =>
                {
                    // The actor owns this queue's mutation plan. Midge rejected it
                    // before WAL submission, so restaging cannot duplicate the write.
                    // Admission is checked again within the original wait budget.
                    txn = self.begin_mutation_transaction()?;
                    prepare(&mut txn)?;
                }
                Err(error) => return Err(error.to_string()),
            }
        }
    }
}
