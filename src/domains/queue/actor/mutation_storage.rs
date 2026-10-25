//! Actor-owned mutation retries require Midge's pre-WAL L0 admission rejection.

use super::recovery_store::{QueueStoreError, QueueTransaction, QueueTransactionMode};
use super::{QueueActor, QueueCommit};
use std::time::{Duration, Instant};

pub(super) enum QueueMutationError {
    Preparation(String),
    Storage { message: String, timed_out: bool },
}

impl QueueMutationError {
    pub(super) fn with_context(self, context: &str) -> Self {
        match self {
            Self::Preparation(message) => Self::Preparation(format!("{context}: {message}")),
            Self::Storage { message, timed_out } => Self::Storage {
                message: format!("{context}: {message}"),
                timed_out,
            },
        }
    }

    pub(super) fn into_response(self) -> crate::domains::queue::QueueResponse {
        match self {
            Self::Preparation(message)
            | Self::Storage {
                message,
                timed_out: false,
            } => crate::domains::queue::QueueResponse::Error { message },
            Self::Storage {
                message,
                timed_out: true,
            } => crate::domains::queue::QueueResponse::Timeout { message },
        }
    }
}

impl std::fmt::Display for QueueMutationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Preparation(message) | Self::Storage { message, .. } => {
                formatter.write_str(message)
            }
        }
    }
}

impl From<String> for QueueMutationError {
    fn from(message: String) -> Self {
        Self::Preparation(message)
    }
}

impl From<QueueStoreError> for QueueMutationError {
    fn from(error: QueueStoreError) -> Self {
        let timed_out = error.is_timeout();
        Self::Storage {
            message: error.to_string(),
            timed_out,
        }
    }
}

impl QueueActor {
    pub(super) fn commit_ack_with_admission_retry(
        &self,
        prepare: impl FnMut(&mut QueueTransaction) -> Result<(), String>,
    ) -> Result<(), QueueMutationError> {
        self.commit_ack_with_admission_budget(Duration::from_secs(30), prepare)
    }

    pub(super) fn commit_ack_with_admission_budget(
        &self,
        budget: Duration,
        mut prepare: impl FnMut(&mut QueueTransaction) -> Result<(), String>,
    ) -> Result<(), QueueMutationError> {
        let txn = super::ack_timing::measure(true, super::ack_timing::Phase::Preparation, || {
            let mut txn = self.begin_mutation_transaction()?;
            prepare(&mut txn)?;
            Ok::<_, QueueMutationError>(txn)
        })?;
        self.commit_prepared_with_admission_budget(txn, QueueCommit::Ack, budget, prepare)
    }

    pub(super) fn commit_prepared_with_admission_retry(
        &self,
        txn: QueueTransaction,
        commit: QueueCommit,
        prepare: impl FnMut(&mut QueueTransaction) -> Result<(), String>,
    ) -> Result<(), QueueMutationError> {
        self.commit_prepared_with_admission_budget(txn, commit, Duration::from_secs(30), prepare)
    }

    fn begin_mutation_transaction(&self) -> Result<QueueTransaction, QueueMutationError> {
        self.persistence
            .store
            .begin(self.queue_key.family.id(), QueueTransactionMode::ReadWrite)
            .map_err(|error| {
                QueueMutationError::from(error)
                    .with_context("Failed to begin queue mutation transaction")
            })
    }

    fn commit_prepared_with_admission_budget(
        &self,
        mut txn: QueueTransaction,
        commit: QueueCommit,
        budget: Duration,
        mut prepare: impl FnMut(&mut QueueTransaction) -> Result<(), String>,
    ) -> Result<(), QueueMutationError> {
        let deadline = Instant::now() + budget;
        loop {
            match Self::commit_transaction_with_pressure_wait(
                txn,
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
                    crate::observability::counter_inc(
                        crate::domains::queue::metrics::METRIC_L0_ADMISSION_RETRIES_TOTAL,
                    );
                    txn = super::ack_timing::measure(
                        matches!(commit, QueueCommit::Ack),
                        super::ack_timing::Phase::Preparation,
                        || {
                            let mut txn = self.begin_mutation_transaction()?;
                            prepare(&mut txn)?;
                            Ok::<_, QueueMutationError>(txn)
                        },
                    )?;
                }
                Err(error) => {
                    if error.is_admission_pressure(self.queue_key.family.id())
                        && Instant::now() >= deadline
                    {
                        crate::observability::counter_inc(
                            crate::domains::queue::metrics::METRIC_ADMISSION_BUDGET_EXHAUSTED_TOTAL,
                        );
                    }
                    return Err(QueueMutationError::from(error));
                }
            }
        }
    }
}
