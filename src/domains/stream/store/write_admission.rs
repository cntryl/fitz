use super::{
    family_to_storage_partition, PromotionTransactionFailure, StreamStore, StreamWriteMode,
};
use std::time::{Duration, Instant};

#[cfg(test)]
use std::sync::atomic::Ordering;

pub(super) struct StreamWriteAdmission {
    family: u32,
    deadline: Instant,
}

impl StreamWriteAdmission {
    pub(super) fn new(family: u64) -> Self {
        let budget = Duration::from_secs(30);
        #[cfg(test)]
        let budget = ADMISSION_BUDGET
            .with(std::cell::Cell::get)
            .unwrap_or(budget);
        Self {
            family: family_to_storage_partition(family),
            deadline: Instant::now() + budget,
        }
    }

    pub(super) fn wait(&self, storage: &crate::storage::FitzStorageEngine) -> Result<(), String> {
        let remaining = self.deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero()
            || !storage
                .wait_for_write_stall_clear(self.family, remaining)
                .map_err(|error| format!("Stream storage admission failed: {error:?}"))?
            || Instant::now() >= self.deadline
        {
            return Err(
                "Stream storage admission remained stalled for its 30-second budget".to_string(),
            );
        }
        Ok(())
    }

    pub(super) fn rejected(&self) {
        // Pending flush slots can briefly outlive the write-stall hint.
        // Yield to storage work without restarting the admission budget.
        std::thread::sleep(
            self.deadline
                .saturating_duration_since(Instant::now())
                .min(Duration::from_millis(1)),
        );
    }
}

#[cfg(test)]
std::thread_local! {
    static ADMISSION_BUDGET: std::cell::Cell<Option<Duration>> = const { std::cell::Cell::new(None) };
    static COMMIT_OUTCOMES: std::cell::RefCell<std::collections::VecDeque<Option<cntryl_midge::MidgeError>>> = const { std::cell::RefCell::new(std::collections::VecDeque::new()) };
}

#[cfg(test)]
pub(super) struct AdmissionBudgetGuard;

#[cfg(test)]
impl Drop for AdmissionBudgetGuard {
    fn drop(&mut self) {
        ADMISSION_BUDGET.with(|budget| budget.set(None));
    }
}

#[cfg(test)]
pub(super) fn override_admission_budget(budget: Duration) -> AdmissionBudgetGuard {
    ADMISSION_BUDGET.with(|current| {
        assert!(current.get().is_none());
        current.set(Some(budget));
    });
    AdmissionBudgetGuard
}

#[cfg(test)]
pub(super) struct CommitOutcomesGuard;

#[cfg(test)]
impl Drop for CommitOutcomesGuard {
    fn drop(&mut self) {
        COMMIT_OUTCOMES.with(|outcomes| outcomes.borrow_mut().clear());
    }
}

#[cfg(test)]
pub(super) fn inject_commit_outcomes(
    outcomes: impl IntoIterator<Item = Option<cntryl_midge::MidgeError>>,
) -> CommitOutcomesGuard {
    COMMIT_OUTCOMES.with(|pending| {
        assert!(pending.borrow().is_empty());
        pending.borrow_mut().extend(outcomes);
    });
    CommitOutcomesGuard
}

impl StreamStore {
    pub(super) fn commit_storage_transaction(
        transaction: cntryl_midge::Transaction,
        options: cntryl_midge::WriteOptions,
    ) -> cntryl_midge::MidgeResult<()> {
        #[cfg(test)]
        if let Some(Some(error)) =
            COMMIT_OUTCOMES.with(|outcomes| outcomes.borrow_mut().pop_front())
        {
            return Err(error);
        }
        transaction.commit(options)
    }
}

impl StreamStore {
    pub(super) fn commit_promotion_frontier_tx(
        &self,
        txn: cntryl_midge::Transaction,
        family: u64,
        mode: StreamWriteMode,
    ) -> Result<(), PromotionTransactionFailure> {
        let write_options = match mode {
            StreamWriteMode::Sync => self.sync_write_options,
            StreamWriteMode::Buffered => self.buffered_write_options,
        };
        #[cfg(test)]
        {
            let delay_ms = self
                .delay_next_promotion_frontier_commit_ms
                .swap(0, Ordering::AcqRel);
            std::thread::sleep(std::time::Duration::from_millis(delay_ms));
            let should_fail = self
                .fail_next_promotion_frontier_commit
                .swap(false, Ordering::AcqRel);

            if should_fail {
                return Err(PromotionTransactionFailure::Other(
                    "Injected stream commit failure".to_string(),
                ));
            }
            if self
                .fence_next_promotion_frontier_commit
                .swap(false, Ordering::AcqRel)
            {
                self.advance_family_writer_epoch(family)
                    .map_err(PromotionTransactionFailure::Other)?;
                return Err(PromotionTransactionFailure::WriteConflict);
            }
            if self
                .conflict_next_promotion_frontier_commit
                .swap(false, Ordering::AcqRel)
            {
                return Err(PromotionTransactionFailure::WriteConflict);
            }
        }
        Self::commit_storage_transaction(txn, write_options).map_err(|error| match error {
            cntryl_midge::MidgeError::WriteConflict(_) => {
                PromotionTransactionFailure::WriteConflict
            }
            other
                if crate::storage::write_admission::is_l0_admission_rejection(
                    &other,
                    family_to_storage_partition(family),
                ) =>
            {
                PromotionTransactionFailure::AdmissionRejected
            }
            other => PromotionTransactionFailure::Other(format!("midge commit error: {other:?}")),
        })?;

        Ok(())
    }
}
