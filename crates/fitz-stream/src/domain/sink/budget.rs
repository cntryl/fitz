//! Synchronous admission credits whose lifetime follows the retained work.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Clone)]
pub(super) struct Budget(Arc<BudgetState>);

struct BudgetState {
    limit: usize,
    used: AtomicUsize,
}

impl Budget {
    pub(super) fn new(limit: usize) -> Self {
        Self(Arc::new(BudgetState {
            limit,
            used: AtomicUsize::new(0),
        }))
    }

    pub(super) fn reserve(&self, amount: usize) -> Option<Reservation> {
        self.0
            .used
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                used.checked_add(amount)
                    .filter(|&next| next <= self.0.limit)
            })
            .ok()?;
        Some(Reservation {
            budget: self.clone(),
            amount,
        })
    }

    #[cfg(any(test, feature = "benchkit"))]
    pub(super) fn used(&self) -> usize {
        self.0.used.load(Ordering::Relaxed)
    }
}

pub(super) struct Reservation {
    budget: Budget,
    amount: usize,
}

impl Reservation {
    pub(super) const fn amount(&self) -> usize {
        self.amount
    }

    pub(super) fn release(&mut self, amount: usize) {
        self.amount -= amount;
        self.budget.0.used.fetch_sub(amount, Ordering::Relaxed);
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.release(self.amount);
    }
}
