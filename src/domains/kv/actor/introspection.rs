//! Read-only crate-internal views over live transaction state.

use super::KvActor;
use crate::domains::kv::KvResourceScope;

/// Named transaction data used only by actor-state regression tests.
#[cfg(test)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct KvTransactionSnapshot {
    pub(crate) tx_id: u64,
    pub(crate) scope: KvResourceScope,
}

impl KvActor {
    #[must_use]
    pub(crate) fn mutation_count_for_tx(&self, tx_id: u64) -> Option<u64> {
        self.transactions.get(&tx_id).map(|tx| tx.mutation_count)
    }

    #[must_use]
    pub(crate) fn resource_scope_for_tx(&self, tx_id: u64) -> Option<KvResourceScope> {
        self.transactions.get(&tx_id).map(|tx| tx.scope.clone())
    }

    #[must_use]
    #[cfg(test)]
    pub(crate) fn active_transaction_snapshots(&self) -> Vec<KvTransactionSnapshot> {
        self.transactions
            .iter()
            .map(|(tx_id, tx)| KvTransactionSnapshot {
                tx_id: *tx_id,
                scope: tx.scope.clone(),
            })
            .collect()
    }

    #[must_use]
    #[cfg(test)]
    pub(crate) fn transaction_count(&self) -> usize {
        self.transactions.len()
    }
}
