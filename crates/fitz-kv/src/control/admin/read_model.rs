use super::model::KvTransaction;
use fitz_core::projection::{FamilyRow, FamilyRows};
use parking_lot::{RwLock, RwLockReadGuard};
use std::sync::Arc;
fn kv_transaction_session_mode(session_id: u64) -> String {
    format!("session:{session_id}:readwrite")
}

fn matches_realm(realm: Option<&str>, value: &str) -> bool {
    realm.is_none_or(|needle| value == needle)
}

fn collect_slice_matches<T: Clone + FamilyRow>(
    items: &FamilyRows<T>,
    include: impl Fn(&T) -> bool,
) -> Vec<T> {
    items.iter().filter(|item| include(item)).cloned().collect()
}

#[derive(Default)]
pub struct AdminReadModel {
    kv_transactions: RwLock<FamilyRows<KvTransaction>>,
}

impl AdminReadModel {
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn replace_kv_transactions(&self, transactions: Vec<KvTransaction>) {
        *self.kv_transactions.write() = transactions.into();
    }

    pub fn upsert_kv_transaction(&self, transaction: KvTransaction) {
        let tx_id = transaction.tx_id;
        let mode = transaction.mode.clone();
        self.kv_transactions
            .write()
            .replace_matching(transaction, |existing| {
                existing.tx_id == tx_id && existing.mode == mode
            });
    }

    pub fn remove_kv_transaction(&self, session_id: u64, tx_id: u64) {
        let session_mode = kv_transaction_session_mode(session_id);
        self.kv_transactions
            .write()
            .retain(|transaction| transaction.tx_id != tx_id || transaction.mode != session_mode);
    }

    pub fn remove_kv_transactions_for_session(&self, session_id: u64) {
        let session_mode = kv_transaction_session_mode(session_id);
        self.kv_transactions
            .write()
            .retain(|transaction| transaction.mode != session_mode);
    }

    pub fn kv_transactions(&self, realm: Option<&str>) -> Vec<KvTransaction> {
        let transactions = self.kv_transactions.read();
        collect_slice_matches(&transactions, |item| matches_realm(realm, &item.realm))
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn kv_transaction_count(&self) -> usize {
        self.kv_transactions.read().len()
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn kv_transaction_count_for_resource(
        &self,
        route_family: u64,
        realm: &str,
        area: &str,
        resource: &str,
    ) -> usize {
        self.kv_transactions
            .read()
            .iter()
            .filter(|transaction| {
                transaction.route_family == route_family
                    && transaction.realm == realm
                    && transaction.area == area
                    && transaction.resource == resource
            })
            .count()
    }

    #[doc(hidden)]
    pub fn kv_transactions_rows(&self) -> RwLockReadGuard<'_, FamilyRows<KvTransaction>> {
        self.kv_transactions.read()
    }
}
