//! Transaction creation, completion, rollback, and idle expiry.

use super::{ActiveKvTx, KvActor, KvInventoryDelta};
use crate::auth::validate_realm_format;
use crate::domains::kv::{KvError, KvResourceScope, KvResponse, TxMode};
use cntryl_midge::TransactionMode;
use std::time::{Duration, Instant};

impl KvActor {
    pub(super) fn handle_begin(
        &mut self,
        scope: KvResourceScope,
        mode: TxMode,
        write_options: cntryl_midge::WriteOptions,
    ) -> KvResponse {
        if validate_realm_format(&scope.realm).is_err() {
            return KvResponse::Error {
                error: KvError::InvalidRealm,
            };
        }

        let Ok(column_family) = Self::resolve_column_family(scope.route_family) else {
            return KvResponse::Error {
                error: KvError::InvalidRouteFamily,
            };
        };
        let transaction_mode = match mode {
            TxMode::ReadOnly => TransactionMode::ReadOnly,
            TxMode::ReadWrite => TransactionMode::ReadWrite,
        };
        let Some(next_tx_id) = self.next_tx_id.checked_add(1) else {
            return KvResponse::Error {
                error: KvError::InvalidRequest("transaction ID space exhausted".to_string()),
            };
        };

        match self.store.begin_tx(column_family, transaction_mode) {
            Ok(tx) => {
                let tx_id = self.next_tx_id;
                self.next_tx_id = next_tx_id;
                let scoped_prefix =
                    Self::realm_resource_prefix(&scope.realm, &scope.area, &scope.resource);
                self.transactions.insert(
                    tx_id,
                    ActiveKvTx {
                        scope,
                        scoped_prefix,
                        column_family,
                        tx,
                        write_options,
                        mutation_count: 0,
                        last_activity: Instant::now(),
                        inventory_delta: KvInventoryDelta::default(),
                    },
                );
                KvResponse::BeginOk { tx_id }
            }
            Err(error) => KvResponse::Error {
                error: Self::map_midge_error(&error),
            },
        }
    }

    pub(super) fn handle_commit(&mut self, tx_id: u64, scope: &KvResourceScope) -> KvResponse {
        let Some(active) = self.transactions.get(&tx_id) else {
            return KvResponse::Error {
                error: KvError::InvalidTxId,
            };
        };
        if let Err(response) = Self::validate_operation_scope(active, scope) {
            return response;
        }

        let Some(mut active) = self.transactions.remove(&tx_id) else {
            return KvResponse::Error {
                error: KvError::InvalidTxId,
            };
        };
        let inventory_scope = active.scope.clone();
        let inventory_column_family = active.column_family;
        let inventory_delta = std::mem::take(&mut active.inventory_delta);
        let inventory_write_options = Self::inventory_write_options(active.write_options);
        match active.tx.commit(active.write_options) {
            Ok(()) => {
                if let Err(error) = Self::apply_inventory_delta(
                    &self.store,
                    inventory_column_family,
                    &inventory_scope,
                    &inventory_delta,
                    inventory_write_options,
                ) {
                    tracing::warn!(?error, "KV inventory estimate update failed");
                }
                KvResponse::CommitOk
            }
            Err(error) => KvResponse::Error {
                error: Self::map_midge_error(&error),
            },
        }
    }

    pub(super) fn handle_rollback(&mut self, tx_id: u64, scope: &KvResourceScope) -> KvResponse {
        let Some(active) = self.transactions.get(&tx_id) else {
            return KvResponse::Error {
                error: KvError::InvalidTxId,
            };
        };
        if let Err(response) = Self::validate_operation_scope(active, scope) {
            return response;
        }

        self.transactions.remove(&tx_id);
        KvResponse::RollbackOk
    }

    pub(crate) fn expire_idle_transactions(&mut self, ttl: Duration) -> Vec<u64> {
        let now = Instant::now();
        let expired = self
            .transactions
            .iter()
            .filter_map(|(tx_id, transaction)| {
                (now.saturating_duration_since(transaction.last_activity) >= ttl).then_some(*tx_id)
            })
            .collect::<Vec<_>>();
        for tx_id in &expired {
            self.transactions.remove(tx_id);
        }
        expired
    }

    pub(crate) fn rollback_transaction(&mut self, tx_id: u64) -> bool {
        self.transactions.remove(&tx_id).is_some()
    }
}
