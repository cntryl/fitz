//! Transaction lookup, activity tracking, and operation-scope validation.

use super::{ActiveKvTx, KvActor};
use crate::domains::kv::{KvError, KvResourceScope, KvResponse};
use std::time::Instant;

impl KvActor {
    pub(super) fn scoped_transaction_or_err(
        &mut self,
        tx_id: u64,
        scope: &KvResourceScope,
    ) -> Result<&mut ActiveKvTx, KvResponse> {
        let transaction = self
            .transactions
            .get_mut(&tx_id)
            .ok_or_else(|| KvResponse::Error {
                error: KvError::InvalidTxId,
            })?;
        transaction.last_activity = Instant::now();
        Self::validate_operation_scope(transaction, scope)?;
        Ok(transaction)
    }

    pub(super) fn validate_operation_scope(
        active: &ActiveKvTx,
        scope: &KvResourceScope,
    ) -> Result<(), KvResponse> {
        if scope.route_family != active.scope.route_family {
            return Err(KvResponse::Error {
                error: KvError::InvalidRouteFamily,
            });
        }
        if scope.realm != active.scope.realm {
            return Err(KvResponse::Error {
                error: KvError::RealmMismatch,
            });
        }
        if scope.area != active.scope.area || scope.resource != active.scope.resource {
            return Err(KvResponse::Error {
                error: KvError::TxScopeViolation {
                    expected: format!("{}/{}", active.scope.area, active.scope.resource),
                    actual: format!("{}/{}", scope.area, scope.resource),
                },
            });
        }
        Ok(())
    }
}
