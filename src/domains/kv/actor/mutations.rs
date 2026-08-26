//! Transaction-scoped GET and mutation operations.

use super::KvActor;
use crate::domains::kv::{KvError, KvResourceScope, KvResponse};
use bytes::Bytes;

impl KvActor {
    pub(super) fn handle_get(
        &mut self,
        tx_id: u64,
        scope: &KvResourceScope,
        key: &Bytes,
    ) -> KvResponse {
        let active = match self.scoped_transaction_or_err(tx_id, scope) {
            Ok(tx) => tx,
            Err(error) => return error,
        };

        let scoped_key = Self::encode_scoped_key(&active.scoped_prefix, key);
        match active.tx.get(&scoped_key) {
            Ok(Some(value)) => KvResponse::GetResult {
                found: true,
                value: Some(value),
            },
            Ok(None) => KvResponse::GetResult {
                found: false,
                value: None,
            },
            Err(error) => KvResponse::Error {
                error: Self::map_midge_error(&error),
            },
        }
    }

    pub(super) fn handle_put(
        &mut self,
        tx_id: u64,
        scope: &KvResourceScope,
        key: &Bytes,
        value: &Bytes,
    ) -> KvResponse {
        let active = match self.scoped_transaction_or_err(tx_id, scope) {
            Ok(tx) => tx,
            Err(error) => return error,
        };

        let scoped_key = Self::encode_scoped_key(&active.scoped_prefix, key);
        match active.tx.put(scoped_key, value.to_vec(), None) {
            Ok(()) => {
                active.mutation_count = active.mutation_count.saturating_add(1);
                active.inventory_delta.mark_incomplete();
                KvResponse::PutOk
            }
            Err(error) => KvResponse::Error {
                error: Self::map_midge_error(&error),
            },
        }
    }

    pub(super) fn handle_insert(
        &mut self,
        tx_id: u64,
        scope: &KvResourceScope,
        key: &Bytes,
        value: &Bytes,
    ) -> KvResponse {
        let active = match self.scoped_transaction_or_err(tx_id, scope) {
            Ok(tx) => tx,
            Err(error) => return error,
        };

        let scoped_key = Self::encode_scoped_key(&active.scoped_prefix, key);
        match active.tx.get(&scoped_key) {
            Ok(Some(_)) => KvResponse::Error {
                error: KvError::AlreadyExists,
            },
            Ok(None) => match active.tx.put(scoped_key, value.to_vec(), None) {
                Ok(()) => {
                    active.mutation_count = active.mutation_count.saturating_add(1);
                    active
                        .inventory_delta
                        .record_insert(key, key.len() + value.len());
                    KvResponse::InsertOk
                }
                Err(error) => KvResponse::Error {
                    error: Self::map_midge_error(&error),
                },
            },
            Err(error) => KvResponse::Error {
                error: Self::map_midge_error(&error),
            },
        }
    }

    pub(super) fn handle_delete(
        &mut self,
        tx_id: u64,
        scope: &KvResourceScope,
        key: &Bytes,
    ) -> KvResponse {
        let active = match self.scoped_transaction_or_err(tx_id, scope) {
            Ok(tx) => tx,
            Err(error) => return error,
        };

        let scoped_key = Self::encode_scoped_key(&active.scoped_prefix, key);
        match active.tx.delete(scoped_key) {
            Ok(()) => {
                active.mutation_count = active.mutation_count.saturating_add(1);
                active.inventory_delta.mark_incomplete();
                KvResponse::DeleteOk
            }
            Err(error) => KvResponse::Error {
                error: Self::map_midge_error(&error),
            },
        }
    }

    pub(super) fn handle_delete_range(
        &mut self,
        tx_id: u64,
        scope: &KvResourceScope,
        start: &Bytes,
        end: &Bytes,
    ) -> KvResponse {
        let active = match self.scoped_transaction_or_err(tx_id, scope) {
            Ok(tx) => tx,
            Err(error) => return error,
        };
        if start >= end {
            return KvResponse::Error {
                error: KvError::InvalidRequest("start must be less than end".to_string()),
            };
        }

        let scoped_start = Self::encode_scoped_key(&active.scoped_prefix, start);
        let scoped_end = Self::encode_scoped_key(&active.scoped_prefix, end);
        match active.tx.delete_range(scoped_start, scoped_end) {
            Ok(()) => {
                active.mutation_count = active.mutation_count.saturating_add(1);
                active.inventory_delta.mark_incomplete();
                KvResponse::DeleteRangeOk
            }
            Err(error) => KvResponse::Error {
                error: Self::map_midge_error(&error),
            },
        }
    }
}
