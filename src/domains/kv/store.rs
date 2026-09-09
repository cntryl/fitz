//! Sole Midge boundary for KV data transactions.

use bytes::Bytes;
use std::sync::Arc;

use super::{KvError, TxMode};
use crate::domains::WritePolicy;

#[derive(Clone)]
pub(crate) struct KvStore {
    engine: crate::storage::FitzStorageEngine,
}

pub(crate) struct KvTransaction {
    inner: cntryl_midge::Transaction,
}

impl KvStore {
    pub(crate) fn new(engine: Arc<cntryl_midge::Engine>) -> Self {
        Self {
            engine: crate::storage::FitzStorageEngine::new(engine),
        }
    }

    pub(crate) fn begin(&self, column_family: u32, mode: TxMode) -> Result<KvTransaction, KvError> {
        let mode = match mode {
            TxMode::ReadOnly => cntryl_midge::TransactionMode::ReadOnly,
            TxMode::ReadWrite => cntryl_midge::TransactionMode::ReadWrite,
        };
        self.engine
            .begin_tx(column_family, mode)
            .map(|inner| KvTransaction { inner })
            .map_err(|error| Self::map_error(&error))
    }

    pub(crate) fn column_families(&self) -> Result<Vec<u32>, KvError> {
        self.engine
            .list_column_families()
            .map(|families| families.into_iter().map(|family| family.id()).collect())
            .map_err(|error| Self::map_error(&error))
    }

    pub(crate) fn map_error(error: &cntryl_midge::MidgeError) -> KvError {
        match error {
            cntryl_midge::MidgeError::WriteConflict(_) | cntryl_midge::MidgeError::Aborted(_) => {
                KvError::Conflict(error.to_string())
            }
            cntryl_midge::MidgeError::Io(_)
            | cntryl_midge::MidgeError::NoSpace(_)
            | cntryl_midge::MidgeError::WriteStall(_)
            | cntryl_midge::MidgeError::LeaseHeld(_)
            | cntryl_midge::MidgeError::LeaseUnavailable(_)
            | cntryl_midge::MidgeError::Busy(_)
            | cntryl_midge::MidgeError::Timeout(_) => {
                KvError::BackendUnavailable(error.to_string())
            }
            cntryl_midge::MidgeError::NotFound
            | cntryl_midge::MidgeError::InvalidArgument(_)
            | cntryl_midge::MidgeError::Corruption(_)
            | cntryl_midge::MidgeError::NotSupported(_)
            | cntryl_midge::MidgeError::Internal(_)
            | cntryl_midge::MidgeError::InvalidPath
            | cntryl_midge::MidgeError::RecoveryFailed(_)
            | cntryl_midge::MidgeError::CompatibilityError(_)
            | cntryl_midge::MidgeError::MemoryModeViolation(_)
            | cntryl_midge::MidgeError::Fenced(_)
            | cntryl_midge::MidgeError::LeaseIndeterminate(_)
            | cntryl_midge::MidgeError::LeaseEpochExhausted
            | cntryl_midge::MidgeError::ResourceLimit(_) => {
                KvError::BackendError(error.to_string())
            }
        }
    }
}

impl From<Arc<cntryl_midge::Engine>> for KvStore {
    fn from(engine: Arc<cntryl_midge::Engine>) -> Self {
        Self::new(engine)
    }
}

impl KvTransaction {
    pub(crate) fn get(&self, key: &[u8]) -> Result<Option<Bytes>, KvError> {
        self.inner
            .get(key)
            .map_err(|error| KvStore::map_error(&error))
    }

    pub(crate) fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), KvError> {
        self.inner
            .put(key, value, None)
            .map_err(|error| KvStore::map_error(&error))
    }

    pub(crate) fn delete(&mut self, key: Vec<u8>) -> Result<(), KvError> {
        self.inner
            .delete(key)
            .map_err(|error| KvStore::map_error(&error))
    }

    pub(crate) fn delete_range(&mut self, start: Vec<u8>, end: Vec<u8>) -> Result<(), KvError> {
        self.inner
            .delete_range(start, end)
            .map_err(|error| KvStore::map_error(&error))
    }

    pub(crate) fn scan(
        &self,
        prefix: &[u8],
        start: Vec<u8>,
        end: Vec<u8>,
        limit: usize,
        reverse: bool,
    ) -> Result<Vec<(Bytes, Bytes)>, KvError> {
        let mut query = cntryl_midge::Query::new()
            .prefix(Bytes::copy_from_slice(prefix))
            .start_key(Bytes::from(start))
            .end_key(Bytes::from(end))
            .limit(limit);
        if reverse {
            query = query.reverse();
        }
        self.inner
            .scan(&query)
            .map_err(|error| KvStore::map_error(&error))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| KvStore::map_error(&error))
    }

    pub(crate) fn scan_all(&self) -> Result<Vec<(Bytes, Bytes)>, KvError> {
        self.inner
            .scan(&cntryl_midge::Query::new())
            .map_err(|error| KvStore::map_error(&error))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| KvStore::map_error(&error))
    }

    pub(crate) fn commit(self, policy: WritePolicy) -> Result<(), KvError> {
        self.inner
            .commit(policy.into())
            .map_err(|error| KvStore::map_error(&error))
    }
}
