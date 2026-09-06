//! Fitz-owned storage facade over the current Midge engine.
//!
//! This is intentionally not a backend abstraction. Midge remains the storage
//! engine; the facade localizes Midge terminology at Fitz construction and
//! store boundaries.

use std::sync::Arc;

mod write_policy;

#[derive(Clone)]
pub(crate) struct FitzStorageEngine {
    inner: Arc<cntryl_midge::Engine>,
}

impl FitzStorageEngine {
    pub(crate) fn new(inner: Arc<cntryl_midge::Engine>) -> Self {
        Self { inner }
    }

    pub(crate) fn inner(&self) -> &cntryl_midge::Engine {
        self.inner.as_ref()
    }

    pub(crate) fn clone_inner(&self) -> Arc<cntryl_midge::Engine> {
        self.inner.clone()
    }

    pub(crate) fn list_column_families(
        &self,
    ) -> cntryl_midge::MidgeResult<Vec<cntryl_midge::ColumnFamilyHandle>> {
        self.inner.list_column_families()
    }

    pub(crate) fn begin_tx(
        &self,
        family: cntryl_midge::ColumnFamilyId,
        mode: cntryl_midge::TransactionMode,
    ) -> cntryl_midge::MidgeResult<cntryl_midge::Transaction> {
        self.inner.begin_tx(family, mode)
    }

    pub(crate) fn flush_cf(
        &self,
        cf: &cntryl_midge::ColumnFamilyHandle,
    ) -> cntryl_midge::MidgeResult<()> {
        self.inner.flush_cf(cf)
    }
}

impl AsRef<cntryl_midge::Engine> for FitzStorageEngine {
    fn as_ref(&self) -> &cntryl_midge::Engine {
        self.inner()
    }
}

impl From<Arc<cntryl_midge::Engine>> for FitzStorageEngine {
    fn from(inner: Arc<cntryl_midge::Engine>) -> Self {
        Self::new(inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_storage() -> FitzStorageEngine {
        let engine = crate::testkit::create_test_engine_with_cfs(vec![1]);
        FitzStorageEngine::new(engine)
    }

    #[test]
    fn should_open_transaction_for_route_family_column_family() {
        // Arrange
        let storage = make_storage();

        // Act
        let result = storage.begin_tx(1, cntryl_midge::TransactionMode::ReadOnly);

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_list_route_family_column_families() {
        // Arrange
        let storage = make_storage();

        // Act
        let families = storage
            .list_column_families()
            .expect("list route-family column families");

        // Assert
        assert!(families.iter().any(|family| family.id() == 1));
    }
}
