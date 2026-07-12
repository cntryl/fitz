//! Fitz-owned storage facade over the current Midge engine.
//!
//! This is intentionally not a backend abstraction. Midge remains the storage
//! engine; the facade localizes Midge terminology at Fitz construction and
//! store boundaries.

use std::sync::Arc;

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

    #[allow(dead_code)]
    pub(crate) fn create_column_family(
        &self,
        name: &str,
    ) -> cntryl_midge::MidgeResult<cntryl_midge::ColumnFamilyHandle> {
        self.inner.create_column_family(name)
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

    #[allow(dead_code)]
    pub(crate) fn scan_collect_all(
        &self,
        family: cntryl_midge::ColumnFamilyId,
        query: &cntryl_midge::Query,
    ) -> cntryl_midge::MidgeResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let txn = self.begin_tx(family, cntryl_midge::TransactionMode::ReadOnly)?;
        let iter = txn.scan(query)?;
        let rows = iter.try_collect()?;
        Ok(rows
            .into_iter()
            .map(|(key, value)| (key.to_vec(), value.to_vec()))
            .collect())
    }

    pub(crate) fn flush_cf(
        &self,
        cf: &cntryl_midge::ColumnFamilyHandle,
    ) -> cntryl_midge::MidgeResult<()> {
        self.inner.flush_cf(cf)
    }

    #[allow(dead_code)]
    pub(crate) fn write_options_sync() -> cntryl_midge::WriteOptions {
        cntryl_midge::WriteOptions::sync()
    }

    #[allow(dead_code)]
    pub(crate) fn write_options_buffered() -> cntryl_midge::WriteOptions {
        cntryl_midge::WriteOptions::buffered()
    }

    #[allow(dead_code)]
    pub(crate) fn write_options_best_effort() -> cntryl_midge::WriteOptions {
        cntryl_midge::WriteOptions::best_effort()
    }

    #[allow(dead_code)]
    pub(crate) fn write_options_cloud_strict() -> cntryl_midge::WriteOptions {
        cntryl_midge::WriteOptions::cloud_strict()
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
    fn should_register_route_family_column_family() {
        // Arrange
        let engine = crate::testkit::create_test_engine_with_cfs(vec![1]);
        let storage = FitzStorageEngine::new(engine);

        // Act
        let created = storage
            .create_column_family("cf_2")
            .expect("create route-family column family");
        let families = storage
            .list_column_families()
            .expect("list route-family column families");

        // Assert
        assert_eq!(created.id(), 2);
        assert!(families.iter().any(|family| family.id() == 2));
    }

    #[test]
    fn should_scan_rows_with_query() {
        // Arrange
        let storage = make_storage();
        let mut tx = storage
            .begin_tx(1, cntryl_midge::TransactionMode::ReadWrite)
            .expect("open write transaction");
        tx.put(b"key".to_vec(), b"value".to_vec(), None)
            .expect("write row");
        tx.commit(FitzStorageEngine::write_options_buffered())
            .expect("commit row");

        // Act
        let rows = storage
            .scan_collect_all(1, &cntryl_midge::Query::new())
            .expect("scan rows");

        // Assert
        assert_eq!(rows, vec![(b"key".to_vec(), b"value".to_vec())]);
    }

    #[test]
    fn should_passthrough_write_options() {
        // Arrange

        // Act
        let sync = FitzStorageEngine::write_options_sync();
        let buffered = FitzStorageEngine::write_options_buffered();
        let best_effort = FitzStorageEngine::write_options_best_effort();
        let cloud_strict = FitzStorageEngine::write_options_cloud_strict();

        // Assert
        assert!(sync.is_sync());
        assert!(!buffered.is_sync());
        assert!(best_effort.is_best_effort());
        assert!(cloud_strict.is_cloud_strict());
    }
}
