//! Storage helpers for benchmarks
//!
//! Provides `MidgeEngine` instances for benchmark execution.
//! Supports both in-memory (fast) and local disk (realistic) storage modes.

use std::sync::Arc;

/// Create an in-memory `MidgeEngine` for benchmarks with Fitz's explicit CF mapping.
///
/// This eliminates disk I/O overhead and provides fast, deterministic
/// storage for benchmark scenarios. All data is lost when the engine
/// is dropped.
#[must_use]
pub fn create_bench_store() -> Arc<cntryl_midge::Engine> {
    create_bench_store_with_cfs([1])
}

/// Create an in-memory `MidgeEngine` with the requested explicit CF mapping.
#[must_use]
pub fn create_bench_store_with_cfs(
    column_families: impl IntoIterator<Item = u32>,
) -> Arc<cntryl_midge::Engine> {
    let store = Arc::new(
        cntryl_midge::Engine::open(cntryl_midge::OpenOptions::in_memory().build())
            .expect("Failed to create in-memory bench store"),
    );
    let mut ids: Vec<_> = column_families.into_iter().collect();
    ids.sort_unstable();
    ids.dedup();

    for expected_id in ids {
        let handle = store
            .create_column_family(&format!("cf_{expected_id}"))
            .unwrap_or_else(|_| panic!("Failed to create benchmark column family {expected_id}"));
        assert_eq!(
            handle.id(),
            expected_id,
            "benchmark column family ID mismatch: expected {expected_id} but got {}",
            handle.id()
        );
    }

    store
}

/// Create a local disk-backed `MidgeEngine` for benchmarks
///
/// This uses a temporary directory for storage, providing realistic
/// persistence characteristics while being automatically cleaned up.
/// Useful for integration/system tier benchmarks that need durable storage.
///
/// # Returns
///
/// A tuple of (Engine, `TempDir`). The `TempDir` must be kept alive for the
/// lifetime of the engine, otherwise the directory will be deleted.
///
/// # Panics
///
/// Panics if the temporary directory or local disk-backed engine cannot be
/// created.
#[must_use]
pub fn create_local_bench_store() -> (Arc<cntryl_midge::Engine>, tempfile::TempDir) {
    let temp_dir =
        tempfile::tempdir().expect("Failed to create temporary directory for local bench store");

    let temp_path = temp_dir.path().to_string_lossy().to_string();

    let store = Arc::new(
        cntryl_midge::Engine::open(cntryl_midge::OpenOptions::local(&temp_path).build())
            .expect("Failed to create local disk store"),
    );

    // Ensure column family 1 exists for durable benchmark/tests that rely on explicit
    // RouteFamily -> ColumnFamily mapping (many tests expect CF=1 to be present).
    // If creation fails because it already exists, ignore the error.
    let _ = store.create_column_family("cf_1");

    (store, temp_dir)
}
