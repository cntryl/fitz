//! Storage helpers for benchmarks
//!
//! Provides MidgeEngine instances for benchmark execution.
//! Supports both in-memory (fast) and local disk (realistic) storage modes.

use std::sync::Arc;

/// Create an in-memory MidgeEngine for benchmarks
///
/// This eliminates disk I/O overhead and provides fast, deterministic
/// storage for benchmark scenarios. All data is lost when the engine
/// is dropped.
///
/// **KNOWN LIMITATION**: Midge's in-memory engine does not support creating
/// additional column families beyond the default (CF=0). This is a temporary
/// workaround until Midge supports CF pre-registration.
pub fn create_bench_store() -> Arc<cntryl_midge::Engine> {
    // TODO: Use create_test_engine_with_cfs(vec![1]) once Midge supports
    // explicit CF creation in in-memory mode. For now, use default engine
    // which only supports CF=0.
    Arc::new(
        cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
            .expect("Failed to create in-memory store"),
    )
}

/// Create a local disk-backed MidgeEngine for benchmarks
///
/// This uses a temporary directory for storage, providing realistic
/// persistence characteristics while being automatically cleaned up.
/// Useful for integration/system tier benchmarks that need durable storage.
///
/// # Returns
///
/// A tuple of (Engine, TempDir). The TempDir must be kept alive for the
/// lifetime of the engine, otherwise the directory will be deleted.
pub fn create_local_bench_store() -> (Arc<cntryl_midge::Engine>, tempfile::TempDir) {
    let temp_dir =
        tempfile::tempdir().expect("Failed to create temporary directory for local bench store");

    let temp_path = temp_dir.path().to_string_lossy().to_string();

    // Change to the temp directory for this operation so Midge uses it
    let original_dir = std::env::current_dir().expect("Failed to get current directory");

    std::env::set_current_dir(&temp_path).expect("Failed to change to temp directory");

    let store = Arc::new(
        cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
            .expect("Failed to create local disk store"),
    );

    // Ensure column family 1 exists for durable benchmark/tests that rely on explicit
    // RouteFamily -> ColumnFamily mapping (many tests expect CF=1 to be present).
    // If creation fails because it already exists, ignore the error.
    let _ = store.create_column_family("cf_1");

    // Restore original directory. On Windows, this can fail if the original directory
    // was deleted, so we try to restore but don't panic if it fails. Instead, change
    // to a known-good directory (the workspace root).
    if std::env::set_current_dir(&original_dir).is_err() {
        // Fallback: try to change to the workspace root
        let _ = std::env::set_current_dir(env!("CARGO_MANIFEST_DIR"));
    }

    (store, temp_dir)
}
