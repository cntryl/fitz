//! Storage helpers for benchmarks
//!
//! Provides in-memory MidgeEngine instances for fast benchmark execution
//! without disk I/O overhead.

use std::sync::Arc;

/// Create an in-memory MidgeEngine for benchmarks
///
/// This eliminates disk I/O overhead and provides fast, deterministic
/// storage for benchmark scenarios. All data is lost when the engine
/// is dropped.
///
/// # Example
/// ```ignore
/// let store = create_bench_store();
/// let actor = QueueActor::new(family, key, store, None);
/// ```
pub fn create_bench_store() -> Arc<cntryl_midge::MidgeEngine> {
    Arc::new(
        cntryl_midge::MidgeEngine::open(cntryl_midge::MidgeOptions::default())
            .expect("Failed to create in-memory store")
    )
}
