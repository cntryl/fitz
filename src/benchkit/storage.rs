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
/// **KNOWN LIMITATION**: Midge's in-memory engine does not support creating
/// additional column families beyond the default (CF=0). This is a temporary
/// workaround until Midge supports CF pre-registration.
///
/// # Example
/// ```ignore
/// let store = create_bench_store();
/// let actor = QueueActor::new(family, key, store, None);
/// ```
pub fn create_bench_store() -> Arc<cntryl_midge::Engine> {
    // TODO: Use create_test_engine_with_cfs(vec![1]) once Midge supports
    // explicit CF creation in in-memory mode. For now, use default engine
    // which only supports CF=0.
    Arc::new(
        cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
            .expect("Failed to create in-memory store"),
    )
}
