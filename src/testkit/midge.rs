//! Test utilities for Midge column family setup
//!
//! # Column Family Requirements
//!
//! **CRITICAL ARCHITECTURAL RULE:**
//! **ALL Fitz tests MUST use explicit column families (CF). The default CF (CF=0) is FORBIDDEN.**
//!
//! Midge (based on `RocksDB`) requires column families to be explicitly created
//! before use. Fitz enforces that every persisted domain maps `RouteFamily` -> `ColumnFamily`.
//!
//! # Required Pattern for Tests
//!
//! ```no_run
//! use fitz::testkit::create_test_engine_with_cfs;
//! use fitz::runtime::routing::RouteFamily;
//! use fitz::domains::queue::{QueueActor, QueueKey};
//! use fitz::utils::idempotency::default_dedup_store;
//!
//! // ✅ CORRECT - Explicit CF configuration
//! let engine = create_test_engine_with_cfs(vec![1, 2, 3]);
//! let key = QueueKey {
//!     family: RouteFamily::new(1),
//!     realm: "realm".to_string(),
//!     area: "area".to_string(),
//!     resource: "resource".to_string(),
//! };
//! let actor = QueueActor::new(RouteFamily::new(1), key, engine, None, default_dedup_store());
//!
//! // ❌ FORBIDDEN - Will panic
//! // let actor = QueueActor::new(RouteFamily::new(0), ...);
//! ```
//!
//! # Why This Rule Exists
//!
//! 1. **Data isolation**: Each `RouteFamily` must have its own `ColumnFamily`
//! 2. **No silent mixing**: Default CF usage would violate isolation
//! 3. **Production parity**: Tests must match production behavior
//! 4. **Architectural invariant**: Explicit mapping is foundational to Fitz design

use cntryl_midge::{Engine, OpenOptions};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Shut down an exclusively owned test engine before its storage directory is removed.
///
/// # Panics
///
/// Panics if another owner still holds the engine or Midge cannot shut it down cleanly.
pub fn shutdown_test_engine(engine: Arc<Engine>) {
    let mut engine = Arc::try_unwrap(engine).unwrap_or_else(|engine| {
        panic!(
            "Midge test shutdown blocked by {} engine references",
            Arc::strong_count(&engine)
        );
    });
    engine
        .shutdown(Duration::from_secs(2))
        .expect("shutdown Midge test engine");
}

/// Create a test Midge engine with support for multiple column families.
///
/// Each call gets an isolated local-disk database path so test state does not
/// leak across engine instances in the same process.
///
/// # Parameters
///
/// * `cf_ids` - List of column family IDs to create (must not include 0)
///
/// # Panics
///
/// Panics if `cf_ids` contains 0 (the default/forbidden CF)
///
/// # Example
///
/// ```no_run
/// # use fitz::testkit::create_test_engine_with_cfs;
/// // Create engine supporting CFs 1, 2, 3
/// let engine = create_test_engine_with_cfs(vec![1, 2, 3]);
/// ```
static ENGINE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Creates a test Midge engine with explicit non-zero column families.
///
/// # Panics
///
/// Panics if `cf_ids` contains 0 or if the test engine cannot be opened.
#[allow(clippy::needless_pass_by_value)]
pub fn create_test_engine_with_cfs(cf_ids: Vec<u32>) -> Arc<Engine> {
    // Validate: CF=0 is FORBIDDEN
    for cf_id in &cf_ids {
        assert!(
            *cf_id != 0,
            "CRITICAL TEST VIOLATION: Attempted to create engine with default CF (CF=0). \
             All Fitz tests MUST use explicit non-zero column families. \
             This enforces the architectural rule: RouteFamily -> ColumnFamily mapping."
        );
    }

    let unique_id = ENGINE_COUNTER.fetch_add(1, Ordering::SeqCst);
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    let db_path = PathBuf::from(format!(
        "target/tmp/fitz_test_engine_{}_{}_{}",
        std::process::id(),
        unique_id,
        timestamp
    ));
    let _ = std::fs::create_dir_all(&db_path);

    let engine = Arc::new(
        Engine::open(
            OpenOptions::local(db_path)
                .build()
                .expect("Failed to build test engine options"),
        )
        .expect("Failed to create test engine"),
    );

    // Explicitly create each requested column family.
    // Midge assigns IDs sequentially starting from 1, so creating CFs in
    // order (1, 2, 3, ...) produces the expected CF IDs.
    for cf_id in &cf_ids {
        let name = format!("cf_{cf_id}");
        let handle = engine
            .create_column_family(&name)
            .unwrap_or_else(|e| panic!("Failed to create column family {cf_id}: {e}"));
        assert_eq!(
            handle.id(),
            *cf_id,
            "Column family ID mismatch: expected {} but got {} for CF '{}'",
            cf_id,
            handle.id(),
            name
        );
    }

    engine
}
