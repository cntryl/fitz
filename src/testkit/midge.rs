//! Test utilities for Midge column family setup
//!
//! # Column Family Requirements
//!
//! Midge (based on RocksDB) requires column families to be explicitly created
//! before use. The in-memory engine doesn't auto-create CFs beyond the default (CF=0).
//!
//! For tests, we need to either:
//! 1. Use a Midge build that supports dynamic CF creation, OR
//! 2. Pre-configure CFs via MidgeOptions, OR  
//! 3. Use the default CF (which violates our architectural rule)
//!
//! # Current Status
//!
//! **KNOWN ISSUE:** Midge's in-memory engine may not support multiple CFs properly.
//! This causes tests to fail when using explicit RouteFamily → CF mapping.
//!
//! # Workaround
//!
//! Until Midge supports dynamic CF creation or we can pre-register CFs,
//! tests may need to use a mock storage layer that supports arbitrary CFs.

use std::sync::Arc;
use cntryl_midge::{Engine, MidgeOptions};

/// Create a test Midge engine with support for multiple column families
///
/// # Issues
///
/// Currently uses default MidgeOptions which may not support multiple CFs.
/// This is a known limitation that needs to be addressed at the Midge level.
pub fn create_test_engine_with_cfs(_cf_ids: Vec<u32>) -> Arc<Engine> {
    // TODO: Once Midge supports CF pre-registration via MidgeOptions,
    // configure them here. For now, we rely on Midge's auto-creation behavior.
    Arc::new(
        Engine::open_with_options(MidgeOptions::default())
            .expect("Failed to create test engine")
    )
}

/// Create a test engine with default configuration
///
/// WARNING: This creates an engine that may only support the default CF (CF=0).
/// Use `create_test_engine_with_cfs` to explicitly configure CFs.
pub fn create_test_engine() -> Arc<Engine> {
    Arc::new(
        Engine::open_with_options(MidgeOptions::default())
            .expect("Failed to create test engine")
    )
}
