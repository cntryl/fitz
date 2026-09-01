//! Test utilities and harnesses for integration tests
//!
//! This module provides reusable test infrastructure for domain testing,
//! including notification and stream domains. Available when compiled with
//! test configuration or the test-helpers feature.

use std::time::Duration;

pub mod body;
pub mod midge;
pub mod notice;
pub mod operator_seed;
pub mod queue;
pub mod stream;
pub mod transport;

// Re-export common test utilities
pub use body::to_bytes;
pub use midge::create_test_engine_with_cfs;
pub use notice::{addr, make_router, route, session_id, TestSink};
pub use operator_seed::{seed_operator_console, OperatorSeedFamily, OperatorSeedReport};
pub use queue::create_test_queue_actor;
pub use stream::{addr_with_family, create_test_db, create_test_store, create_test_stream_actor};
pub use transport::{TestClient, TestServer, TestWebSocketClient, TlvFrameBuilder, TlvFrameParser};

/// Scale a test deadline by `FITZ_TEST_TIMEOUT_MULTIPLIER`.
///
/// Hosted parallel suites opt into extra scheduling headroom while local tests
/// retain their literal deadlines. Invalid and zero multipliers are ignored.
#[must_use]
pub fn scaled_test_timeout(timeout: Duration) -> Duration {
    let multiplier = std::env::var("FITZ_TEST_TIMEOUT_MULTIPLIER")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(1);
    timeout.saturating_mul(multiplier)
}
