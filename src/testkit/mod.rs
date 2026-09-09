//! Test utilities and harnesses for integration tests
//!
//! This module provides reusable test infrastructure for domain testing,
//! including notification and stream domains. Available when compiled with
//! repository integration-test and benchmark targets.

use std::time::Duration;

pub mod body;
mod domains;
pub mod midge;
pub mod notice;
pub mod operator_seed;
pub mod stream;
pub mod transport;

/// Low-level domain machinery used only by repository integration tests.
///
/// Production callers should exercise domains through the protocol clients or
/// [`DomainRuntimeFixture`]. Keeping these exports under the explicitly hidden
/// testkit prevents actors and stores from becoming domain API contracts.
#[doc(hidden)]
pub mod domain_internals {
    pub mod kv {
        pub use crate::domains::kv::actor::KvActor;
    }

    pub mod queue {
        pub use crate::domains::queue::actor::*;
    }

    pub mod schedule {
        pub use crate::domains::schedule::actor::*;
        pub use crate::domains::schedule::store::*;
    }

    pub mod stream {
        pub use crate::domains::stream::actor::*;
        pub use crate::domains::stream::storage::*;
        pub use crate::domains::stream::store::*;
    }
}

// Re-export common test utilities
pub use body::to_bytes;
pub use domains::{create_domain_runtime_fixture, DomainRuntimeFixture};
pub use midge::create_test_engine_with_cfs;
pub use notice::{addr, make_router, route, session_id, TestSink};
pub use operator_seed::{seed_operator_console, OperatorSeedFamily, OperatorSeedReport};
pub use stream::{addr_with_family, create_test_db, create_test_store, create_test_stream_actor};
pub use transport::{TestClient, TestServer, TestWebSocketClient, TlvFrameBuilder, TlvFrameParser};

/// Open storage through the transport-owned boot path for integration tests.
///
/// # Errors
/// Returns the storage initialization error produced by the configured backend.
pub async fn init_storage(
    config: &crate::boot::BootConfig,
) -> crate::boot::BootResult<std::sync::Arc<cntryl_midge::Engine>> {
    crate::api::storage_runtime::init(config).await
}

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
