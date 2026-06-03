//! Test utilities and harnesses for integration tests
//!
//! This module provides reusable test infrastructure for domain testing,
//! including notification and stream domains. Available when compiled with
//! test configuration or the test-helpers feature.

pub mod lease;
pub mod midge;
pub mod notice;
pub mod queue;
pub mod rpc;
pub mod stream;
pub mod transport;

// Backwards compatibility alias
pub use notice as notification;

// Re-export common test utilities
pub use lease::create_test_lease_context;
pub use midge::create_test_engine_with_cfs;
pub use notice::{TestSink, addr, make_router, route, session_id};
pub use queue::create_test_queue_actor;
pub use rpc::{
    create_test_inbox, create_test_inbox_context, create_test_rpc_actor_with_timeout,
    create_test_rpc_context,
};
pub use stream::{addr_with_family, create_test_db, create_test_store, create_test_stream_actor};
pub use transport::{TestClient, TestServer, TestWebSocketClient, TlvFrameBuilder, TlvFrameParser};
