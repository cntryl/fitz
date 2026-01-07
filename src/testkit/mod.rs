//! Test utilities and harnesses for integration tests
//!
//! This module provides reusable test infrastructure for domain testing,
//! including notification and stream domains. Available when compiled with
//! test configuration or the test-helpers feature.

pub mod notification;
pub mod stream;
pub mod queue;
pub mod rpc;
pub mod lease;

// Re-export common test utilities
pub use stream::{create_test_db, create_test_store, addr_with_family, create_test_stream_actor, create_test_area_actor};
pub use notification::{TestSink, make_router, route, addr, session_id};
pub use queue::create_test_queue_actor;
pub use rpc::{create_test_inbox, create_test_inbox_context, create_test_rpc_actor_with_timeout, create_test_rpc_context};
pub use lease::create_test_lease_context;
