//! Fitz - Actor-based distributed systems platform
//!
//! Fitz provides a high-performance actor runtime with built-in support for
//! distributed messaging, storage, and domain-specific protocols.

pub mod api;
pub mod auth;
pub mod boot;
pub mod client;
pub mod control;
pub mod dispatch;
pub mod domains;
pub mod observability;
pub mod prelude;
pub mod protocol;
pub use fitz_core::runtime;
pub mod session;
pub mod snapshot;
pub(crate) use fitz_storage::storage;
pub mod utils;

// Test utilities for integration tests
#[cfg_attr(not(test), doc(hidden))]
#[cfg(any(test, feature = "test-support"))]
pub mod testkit;

// Benchmark utilities for performance testing
#[cfg_attr(not(test), doc(hidden))]
#[cfg(any(test, feature = "test-support"))]
pub mod benchkit;
