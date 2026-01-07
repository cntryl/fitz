//! Fitz - Actor-based distributed systems platform
//!
//! Fitz provides a high-performance actor runtime with built-in support for
//! distributed messaging, storage, and domain-specific protocols.

pub mod api;
pub mod control;
pub mod domains;
pub mod auth;
pub mod prelude;
pub mod protocol;
pub mod runtime;
pub mod session;
pub mod utils;

// Test utilities for integration tests
#[cfg_attr(not(test), doc(hidden))]
pub mod testkit;

// Benchmark utilities for performance testing
#[cfg_attr(not(test), doc(hidden))]
pub mod benchkit;
