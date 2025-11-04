//! fitz - crate root
//!
//! Minimal public surface for the project. Add more implementation in modules.

pub mod authz;
pub mod config;
pub mod control;
pub mod core;
pub mod protocol;
pub mod storage;
pub mod transport;

// Re-export common items (fill in as modules are implemented)
// pub use storage::mem::MemStore;
// pub use transport::http::HttpTransport;
