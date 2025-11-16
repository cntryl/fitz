//! KV domain - simple key-value storage

mod handler;
mod service;
mod types;

// Re-export public API
pub use handler::KvDomain;
pub use service::KvService;
pub use types::KvOperation;
