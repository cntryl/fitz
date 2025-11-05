//! KV domain - simple key-value storage

mod types;
mod service;
mod handler;

// Re-export public API
// pub use types::*;
// pub use service::KvService;
pub use handler::KvDomain;
