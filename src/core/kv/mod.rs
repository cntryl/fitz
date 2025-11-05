//! KV domain - simple key-value storage

mod types;
mod store;
mod service;
mod handler;

// Re-export public API
pub use types::KvOperation;
pub use store::KvDomainStore;
pub use service::KvService;
pub use handler::KvDomain;
