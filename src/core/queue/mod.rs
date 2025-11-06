//! Queue domain - durable message queue with lease semantics

mod handler;
mod service;
pub mod types;

// Re-export public API
pub use handler::QueueDomain;
pub use service::QueueService;
pub use types::{QueueConfig, QueueMessage, QueueScope, QueueStats};
