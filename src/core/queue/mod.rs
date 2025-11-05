//! Queue domain - durable message queue with lease semantics

pub mod types;
mod service;
mod handler;

// Re-export public API
pub use types::{QueueConfig, QueueMessage, QueueScope, QueueStats};
pub use service::QueueService;
pub use handler::QueueDomain;
