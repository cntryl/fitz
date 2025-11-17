//! Queue domain - durable message queue with lease semantics

mod encoding;
mod handler;
mod service;
pub mod types;

// Re-export public API
pub use encoding::{
    build_enqueue_response, build_error_response, build_list_response, build_reserve_response,
    build_success_response, parse_tlv_payload,
};
pub use handler::QueueDomain;
pub use service::QueueService;
pub use types::{QueueConfig, QueueMessage, QueueOperation, QueueScope, QueueStats};
