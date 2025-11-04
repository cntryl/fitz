//! Stream domain - event log with gap detection and subscriptions

pub mod types;
mod service;

// Re-export public API
pub use types::{AppendResult, AreaReadResponse, ExpectedRevision, StreamError, StreamEvent};
// pub use service::StreamService;
