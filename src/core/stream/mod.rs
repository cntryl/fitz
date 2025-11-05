//! Stream domain - event log with gap detection and subscriptions

pub mod types;
mod service;
mod handler;

// Re-export public API
pub use types::{AppendResult, AreaReadResponse, ExpectedRevision, StreamError, StreamEvent, StreamOperation};
pub use service::StreamService;
pub use handler::StreamDomain;
