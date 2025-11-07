//! Stream domain - event log with gap detection and subscriptions

mod encoding;
mod handler;
mod service;
pub mod types;

// Re-export public API
pub use handler::StreamDomain;
pub use service::StreamService;
pub use types::{
    AppendResult, AreaReadResponse, ExpectedRevision, StreamError, StreamEvent, StreamOperation,
};
