//! Notice domain - ephemeral pub/sub notifications

mod types;
mod service;
mod handler;

// Re-export public API
pub use types::*;
pub use service::NoticeService;
pub use handler::NoticeDomain;
