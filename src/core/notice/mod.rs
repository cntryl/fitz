//! Notice domain - ephemeral pub/sub notifications

mod encoding;
mod handler;
mod service;
mod types;

// Re-export public API
pub use handler::NoticeDomain;
pub use service::NoticeService;
pub use types::NoticeOperation;
