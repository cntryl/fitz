//! Notice domain - ephemeral pub/sub notifications

mod handler;
mod service;
mod types;
mod route_table;

// Re-export public API
pub use handler::NoticeDomain;
pub use service::NoticeService;
pub use route_table::{RouteTable, RtSubscription};
pub use types::*;
