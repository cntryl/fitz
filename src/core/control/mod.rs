//! Control domain - system control plane

mod types;
mod service;
mod handler;

// Re-export public API
pub use types::*;
pub use service::ControlService;
pub use handler::ControlDomain;
