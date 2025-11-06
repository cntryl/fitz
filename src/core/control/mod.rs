//! Control domain - system control plane

mod handler;
mod service;
mod types;

// Re-export public API
pub use handler::ControlDomain;
pub use service::ControlService;
pub use types::*;
