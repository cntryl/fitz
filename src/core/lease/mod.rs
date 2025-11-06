//! Lease domain - ephemeral resource locks

mod handler;
pub mod service;
mod types;

// Re-export public API
// pub use types::*;
pub use handler::LeaseDomain;
pub use service::LeaseService;
