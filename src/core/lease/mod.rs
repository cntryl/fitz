//! Lease domain - ephemeral resource locks

mod types;
mod service;
mod handler;

// Re-export public API
// pub use types::*;
// pub use service::LeaseService;
pub use handler::LeaseDomain;
