//! Lease domain - ephemeral resource locks

mod handler;
mod service;
mod types;

// Re-export public API
pub use handler::LeaseDomain;
pub use service::LeaseService;
pub use types::LeaseOperation;
