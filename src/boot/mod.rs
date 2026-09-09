//! Synchronous broker composition and configuration.

pub(crate) mod domain_interfaces;
pub(crate) mod domains;
pub mod observability;
pub mod resource_limits;
pub mod runtime;
pub mod stats;

pub use resource_limits::enforce_startup_resource_limits;
pub use runtime::{BootConfig, BootResult};
pub use stats::{BrokerLifecycleState, Runtime};
