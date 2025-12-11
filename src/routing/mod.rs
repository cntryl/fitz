//! Route parsing and matching DSL.
//!
//! Provides:
//! - Route path parser (scheme://realm/area/resource/operation)
//! - Wildcard and variable matching
//! - Match tree for efficient routing
//! - Static route registry for RouterActor

pub mod path;
pub mod matcher;
pub mod registry;
