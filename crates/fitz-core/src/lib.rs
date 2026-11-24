//! Synchronous broker runtime, wire framing, and routing.

pub mod auth;
pub mod dispatch;
pub mod domains;
pub mod observability;
pub mod protocol;
pub mod runtime;
pub mod session;
pub mod utils;

#[doc(hidden)]
pub mod testkit;

pub mod projection;

pub mod snapshot;
