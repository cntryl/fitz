mod domain_sink_impl;
mod lifecycle_and_admin;
mod mailbox_sink_impl;
mod model;

pub use model::LeaseDomainSink;

#[cfg(test)]
use model::*;

#[cfg(test)]
mod tests;
