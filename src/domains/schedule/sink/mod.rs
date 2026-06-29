mod domain_sink_impl;
mod mailbox_sink_impl;
mod model;

pub use model::ScheduleDomainSink;

#[cfg(test)]
use model::*;

#[cfg(test)]
mod tests;
