mod domain_sink_impl;
mod mailbox_sink_impl;
mod model;
#[cfg(test)]
mod test_helpers;

pub use model::ScheduleDomainSink;

#[cfg(test)]
use model::*;

#[cfg(test)]
mod tests;
