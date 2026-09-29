mod actor_registry;
mod actors;
mod cleanup;
mod delivery;
mod facade;
mod ingress;
mod mailbox;
mod maintenance_clock;
mod model;
mod observability;
mod reservation_book;
mod responses;
mod subscriptions;

pub(crate) use model::QueueDomain;

/// Failed fast-mode flush attempts. A sustained non-zero rate means accepted
/// Queue writes are outliving the configured loss window without reaching disk.
pub(crate) const METRIC_QUEUE_FAST_FLUSH_FAILURES: &str = "fitz_queue_fast_flush_failures_total";

#[cfg(test)]
mod tests;
