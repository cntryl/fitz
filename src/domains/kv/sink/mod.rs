//! KV runtime sink, admin façade, and behavior-focused internal modules.

mod admin;
mod cleanup;
mod commands;
mod delivery;
mod lifecycle;
mod locks;
mod mailbox;
mod observability;
mod operations;
mod responses;
mod state;
mod subscriptions;
#[cfg(test)]
mod test_support;
mod transactions;
mod write_policy;

pub use admin::{
    AdminKvCommittedPair, AdminKvPrefixScanResult, AdminKvRowsRequest, AdminKvRowsResult,
};
pub(super) use locks::KvResourceLockKey;
pub use state::KvDomainSink;

#[cfg(test)]
mod tests;
