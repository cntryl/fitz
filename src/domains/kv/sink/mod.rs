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

pub(crate) use admin::{AdminKvPrefixScanResult, AdminKvRowsRequest, AdminKvRowsResult};
pub(super) use locks::KvResourceLockKey;
pub(crate) use state::KvDomain;

#[cfg(test)]
mod tests;
