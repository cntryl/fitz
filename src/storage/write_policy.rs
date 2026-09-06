//! Translate Fitz guarantees to the current engine only at the storage boundary.

use crate::domains::WritePolicy;
use cntryl_midge::{DurabilityPolicy, WriteOptions};

impl From<WritePolicy> for WriteOptions {
    fn from(policy: WritePolicy) -> Self {
        match policy {
            WritePolicy::Sync => Self::sync(),
            WritePolicy::Buffered => Self::buffered(),
            WritePolicy::BestEffort => Self::best_effort(),
            WritePolicy::CloudAsync => Self::cloud_async(),
            WritePolicy::CloudStrict => Self::cloud_strict(),
        }
    }
}

// Preserve construction with existing broker configuration and embedding APIs.
impl From<WriteOptions> for WritePolicy {
    fn from(options: WriteOptions) -> Self {
        match options.policy() {
            DurabilityPolicy::Sync => Self::Sync,
            DurabilityPolicy::Buffered => Self::Buffered,
            DurabilityPolicy::BestEffort => Self::BestEffort,
            DurabilityPolicy::CloudAsync => Self::CloudAsync,
            DurabilityPolicy::CloudStrict => Self::CloudStrict,
        }
    }
}
