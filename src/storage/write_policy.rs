//! Translate Fitz guarantees to the current engine only at the storage boundary.

use crate::domains::WritePolicy;
use cntryl_midge::WriteOptions;

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
