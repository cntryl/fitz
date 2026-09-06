//! Fitz write guarantees, independent of the storage engine's option types.

/// Explicit persistence policy. There is no default: callers must choose the
/// acknowledgement guarantee appropriate for the configured storage mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WritePolicy {
    /// Wait for the local WAL to be synced. For local storage only.
    Sync,
    /// Write the local WAL without waiting for fsync. For local storage only.
    Buffered,
    /// Make data visible without a WAL durability guarantee before a flush.
    BestEffort,
    /// Make data locally visible while cloud persistence proceeds asynchronously.
    CloudAsync,
    /// Wait for the write to reach cloud storage before acknowledging it.
    CloudStrict,
}
