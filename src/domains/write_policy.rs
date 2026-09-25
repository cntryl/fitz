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

impl WritePolicy {
    /// The weaker policy used for best-effort bookkeeping written alongside
    /// data committed under `self`: cloud policies keep cloud persistence
    /// asynchronously, local policies buffer the WAL.
    #[must_use]
    pub(crate) const fn buffered_companion(self) -> Self {
        match self {
            Self::CloudAsync | Self::CloudStrict => Self::CloudAsync,
            Self::Sync | Self::Buffered | Self::BestEffort => Self::Buffered,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::WritePolicy;

    #[test]
    fn should_pair_cloud_policies_with_cloud_async_companion() {
        // Arrange
        let policies = [
            WritePolicy::Sync,
            WritePolicy::Buffered,
            WritePolicy::BestEffort,
            WritePolicy::CloudAsync,
            WritePolicy::CloudStrict,
        ];

        // Act
        let companions = policies.map(WritePolicy::buffered_companion);

        // Assert
        assert_eq!(
            companions,
            [
                WritePolicy::Buffered,
                WritePolicy::Buffered,
                WritePolicy::Buffered,
                WritePolicy::CloudAsync,
                WritePolicy::CloudAsync,
            ]
        );
    }
}
