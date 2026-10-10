//! The two explicit client choices carried by KV and Stream COMMIT.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitPersistence {
    /// Return after a buffered local commit or a CloudAsync commit.
    Buffered,
    /// Wait for a synced local WAL or acknowledged CloudStrict commit.
    Sync,
}

impl CommitPersistence {
    pub(crate) fn decode(value: u8) -> Result<Self, String> {
        match value {
            0 => Ok(Self::Buffered),
            1 => Ok(Self::Sync),
            _ => Err(format!("Invalid commit persistence: {value}")),
        }
    }

    pub(crate) const fn storage_policy(self, cloud: bool) -> super::WritePolicy {
        match (self, cloud) {
            (Self::Buffered, false) => super::WritePolicy::Buffered,
            (Self::Sync, false) => super::WritePolicy::Sync,
            (Self::Buffered, true) => super::WritePolicy::CloudAsync,
            (Self::Sync, true) => super::WritePolicy::CloudStrict,
        }
    }
}
