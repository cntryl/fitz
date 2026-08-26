//! BEGIN write-option rewriting from broker configuration.
//!
//! Production delivery and the test-only `ApplyWriteOptions` mailbox probe
//! both call this single policy function.

use super::state::KvDomainRuntime;

impl KvDomainRuntime<'_> {
    pub(super) fn apply_write_options(
        &self,
        message: crate::domains::kv::KvMessage,
    ) -> crate::domains::kv::KvMessage {
        match message {
            crate::domains::kv::KvMessage::Begin {
                scope,
                mode,
                write_options,
            } if write_options.is_sync()
                || write_options == cntryl_midge::WriteOptions::buffered() =>
            {
                let write_options = if write_options.is_sync() {
                    self.core.sync_write_options
                } else {
                    self.core.buffered_write_options
                };
                crate::domains::kv::KvMessage::Begin {
                    scope,
                    mode,
                    write_options,
                }
            }
            message => message,
        }
    }
}
