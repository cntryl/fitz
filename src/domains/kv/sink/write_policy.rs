//! BEGIN write-option rewriting from broker configuration.
//!
//! Production delivery and the test-only `ApplyWriteOptions` mailbox probe
//! both call this single policy function.

use super::state::KvDomainRuntime;
use crate::domains::kv::write_policy::resolve_policy;

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
            } => {
                let write_options = resolve_policy(
                    write_options,
                    self.core.buffered_write_options.into(),
                    self.core.sync_write_options.into(),
                );
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
