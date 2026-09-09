//! BEGIN write-option rewriting from broker configuration.
//!
//! Production delivery and the test-only `ApplyWriteOptions` family probe
//! both call this single policy function.

use super::state::KvFamilyRuntime;
use crate::domains::kv::write_policy::resolve_policy;

impl KvFamilyRuntime<'_> {
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
                    self.core.buffered_write_policy,
                    self.core.sync_write_policy,
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
