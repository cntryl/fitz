//! Borrowed classification of Midge failures into KV protocol errors.

use super::KvActor;
use crate::domains::kv::KvError;

impl KvActor {
    /// Map a Midge error to the KV domain contract.
    ///
    /// Classification is structural: backend message wording never changes
    /// the public KV error category or client retry behavior.
    pub(super) fn map_midge_error(error: &cntryl_midge::MidgeError) -> KvError {
        match error {
            cntryl_midge::MidgeError::WriteConflict(_) | cntryl_midge::MidgeError::Aborted(_) => {
                KvError::Conflict(error.to_string())
            }
            cntryl_midge::MidgeError::Io(_)
            | cntryl_midge::MidgeError::NoSpace(_)
            | cntryl_midge::MidgeError::WriteStall(_)
            | cntryl_midge::MidgeError::LeaseHeld(_)
            | cntryl_midge::MidgeError::LeaseUnavailable(_)
            | cntryl_midge::MidgeError::Busy(_)
            | cntryl_midge::MidgeError::Timeout(_) => {
                KvError::BackendUnavailable(error.to_string())
            }
            cntryl_midge::MidgeError::NotFound
            | cntryl_midge::MidgeError::InvalidArgument(_)
            | cntryl_midge::MidgeError::Corruption(_)
            | cntryl_midge::MidgeError::NotSupported(_)
            | cntryl_midge::MidgeError::Internal(_)
            | cntryl_midge::MidgeError::InvalidPath
            | cntryl_midge::MidgeError::RecoveryFailed(_)
            | cntryl_midge::MidgeError::CompatibilityError(_)
            | cntryl_midge::MidgeError::MemoryModeViolation(_)
            | cntryl_midge::MidgeError::Fenced(_)
            | cntryl_midge::MidgeError::LeaseIndeterminate(_)
            | cntryl_midge::MidgeError::LeaseEpochExhausted
            | cntryl_midge::MidgeError::ResourceLimit(_) => {
                KvError::BackendError(error.to_string())
            }
        }
    }
}
