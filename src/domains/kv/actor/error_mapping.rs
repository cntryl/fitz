//! Borrowed classification of Midge failures into KV protocol errors.

use super::KvActor;
use crate::domains::kv::KvError;

impl KvActor {
    /// Map a Midge error to the KV domain contract.
    ///
    /// Only a small set of `MidgeError` variants are mapped as typed errors.
    /// The remaining variants use best-effort message classification so older
    /// or less-structured storage failures remain compatible.
    pub(super) fn map_midge_error(error: &cntryl_midge::MidgeError) -> KvError {
        match error {
            cntryl_midge::MidgeError::Timeout(_) | cntryl_midge::MidgeError::Busy(_) => {
                KvError::BackendUnavailable(error.to_string())
            }
            _ => Self::classify_midge_message(&error.to_string()),
        }
    }

    pub(super) fn classify_midge_message(message: &str) -> KvError {
        let message = message.to_string();
        let normalized = message.to_lowercase();
        if normalized.contains("conflict")
            || normalized.contains("abort")
            || normalized.contains("retry")
        {
            return KvError::Conflict(message);
        }
        if normalized.contains("unavailable")
            || normalized.contains("i/o")
            || normalized.contains("disk")
            || normalized.contains("os error")
            || normalized.contains("closed")
            || normalized.contains("corrupt")
        {
            return KvError::BackendUnavailable(message);
        }
        KvError::BackendError(message)
    }
}
