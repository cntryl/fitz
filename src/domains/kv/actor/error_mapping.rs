//! Borrowed classification of Midge failures into KV protocol errors.

use super::KvActor;
use crate::domains::kv::KvError;

impl KvActor {
    /// Map a Midge error to the KV domain contract.
    ///
    /// Typed variants are matched before falling back to message text. Text
    /// classification cannot see the difference between transient saturation
    /// and a permanent fault, so anything storage states explicitly must be
    /// honoured explicitly - otherwise a bounded storage timeout is reported
    /// to the client as a permanent backend failure.
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
