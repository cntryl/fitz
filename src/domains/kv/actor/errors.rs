use super::KvActor;
use crate::domains::kv::KvError;

impl KvActor {
    /// Map a Midge error to the KV domain contract.
    #[allow(clippy::needless_pass_by_value)]
    pub(super) fn map_midge_error(error: cntryl_midge::MidgeError) -> KvError {
        Self::classify_midge_message(&error.to_string())
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
