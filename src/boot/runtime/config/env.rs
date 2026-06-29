use super::{
    DEFAULT_DRAIN_CLOSE_REASON, DEFAULT_DRAIN_GRACE_SECONDS, DEFAULT_QUEUE_LOSS_WINDOW_MS,
    ENV_DRAIN_CLOSE_REASON, ENV_DRAIN_GRACE_SECONDS, ENV_QUEUE_LOSS_WINDOW_MS,
};

pub(super) fn env_non_empty(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

pub(super) fn required_env(key: &str) -> Result<String, String> {
    env_non_empty(key).ok_or_else(|| format!("cloud storage requires {}", key))
}

pub(super) fn required_region() -> Result<String, String> {
    env_non_empty("FITZ_STORAGE_REGION")
        .or_else(|| env_non_empty("AWS_REGION"))
        .or_else(|| env_non_empty("AWS_DEFAULT_REGION"))
        .ok_or_else(|| "aws-s3 storage requires FITZ_STORAGE_REGION or AWS_REGION".to_string())
}

pub(super) fn env_bool(key: &str, default: bool) -> Result<bool, String> {
    match env_non_empty(key) {
        Some(value) => value
            .parse::<bool>()
            .map_err(|_| format!("{} must be true or false", key)),
        None => Ok(default),
    }
}

pub(super) fn drain_grace_seconds_from_env() -> (u64, Option<String>) {
    let Some(value) = env_non_empty(ENV_DRAIN_GRACE_SECONDS) else {
        return (DEFAULT_DRAIN_GRACE_SECONDS, None);
    };

    match value.parse::<u64>() {
        Ok(0) => (
            0,
            Some(format!("{ENV_DRAIN_GRACE_SECONDS} must be greater than 0")),
        ),
        Ok(seconds) => (seconds, None),
        Err(_) => (
            0,
            Some(format!(
                "{ENV_DRAIN_GRACE_SECONDS} must be an unsigned integer second count"
            )),
        ),
    }
}

pub(super) fn queue_loss_window_ms_from_env() -> (u64, Option<String>) {
    let Some(value) = env_non_empty(ENV_QUEUE_LOSS_WINDOW_MS) else {
        return (DEFAULT_QUEUE_LOSS_WINDOW_MS, None);
    };

    match value.parse::<u64>() {
        Ok(0) => (
            0,
            Some(format!("{ENV_QUEUE_LOSS_WINDOW_MS} must be greater than 0")),
        ),
        Ok(milliseconds) => (milliseconds, None),
        Err(_) => (
            0,
            Some(format!(
                "{ENV_QUEUE_LOSS_WINDOW_MS} must be an unsigned integer millisecond count"
            )),
        ),
    }
}

pub(super) fn drain_close_reason_from_env() -> String {
    env_non_empty(ENV_DRAIN_CLOSE_REASON).unwrap_or_else(|| DEFAULT_DRAIN_CLOSE_REASON.to_string())
}
