use crate::boot::Runtime;
use crate::runtime::routing::RouteFamily;
use schemars::JsonSchema;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub(crate) struct RuntimeDrainResponse {
    pub(crate) lifecycle_state: &'static str,
    pub(crate) active_sessions: usize,
    pub(crate) drain_grace_seconds: u64,
    pub(crate) drain_started_epoch_ms: Option<u64>,
    pub(crate) drain_deadline_epoch_ms: Option<u64>,
    pub(crate) close_reason: String,
}

/// Shared command path for REST and MCP runtime drain requests.
pub(crate) fn begin_runtime_drain(runtime: &Runtime) -> RuntimeDrainResponse {
    runtime.begin_drain();
    RuntimeDrainResponse {
        lifecycle_state: runtime.lifecycle_state().as_str(),
        active_sessions: runtime.session_count(),
        drain_grace_seconds: runtime.drain_grace_seconds(),
        drain_started_epoch_ms: runtime.drain_started_epoch_ms(),
        drain_deadline_epoch_ms: runtime.drain_deadline_epoch_ms(),
        close_reason: runtime.drain_close_reason(),
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum QueueDeadLetterOperation {
    Replay,
    Purge,
}

/// Shared Queue dead-letter command path for REST and explicitly confirmed MCP actions.
///
/// # Errors
/// Returns an error when the family is invalid, the Queue domain is unavailable, or persistence fails.
pub(crate) fn queue_dead_letter(
    runtime: &Runtime,
    family: u64,
    realm: &str,
    area: &str,
    resource: &str,
    message_id: u64,
    operation: QueueDeadLetterOperation,
) -> Result<bool, String> {
    let family = RouteFamily::try_from(family)
        .map_err(|_| "Route family exceeds the supported u32 range".to_string())?;
    match operation {
        QueueDeadLetterOperation::Replay => {
            runtime.queue_replay_dead_letter(family, realm, area, resource, message_id)
        }
        QueueDeadLetterOperation::Purge => {
            runtime.queue_purge_dead_letter(family, realm, area, resource, message_id)
        }
    }
}
