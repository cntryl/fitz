use super::{
    action_name, ConfirmActionArguments, McpActionState, McpAuditDecision, McpCapabilityPolicy,
    McpExecutionContext, PreviewQueueDeadLetterArguments, Runtime, Uuid, CONFIRM_DRAIN_TOOL,
    CONFIRM_QUEUE_TOOL, PREVIEW_DRAIN_TOOL, PREVIEW_QUEUE_TOOL,
};

impl McpActionState {
    /// Capture target authority before admission. A later confirmation can
    /// consume the preview while its observer still needs to audit an interruption.
    pub(in crate::api::mcp::transport) fn interruption_authorized(
        &self,
        tool_name: &str,
        arguments: Option<&serde_json::Value>,
        context: &McpExecutionContext,
        policy: &McpCapabilityPolicy,
        fingerprint: [u8; 32],
        runtime: &Runtime,
    ) -> bool {
        match tool_name {
            PREVIEW_DRAIN_TOOL | CONFIRM_DRAIN_TOOL => Self::authorized_runtime(context, policy),
            PREVIEW_QUEUE_TOOL => {
                let Some(parsed) = arguments.and_then(|arguments| {
                    serde_json::from_value::<PreviewQueueDeadLetterArguments>(arguments.clone())
                        .ok()
                }) else {
                    return false;
                };
                Self::authorized_queue(
                    context,
                    policy,
                    runtime,
                    &parsed.target(),
                    &parsed.operation,
                )
            }
            CONFIRM_QUEUE_TOOL => {
                let Some(parsed) = arguments.and_then(|arguments| {
                    serde_json::from_value::<ConfirmActionArguments>(arguments.clone()).ok()
                }) else {
                    return false;
                };
                let preview = self.previews.lock().get(&parsed.challenge_id).cloned();
                preview.is_some_and(|preview| {
                    context.principal_name().as_deref() == Some(preview.username.as_str())
                        && preview.token_fingerprint == fingerprint
                        && preview.action.confirmation_tool() == tool_name
                        && Self::authorized_action(context, policy, runtime, &preview.action)
                })
            }
            _ => false,
        }
    }

    /// Persist an authorized request's interruption separately from its command outcome.
    pub(in crate::api::mcp::transport) fn record_request_interruption(
        &self,
        tool_name: &str,
        challenge_id: Option<&str>,
        context: &McpExecutionContext,
        fingerprint: [u8; 32],
        outcome: &'static str,
        authorized: bool,
    ) -> Result<(), String> {
        context.record_action_audit(tool_name, outcome, McpAuditDecision::Denied);
        if !authorized {
            return Ok(());
        }
        let username = context.principal_name().unwrap_or_else(|| "unknown".into());
        let requested_id = challenge_id
            .and_then(|value| Uuid::parse_str(value).ok())
            .map(|value| value.to_string());
        let operation_id = requested_id
            .filter(|id| {
                self.previews.lock().get(id).is_none_or(|preview| {
                    preview.username == username && preview.token_fingerprint == fingerprint
                })
            })
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        if !self.audit.append(
            &operation_id,
            &username,
            action_name(tool_name),
            "redacted",
            outcome,
        ) {
            return Err("action request interruption could not be durably audited".into());
        }
        Ok(())
    }
}
