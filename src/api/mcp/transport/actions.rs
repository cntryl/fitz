mod audit;

use self::audit::McpActionAuditSink;
use crate::api::admin::auth::AdminPrincipal;
use crate::api::admin::commands::{
    begin_runtime_drain, queue_dead_letter, QueueDeadLetterOperation, RuntimeDrainResponse,
};
use crate::api::mcp::{
    McpAuditDecision, McpCapabilityClass, McpCapabilityPolicy, McpExecutionContext,
    McpResourceDetailRequest, McpScopedResourceRequest,
};
use crate::auth::Access;
use crate::boot::Runtime;
use rmcp::model::{Tool, ToolAnnotations};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Map;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;

const PREVIEW_TTL: Duration = Duration::from_secs(60);
const MAX_PREVIEWS: usize = 256;
const MAX_TARGET_BYTES: usize = 512;
pub(super) const PREVIEW_DRAIN_TOOL: &str = "preview_runtime_drain";
pub(super) const CONFIRM_DRAIN_TOOL: &str = "confirm_runtime_drain";
pub(super) const PREVIEW_QUEUE_TOOL: &str = "preview_queue_dead_letter_action";
pub(super) const CONFIRM_QUEUE_TOOL: &str = "confirm_queue_dead_letter_action";

#[derive(Clone)]
pub(super) struct McpActionState {
    runtime_target: Arc<str>,
    audit: Arc<McpActionAuditSink>,
    previews: Arc<parking_lot::Mutex<HashMap<String, PendingAction>>>,
}

#[derive(Debug, Clone)]
struct PendingAction {
    username: String,
    token_fingerprint: [u8; 32],
    action: Action,
    target: String,
    confirmation: String,
    observed_state: [u8; 32],
    expires_at: Instant,
}

#[derive(Debug, Clone)]
enum Action {
    RuntimeDrain,
    QueueDeadLetter {
        target: QueueTarget,
        operation: QueueDeadLetterOperation,
    },
}

impl Action {
    fn confirmation_tool(&self) -> &'static str {
        match self {
            Self::RuntimeDrain => CONFIRM_DRAIN_TOOL,
            Self::QueueDeadLetter { .. } => CONFIRM_QUEUE_TOOL,
        }
    }

    fn audit_name(&self) -> &'static str {
        match self {
            Self::RuntimeDrain => "runtime.drain",
            Self::QueueDeadLetter {
                operation: QueueDeadLetterOperation::Replay,
                ..
            } => "queue.dead-letter.replay",
            Self::QueueDeadLetter {
                operation: QueueDeadLetterOperation::Purge,
                ..
            } => "queue.dead-letter.purge",
        }
    }

    fn confirmation_verb(&self) -> &'static str {
        match self {
            Self::RuntimeDrain => "DRAIN",
            Self::QueueDeadLetter {
                operation: QueueDeadLetterOperation::Replay,
                ..
            } => "REPLAY",
            Self::QueueDeadLetter {
                operation: QueueDeadLetterOperation::Purge,
                ..
            } => "PURGE",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum QueueOperationInput {
    Replay,
    Purge,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct NoArguments {}

#[derive(Debug, Deserialize, JsonSchema)]
struct PreviewQueueDeadLetterArguments {
    operation: QueueOperationInput,
    route_family: u64,
    realm: String,
    area: String,
    resource: String,
    message_id: u64,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct ConfirmActionArguments {
    challenge_id: String,
    target: String,
    confirmation: String,
}

#[derive(Debug, Serialize, JsonSchema)]
struct ActionPreviewOutput {
    challenge_id: String,
    action: String,
    target: String,
    confirmation: String,
    observed_state_sha256: String,
    expires_in_seconds: u64,
}

#[derive(Debug, Serialize, JsonSchema)]
struct ActionResultOutput {
    operation_id: String,
    action: String,
    target: String,
    outcome: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    runtime: Option<RuntimeDrainResponse>,
}

#[derive(Debug, Clone)]
struct QueueTarget {
    route_family: u64,
    realm: String,
    area: String,
    resource: String,
    message_id: u64,
}

impl QueueTarget {
    fn scope(&self) -> McpScopedResourceRequest {
        McpScopedResourceRequest {
            resource: McpResourceDetailRequest {
                scheme: "queue".to_string(),
                realm: self.realm.clone(),
                area: self.area.clone(),
                resource: self.resource.clone(),
                queue_family: None,
                limit: None,
            },
            route_family: Some(self.route_family),
        }
    }

    fn display(&self) -> String {
        format!(
            "route_family={};queue://{}/{}/{};message_id={}",
            self.route_family, self.realm, self.area, self.resource, self.message_id
        )
    }
}

impl McpActionState {
    pub(super) fn from_env() -> Result<Option<Self>, String> {
        match std::env::var("FITZ_MCP_MUTATIONS_ENABLED")
            .ok()
            .map(|value| value.trim().to_ascii_lowercase())
            .as_deref()
        {
            None | Some("false" | "0") => return Ok(None),
            Some("true" | "1") => {}
            Some(_) => return Err("FITZ_MCP_MUTATIONS_ENABLED must be true or false".into()),
        }
        let runtime_target = std::env::var("FITZ_MCP_ACTION_TARGET")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| {
                !value.is_empty() && value.len() <= 128 && !value.chars().any(char::is_control)
            })
            .ok_or("FITZ_MCP_ACTION_TARGET is required and must be a bounded safe identifier")?;
        let audit_path = std::env::var("FITZ_MCP_ACTION_AUDIT_FILE")
            .ok()
            .map(PathBuf::from)
            .ok_or("FITZ_MCP_ACTION_AUDIT_FILE is required when MCP mutations are enabled")?;
        Self::open(&runtime_target, audit_path).map(Some)
    }

    pub(super) fn open(runtime_target: &str, audit_path: PathBuf) -> Result<Self, String> {
        Ok(Self {
            runtime_target: runtime_target.into(),
            audit: Arc::new(McpActionAuditSink::open(audit_path)?),
            previews: Arc::new(parking_lot::Mutex::new(HashMap::new())),
        })
    }

    pub(super) fn tool_definitions(
        context: &McpExecutionContext,
        policy: &McpCapabilityPolicy,
    ) -> Vec<Tool> {
        let mut tools = Vec::new();
        if policy.allows(McpCapabilityClass::Mutate) {
            tools.push(
                Tool::new(
                    PREVIEW_QUEUE_TOOL,
                    "Preview one authorized Queue dead-letter replay or purge, including the exact family, route, message id, and observed state.",
                    Map::new(),
                )
                .with_input_schema::<PreviewQueueDeadLetterArguments>()
                .with_output_schema::<ActionPreviewOutput>()
                .with_annotations(read_only_action_annotations()),
            );
            tools.push(
                Tool::new(
                    CONFIRM_QUEUE_TOOL,
                    "Execute one previewed Queue dead-letter action after exact target confirmation. A timeout or indeterminate response must not be retried.",
                    Map::new(),
                )
                .with_input_schema::<ConfirmActionArguments>()
                .with_output_schema::<ActionResultOutput>()
                .with_annotations(destructive_action_annotations()),
            );
        }
        if Self::authorized_runtime(context, policy) {
            tools.push(
                Tool::new(
                    PREVIEW_DRAIN_TOOL,
                    "Preview the exact broker target and current lifecycle/session state for a runtime drain. No state changes occur.",
                    Map::new(),
                )
                .with_input_schema::<NoArguments>()
                .with_output_schema::<ActionPreviewOutput>()
                .with_annotations(read_only_action_annotations()),
            );
            tools.push(
                Tool::new(
                    CONFIRM_DRAIN_TOOL,
                    "Start one runtime drain after a fresh preview and exact target confirmation. The challenge is one-use; an indeterminate result must not be retried.",
                    Map::new(),
                )
                .with_input_schema::<ConfirmActionArguments>()
                .with_output_schema::<ActionResultOutput>()
                .with_annotations(destructive_action_annotations()),
            );
        }
        tools.sort_by(|left, right| left.name.cmp(&right.name));
        tools
    }

    pub(super) fn is_tool(name: &str) -> bool {
        matches!(
            name,
            PREVIEW_DRAIN_TOOL | CONFIRM_DRAIN_TOOL | PREVIEW_QUEUE_TOOL | CONFIRM_QUEUE_TOOL
        )
    }

    /// Argument-free gate run before parsing, admission or any durable write.
    /// Callers without the tool's action capability get only a counted in-memory
    /// denial, so they cannot fill the bounded durable audit file. Every later
    /// preview, confirmation and dispatch authority check still runs.
    pub(super) fn require_action_capability(
        tool_name: &str,
        context: &McpExecutionContext,
        policy: &McpCapabilityPolicy,
    ) -> Result<(), String> {
        let capable = match tool_name {
            PREVIEW_DRAIN_TOOL | CONFIRM_DRAIN_TOOL => Self::authorized_runtime(context, policy),
            PREVIEW_QUEUE_TOOL | CONFIRM_QUEUE_TOOL => {
                context.principal.is_some() && policy.allows(McpCapabilityClass::Mutate)
            }
            _ => false,
        };
        if capable {
            return Ok(());
        }
        context.record_action_audit(tool_name, "capability_denied", McpAuditDecision::Denied);
        Err("MCP action request was denied".into())
    }

    pub(super) fn preview_runtime_drain(
        &self,
        context: &McpExecutionContext,
        policy: &McpCapabilityPolicy,
        token_fingerprint: [u8; 32],
        runtime: &Runtime,
    ) -> Result<serde_json::Value, String> {
        if !Self::authorized_runtime(context, policy) {
            return self.denied(context, PREVIEW_DRAIN_TOOL, "authority_denied");
        }
        let observation = runtime_drain_observation(runtime);
        let observation_hash = digest_value(&observation)?;
        self.issue_preview(
            Action::RuntimeDrain,
            self.runtime_target.to_string(),
            observation_hash,
            context,
            token_fingerprint,
        )
    }

    pub(super) fn preview_queue_dead_letter(
        &self,
        arguments: Option<serde_json::Value>,
        context: &McpExecutionContext,
        policy: &McpCapabilityPolicy,
        token_fingerprint: [u8; 32],
        runtime: &Runtime,
    ) -> Result<serde_json::Value, String> {
        let Some(arguments) = arguments else {
            return self.denied(context, PREVIEW_QUEUE_TOOL, "missing_arguments");
        };
        let parsed: PreviewQueueDeadLetterArguments = match serde_json::from_value(arguments) {
            Ok(parsed) => parsed,
            Err(_) => return self.denied(context, PREVIEW_QUEUE_TOOL, "invalid_arguments"),
        };
        let target = QueueTarget {
            route_family: parsed.route_family,
            realm: parsed.realm,
            area: parsed.area,
            resource: parsed.resource,
            message_id: parsed.message_id,
        };
        if !Self::authorized_queue(context, policy, runtime, &target, &parsed.operation) {
            return self.denied(context, PREVIEW_QUEUE_TOOL, "authority_denied");
        }
        let observation = match queue_dead_letter_observation(runtime, &target) {
            Err(_) => {
                return self.denied(context, PREVIEW_QUEUE_TOOL, "state_observation_failed");
            }
            Ok(Some(observation)) => observation,
            Ok(None) => return self.denied(context, PREVIEW_QUEUE_TOOL, "dead_letter_not_found"),
        };
        let observed_state = digest_value(&observation)?;
        let action = Action::QueueDeadLetter {
            target: target.clone(),
            operation: match parsed.operation {
                QueueOperationInput::Replay => QueueDeadLetterOperation::Replay,
                QueueOperationInput::Purge => QueueDeadLetterOperation::Purge,
            },
        };
        let mut display_target = target.display();
        display_target.push_str(";observed_state_sha256=");
        display_target.push_str(&hex::encode(observed_state));
        self.issue_preview(
            action,
            display_target,
            observed_state,
            context,
            token_fingerprint,
        )
    }

    pub(super) fn consume_confirmation(
        &self,
        tool_name: &str,
        arguments: Option<serde_json::Value>,
        context: &McpExecutionContext,
        policy: &McpCapabilityPolicy,
        token_fingerprint: [u8; 32],
        runtime: &Runtime,
    ) -> Result<ActionTicket, String> {
        let Some(arguments) = arguments else {
            return self.denied(context, tool_name, "missing_arguments");
        };
        let arguments: ConfirmActionArguments = match serde_json::from_value(arguments) {
            Ok(arguments) => arguments,
            Err(_) => return self.denied(context, tool_name, "invalid_arguments"),
        };
        let Some(username) = context
            .principal
            .as_ref()
            .map(|principal| principal.username.clone())
        else {
            return self.denied(context, tool_name, "authentication_required");
        };
        let ticket = {
            let mut previews = self.previews.lock();
            let Some(preview) = previews.get(&arguments.challenge_id).cloned() else {
                return self.denied(context, tool_name, "challenge_missing_or_used");
            };
            if preview.expires_at <= Instant::now() {
                previews.remove(&arguments.challenge_id);
                return self.denied(context, tool_name, "challenge_expired");
            }
            if preview.action.confirmation_tool() != tool_name
                || preview.username != username
                || preview.token_fingerprint != token_fingerprint
                || preview.target != arguments.target
                || preview.confirmation != arguments.confirmation
            {
                return self.denied(context, tool_name, "confirmation_mismatch");
            }
            if !Self::authorized_action(context, policy, runtime, &preview.action) {
                return self.denied(context, tool_name, "authority_revalidation_denied");
            }
            previews.remove(&arguments.challenge_id);
            ActionTicket {
                operation_id: arguments.challenge_id,
                username,
                token_fingerprint,
                action: preview.action,
                target: preview.target,
                observed_state: preview.observed_state,
            }
        };
        context.record_action_audit(
            tool_name,
            "confirmation_accepted",
            McpAuditDecision::Allowed,
        );
        Ok(ticket)
    }

    #[allow(clippy::too_many_lines)] // Intent, revalidation, dispatch and outcome audit order is security-sensitive.
    pub(super) fn execute(
        &self,
        ticket: ActionTicket,
        context: &McpExecutionContext,
        policy: &McpCapabilityPolicy,
        token_fingerprint: [u8; 32],
        runtime: &Runtime,
    ) -> Result<serde_json::Value, String> {
        let confirmation_tool = ticket.action.confirmation_tool();
        if !self.audit.append(
            &ticket.operation_id,
            &ticket.username,
            ticket.action.audit_name(),
            &ticket.target,
            "intent",
        ) {
            context.record_action_audit(
                confirmation_tool,
                "audit_intent_unavailable",
                McpAuditDecision::Denied,
            );
            return Err(
                "action audit intent could not be persisted; command was not started".into(),
            );
        }
        if context
            .principal
            .as_ref()
            .map(|principal| &principal.username)
            != Some(&ticket.username)
            || ticket.token_fingerprint != token_fingerprint
        {
            return self.finish_known_failure(
                &ticket,
                context,
                confirmation_tool,
                "authority_revalidation_denied",
                "action authority changed before dispatch; no command was started".to_string(),
            );
        }
        if !Self::authorized_action(context, policy, runtime, &ticket.action) {
            return self.finish_known_failure(
                &ticket,
                context,
                confirmation_tool,
                "authority_revalidation_denied",
                "action authority changed before dispatch; no command was started".to_string(),
            );
        }

        let (outcome, runtime_result) = match &ticket.action {
            Action::RuntimeDrain => {
                let current = digest_value(&runtime_drain_observation(runtime))?;
                if current != ticket.observed_state {
                    return self.finish_known_failure(
                        &ticket,
                        context,
                        confirmation_tool,
                        "state_changed",
                        "runtime state changed after preview; request a fresh preview".to_string(),
                    );
                }
                ("completed", Some(begin_runtime_drain(runtime)))
            }
            Action::QueueDeadLetter { target, operation } => {
                let Ok(current) = queue_dead_letter_current_observation(runtime, target) else {
                    return self.finish_known_failure(
                        &ticket,
                        context,
                        confirmation_tool,
                        "state_revalidation_failed",
                        "Queue state could not be revalidated; no command was started".into(),
                    );
                };
                let current_hash = current.as_ref().map(digest_value).transpose()?;
                if current_hash != Some(ticket.observed_state) {
                    return self.finish_known_failure(
                        &ticket,
                        context,
                        confirmation_tool,
                        "state_changed",
                        "Queue dead-letter state changed after preview; request a fresh preview"
                            .to_string(),
                    );
                }
                let result = queue_dead_letter(
                    runtime,
                    target.route_family,
                    &target.realm,
                    &target.area,
                    &target.resource,
                    target.message_id,
                    *operation,
                );
                match result {
                    Ok(true) => ("completed", None),
                    Ok(false) => ("not_found", None),
                    Err(error) => {
                        self.audit.append(
                            &ticket.operation_id,
                            &ticket.username,
                            ticket.action.audit_name(),
                            &ticket.target,
                            "indeterminate",
                        );
                        context.record_action_audit(
                            confirmation_tool,
                            "indeterminate_do_not_retry",
                            McpAuditDecision::Denied,
                        );
                        return Err(format!(
                            "Queue action outcome is indeterminate; do not retry; inspect operation {}: {error}",
                            ticket.operation_id
                        ));
                    }
                }
            }
        };

        if !self.audit.append(
            &ticket.operation_id,
            &ticket.username,
            ticket.action.audit_name(),
            &ticket.target,
            outcome,
        ) {
            context.record_action_audit(
                confirmation_tool,
                "indeterminate_do_not_retry",
                McpAuditDecision::Denied,
            );
            return Err(format!(
                "action outcome is indeterminate; do not retry; inspect operation {}",
                ticket.operation_id
            ));
        }
        context.record_action_audit(confirmation_tool, outcome, McpAuditDecision::Allowed);
        serde_json::to_value(ActionResultOutput {
            operation_id: ticket.operation_id,
            action: ticket.action.audit_name().to_string(),
            target: ticket.target,
            outcome: outcome.to_string(),
            runtime: runtime_result,
        })
        .map_err(|_| "action completed but its response could not be encoded".to_string())
    }

    /// Persist the request's interruption separately from any later synchronous command outcome.
    pub(super) fn record_request_interruption(
        &self,
        tool_name: &str,
        challenge_id: Option<&str>,
        context: &McpExecutionContext,
        fingerprint: [u8; 32],
        outcome: &'static str,
    ) -> Result<(), String> {
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
        context.record_action_audit(tool_name, outcome, McpAuditDecision::Denied);
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

    fn issue_preview(
        &self,
        action: Action,
        target: String,
        observed_state: [u8; 32],
        context: &McpExecutionContext,
        token_fingerprint: [u8; 32],
    ) -> Result<serde_json::Value, String> {
        if target.len() > MAX_TARGET_BYTES {
            return self.denied(context, "mcp_action_preview", "target_over_limit");
        }
        let username = context
            .principal
            .as_ref()
            .map(|principal| principal.username.clone())
            .ok_or_else(|| "authenticated principal is required".to_string())?;
        let challenge_id = Uuid::new_v4().to_string();
        let audit_action = action.audit_name();
        let action_name = audit_action.to_string();
        let confirmation = format!("{} {target}", action.confirmation_verb());
        let mut previews = self.previews.lock();
        previews.retain(|_, preview| preview.expires_at > Instant::now());
        if previews.len() >= MAX_PREVIEWS {
            drop(previews);
            return self.denied(context, "mcp_action_preview", "preview_capacity_full");
        }
        let pending = PendingAction {
            username: username.clone(),
            token_fingerprint,
            action,
            target: target.clone(),
            confirmation: confirmation.clone(),
            observed_state,
            expires_at: Instant::now() + PREVIEW_TTL,
        };
        previews.insert(challenge_id.clone(), pending);
        drop(previews);
        if !self
            .audit
            .append(&challenge_id, &username, audit_action, &target, "preview")
        {
            self.previews.lock().remove(&challenge_id);
            context.record_action_audit(
                "mcp_action_preview",
                "audit_unavailable",
                McpAuditDecision::Denied,
            );
            return Err("action audit sink is unavailable; no preview was issued".into());
        }
        context.record_action_audit(
            "mcp_action_preview",
            "preview_issued",
            McpAuditDecision::Allowed,
        );
        serde_json::to_value(ActionPreviewOutput {
            challenge_id,
            action: action_name,
            target,
            confirmation,
            observed_state_sha256: hex::encode(observed_state),
            expires_in_seconds: PREVIEW_TTL.as_secs(),
        })
        .map_err(|_| "could not encode action preview".to_string())
    }

    fn finish_known_failure(
        &self,
        ticket: &ActionTicket,
        context: &McpExecutionContext,
        confirmation_tool: &str,
        outcome: &'static str,
        message: String,
    ) -> Result<serde_json::Value, String> {
        if !self.audit.append(
            &ticket.operation_id,
            &ticket.username,
            ticket.action.audit_name(),
            &ticket.target,
            outcome,
        ) {
            context.record_action_audit(
                confirmation_tool,
                "indeterminate_do_not_retry",
                McpAuditDecision::Denied,
            );
            return Err(format!(
                "action outcome is indeterminate; do not retry; inspect operation {}",
                ticket.operation_id
            ));
        }
        context.record_action_audit(confirmation_tool, outcome, McpAuditDecision::Denied);
        Err(message)
    }

    fn denied<T>(
        &self,
        context: &McpExecutionContext,
        tool_name: &str,
        reason: &str,
    ) -> Result<T, String> {
        context.record_action_audit(tool_name, reason, McpAuditDecision::Denied);
        let username = context
            .principal_name()
            .unwrap_or_else(|| "unknown".to_string());
        let operation_id = Uuid::new_v4().to_string();
        if !self.audit.append(
            &operation_id,
            &username,
            action_name(tool_name),
            "redacted",
            reason,
        ) {
            return Err("action denial could not be durably audited".into());
        }
        Err("MCP action request was denied".into())
    }

    fn authorized_action(
        context: &McpExecutionContext,
        policy: &McpCapabilityPolicy,
        runtime: &Runtime,
        action: &Action,
    ) -> bool {
        match action {
            Action::RuntimeDrain => Self::authorized_runtime(context, policy),
            Action::QueueDeadLetter { target, operation } => {
                let input = match operation {
                    QueueDeadLetterOperation::Replay => QueueOperationInput::Replay,
                    QueueDeadLetterOperation::Purge => QueueOperationInput::Purge,
                };
                Self::authorized_queue(context, policy, runtime, target, &input)
            }
        }
    }

    fn authorized_runtime(context: &McpExecutionContext, policy: &McpCapabilityPolicy) -> bool {
        let Some(AdminPrincipal {
            route_family_access,
            ..
        }) = context.principal.as_ref()
        else {
            return false;
        };
        route_family_access.is_wildcard()
            && policy.allows(McpCapabilityClass::Mutate)
            && policy.allows(McpCapabilityClass::Admin)
            && crate::runtime::DomainKind::ALL.into_iter().all(|domain| {
                context.permissions.allows_registration_pattern(
                    &crate::runtime::matcher::Pattern::new(domain.wildcard_route()),
                    Access::Write,
                )
            })
    }

    fn authorized_queue(
        context: &McpExecutionContext,
        policy: &McpCapabilityPolicy,
        runtime: &Runtime,
        target: &QueueTarget,
        operation: &QueueOperationInput,
    ) -> bool {
        let Some(principal) = context.principal.as_ref() else {
            return false;
        };
        let family_is_valid = u32::try_from(target.route_family)
            .is_ok_and(|family| runtime.admin_auth().is_provisioned_route_family(family));
        let required_capability = policy.allows(McpCapabilityClass::Mutate)
            && (matches!(operation, QueueOperationInput::Replay)
                || policy.allows(McpCapabilityClass::Admin));
        let request = target.scope();
        let valid_route = request.validate().is_ok();
        family_is_valid
            && valid_route
            && principal
                .route_family_access
                .allows(&target.route_family.to_string())
            && required_capability
            && context.permissions.allows_route(
                &format!(
                    "queue://{}/{}/{}",
                    target.realm, target.area, target.resource
                ),
                Access::Write,
            )
    }
}

#[derive(Debug, Clone)]
pub(super) struct ActionTicket {
    pub(super) operation_id: String,
    username: String,
    token_fingerprint: [u8; 32],
    action: Action,
    target: String,
    observed_state: [u8; 32],
}

fn read_only_action_annotations() -> ToolAnnotations {
    ToolAnnotations::new().read_only(true).open_world(false)
}

fn destructive_action_annotations() -> ToolAnnotations {
    ToolAnnotations::new()
        .read_only(false)
        .destructive(true)
        .idempotent(false)
        .open_world(false)
}

fn action_name(tool_name: &str) -> &'static str {
    match tool_name {
        PREVIEW_DRAIN_TOOL | CONFIRM_DRAIN_TOOL => "runtime.drain",
        PREVIEW_QUEUE_TOOL | CONFIRM_QUEUE_TOOL => "queue.dead-letter",
        _ => "unknown",
    }
}

fn runtime_drain_observation(runtime: &Runtime) -> serde_json::Value {
    serde_json::json!({
        "lifecycle_state": runtime.lifecycle_state().as_str(),
        "active_sessions": runtime.session_count(),
        "drain_grace_seconds": runtime.drain_grace_seconds(),
    })
}

fn queue_dead_letter_observation(
    runtime: &Runtime,
    target: &QueueTarget,
) -> Result<Option<serde_json::Value>, String> {
    let current = runtime.queue_dead_letter(
        target.route_family,
        &target.realm,
        &target.area,
        &target.resource,
        target.message_id,
    );
    encode_queue_dead_letter_observation(current)
}

fn queue_dead_letter_current_observation(
    runtime: &Runtime,
    target: &QueueTarget,
) -> Result<Option<serde_json::Value>, String> {
    encode_queue_dead_letter_observation(runtime.queue_inspect_dead_letter(
        target.route_family,
        &target.realm,
        &target.area,
        &target.resource,
        target.message_id,
    )?)
}

fn encode_queue_dead_letter_observation(
    current: Option<crate::control::admin::QueueDeadLetter>,
) -> Result<Option<serde_json::Value>, String> {
    current
        .map(|message| {
            let reason_hash: [u8; 32] = Sha256::digest(message.reason.as_bytes()).into();
            serde_json::to_value((
                message.message_id,
                message.family,
                message.realm,
                message.area,
                message.resource,
                message.dead_lettered_at,
                message.attempts,
                hex::encode(reason_hash),
            ))
            .map_err(|_| "could not encode Queue dead-letter state".to_string())
        })
        .transpose()
}

fn digest_value(value: &serde_json::Value) -> Result<[u8; 32], String> {
    let bytes = serde_json::to_vec(value)
        .map_err(|_| "could not encode action state fingerprint".to_string())?;
    Ok(Sha256::digest(bytes).into())
}

#[cfg(test)]
mod tests;
