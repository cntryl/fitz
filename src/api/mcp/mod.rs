//! MCP tool registry and safety primitives
//!
//! This module defines the first read-only MCP surface for Fitz. It reuses the
//! admin troubleshooting and stats read models so MCP tools mirror the same
//! bounded control-plane facts exposed through REST.

use crate::api::admin::auth::AdminPrincipal;
use crate::api::admin::troubleshooting::{
    kv_resource_timeline, lease_resource_timeline, notice_resource_timeline,
    queue_resource_timeline, rpc_resource_timeline, schedule_resource_timeline,
    stream_resource_timeline,
};
use crate::api::admin::{
    build_global_stats, build_global_troubleshooting, kv_detail, lease_detail, notice_detail,
    queue_detail, rpc_operations, schedule_detail, stream_detail, ResourcePath,
};
use crate::auth::Access;
use crate::boot::Runtime;
use crate::session::permissions::SessionPermissions;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpCapabilityClass {
    Summary,
    Inspect,
    Explain,
    Mutate,
    Admin,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct McpCostBudget {
    pub max_result_items: usize,
    pub max_result_bytes: usize,
    pub max_runtime_ms: u64,
}

impl McpCostBudget {
    #[must_use]
    pub const fn summary() -> Self {
        Self {
            max_result_items: 1,
            max_result_bytes: 64 * 1024,
            max_runtime_ms: 50,
        }
    }

    #[must_use]
    pub const fn inspect() -> Self {
        Self {
            max_result_items: 8,
            max_result_bytes: 128 * 1024,
            max_runtime_ms: 100,
        }
    }

    #[must_use]
    pub const fn timeline() -> Self {
        Self {
            max_result_items: 50,
            max_result_bytes: 256 * 1024,
            max_runtime_ms: 200,
        }
    }

    #[must_use]
    pub fn allows_value(&self, value: &Value) -> bool {
        serde_json::to_vec(value).is_ok_and(|bytes| bytes.len() <= self.max_result_bytes)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpCapabilityPolicy {
    allowed_classes: BTreeSet<McpCapabilityClass>,
}

impl McpCapabilityPolicy {
    pub fn from_classes(classes: impl IntoIterator<Item = McpCapabilityClass>) -> Self {
        Self {
            allowed_classes: classes.into_iter().collect(),
        }
    }

    #[must_use]
    pub fn summary_only() -> Self {
        Self::from_classes([McpCapabilityClass::Summary])
    }

    #[must_use]
    pub fn read_only() -> Self {
        Self::from_classes([
            McpCapabilityClass::Summary,
            McpCapabilityClass::Inspect,
            McpCapabilityClass::Explain,
        ])
    }

    #[must_use]
    pub fn allows(&self, capability: McpCapabilityClass) -> bool {
        self.allowed_classes.contains(&capability)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct McpToolDescriptor {
    pub name: String,
    pub capability: McpCapabilityClass,
    pub summary: String,
    pub rest_path: String,
    pub budget: McpCostBudget,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct McpResourceDetailRequest {
    pub scheme: String,
    pub realm: String,
    pub area: String,
    pub resource: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_family: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
}

impl McpResourceDetailRequest {
    fn scope_route(&self) -> String {
        format!(
            "{}://{}/{}/{}",
            self.scheme, self.realm, self.area, self.resource
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum McpAuditDecision {
    Allowed,
    Denied,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct McpAuditRecord {
    pub principal: Option<String>,
    pub tool_name: String,
    pub capability: McpCapabilityClass,
    pub scope_route: Option<String>,
    pub argument_summary: String,
    pub decision: McpAuditDecision,
    pub result_summary: String,
}

#[derive(Debug, Clone)]
pub struct McpExecutionContext {
    pub principal: Option<AdminPrincipal>,
    pub permissions: SessionPermissions,
    audit_log: Arc<Mutex<Vec<McpAuditRecord>>>,
}

impl McpExecutionContext {
    #[must_use]
    pub fn authenticated(principal: AdminPrincipal, permissions: SessionPermissions) -> Self {
        Self {
            principal: Some(principal),
            permissions,
            audit_log: Arc::new(Mutex::new(Vec::new())),
        }
    }

    #[must_use]
    pub fn anonymous(permissions: SessionPermissions) -> Self {
        Self {
            principal: None,
            permissions,
            audit_log: Arc::new(Mutex::new(Vec::new())),
        }
    }

    #[must_use]
    pub fn audit_records(&self) -> Vec<McpAuditRecord> {
        self.audit_log.lock().clone()
    }

    fn record_audit(&self, record: McpAuditRecord) {
        self.audit_log.lock().push(record);
    }

    fn principal_name(&self) -> Option<String> {
        self.principal
            .as_ref()
            .map(|principal| principal.username.clone())
    }
}

#[derive(Debug, Clone)]
enum McpInvocation {
    Global,
    Resource(McpResourceDetailRequest),
}

impl McpInvocation {
    fn scope_route(&self) -> Option<String> {
        match self {
            McpInvocation::Global => None,
            McpInvocation::Resource(request) => Some(request.scope_route()),
        }
    }

    fn resource_request(&self) -> Option<&McpResourceDetailRequest> {
        match self {
            McpInvocation::Global => None,
            McpInvocation::Resource(request) => Some(request),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum McpToolError {
    UnknownTool {
        tool_name: String,
    },
    AuthenticationRequired {
        tool_name: String,
    },
    ScopeDenied {
        tool_name: String,
        scope_route: String,
    },
    CapabilityDenied {
        tool_name: String,
        capability: McpCapabilityClass,
    },
    InvalidArguments {
        tool_name: String,
        reason: String,
    },
    BudgetExceeded {
        tool_name: String,
        observed_bytes: usize,
        budget: McpCostBudget,
    },
    Serialization {
        tool_name: String,
        reason: String,
    },
}

impl fmt::Display for McpToolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            McpToolError::UnknownTool { tool_name } => {
                write!(f, "Unknown MCP tool: {tool_name}")
            }
            McpToolError::AuthenticationRequired { tool_name } => {
                write!(
                    f,
                    "MCP tool {tool_name} requires an authenticated principal"
                )
            }
            McpToolError::ScopeDenied {
                tool_name,
                scope_route,
            } => write!(
                f,
                "MCP tool {tool_name} is not authorized for scope {scope_route}"
            ),
            McpToolError::CapabilityDenied {
                tool_name,
                capability,
            } => write!(f, "MCP tool {tool_name} requires {capability:?} capability"),
            McpToolError::InvalidArguments { tool_name, reason } => {
                write!(
                    f,
                    "MCP tool {tool_name} received invalid arguments: {reason}"
                )
            }
            McpToolError::BudgetExceeded {
                tool_name,
                observed_bytes,
                budget,
            } => write!(
                f,
                "MCP tool {tool_name} exceeded budget: {observed_bytes} bytes > {} bytes",
                budget.max_result_bytes
            ),
            McpToolError::Serialization { tool_name, reason } => {
                write!(f, "MCP tool {tool_name} failed to serialize: {reason}")
            }
        }
    }
}

impl std::error::Error for McpToolError {}

pub type McpToolResult<T> = Result<T, McpToolError>;

type ToolHandler = fn(&Runtime, &McpInvocation) -> McpToolResult<Value>;

#[derive(Clone, Debug)]
struct McpToolDefinition {
    descriptor: McpToolDescriptor,
    handler: ToolHandler,
}

impl McpToolDefinition {
    fn new(descriptor: McpToolDescriptor, handler: ToolHandler) -> Self {
        Self {
            descriptor,
            handler,
        }
    }
}

#[derive(Debug, Clone)]
pub struct McpToolRegistry {
    tools: Vec<McpToolDefinition>,
}

impl McpToolRegistry {
    #[must_use]
    pub fn summary_only() -> Self {
        Self {
            tools: vec![
                Self::global_stats_tool(),
                Self::global_troubleshooting_tool(),
            ],
        }
    }

    #[must_use]
    pub fn read_only() -> Self {
        Self {
            tools: vec![
                Self::global_stats_tool(),
                Self::global_troubleshooting_tool(),
                Self::resource_detail_tool(),
                Self::resource_timeline_tool(),
                Self::global_explanation_tool(),
            ],
        }
    }

    #[must_use]
    pub fn tool_descriptors(&self) -> Vec<McpToolDescriptor> {
        self.tools
            .iter()
            .map(|tool| tool.descriptor.clone())
            .collect()
    }

    pub fn execute(
        &self,
        tool_name: &str,
        runtime: &Runtime,
        context: &McpExecutionContext,
        policy: &McpCapabilityPolicy,
        arguments: Option<&Value>,
    ) -> McpToolResult<Value> {
        let tool = self
            .tools
            .iter()
            .find(|tool| tool.descriptor.name == tool_name)
            .ok_or_else(|| McpToolError::UnknownTool {
                tool_name: tool_name.to_string(),
            })?;

        let argument_summary = arguments
            .and_then(|value| serde_json::to_string(value).ok())
            .unwrap_or_else(|| "null".to_string());
        let invocation = prepare_invocation(tool_name, arguments)?;
        let scope_route = invocation.scope_route();

        if context.principal.is_none() {
            let error = McpToolError::AuthenticationRequired {
                tool_name: tool.descriptor.name.clone(),
            };
            record_audit(
                context,
                &tool.descriptor,
                scope_route,
                argument_summary,
                McpAuditDecision::Denied,
                error.to_string(),
            );
            return Err(error);
        }

        if let Some(scope_route) = scope_route.clone() {
            if !context.permissions.allows_route(&scope_route, Access::Read) {
                let error = McpToolError::ScopeDenied {
                    tool_name: tool.descriptor.name.clone(),
                    scope_route: scope_route.clone(),
                };
                record_audit(
                    context,
                    &tool.descriptor,
                    Some(scope_route),
                    argument_summary,
                    McpAuditDecision::Denied,
                    error.to_string(),
                );
                return Err(error);
            }
        }

        if !policy.allows(tool.descriptor.capability) {
            let error = McpToolError::CapabilityDenied {
                tool_name: tool.descriptor.name.clone(),
                capability: tool.descriptor.capability,
            };
            record_audit(
                context,
                &tool.descriptor,
                scope_route,
                argument_summary,
                McpAuditDecision::Denied,
                error.to_string(),
            );
            return Err(error);
        }

        let value = (tool.handler)(runtime, &invocation)?;
        let encoded = serde_json::to_vec(&value).map_err(|error| McpToolError::Serialization {
            tool_name: tool.descriptor.name.clone(),
            reason: error.to_string(),
        })?;

        if !tool.descriptor.budget.allows_value(&value) {
            let error = McpToolError::BudgetExceeded {
                tool_name: tool.descriptor.name.clone(),
                observed_bytes: encoded.len(),
                budget: tool.descriptor.budget,
            };
            record_audit(
                context,
                &tool.descriptor,
                scope_route,
                argument_summary,
                McpAuditDecision::Denied,
                error.to_string(),
            );
            return Err(error);
        }

        record_audit(
            context,
            &tool.descriptor,
            scope_route,
            argument_summary,
            McpAuditDecision::Allowed,
            format!("ok:{}bytes", encoded.len()),
        );

        Ok(value)
    }

    fn global_stats_tool() -> McpToolDefinition {
        McpToolDefinition::new(
            McpToolDescriptor {
                name: "get_global_stats".to_string(),
                capability: McpCapabilityClass::Summary,
                summary:
                    "Mirror the bounded global stats summary already exposed through /api/v1/stats"
                        .to_string(),
                rest_path: "/api/v1/stats".to_string(),
                budget: McpCostBudget::summary(),
            },
            |runtime, _invocation| {
                serialize_tool_output("get_global_stats", build_global_stats(runtime))
            },
        )
    }

    fn global_troubleshooting_tool() -> McpToolDefinition {
        McpToolDefinition::new(
            McpToolDescriptor {
                name: "get_global_troubleshooting".to_string(),
                capability: McpCapabilityClass::Summary,
                summary:
                    "Mirror the bounded global troubleshooting guidance already exposed through /api/v1/troubleshooting"
                        .to_string(),
                rest_path: "/api/v1/troubleshooting".to_string(),
                budget: McpCostBudget::summary(),
            },
            |runtime, _invocation| {
                serialize_tool_output("get_global_troubleshooting", build_global_troubleshooting(runtime))
            },
        )
    }

    fn resource_detail_tool() -> McpToolDefinition {
        McpToolDefinition::new(
            McpToolDescriptor {
                name: "inspect_resource_detail".to_string(),
                capability: McpCapabilityClass::Inspect,
                summary:
                    "Inspect a bounded per-resource troubleshooting detail using the same admin read models"
                        .to_string(),
                rest_path: "/api/v1/:scheme/:realm/:area/:resource".to_string(),
                budget: McpCostBudget::inspect(),
            },
            build_resource_detail_value,
        )
    }

    fn resource_timeline_tool() -> McpToolDefinition {
        McpToolDefinition::new(
            McpToolDescriptor {
                name: "inspect_resource_timeline".to_string(),
                capability: McpCapabilityClass::Inspect,
                summary:
                    "Inspect bounded recent transitions for a resource using the same admin timeline builders"
                        .to_string(),
                rest_path: "/api/v1/:scheme/:realm/:area/:resource/events".to_string(),
                budget: McpCostBudget::timeline(),
            },
            build_resource_timeline_value,
        )
    }

    fn global_explanation_tool() -> McpToolDefinition {
        McpToolDefinition::new(
            McpToolDescriptor {
                name: "explain_global_troubleshooting".to_string(),
                capability: McpCapabilityClass::Explain,
                summary:
                    "Explain the current global incident summary and bounded next-query guidance"
                        .to_string(),
                rest_path: "/api/v1/troubleshooting".to_string(),
                budget: McpCostBudget::summary(),
            },
            |runtime, _invocation| {
                serialize_tool_output(
                    "explain_global_troubleshooting",
                    build_global_troubleshooting(runtime),
                )
            },
        )
    }
}

fn prepare_invocation(tool_name: &str, arguments: Option<&Value>) -> McpToolResult<McpInvocation> {
    match tool_name {
        "inspect_resource_detail" | "inspect_resource_timeline" => {
            let arguments = arguments.ok_or_else(|| McpToolError::InvalidArguments {
                tool_name: tool_name.to_string(),
                reason: "missing request payload".to_string(),
            })?;

            let request: McpResourceDetailRequest = serde_json::from_value(arguments.clone())
                .map_err(|error| McpToolError::InvalidArguments {
                    tool_name: tool_name.to_string(),
                    reason: error.to_string(),
                })?;

            Ok(McpInvocation::Resource(request))
        }
        _ => Ok(McpInvocation::Global),
    }
}

fn record_audit(
    context: &McpExecutionContext,
    descriptor: &McpToolDescriptor,
    scope_route: Option<String>,
    argument_summary: String,
    decision: McpAuditDecision,
    result_summary: String,
) {
    context.record_audit(McpAuditRecord {
        principal: context.principal_name(),
        tool_name: descriptor.name.clone(),
        capability: descriptor.capability,
        scope_route,
        argument_summary,
        decision,
        result_summary,
    });
}

fn serialize_tool_output<T: Serialize>(tool_name: &str, output: T) -> McpToolResult<Value> {
    serde_json::to_value(output).map_err(|error| McpToolError::Serialization {
        tool_name: tool_name.to_string(),
        reason: error.to_string(),
    })
}

fn build_resource_detail_value(
    runtime: &Runtime,
    invocation: &McpInvocation,
) -> McpToolResult<Value> {
    let tool_name = "inspect_resource_detail";
    let request = invocation
        .resource_request()
        .ok_or_else(|| McpToolError::InvalidArguments {
            tool_name: tool_name.to_string(),
            reason: "missing request payload".to_string(),
        })?;

    let path = ResourcePath {
        realm: &request.realm,
        area: &request.area,
        resource: &request.resource,
    };

    match request.scheme.as_str() {
        "kv" => serialize_tool_output(tool_name, kv_detail(runtime, &path, None)),
        "queue" => serialize_tool_output(
            tool_name,
            queue_detail(runtime, &path, request.queue_family),
        ),
        "stream" => serialize_tool_output(tool_name, stream_detail(runtime, &path, None)),
        "lease" => serialize_tool_output(tool_name, lease_detail(runtime, &path, None)),
        "schedule" => serialize_tool_output(tool_name, schedule_detail(runtime, &path, None)),
        "notice" => serialize_tool_output(tool_name, notice_detail(runtime, &path, None)),
        "rpc" => serialize_tool_output(tool_name, rpc_operations(runtime, &path, None)),
        other => Err(McpToolError::InvalidArguments {
            tool_name: tool_name.to_string(),
            reason: format!("unsupported resource scheme: {other}"),
        }),
    }
}

fn build_resource_timeline_value(
    runtime: &Runtime,
    invocation: &McpInvocation,
) -> McpToolResult<Value> {
    let tool_name = "inspect_resource_timeline";
    let request = invocation
        .resource_request()
        .ok_or_else(|| McpToolError::InvalidArguments {
            tool_name: tool_name.to_string(),
            reason: "missing request payload".to_string(),
        })?;

    let limit = request
        .limit
        .unwrap_or_else(|| McpCostBudget::timeline().max_result_items)
        .clamp(1, McpCostBudget::timeline().max_result_items);
    let path = ResourcePath {
        realm: &request.realm,
        area: &request.area,
        resource: &request.resource,
    };
    let read_model = runtime.admin_read_model();

    match request.scheme.as_str() {
        "kv" => serialize_tool_output(
            tool_name,
            kv_resource_timeline(&read_model.kv_transactions(None), &path, limit),
        ),
        "queue" => serialize_tool_output(
            tool_name,
            queue_resource_timeline(
                &read_model.queues(None),
                &read_model.queue_inflight(None),
                &read_model.queue_dead_letters(None),
                &path,
                request.queue_family,
                limit,
            ),
        ),
        "stream" => serialize_tool_output(
            tool_name,
            stream_resource_timeline(&read_model.streams(None), &path, limit),
        ),
        "lease" => serialize_tool_output(
            tool_name,
            lease_resource_timeline(&read_model.leases(None), &path, limit),
        ),
        "notice" => serialize_tool_output(
            tool_name,
            notice_resource_timeline(
                &read_model.notice_subscriptions(None, None),
                &read_model.notice_routes(None),
                &path,
                limit,
            ),
        ),
        "rpc" => serialize_tool_output(
            tool_name,
            rpc_resource_timeline(
                &read_model.rpc_workers(None),
                &read_model.rpc_pending(None),
                &path,
                limit,
            ),
        ),
        "schedule" => serialize_tool_output(
            tool_name,
            schedule_resource_timeline(
                &read_model.schedules(None),
                runtime.schedule_pending_fire_claims(),
                runtime.schedule_pending_ack_retries(),
                runtime.schedule_oldest_pending_claim_age_seconds(),
                runtime.schedule_notify_failures(),
                runtime.schedule_ack_failures(),
                runtime.schedule_overdue_normalizations(),
                runtime.schedule_pending_claims_expired_total(),
                runtime.schedule_pending_claim_cleanup_failures_total(),
                &path,
                limit,
            ),
        ),
        other => Err(McpToolError::InvalidArguments {
            tool_name: tool_name.to_string(),
            reason: format!("unsupported resource scheme: {other}"),
        }),
    }
}

#[cfg(test)]
mod tests;
