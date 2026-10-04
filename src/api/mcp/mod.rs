//! MCP tool registry and safety primitives
//!
//! This module defines Fitz's MCP surface. It reuses admin read models and
//! shared command paths so MCP mirrors the control-plane facts and guarded
//! operations exposed through REST.

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
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;
use std::fmt;

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
            max_result_items: 256,
            max_result_bytes: 64 * 1024,
            max_runtime_ms: 50,
        }
    }

    #[must_use]
    pub const fn inspect() -> Self {
        Self {
            max_result_items: 512,
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
    pub const fn collection() -> Self {
        Self {
            max_result_items: 256,
            max_result_bytes: 512 * 1024,
            max_runtime_ms: 200,
        }
    }

    #[must_use]
    pub const fn topology() -> Self {
        Self {
            max_result_items: 2_048,
            max_result_bytes: 512 * 1024,
            max_runtime_ms: 250,
        }
    }

    #[must_use]
    pub const fn metrics() -> Self {
        Self {
            max_result_items: 256,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
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

pub mod catalog;
mod scope;
mod stdio_adapter;
pub(crate) mod transport;

pub use scope::{McpAdminScopeRequest, McpScopedResourceRequest};
pub use stdio_adapter::run_stdio_adapter;

mod audit;
pub(crate) use audit::McpAuditBuffer;
pub use audit::{McpAuditDecision, McpAuditRecord, McpExecutionContext};
pub(crate) mod telemetry;

#[derive(Debug, Clone)]
enum McpInvocation {
    Global,
    Resource(McpScopedResourceRequest),
    AdminScope(McpAdminScopeRequest),
}

impl McpInvocation {
    fn scope_route(&self) -> Option<String> {
        match self {
            McpInvocation::Global => None,
            McpInvocation::Resource(request) => Some(request.resource.scope_route()),
            McpInvocation::AdminScope(request) => request
                .route_family
                .map(|family| format!("route_family:{family}")),
        }
    }

    fn allows_family_access(&self, principal: &AdminPrincipal) -> bool {
        self.route_family().map_or_else(
            || principal.route_family_access.is_wildcard(),
            |family| principal.route_family_access.allows(&family.to_string()),
        )
    }

    fn route_family(&self) -> Option<u64> {
        match self {
            Self::Global => None,
            Self::Resource(request) => request.effective_family(),
            Self::AdminScope(request) => request.route_family,
        }
    }

    fn resource_request(&self) -> Option<&McpResourceDetailRequest> {
        match self {
            McpInvocation::Resource(request) => Some(&request.resource),
            McpInvocation::Global | McpInvocation::AdminScope(_) => None,
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
    ResultItemsExceeded {
        tool_name: String,
        observed_items: usize,
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
            McpToolError::ResultItemsExceeded {
                tool_name,
                observed_items,
                budget,
            } => write!(
                f,
                "MCP tool {tool_name} exceeded item budget: {observed_items} items > {} items",
                budget.max_result_items
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
                Self::discovery_tool(),
                Self::sessions_tool(),
                Self::topology_tool(),
                Self::metrics_tool(),
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

    /// Executes the named MCP tool against the current runtime and policy context.
    ///
    /// # Errors
    ///
    /// Returns an error when the tool is unknown, the arguments are invalid,
    /// or execution is rejected by capability or runtime checks.
    #[allow(clippy::too_many_lines)] // Tool selection and its common authorization/audit envelope stay together.
    pub fn execute(
        &self,
        tool_name: &str,
        runtime: &Runtime,
        context: &McpExecutionContext,
        policy: &McpCapabilityPolicy,
        arguments: Option<&Value>,
    ) -> McpToolResult<Value> {
        let tool = self.find_tool(tool_name, context)?;

        let argument_summary = arguments.map_or("absent", |_| "provided").to_string();
        let invocation = match prepare_invocation(tool_name, arguments) {
            Ok(invocation) => invocation,
            Err(error) => {
                record_audit(
                    context,
                    &tool.descriptor,
                    None,
                    argument_summary,
                    McpAuditDecision::Denied,
                    "invalid_arguments".to_string(),
                );
                return Err(error);
            }
        };
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

        authorize_scope(
            runtime,
            context,
            &tool.descriptor,
            &invocation,
            scope_route.as_deref(),
            &argument_summary,
        )?;

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

        let value = match (tool.handler)(runtime, &invocation) {
            Ok(value) => value,
            Err(error) => {
                record_audit(
                    context,
                    &tool.descriptor,
                    scope_route,
                    argument_summary,
                    McpAuditDecision::Denied,
                    "handler_error".to_string(),
                );
                return Err(error);
            }
        };
        let encoded = serde_json::to_vec(&value).map_err(|error| McpToolError::Serialization {
            tool_name: tool.descriptor.name.clone(),
            reason: error.to_string(),
        })?;

        let observed_items = count_result_items(&value);
        if observed_items > tool.descriptor.budget.max_result_items {
            let error = McpToolError::ResultItemsExceeded {
                tool_name: tool.descriptor.name.clone(),
                observed_items,
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

        if encoded.len() > tool.descriptor.budget.max_result_bytes {
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

    fn find_tool(
        &self,
        tool_name: &str,
        context: &McpExecutionContext,
    ) -> McpToolResult<&McpToolDefinition> {
        self.tools
            .iter()
            .find(|tool| tool.descriptor.name == tool_name)
            .ok_or_else(|| {
                context.record_audit(McpAuditRecord {
                    correlation_id: None,
                    principal: context.principal_name(),
                    tool_name: "unknown".to_string(),
                    capability: McpCapabilityClass::Summary,
                    scope_route: None,
                    argument_summary: "redacted".to_string(),
                    decision: McpAuditDecision::Denied,
                    result_summary: "unknown_tool".to_string(),
                    duration_ms: 0,
                });
                McpToolError::UnknownTool {
                    tool_name: tool_name.to_string(),
                }
            })
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

    fn discovery_tool() -> McpToolDefinition {
        McpToolDefinition::new(
            McpToolDescriptor {
                name: "get_mcp_discovery".to_string(),
                capability: McpCapabilityClass::Summary,
                summary: "Return the supported Fitz MCP protocol revisions, domains, resources, and authorization model".to_string(),
                rest_path: "/.well-known/oauth-protected-resource/mcp".to_string(),
                budget: McpCostBudget::summary(),
            },
            |_runtime, _invocation| {
                Ok(serde_json::json!({
                    "endpoint": "/mcp",
                    "protocol_revisions": [
                        { "version": "2026-07-28", "lifecycle": "stateless per-request discovery" },
                        { "version": "2025-11-25", "lifecycle": "initialize and session" }
                    ],
                    "domains": crate::runtime::DomainKind::ALL
                        .map(crate::runtime::DomainKind::as_str),
                    "resources": ["domain-guarantees", "operational-fields", "troubleshooting"],
                    "authority": "OAuth grants map to AdminPrincipal route-family authority, SessionPermissions, and MCP capabilities; realm and route_family remain independent",
                    "mutations_default_enabled": false,
                    "diagnostics_are": "current broker read-model snapshots"
                }))
            },
        )
    }

    fn sessions_tool() -> McpToolDefinition {
        McpToolDefinition::new(
            McpToolDescriptor {
                name: "get_sessions".to_string(),
                capability: McpCapabilityClass::Inspect,
                summary: "List bounded active-session diagnostics for all authorized route families or one explicit family".to_string(),
                rest_path: "/api/v1/:route_family/sessions".to_string(),
                budget: McpCostBudget::collection(),
            },
            |runtime, invocation| {
                let family = invocation.route_family();
                let mut sessions = runtime.list_sessions();
                sessions.retain(|session| family.is_none_or(|value| session.route_family == value));
                sessions.sort_by(|left, right| left.session_id.cmp(&right.session_id));
                let total = sessions.len();
                let limit = McpCostBudget::collection().max_result_items;
                let truncated = total > limit;
                sessions.truncate(limit);
                Ok(serde_json::json!({
                    "route_family": family,
                    "sessions": sessions,
                    "truncated": truncated,
                    "limit": limit
                }))
            },
        )
    }

    fn topology_tool() -> McpToolDefinition {
        McpToolDefinition::new(
            McpToolDescriptor {
                name: "get_topology".to_string(),
                capability: McpCapabilityClass::Inspect,
                summary: "Read the bounded admin topology snapshot, optionally for one explicit route family".to_string(),
                rest_path: "/api/v1/:route_family/topology".to_string(),
                budget: McpCostBudget::topology(),
            },
            |runtime, invocation| {
                Ok(crate::api::admin::mcp_topology_value(
                    runtime,
                    invocation.route_family(),
                ))
            },
        )
    }

    fn metrics_tool() -> McpToolDefinition {
        McpToolDefinition::new(
            McpToolDescriptor {
                name: "get_structured_metrics".to_string(),
                capability: McpCapabilityClass::Inspect,
                summary: "Read bounded structured admin metrics for all authorized families or one explicit route family".to_string(),
                rest_path: "/api/v1/:route_family/metrics".to_string(),
                budget: McpCostBudget::metrics(),
            },
            |runtime, invocation| {
                Ok(crate::api::admin::metrics::mcp_structured_metrics_value(
                    runtime,
                    invocation.route_family(),
                    McpCostBudget::metrics().max_result_items,
                ))
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

            let request: McpScopedResourceRequest = serde_json::from_value(arguments.clone())
                .map_err(|error| McpToolError::InvalidArguments {
                    tool_name: tool_name.to_string(),
                    reason: error.to_string(),
                })?;

            request
                .validate()
                .map_err(|reason| McpToolError::InvalidArguments {
                    tool_name: tool_name.to_string(),
                    reason,
                })?;
            Ok(McpInvocation::Resource(request))
        }
        "get_sessions" | "get_topology" | "get_structured_metrics" => {
            let request: McpAdminScopeRequest = match arguments {
                Some(arguments) => serde_json::from_value(arguments.clone()),
                None => serde_json::from_value(serde_json::json!({})),
            }
            .map_err(|error| McpToolError::InvalidArguments {
                tool_name: tool_name.to_string(),
                reason: error.to_string(),
            })?;
            request
                .validate()
                .map_err(|reason| McpToolError::InvalidArguments {
                    tool_name: tool_name.to_string(),
                    reason,
                })?;
            Ok(McpInvocation::AdminScope(request))
        }
        _ => Ok(McpInvocation::Global),
    }
}

fn authorize_scope(
    runtime: &Runtime,
    context: &McpExecutionContext,
    descriptor: &McpToolDescriptor,
    invocation: &McpInvocation,
    scope_route: Option<&str>,
    argument_summary: &str,
) -> McpToolResult<()> {
    let family_is_provisioned = invocation.route_family().is_none_or(|family| {
        u32::try_from(family)
            .is_ok_and(|family| runtime.admin_auth().is_provisioned_route_family(family))
    });
    let has_read_permission = match invocation {
        McpInvocation::Resource(_) => {
            scope_route.is_some_and(|route| context.permissions.allows_route(route, Access::Read))
        }
        McpInvocation::Global | McpInvocation::AdminScope(_) => {
            crate::runtime::DomainKind::ALL.into_iter().all(|domain| {
                context.permissions.allows_registration_pattern(
                    &crate::runtime::matcher::Pattern::new(domain.wildcard_route()),
                    Access::Read,
                )
            })
        }
    };
    if family_is_provisioned
        && has_read_permission
        && context
            .principal
            .as_ref()
            .is_some_and(|principal| invocation.allows_family_access(principal))
    {
        return Ok(());
    }
    let error = McpToolError::ScopeDenied {
        tool_name: descriptor.name.clone(),
        scope_route: scope_route.unwrap_or("all route families").to_string(),
    };
    record_audit(
        context,
        descriptor,
        scope_route.map(str::to_string),
        argument_summary.to_string(),
        McpAuditDecision::Denied,
        error.to_string(),
    );
    Err(error)
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
        correlation_id: None,
        principal: context.principal_name(),
        tool_name: descriptor.name.clone(),
        capability: descriptor.capability,
        scope_route,
        argument_summary,
        decision,
        result_summary,
        duration_ms: 0,
    });
}

fn count_result_items(value: &Value) -> usize {
    match value {
        Value::Array(items) => items.iter().fold(items.len(), |count, item| {
            count.saturating_add(count_result_items(item))
        }),
        Value::Object(fields) => fields.values().fold(0usize, |count, item| {
            count.saturating_add(count_result_items(item))
        }),
        _ => 0,
    }
}

fn serialize_tool_output<T: Serialize>(tool_name: &str, output: T) -> McpToolResult<Value> {
    serde_json::to_value(output).map_err(|error| McpToolError::Serialization {
        tool_name: tool_name.to_string(),
        reason: error.to_string(),
    })
}

mod resources;
use resources::{build_resource_detail_value, build_resource_timeline_value};

#[cfg(test)]
mod tests;

#[cfg(test)]
mod family_tests;

#[cfg(test)]
mod scope_tests;
