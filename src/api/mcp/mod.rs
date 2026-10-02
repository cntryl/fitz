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
pub use scope::McpScopedResourceRequest;

mod audit;
pub use audit::{McpAuditDecision, McpAuditRecord, McpExecutionContext};

#[derive(Debug, Clone)]
enum McpInvocation {
    Global,
    Resource(McpScopedResourceRequest),
}

impl McpInvocation {
    fn scope_route(&self) -> Option<String> {
        match self {
            McpInvocation::Global => None,
            McpInvocation::Resource(request) => Some(request.resource.scope_route()),
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
        }
    }

    fn resource_request(&self) -> Option<&McpResourceDetailRequest> {
        match self {
            McpInvocation::Global => None,
            McpInvocation::Resource(request) => Some(&request.resource),
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

    /// Executes the named MCP tool against the current runtime and policy context.
    ///
    /// # Errors
    ///
    /// Returns an error when the tool is unknown, the arguments are invalid,
    /// or execution is rejected by capability or runtime checks.
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
                    principal: context.principal_name(),
                    tool_name: "unknown".to_string(),
                    capability: McpCapabilityClass::Summary,
                    scope_route: None,
                    argument_summary: "redacted".to_string(),
                    decision: McpAuditDecision::Denied,
                    result_summary: "unknown_tool".to_string(),
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
        _ => Ok(McpInvocation::Global),
    }
}

fn authorize_scope(
    context: &McpExecutionContext,
    descriptor: &McpToolDescriptor,
    invocation: &McpInvocation,
    scope_route: Option<&str>,
    argument_summary: &str,
) -> McpToolResult<()> {
    if scope_route.as_ref().map_or_else(
        || {
            crate::runtime::DomainKind::ALL.into_iter().all(|domain| {
                context.permissions.allows_registration_pattern(
                    &crate::runtime::matcher::Pattern::new(domain.wildcard_route()),
                    Access::Read,
                )
            })
        },
        |route| context.permissions.allows_route(route, Access::Read),
    ) && context
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

mod resources;
use resources::{build_resource_detail_value, build_resource_timeline_value};

#[cfg(test)]
mod tests;

#[cfg(test)]
mod family_tests;

#[cfg(test)]
mod scope_tests;
