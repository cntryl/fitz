use super::actions::{
    McpActionState, CONFIRM_DRAIN_TOOL, CONFIRM_QUEUE_TOOL, PREVIEW_DRAIN_TOOL, PREVIEW_QUEUE_TOOL,
};
use super::oauth::AuthenticatedRequest;
use crate::api::mcp::{
    McpCapabilityClass, McpCapabilityPolicy, McpExecutionContext, McpResourceDetailRequest,
    McpScopedResourceRequest, McpToolRegistry,
};
use crate::boot::Runtime;
use rmcp::model::{
    CallToolRequestParams, CallToolResponse, CallToolResult, ContentBlock, GetPromptRequestParams,
    GetPromptResponse, GetPromptResult, Implementation, ListPromptsResult,
    ListResourceTemplatesResult, ListResourcesResult, ListToolsResult, PaginatedRequestParams,
    PromptMessage, ReadResourceRequestParams, ReadResourceResponse, ReadResourceResult,
    ResourceContents, Role, ServerCapabilities, ServerConfig,
};
use rmcp::service::RequestContext;
use rmcp::{ErrorData, RoleServer, ServerHandler};
use std::borrow::Cow;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

const MAX_ARGUMENT_BYTES: usize = 16 * 1024;
const MAX_MCP_RESPONSE_BYTES: usize = 600 * 1024;
pub(super) const MAX_EXECUTIONS: usize = 32;
const READ_PROMPT: &str = "inspect_resource";
const TIMELINE_PROMPT: &str = "review_resource_timeline";

#[derive(Clone)]
pub(super) struct McpServer {
    runtime: Arc<Runtime>,
    registry: McpToolRegistry,
    context: McpExecutionContext,
    policy: McpCapabilityPolicy,
    token_fingerprint: [u8; 32],
    actions: Option<Arc<McpActionState>>,
    execution_slots: Arc<Semaphore>,
}

impl McpServer {
    pub(super) fn from_authenticated_request(
        runtime: Arc<Runtime>,
        authenticated: AuthenticatedRequest,
        actions: Option<Arc<McpActionState>>,
        execution_slots: Arc<Semaphore>,
    ) -> Self {
        Self {
            runtime,
            registry: McpToolRegistry::read_only(),
            context: authenticated.context,
            policy: authenticated.policy,
            token_fingerprint: authenticated.token_fingerprint,
            actions,
            execution_slots,
        }
    }

    fn tool_list(&self) -> Vec<rmcp::model::Tool> {
        let mut tools = self.registry.protocol_tools();
        if self.actions.is_some() {
            tools.extend(McpActionState::tool_definitions(
                &self.context,
                &self.policy,
            ));
        }
        tools.sort_by(|left, right| left.name.cmp(&right.name));
        tools
    }

    #[allow(clippy::too_many_lines)] // Admission, cancellation and wait-budget handling stay ordered here.
    async fn execute_read_tool(
        &self,
        context: McpExecutionContext,
        cancellation_token: CancellationToken,
        tool_name: String,
        arguments: Option<serde_json::Value>,
    ) -> Result<serde_json::Value, String> {
        let encoded_arguments = arguments
            .as_ref()
            .map(serde_json::to_vec)
            .transpose()
            .map_err(|_| "could not encode arguments".to_string())?
            .unwrap_or_default();
        if encoded_arguments.len() > MAX_ARGUMENT_BYTES {
            context.record_transport_denial(&tool_name, "arguments_over_limit");
            return Err("arguments exceed the MCP request limit".into());
        }

        let budget = self
            .registry
            .tool_descriptors()
            .into_iter()
            .find(|descriptor| descriptor.name == tool_name)
            .map(|descriptor| descriptor.budget)
            .ok_or_else(|| "unknown tool".to_string())?;
        let permit = self
            .execution_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| {
                crate::api::mcp::telemetry::record_overload();
                context.record_transport_denial(&tool_name, "execution_capacity_full");
                "MCP execution capacity is full; retry later".to_string()
            })?;
        let runtime = self.runtime.clone();
        let registry = self.registry.clone();
        let context_for_worker = context.clone();
        let policy = self.policy.clone();
        let arguments = arguments.as_ref();
        let arguments = arguments.cloned();
        let name = tool_name.clone();
        let execution = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let arguments = arguments.as_ref();
            registry.execute(
                &name,
                runtime.as_ref(),
                &context_for_worker,
                &policy,
                arguments,
            )
        });
        let result = match tokio::select! {
            () = cancellation_token.cancelled() => {
                crate::api::mcp::telemetry::record_cancellation();
                context.record_transport_denial(&tool_name, "request_cancelled");
                return Err("MCP request was cancelled; any running synchronous read may finish in the background".into());
            }
            result = tokio::time::timeout(
                Duration::from_millis(budget.max_runtime_ms.max(1)),
                execution,
            ) => result,
        } {
            Ok(Ok(Ok(value))) => value,
            Ok(Ok(Err(error))) => return Err(error.to_string()),
            Ok(Err(_)) => {
                context.record_transport_denial(&tool_name, "execution_failed");
                return Err("MCP tool execution failed".into());
            }
            Err(_) => {
                context.record_transport_denial(&tool_name, "runtime_budget_exceeded");
                return Err("MCP tool exceeded its execution time budget".into());
            }
        };
        Ok(result)
    }

    #[allow(clippy::too_many_lines)] // The action worker and indeterminate timeout path are one protocol operation.
    async fn execute_action_tool(
        &self,
        context: McpExecutionContext,
        cancellation_token: CancellationToken,
        tool_name: &str,
        arguments: Option<serde_json::Value>,
    ) -> Result<serde_json::Value, String> {
        let Some(actions) = &self.actions else {
            return Err("MCP mutations are disabled".into());
        };
        let argument_bytes = arguments
            .as_ref()
            .map(serde_json::to_vec)
            .transpose()
            .map_err(|_| "could not encode action arguments".to_string())?
            .unwrap_or_default();
        if argument_bytes.len() > MAX_ARGUMENT_BYTES {
            context.record_transport_denial(tool_name, "arguments_over_limit");
            return Err("arguments exceed the MCP request limit".into());
        }
        if !matches!(
            tool_name,
            PREVIEW_DRAIN_TOOL | CONFIRM_DRAIN_TOOL | PREVIEW_QUEUE_TOOL | CONFIRM_QUEUE_TOOL
        ) {
            return Err("unknown MCP action".into());
        }
        let permit: OwnedSemaphorePermit = self
            .execution_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| {
                crate::api::mcp::telemetry::record_overload();
                context.record_transport_denial(tool_name, "execution_capacity_full");
                "MCP execution capacity is full; retry later".to_string()
            })?;
        let actions = actions.clone();
        let runtime = self.runtime.clone();
        let context_for_worker = context.clone();
        let policy = self.policy.clone();
        let fingerprint = self.token_fingerprint;
        let request_target = arguments
            .as_ref()
            .and_then(|value| value.get("challenge_id"))
            .and_then(serde_json::Value::as_str)
            .filter(|value| value.len() <= 64)
            .unwrap_or("unknown")
            .to_string();
        let operation_tool = tool_name.to_string();
        let execution = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            if operation_tool == PREVIEW_DRAIN_TOOL {
                return actions.preview_runtime_drain(
                    &context_for_worker,
                    &policy,
                    fingerprint,
                    runtime.as_ref(),
                );
            }
            if operation_tool == PREVIEW_QUEUE_TOOL {
                return actions.preview_queue_dead_letter(
                    arguments,
                    &context_for_worker,
                    &policy,
                    fingerprint,
                    runtime.as_ref(),
                );
            }
            let ticket = actions.consume_confirmation(
                &operation_tool,
                arguments,
                &context_for_worker,
                &policy,
                fingerprint,
                runtime.as_ref(),
            )?;
            actions.execute(
                ticket,
                &context_for_worker,
                &policy,
                fingerprint,
                runtime.as_ref(),
            )
        });
        let result = match tokio::select! {
            () = cancellation_token.cancelled() => {
                crate::api::mcp::telemetry::record_cancellation();
                let preview = matches!(tool_name, PREVIEW_DRAIN_TOOL | PREVIEW_QUEUE_TOOL);
                context.record_action_audit(
                    tool_name,
                    if preview { "request_cancelled" } else { "indeterminate_do_not_retry" },
                    crate::api::mcp::McpAuditDecision::Denied,
                );
                return Err(if preview {
                    "MCP action preview request was cancelled".into()
                } else {
                    format!(
                        "action outcome is indeterminate; do not retry; inspect challenge {request_target}"
                    )
                });
            }
            result = tokio::time::timeout(Duration::from_secs(2), execution) => result,
        } {
            Ok(Ok(Ok(value))) => value,
            Ok(Ok(Err(message))) => {
                return Err(message);
            }
            Ok(Err(_)) | Err(_) => {
                return Err(format!(
                    "action outcome is indeterminate; do not retry; inspect challenge {request_target}"
                ));
            }
        };
        Ok(result)
    }

    fn parse_resource_uri(uri: &str) -> Result<McpScopedResourceRequest, String> {
        let parsed = url::Url::parse(uri).map_err(|_| "resource URI is invalid".to_string())?;
        if parsed.scheme() != "fitz"
            || parsed.host_str() != Some("resource")
            || parsed.username() != ""
            || parsed.password().is_some()
            || parsed.port().is_some()
            || parsed.query().is_some()
            || parsed.fragment().is_some()
        {
            return Err("resource URI is outside the Fitz resource namespace".into());
        }
        let parts: Vec<String> = parsed
            .path_segments()
            .ok_or_else(|| "resource URI has no path".to_string())?
            .map(|part| {
                percent_encoding::percent_decode_str(part)
                    .decode_utf8()
                    .map(Cow::into_owned)
                    .map_err(|_| "resource URI contains invalid UTF-8".to_string())
            })
            .collect::<Result<_, _>>()?;
        if parts.len() != 5 || parts.iter().any(String::is_empty) {
            return Err(
                "resource URI must contain domain, route family, realm, area and resource".into(),
            );
        }
        let family = parts[1]
            .parse::<u64>()
            .map_err(|_| "resource route family must be a concrete number".to_string())?;
        let request = McpScopedResourceRequest {
            resource: McpResourceDetailRequest {
                scheme: parts[0].clone(),
                realm: parts[2].clone(),
                area: parts[3].clone(),
                resource: parts[4].clone(),
                queue_family: None,
                limit: None,
            },
            route_family: Some(family),
        };
        request.validate()?;
        Ok(request)
    }
}

impl ServerHandler for McpServer {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(
            ServerCapabilities::builder()
                .enable_tools()
                .enable_resources()
                .enable_prompts()
                .build(),
        )
        .with_protocol_version(crate::api::mcp::catalog::primary_revision())
        .with_server_info(Implementation::new("fitz", env!("CARGO_PKG_VERSION")))
        .with_instructions(
            "Fitz MCP reads operational snapshots. Every resource read is authorized against the supplied realm and route family.",
        )
    }

    fn supported_protocol_versions(&self) -> Cow<'static, [rmcp::model::ProtocolVersion]> {
        Cow::Owned(vec![
            crate::api::mcp::catalog::primary_revision(),
            crate::api::mcp::catalog::compatibility_revision(),
        ])
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        request_context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        let request_timer = crate::api::mcp::telemetry::begin_request();
        let name = request.name.to_string();
        let arguments = request.arguments.map(serde_json::Value::Object);
        let context = self
            .context
            .with_correlation_id(format!("{:?}", request_context.id));
        let result = if self
            .actions
            .as_ref()
            .is_some_and(|_| McpActionState::is_tool(&name))
        {
            self.execute_action_tool(context, request_context.ct, &name, arguments)
                .await
        } else {
            self.execute_read_tool(context, request_context.ct, name.clone(), arguments)
                .await
        };
        request_timer.finish(result.is_err());
        match result {
            Ok(value) => {
                let response = CallToolResult::structured(value);
                if !response_fits(&response) {
                    self.context
                        .record_transport_denial(&name, "protocol_response_over_limit");
                    return Ok(CallToolResult::error(vec![ContentBlock::text(
                        "MCP response exceeds the protocol response limit",
                    )])
                    .into());
                }
                Ok(response.into())
            }
            Err(message) => {
                let bounded = truncate_utf8(&message, 1_024);
                let response = CallToolResult::error(vec![ContentBlock::text(bounded)]);
                if response_fits(&response) {
                    Ok(response.into())
                } else {
                    Ok(
                        CallToolResult::error(vec![ContentBlock::text("MCP request failed")])
                            .into(),
                    )
                }
            }
        }
    }

    #[allow(clippy::unused_async_trait_impl)]
    async fn list_tools(
        &self,
        request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        if request.and_then(|request| request.cursor).is_some() {
            return Err(ErrorData::invalid_params("unknown tools cursor", None));
        }
        Ok(ListToolsResult::with_all_items(self.tool_list())
            .with_cache_scope(rmcp::model::CacheScope::Private))
    }

    fn get_tool(&self, name: &str) -> Option<rmcp::model::Tool> {
        self.tool_list().into_iter().find(|tool| tool.name == name)
    }

    #[allow(clippy::unused_async_trait_impl)]
    async fn list_resources(
        &self,
        request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, ErrorData> {
        if request.and_then(|request| request.cursor).is_some() {
            return Err(ErrorData::invalid_params("unknown resources cursor", None));
        }
        Ok(
            ListResourcesResult::with_all_items(content::documentation_resources())
                .with_cache_scope(rmcp::model::CacheScope::Private),
        )
    }

    #[allow(clippy::unused_async_trait_impl)]
    async fn list_resource_templates(
        &self,
        request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListResourceTemplatesResult, ErrorData> {
        if request.and_then(|request| request.cursor).is_some() {
            return Err(ErrorData::invalid_params(
                "unknown resource template cursor",
                None,
            ));
        }
        if !self.policy.allows(McpCapabilityClass::Inspect) {
            return Ok(ListResourceTemplatesResult::default());
        }
        Ok(
            ListResourceTemplatesResult::with_all_items(content::resource_templates())
                .with_cache_scope(rmcp::model::CacheScope::Private),
        )
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        request_context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResponse, ErrorData> {
        if let Some(document) = content::documentation(&request.uri) {
            let result = ReadResourceResult::new(vec![ResourceContents::text(
                document,
                request.uri.clone(),
            )
            .with_mime_type("text/markdown")])
            .with_cache_scope(rmcp::model::CacheScope::Private);
            if !response_fits(&result) {
                return Err(ErrorData::invalid_params(
                    "MCP documentation response exceeds the protocol response limit",
                    None,
                ));
            }
            return Ok(result.into());
        }
        if !self.policy.allows(McpCapabilityClass::Inspect) {
            return Err(ErrorData::invalid_params(
                "MCP inspect capability is required for operational resources",
                None,
            ));
        }
        let scoped_request = match Self::parse_resource_uri(&request.uri) {
            Ok(parsed) => parsed,
            Err(message) => {
                self.context
                    .record_transport_denial("resources/read", "invalid_resource_uri");
                return Err(ErrorData::invalid_params(message, None));
            }
        };
        let arguments = serde_json::to_value(scoped_request)
            .map_err(|_| ErrorData::internal_error("could not encode resource scope", None))?;
        let execution_context = self
            .context
            .with_correlation_id(format!("{:?}", request_context.id));
        let value = self
            .execute_read_tool(
                execution_context,
                request_context.ct,
                "inspect_resource_detail".to_string(),
                Some(arguments),
            )
            .await
            .map_err(|message| ErrorData::invalid_params(message, None))?;
        let content = ResourceContents::text(value.to_string(), request.uri)
            .with_mime_type("application/json");
        let result = ReadResourceResult::new(vec![content])
            .with_cache_scope(rmcp::model::CacheScope::Private);
        if !response_fits(&result) {
            return Err(ErrorData::invalid_params(
                "MCP resource response exceeds the protocol response limit",
                None,
            ));
        }
        Ok(result.into())
    }

    #[allow(clippy::unused_async_trait_impl)]
    async fn list_prompts(
        &self,
        request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListPromptsResult, ErrorData> {
        if request.and_then(|request| request.cursor).is_some() {
            return Err(ErrorData::invalid_params("unknown prompts cursor", None));
        }
        let prompts = content::prompts()
            .into_iter()
            .filter(|prompt| {
                if prompt.name == "diagnose_broker" {
                    self.policy.allows(McpCapabilityClass::Summary)
                } else {
                    self.policy.allows(McpCapabilityClass::Inspect)
                }
            })
            .collect();
        Ok(ListPromptsResult::with_all_items(prompts)
            .with_cache_scope(rmcp::model::CacheScope::Private))
    }

    #[allow(clippy::too_many_lines)] // A prompt composes bounded authorized reads in request order.
    async fn get_prompt(
        &self,
        request: GetPromptRequestParams,
        request_context: RequestContext<RoleServer>,
    ) -> Result<GetPromptResponse, ErrorData> {
        let execution_context = self
            .context
            .with_correlation_id(format!("{:?}", request_context.id));
        let cancellation_token = request_context.ct;
        let messages = if request.name == "diagnose_broker" {
            if request
                .arguments
                .as_ref()
                .is_some_and(|arguments| !arguments.is_empty())
            {
                return Err(ErrorData::invalid_params(
                    "diagnose_broker takes no arguments",
                    None,
                ));
            }
            let stats = self
                .execute_read_tool(
                    execution_context.clone(),
                    cancellation_token.clone(),
                    "get_global_stats".to_string(),
                    None,
                )
                .await
                .map_err(|message| ErrorData::invalid_params(message, None))?;
            let troubleshooting = self
                .execute_read_tool(
                    execution_context,
                    cancellation_token,
                    "get_global_troubleshooting".to_string(),
                    None,
                )
                .await
                .map_err(|message| ErrorData::invalid_params(message, None))?;
            vec![PromptMessage::new_text(
                Role::User,
                format!(
                    "Summarize these authorized global Fitz broker snapshots. Separate observed values from hypotheses and recommend bounded follow-up reads.\nGlobal stats:\n{stats}\nGlobal troubleshooting:\n{troubleshooting}"
                ),
            )]
        } else {
            let prompt_kind = match request.name.as_str() {
                READ_PROMPT => PromptKind::Detail,
                TIMELINE_PROMPT => PromptKind::Timeline,
                "diagnose_queue" => PromptKind::Domain("queue"),
                "diagnose_stream" => PromptKind::Domain("stream"),
                "diagnose_kv" => PromptKind::Domain("kv"),
                "diagnose_lease" => PromptKind::Domain("lease"),
                "diagnose_schedule" => PromptKind::Domain("schedule"),
                "diagnose_notice" => PromptKind::Domain("notice"),
                "diagnose_rpc" => PromptKind::Domain("rpc"),
                _ => return Err(ErrorData::invalid_params("unknown Fitz prompt", None)),
            };
            let arguments = request
                .arguments
                .ok_or_else(|| ErrorData::invalid_params("prompt arguments are required", None))?;
            let scoped = parse_resource_prompt_arguments(&arguments, prompt_kind)
                .map_err(|message| ErrorData::invalid_params(message, None))?;
            let arguments = serde_json::to_value(&scoped)
                .map_err(|_| ErrorData::internal_error("could not encode prompt scope", None))?;
            match prompt_kind {
                PromptKind::Detail => {
                    let detail = self
                        .execute_read_tool(
                            execution_context,
                            cancellation_token,
                            "inspect_resource_detail".to_string(),
                            Some(arguments),
                        )
                        .await
                        .map_err(|message| ErrorData::invalid_params(message, None))?;
                    vec![PromptMessage::new_text(
                        Role::User,
                        format!(
                            "Use these authorized Fitz resource details to answer the request. Treat route_family and realm as independent scope values.\nResource detail:\n{detail}"
                        ),
                    )]
                }
                PromptKind::Timeline => {
                    let timeline = self
                        .execute_read_tool(
                            execution_context,
                            cancellation_token,
                            "inspect_resource_timeline".to_string(),
                            Some(arguments),
                        )
                        .await
                        .map_err(|message| ErrorData::invalid_params(message, None))?;
                    vec![PromptMessage::new_text(
                        Role::User,
                        format!(
                            "Review these authorized bounded Fitz resource timeline entries. Treat route_family and realm as independent scope values.\nRecent timeline:\n{timeline}"
                        ),
                    )]
                }
                PromptKind::Domain(_) => {
                    let detail = self
                        .execute_read_tool(
                            execution_context.clone(),
                            cancellation_token.clone(),
                            "inspect_resource_detail".to_string(),
                            Some(arguments.clone()),
                        )
                        .await
                        .map_err(|message| ErrorData::invalid_params(message, None))?;
                    let timeline = self
                        .execute_read_tool(
                            execution_context,
                            cancellation_token,
                            "inspect_resource_timeline".to_string(),
                            Some(arguments),
                        )
                        .await
                        .map_err(|message| ErrorData::invalid_params(message, None))?;
                    vec![PromptMessage::new_text(
                        Role::User,
                        format!(
                            "Diagnose the requested Fitz resource using only these authorized snapshots. Distinguish observed values from hypotheses; suggest bounded follow-up checks. route_family and realm are independent.\nResource detail:\n{detail}\nRecent timeline:\n{timeline}"
                        ),
                    )]
                }
            }
        };
        let result = GetPromptResult::new(messages);
        if !response_fits(&result) {
            return Err(ErrorData::invalid_params(
                "MCP prompt response exceeds the protocol response limit",
                None,
            ));
        }
        Ok(result.into())
    }
}

#[derive(Clone, Copy)]
enum PromptKind {
    Detail,
    Timeline,
    Domain(&'static str),
}

fn parse_resource_prompt_arguments(
    arguments: &serde_json::Map<String, serde_json::Value>,
    kind: PromptKind,
) -> Result<McpScopedResourceRequest, String> {
    let expected: &[&str] = match kind {
        PromptKind::Detail => &["scheme", "route_family", "realm", "area", "resource"],
        PromptKind::Timeline => &[
            "scheme",
            "route_family",
            "realm",
            "area",
            "resource",
            "limit",
        ],
        PromptKind::Domain(_) => &["route_family", "realm", "area", "resource", "limit"],
    };
    if arguments
        .keys()
        .any(|name| !expected.contains(&name.as_str()))
    {
        return Err("prompt contains an unsupported argument".into());
    }
    let string_argument = |name: &str| -> Result<&str, String> {
        arguments
            .get(name)
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| format!("prompt argument {name} must be a non-empty string"))
    };
    let scheme = match kind {
        PromptKind::Domain(domain) => domain,
        PromptKind::Detail | PromptKind::Timeline => string_argument("scheme")?,
    };
    let route_family = string_argument("route_family")?
        .parse::<u64>()
        .map_err(|_| "route_family must be a concrete number".to_string())?;
    let limit = arguments
        .get("limit")
        .map(|value| {
            value
                .as_str()
                .ok_or_else(|| "limit must be a string containing a number".to_string())?
                .parse::<usize>()
                .map_err(|_| "limit must be a positive integer".to_string())
        })
        .transpose()?;
    if limit.is_some_and(|limit| !(1..=50).contains(&limit)) {
        return Err("limit must be from 1 through 50".into());
    }
    let request = McpScopedResourceRequest {
        resource: McpResourceDetailRequest {
            scheme: scheme.to_string(),
            realm: string_argument("realm")?.to_string(),
            area: string_argument("area")?.to_string(),
            resource: string_argument("resource")?.to_string(),
            queue_family: None,
            limit,
        },
        route_family: Some(route_family),
    };
    request.validate()?;
    Ok(request)
}

fn response_fits<T: serde::Serialize>(response: &T) -> bool {
    serde_json::to_vec(&serde_json::json!({
        "jsonrpc": "2.0",
        "id": "mcp-budget-probe",
        "result": response,
    }))
    .is_ok_and(|bytes| bytes.len() <= MAX_MCP_RESPONSE_BYTES)
}

fn truncate_utf8(value: &str, max_bytes: usize) -> String {
    let mut end = value.len().min(max_bytes);
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].to_string()
}

mod content;
#[cfg(test)]
mod tests;
