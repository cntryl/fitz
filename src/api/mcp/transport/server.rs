use super::actions::{
    McpActionState, CONFIRM_DRAIN_TOOL, CONFIRM_QUEUE_TOOL, PREVIEW_DRAIN_TOOL, PREVIEW_QUEUE_TOOL,
};
use super::catalog_resources;
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
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

const MAX_ARGUMENT_BYTES: usize = 16 * 1024;
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
            .ok_or_else(|| {
                context.record_transport_denial("tools/call", "unknown_tool");
                "unknown tool".to_string()
            })?;
        let permit = super::execution::admit(
            &self.execution_slots,
            &context,
            &tool_name,
            &cancellation_token,
        )?;
        let runtime = self.runtime.clone();
        let registry = self.registry.clone();
        let context_for_worker = context.clone();
        let policy = self.policy.clone();
        let arguments = arguments.as_ref();
        let arguments = arguments.cloned();
        let name = tool_name.clone();
        let execution = super::execution::spawn(
            permit,
            cancellation_token.clone(),
            context.clone(),
            tool_name.clone(),
            move || {
                let arguments = arguments.as_ref();
                registry
                    .execute(
                        &name,
                        runtime.as_ref(),
                        &context_for_worker,
                        &policy,
                        arguments,
                    )
                    .map_err(|error| error.to_string())
            },
        );
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
            Ok(Ok(Err(error))) => return Err(error),
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
        let request_target = arguments
            .as_ref()
            .and_then(|value| value.get("challenge_id"))
            .and_then(serde_json::Value::as_str)
            .and_then(|value| uuid::Uuid::parse_str(value).ok())
            .map_or_else(|| "unknown".into(), |id| id.to_string());
        let argument_bytes = arguments
            .as_ref()
            .map(serde_json::to_vec)
            .transpose()
            .map_err(|_| "could not encode action arguments".to_string())?
            .unwrap_or_default();
        if argument_bytes.len() > MAX_ARGUMENT_BYTES {
            actions.record_request_interruption(
                tool_name,
                Some(&request_target),
                &context,
                self.token_fingerprint,
                "arguments_over_limit",
            )?;
            return Err("arguments exceed the MCP request limit".into());
        }
        if !matches!(
            tool_name,
            PREVIEW_DRAIN_TOOL | CONFIRM_DRAIN_TOOL | PREVIEW_QUEUE_TOOL | CONFIRM_QUEUE_TOOL
        ) {
            return Err("unknown MCP action".into());
        }
        let permit = match super::execution::admit(
            &self.execution_slots,
            &context,
            tool_name,
            &cancellation_token,
        ) {
            Ok(permit) => permit,
            Err(message) => {
                actions.record_request_interruption(
                    tool_name,
                    Some(&request_target),
                    &context,
                    self.token_fingerprint,
                    "request_not_admitted",
                )?;
                return Err(message);
            }
        };
        let actions_for_observer = actions.clone();
        let actions = actions.clone();
        let runtime = self.runtime.clone();
        let context_for_worker = context.clone();
        let policy = self.policy.clone();
        let fingerprint = self.token_fingerprint;
        let operation_tool = tool_name.to_string();
        let worker_cancellation = cancellation_token.clone();
        let worker_target = request_target.clone();
        let execution = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            if worker_cancellation.is_cancelled() {
                actions.record_request_interruption(
                    &operation_tool,
                    Some(&worker_target),
                    &context_for_worker,
                    fingerprint,
                    "request_cancelled_before_worker_start",
                )?;
                return Err("MCP action was cancelled before execution".into());
            }
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
                if let Err(error) = actions_for_observer.record_request_interruption(tool_name,
                    Some(&request_target), &context, fingerprint,
                    if preview { "request_cancelled" } else { "indeterminate_do_not_retry" }) {
                    return Err(format!("{error}; any admitted action outcome may be indeterminate; do not retry"));
                }
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
                if let Err(error) = actions_for_observer.record_request_interruption(
                    tool_name,
                    Some(&request_target),
                    &context,
                    fingerprint,
                    "indeterminate_do_not_retry",
                ) {
                    return Err(format!(
                        "{error}; action outcome is indeterminate; do not retry"
                    ));
                }
                return Err(format!(
                    "action outcome is indeterminate; do not retry; inspect challenge {request_target}"
                ));
            }
        };
        Ok(result)
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
            ListResourcesResult::with_all_items(catalog_resources::documents())
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
        Ok(ListResourceTemplatesResult::with_all_items(
            catalog_resources::templates()
                .into_iter()
                .filter(|template| {
                    self.policy.allows(if template.name == "broker_summary_v1" {
                        McpCapabilityClass::Summary
                    } else {
                        McpCapabilityClass::Inspect
                    })
                })
                .collect(),
        )
        .with_cache_scope(rmcp::model::CacheScope::Private))
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        request_context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResponse, ErrorData> {
        if let Some(document) = catalog_resources::document(&request.uri) {
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
        let target = match catalog_resources::parse(&request.uri) {
            Ok(parsed) => parsed,
            Err(message) => {
                self.context
                    .record_transport_denial("resources/read", "invalid_resource_uri");
                return Err(ErrorData::invalid_params(message, None));
            }
        };
        let (tool, arguments) = target
            .tool_arguments()
            .map_err(|message| ErrorData::invalid_params(message, None))?;
        let execution_context = self
            .context
            .with_correlation_id(format!("{:?}", request_context.id));
        let value = self
            .execute_read_tool(
                execution_context,
                request_context.ct,
                tool.to_string(),
                arguments,
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
        let mut messages = messages;
        messages.insert(
            0,
            PromptMessage::new_text(Role::User, catalog_resources::DIAGNOSTIC_INSTRUCTIONS),
        );
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
    super::limits::result_fits(response)
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
