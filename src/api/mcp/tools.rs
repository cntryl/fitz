//! Definitions for bounded operational read tools.
use super::{
    build_resource_detail_value, build_resource_timeline_value, serialize_tool_output,
    McpCapabilityClass, McpCostBudget, McpToolDefinition, McpToolDescriptor, McpToolRegistry,
    McpToolResult, Runtime, Value,
};
impl McpToolRegistry {
    pub(super) fn global_stats_tool() -> McpToolDefinition {
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
            |runtime, _invocation, _context| bounded_global(runtime, "get_global_stats", true),
        )
    }

    pub(super) fn global_troubleshooting_tool() -> McpToolDefinition {
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
            |runtime, _invocation, _context| {
                bounded_global(runtime, "get_global_troubleshooting", false)
            },
        )
    }

    pub(super) fn resource_detail_tool() -> McpToolDefinition {
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
            |runtime, invocation, _context| build_resource_detail_value(runtime, invocation),
        )
    }

    pub(super) fn resource_timeline_tool() -> McpToolDefinition {
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
            |runtime, invocation, _context| build_resource_timeline_value(runtime, invocation),
        )
    }

    pub(super) fn global_explanation_tool() -> McpToolDefinition {
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
            |runtime, _invocation, _context| {
                bounded_global(runtime, "explain_global_troubleshooting", false)
            },
        )
    }

    pub(super) fn discovery_tool() -> McpToolDefinition {
        McpToolDefinition::new(
            McpToolDescriptor {
                name: "get_mcp_discovery".to_string(),
                capability: McpCapabilityClass::Summary,
                summary: "Return the supported Fitz MCP protocol revisions, domains, resources, and authorization model".to_string(),
                rest_path: "/.well-known/oauth-protected-resource/mcp".to_string(),
                budget: McpCostBudget::summary(),
            },
            |runtime, _invocation, context| {
                let (families, truncated) = runtime.admin_auth().bounded_provisioned_route_families(256, context.principal.as_ref().map(|principal| &principal.route_family_access));
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
                    "diagnostics_are": "cached administrative projections with unknown publication age",
                    "broker": { "version": env!("CARGO_PKG_VERSION"), "ready": null, "draining": runtime.is_draining(), "traffic_status": runtime.traffic_status(), "fatal_domain_failure": runtime.has_fatal_domain_failure() },
                    "route_families": families.into_iter().map(|family| family.to_string()).collect::<Vec<_>>(),
                    "inventory": "list_resource_inventory: cursor pagination over scoped resource names; realm and route family are independent",
                    "_meta": { "source": "fixed runtime flags and configured features; cached diagnostic rows are separate", "cached_projection": false, "partial": truncated, "unavailable": ["aggregate readiness requires a full domain-family health traversal; use the health HTTP endpoint"] }
                }))
            },
        )
    }

    pub(super) fn sessions_tool() -> McpToolDefinition {
        McpToolDefinition::new(
            McpToolDescriptor {
                name: "get_sessions".to_string(),
                capability: McpCapabilityClass::Inspect,
                summary: "List bounded active-session diagnostics for all authorized route families or one explicit family".to_string(),
                rest_path: "/api/v1/:route_family/sessions".to_string(),
                budget: McpCostBudget::collection(),
            },
            |runtime, invocation, _context| {
                let family = invocation.route_family();
                let limit = McpCostBudget::collection().max_result_items;
                let (sessions, truncated) = runtime.admin_read_model().bounded_sessions(family, limit);
                Ok(serde_json::json!({
                    "route_family": family,
                    "sessions": sessions,
                    "truncated": truncated,
                    "limit": limit
                }))
            },
        )
    }

    pub(super) fn topology_tool() -> McpToolDefinition {
        McpToolDefinition::new(
            McpToolDescriptor {
                name: "get_topology".to_string(),
                capability: McpCapabilityClass::Inspect,
                summary: "Read the bounded admin topology snapshot, optionally for one explicit route family".to_string(),
                rest_path: "/api/v1/:route_family/topology".to_string(),
                budget: McpCostBudget::topology(),
            },
            |runtime, invocation, _context| {
                Ok(crate::api::admin::mcp_topology_value(
                    runtime,
                    invocation.route_family(),
                ))
            },
        )
    }

    pub(super) fn metrics_tool() -> McpToolDefinition {
        McpToolDefinition::new(
            McpToolDescriptor {
                name: "get_structured_metrics".to_string(),
                capability: McpCapabilityClass::Inspect,
                summary: "Read bounded structured admin metrics for all authorized families or one explicit route family".to_string(),
                rest_path: "/api/v1/:route_family/metrics".to_string(),
                budget: McpCostBudget::metrics(),
            },
            |runtime, invocation, _context| {
                Ok(crate::api::admin::metrics::mcp_structured_metrics_value(
                    runtime,
                    invocation.route_family(),
                    McpCostBudget::metrics().max_result_items,
                ))
            },
        )
    }
}

fn bounded_global(runtime: &Runtime, name: &str, stats: bool) -> McpToolResult<Value> {
    let snapshot = runtime.admin_read_model().bounded_snapshot(None, 256);
    let mut value = if stats {
        serialize_tool_output(
            name,
            crate::api::admin::build_bounded_global_stats(runtime, &snapshot),
        )?
    } else {
        serialize_tool_output(
            name,
            crate::api::admin::troubleshooting::build_bounded_runtime_diagnostics(
                runtime, &snapshot,
            )
            .global,
        )?
    };
    value["_meta"] = serde_json::json!({ "partial": snapshot.truncated, "collection_limit": snapshot.limit_per_collection, "source": "shared admin projections and broker counters", "unavailable": if snapshot.truncated { vec!["unobserved projection rows beyond the collection bound"] } else { Vec::<&str>::new() } });
    if stats {
        value["_meta"]["unavailable_fields"] = serde_json::json!([
            "/domains/kv/keys_total",
            "/domains/kv/operations_per_second",
            "/domains/stream/watermark_lag_buckets",
            "/domains/schedule/executions_per_minute",
            "/domains/schedule/subscriptions_active",
            "/domains/schedule/pending_ack_retries",
            "/domains/schedule/oldest_pending_claim_age_seconds",
            "/domains/schedule/notify_failures_total",
            "/domains/schedule/ack_failures_total",
            "/domains/schedule/overdue_normalizations_total"
        ]);
        value["_meta"]["unavailable"].as_array_mut().expect("array").push(serde_json::json!("fields in unavailable_fields use DTO defaults; their values are unknown, not measured zero"));
    } else {
        value["_meta"]["unavailable"].as_array_mut().expect("array").push(serde_json::json!("resource-attributed Schedule claim, retry, age, failure and normalization pressure; aggregate domain latency cannot establish a resource cause"));
    }
    Ok(value)
}
