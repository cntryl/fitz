use super::{
    serialize_tool_output, McpCostBudget, McpInvocation, McpToolError, McpToolResult, Runtime,
    Value,
};
use crate::api::admin::troubleshooting::{
    kv_resource_timeline, lease_resource_timeline, notice_resource_timeline,
    queue_resource_timeline, rpc_resource_timeline, schedule_resource_timeline,
    stream_resource_timeline,
};
use crate::api::admin::ResourcePath;
use crate::runtime::DomainKind;

const RPC_DEADLINES_UNAVAILABLE: &str = "per-call deadlines are unavailable from the admin projection; request age does not identify the remaining deadline";
const RPC_OUTCOMES_UNAVAILABLE: &str = "per-call execution, completion and failure outcomes are unavailable from the admin projection; caller completion does not establish worker cleanup";

pub(super) fn build_resource_detail_value(
    runtime: &Runtime,
    invocation: &McpInvocation,
) -> McpToolResult<Value> {
    let name = "inspect_resource_detail";
    let request = invocation
        .resource_request()
        .expect("validated resource invocation");
    let family = invocation.route_family();
    let snapshot = collect_resource(runtime, invocation, name)?;
    if snapshot.truncated {
        return Err(McpToolError::InvalidArguments { tool_name: name.into(), reason: "resource detail exceeds its bounded rows or wildcard-registration scan; evidence is incomplete".into() });
    }
    let path = ResourcePath {
        realm: &request.realm,
        area: &request.area,
        resource: &request.resource,
    };
    let domain = DomainKind::from_scheme(&request.scheme).expect("validated domain");
    let mut value = crate::api::admin::snapshot_detail(runtime, &snapshot, &path, family, domain)
        .map_err(|reason| McpToolError::InvalidArguments {
        tool_name: name.into(),
        reason,
    })?;
    if domain == DomainKind::Rpc {
        value["_meta"] = serde_json::json!({"unavailable": ["wildcard operation names cannot be enumerated; worker counters and latency from wildcard registrations cannot be attributed to this resource", RPC_DEADLINES_UNAVAILABLE, RPC_OUTCOMES_UNAVAILABLE]});
    }
    Ok(value)
}

pub(super) fn build_resource_timeline_value(
    runtime: &Runtime,
    invocation: &McpInvocation,
) -> McpToolResult<Value> {
    let name = "inspect_resource_timeline";
    let request = invocation
        .resource_request()
        .expect("validated resource invocation");
    let family = invocation.route_family();
    let snapshot = collect_resource(runtime, invocation, name)?;
    let limit = request
        .limit
        .unwrap_or(McpCostBudget::timeline().max_result_items)
        .clamp(1, McpCostBudget::timeline().max_result_items);
    let path = ResourcePath {
        realm: &request.realm,
        area: &request.area,
        resource: &request.resource,
    };
    let domain = DomainKind::from_scheme(&request.scheme).expect("validated domain");
    let mut timeline = match domain {
        DomainKind::Kv => kv_resource_timeline(&snapshot.kv_transactions, &path, limit),
        DomainKind::Queue => queue_resource_timeline(
            &snapshot.queues,
            &snapshot.queue_inflight,
            &snapshot.queue_dead_letters,
            &path,
            family,
            limit,
        ),
        DomainKind::Stream => stream_resource_timeline(&snapshot.streams, &path, limit),
        DomainKind::Lease => lease_resource_timeline(&snapshot.leases, &path, limit),
        DomainKind::Notice => notice_resource_timeline(
            &snapshot.notice_subscriptions,
            &snapshot.notice_routes,
            &path,
            limit,
        ),
        DomainKind::Rpc => {
            rpc_resource_timeline(&snapshot.rpc_workers, &snapshot.rpc_pending, &path, limit)
        }
        DomainKind::Schedule => {
            schedule_resource_timeline(&snapshot.schedules, 0, 0, 0, 0, 0, 0, &path, limit)
        }
    };
    if let Some(family) = family {
        timeline.family = Some(family);
        for event in &mut timeline.events {
            event.family = Some(family);
        }
    }
    let mut value = serialize_tool_output(name, timeline)?;
    let mut unavailable = vec![
        "durable event history; timeline contains current projection observations",
        "unattributed Schedule pressure counters",
        "wildcard Notice/RPC aggregate delivery, publication, handled counts and latency cannot be attributed to this resource",
    ];
    if domain == DomainKind::Rpc {
        unavailable.extend([RPC_DEADLINES_UNAVAILABLE, RPC_OUTCOMES_UNAVAILABLE]);
    }
    value["_meta"] = serde_json::json!({ "partial": snapshot.truncated, "collection_limit": snapshot.limit_per_collection, "unavailable": unavailable });
    Ok(value)
}

fn collect_resource(
    runtime: &Runtime,
    invocation: &McpInvocation,
    tool_name: &str,
) -> McpToolResult<crate::control::admin::read_model::AdminSnapshot> {
    let request = invocation
        .resource_request()
        .expect("validated resource invocation");
    let (families, truncated) = invocation.route_family().map_or_else(
        || {
            runtime
                .admin_auth()
                .bounded_provisioned_route_families(64, None)
        },
        |family| (vec![family], false),
    );
    if truncated {
        return Err(McpToolError::InvalidArguments { tool_name: tool_name.into(), reason: "all-family resource reads exceed the family budget; select an explicit route_family".into() });
    }
    let snapshot = runtime.admin_read_model().bounded_resource_snapshot(
        &families,
        crate::control::admin::read_model::InventoryScope {
            realm: Some(&request.realm),
            area: Some(&request.area),
            resource: Some(&request.resource),
            ..Default::default()
        },
        DomainKind::from_scheme(&request.scheme).expect("validated domain"),
        512,
    );
    if request.scheme == "stream"
        && invocation.route_family().is_none()
        && snapshot.streams.len() > 1
    {
        return Err(McpToolError::InvalidArguments {
            tool_name: tool_name.into(),
            reason: "multiple route families contain this Stream; select an explicit route_family"
                .into(),
        });
    }
    Ok(snapshot)
}
