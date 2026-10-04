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

pub(super) fn build_resource_detail_value(
    runtime: &Runtime,
    invocation: &McpInvocation,
) -> McpToolResult<Value> {
    let name = "inspect_resource_detail";
    let request = invocation
        .resource_request()
        .expect("validated resource invocation");
    let family = invocation.route_family();
    let snapshot = runtime.admin_read_model().bounded_snapshot(family, 512);
    if snapshot.truncated {
        return Err(McpToolError::InvalidArguments { tool_name: name.into(), reason: "resource detail collection exceeds its scan budget; use inventory pagination or a narrower family".into() });
    }
    let path = ResourcePath {
        realm: &request.realm,
        area: &request.area,
        resource: &request.resource,
    };
    let domain = DomainKind::from_scheme(&request.scheme).expect("validated domain");
    crate::api::admin::snapshot_detail(runtime, &snapshot, &path, family, domain).map_err(
        |reason| McpToolError::InvalidArguments {
            tool_name: name.into(),
            reason,
        },
    )
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
    let snapshot = runtime.admin_read_model().bounded_snapshot(family, 512);
    let limit = request
        .limit
        .unwrap_or(McpCostBudget::timeline().max_result_items)
        .clamp(1, McpCostBudget::timeline().max_result_items);
    let path = ResourcePath {
        realm: &request.realm,
        area: &request.area,
        resource: &request.resource,
    };
    let mut timeline = match DomainKind::from_scheme(&request.scheme).expect("validated domain") {
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
        DomainKind::Schedule => schedule_resource_timeline(
            &snapshot.schedules,
            snapshot.pending_fire_claims,
            0,
            0,
            0,
            0,
            0,
            &path,
            limit,
        ),
    };
    if let Some(family) = family {
        timeline.family = Some(family);
        for event in &mut timeline.events {
            event.family = Some(family);
        }
    }
    let mut value = serialize_tool_output(name, timeline)?;
    value["_meta"] = serde_json::json!({ "partial": snapshot.truncated, "collection_limit": snapshot.limit_per_collection, "unavailable": ["durable event history; timeline contains current projection observations", "unattributed Schedule pressure counters"] });
    Ok(value)
}
