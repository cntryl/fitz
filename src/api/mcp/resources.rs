use super::{
    kv_detail, kv_resource_timeline, lease_detail, lease_resource_timeline, notice_detail,
    notice_resource_timeline, queue_detail, queue_resource_timeline, rpc_operations,
    rpc_resource_timeline, schedule_detail, schedule_resource_timeline, serialize_tool_output,
    stream_detail, stream_resource_timeline, McpCostBudget, McpInvocation, McpToolError,
    McpToolResult, ResourcePath, Runtime, Value,
};
use crate::api::admin::troubleshooting::ResourceTimeline;

pub(super) fn build_resource_detail_value(
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

    let family = invocation.route_family();
    let path = ResourcePath {
        realm: &request.realm,
        area: &request.area,
        resource: &request.resource,
    };

    match crate::runtime::DomainKind::from_scheme(&request.scheme) {
        Some(crate::runtime::DomainKind::Kv) => {
            serialize_tool_output(tool_name, kv_detail(runtime, &path, family))
        }
        Some(crate::runtime::DomainKind::Queue) => {
            serialize_tool_output(tool_name, queue_detail(runtime, &path, family))
        }
        Some(crate::runtime::DomainKind::Stream) => {
            serialize_tool_output(tool_name, stream_detail(runtime, &path, family))
        }
        Some(crate::runtime::DomainKind::Lease) => {
            serialize_tool_output(tool_name, lease_detail(runtime, &path, family))
        }
        Some(crate::runtime::DomainKind::Schedule) => {
            serialize_tool_output(tool_name, schedule_detail(runtime, &path, family))
        }
        Some(crate::runtime::DomainKind::Notice) => {
            serialize_tool_output(tool_name, notice_detail(runtime, &path, family))
        }
        Some(crate::runtime::DomainKind::Rpc) => {
            serialize_tool_output(tool_name, rpc_operations(runtime, &path, family))
        }
        None => Err(McpToolError::InvalidArguments {
            tool_name: tool_name.to_string(),
            reason: format!("unsupported resource scheme: {}", request.scheme),
        }),
    }
}

pub(super) fn build_resource_timeline_value(
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
    let family = invocation.route_family();
    let path = ResourcePath {
        realm: &request.realm,
        area: &request.area,
        resource: &request.resource,
    };
    let read_model = runtime.admin_read_model();

    match crate::runtime::DomainKind::from_scheme(&request.scheme) {
        Some(crate::runtime::DomainKind::Kv) => serialize_timeline(
            tool_name,
            family,
            kv_resource_timeline(
                &in_family(
                    read_model.kv_transactions(Some(&request.realm)),
                    family,
                    |item| item.route_family,
                ),
                &path,
                limit,
            ),
        ),
        Some(crate::runtime::DomainKind::Queue) => serialize_timeline(
            tool_name,
            family,
            scoped_queue_timeline(&read_model, &path, family, limit),
        ),
        Some(crate::runtime::DomainKind::Stream) => serialize_timeline(
            tool_name,
            family,
            stream_resource_timeline(
                &in_family(read_model.streams(Some(&request.realm)), family, |item| {
                    item.route_family
                }),
                &path,
                limit,
            ),
        ),
        Some(crate::runtime::DomainKind::Lease) => serialize_timeline(
            tool_name,
            family,
            lease_resource_timeline(
                &in_family(read_model.leases(Some(&request.realm)), family, |item| {
                    item.route_family
                }),
                &path,
                limit,
            ),
        ),
        Some(crate::runtime::DomainKind::Notice) => serialize_timeline(
            tool_name,
            family,
            scoped_notice_timeline(&read_model, &path, family, limit),
        ),
        Some(crate::runtime::DomainKind::Rpc) => serialize_timeline(
            tool_name,
            family,
            rpc_resource_timeline(
                &in_family(
                    read_model.rpc_workers(Some(&request.realm)),
                    family,
                    |item| item.route_family,
                ),
                &in_family(
                    read_model.rpc_pending(Some(&request.realm)),
                    family,
                    |item| item.route_family,
                ),
                &path,
                limit,
            ),
        ),
        Some(crate::runtime::DomainKind::Schedule) => serialize_timeline(
            tool_name,
            family,
            scoped_schedule_timeline(runtime, &path, family, limit),
        ),
        None => Err(McpToolError::InvalidArguments {
            tool_name: tool_name.to_string(),
            reason: format!("unsupported resource scheme: {}", request.scheme),
        }),
    }
}

fn serialize_timeline(
    tool_name: &str,
    family: Option<u64>,
    mut timeline: crate::api::admin::troubleshooting::ResourceTimeline,
) -> McpToolResult<Value> {
    if let Some(family) = family {
        timeline.family = Some(family);
        for event in &mut timeline.events {
            event.family = Some(family);
        }
    }
    serialize_tool_output(tool_name, timeline)
}

fn in_family<T>(items: Vec<T>, family: Option<u64>, family_of: impl Fn(&T) -> u64) -> Vec<T> {
    items
        .into_iter()
        .filter(|item| family.is_none_or(|family| family_of(item) == family))
        .collect()
}

fn scoped_schedule_timeline(
    runtime: &Runtime,
    path: &ResourcePath<'_>,
    family: Option<u64>,
    limit: usize,
) -> ResourceTimeline {
    let read_model = runtime.admin_read_model();
    // Global pressure counters have no family attribution. Zero suppresses their
    // optional notes; scoped results do not report zero-valued global counters.
    schedule_resource_timeline(
        &in_family(read_model.schedules(Some(path.realm)), family, |item| {
            item.route_family
        }),
        if family.is_none() {
            runtime.schedule_pending_fire_claims()
        } else {
            0
        },
        if family.is_none() {
            runtime.schedule_pending_ack_retries()
        } else {
            0
        },
        if family.is_none() {
            runtime.schedule_oldest_pending_claim_age_seconds()
        } else {
            0
        },
        if family.is_none() {
            runtime.schedule_notify_failures()
        } else {
            0
        },
        if family.is_none() {
            runtime.schedule_ack_failures()
        } else {
            0
        },
        if family.is_none() {
            runtime.schedule_overdue_normalizations()
        } else {
            0
        },
        path,
        limit,
    )
}
fn scoped_queue_timeline(
    read_model: &crate::control::admin::read_model::AdminReadModel,
    path: &ResourcePath<'_>,
    family: Option<u64>,
    limit: usize,
) -> ResourceTimeline {
    queue_resource_timeline(
        &in_family(read_model.queues(Some(path.realm)), family, |item| {
            item.family
        }),
        &in_family(
            read_model.queue_inflight(Some(path.realm)),
            family,
            |item| item.family,
        ),
        &in_family(
            read_model.queue_dead_letters(Some(path.realm)),
            family,
            |item| item.family,
        ),
        path,
        family,
        limit,
    )
}

fn scoped_notice_timeline(
    read_model: &crate::control::admin::read_model::AdminReadModel,
    path: &ResourcePath<'_>,
    family: Option<u64>,
    limit: usize,
) -> ResourceTimeline {
    notice_resource_timeline(
        &in_family(
            read_model.notice_subscriptions(Some(path.realm), None),
            family,
            |item| item.route_family,
        ),
        &in_family(read_model.notice_routes(Some(path.realm)), family, |item| {
            item.route_family
        }),
        path,
        limit,
    )
}
