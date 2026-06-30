use crate::api::admin::list::{QueueInflight, QueueInfo};
use crate::api::admin::stats;
use crate::api::admin::topology::helpers::{
    add_broker_domain_flow, count_u64, count_usize, domain_node_id, saturating_usize,
    scope_for_resource, scope_with_session, scoped_resource, scoped_state, session_node_id,
    top_resources, topology_connection, topology_lane, topology_state,
};
use crate::api::admin::topology::types::{
    TopologyConnectionBuilder, TopologyConnectionKind, TopologyLane, TopologyScopedResource,
    TopologyState,
};

pub(in crate::api::admin::topology) fn queue_lane(
    stats: &stats::QueueStats,
    queues: &[QueueInfo],
    inflight: &[QueueInflight],
    connections: &mut TopologyConnectionBuilder,
) -> TopologyLane {
    let pressure = stats.messages_dead_lettered
        + stats.messages_ready
        + stats.messages_delayed
        + stats.messages_pending
        + saturating_usize(stats.failure_total)
        + saturating_usize(stats.notify_drops_total)
        + saturating_usize(stats.complete_rejected_total);
    let activity = stats.operations_per_second > 0.0 || stats.inflight_active > 0;
    let lane_state = topology_state(&stats.diagnostics, pressure > 0, activity);
    let counters = vec![
        count_usize("ready", "Ready", stats.messages_ready),
        count_usize("delayed", "Delayed", stats.messages_delayed),
        count_usize("pending", "Pending", stats.messages_pending),
        count_usize("inflight", "Inflight", stats.inflight_active),
        count_usize("dead_letters", "Dead letters", stats.messages_dead_lettered),
        count_u64(
            "oldest_backlog_age_seconds",
            "Oldest backlog age",
            stats.oldest_backlog_age_seconds,
        ),
    ];

    add_broker_domain_flow(
        connections,
        "queue",
        &lane_state,
        stats.operations_per_second,
        counters.clone(),
    );

    for item in inflight {
        connections.push(topology_connection(
            (
                format!("queue-inflight:{}:{}", item.family, item.message_id),
                domain_node_id("queue"),
                session_node_id(&item.session_id),
            ),
            TopologyConnectionKind::QueueInflightConsumer,
            format!("{} / {} inflight", item.area, item.resource),
            TopologyState::Flowing,
            scope_with_session(
                scope_for_resource(&item.realm, &item.area, &item.resource, Some(item.family)),
                item.session_id.clone(),
            ),
            vec![
                count_u64("message_id", "Message", item.message_id),
                count_usize("attempts", "Attempts", item.attempts),
            ],
        ));
    }

    topology_lane(
        ("queue", "Queue"),
        lane_state,
        stats.operations_per_second,
        &stats.diagnostics,
        counters,
        (stats.inflight_active, 0),
        top_queue_resources(queues),
    )
}

fn top_queue_resources(queues: &[QueueInfo]) -> Vec<TopologyScopedResource> {
    let resources = queues
        .iter()
        .map(|queue| {
            let counters = vec![
                count_usize("ready", "Ready", queue.messages_ready),
                count_usize("delayed", "Delayed", queue.messages_delayed),
                count_usize("inflight", "Inflight", queue.messages_inflight),
                count_usize("dead_letters", "Dead letters", queue.messages_dead_lettered),
                count_u64(
                    "oldest_backlog_age_seconds",
                    "Oldest backlog age",
                    queue.oldest_backlog_age_seconds,
                ),
            ];
            let state = scoped_state(
                queue.messages_dead_lettered > 0,
                queue.messages_ready + queue.messages_delayed + queue.messages_inflight > 0,
                queue.messages_total > 0,
            );

            scoped_resource(
                "queue",
                format!("{} / {} / {}", queue.realm, queue.area, queue.resource),
                state,
                scope_for_resource(
                    &queue.realm,
                    &queue.area,
                    &queue.resource,
                    Some(queue.family),
                ),
                counters,
            )
        })
        .collect::<Vec<_>>();

    top_resources(resources)
}
