use crate::api::admin::list::ScheduleInfo;
use crate::api::admin::stats;
use crate::api::admin::topology::helpers::{
    add_broker_domain_flow, count_u64, count_usize, counter, domain_node_id, saturating_usize,
    scope_for_resource, scoped_resource, top_resources, topology_connection, topology_lane,
    topology_state,
};
use crate::api::admin::topology::types::{
    TopologyConnectionBuilder, TopologyConnectionKind, TopologyLane, TopologyScope,
    TopologyScopedResource, TopologyState,
};

pub(in crate::api::admin::topology) fn schedule_lane(
    stats: &stats::ScheduleStats,
    schedules: &[ScheduleInfo],
    connections: &mut TopologyConnectionBuilder,
) -> TopologyLane {
    let pressure = stats.pending_fire_claims
        + stats.pending_ack_retries
        + saturating_usize(stats.notify_failures_total)
        + saturating_usize(stats.ack_failures_total)
        + saturating_usize(stats.create_persistence_failures_total)
        + saturating_usize(stats.upsert_persistence_failures_total)
        + saturating_usize(stats.cancel_persistence_failures_total);
    let activity = stats.executions_per_minute > 0.0
        || stats.schedules_active > 0
        || stats.subscriptions_active > 0;
    let lane_state = topology_state(&stats.diagnostics, pressure > 0, activity);
    let activity_per_second = stats.executions_per_minute / 60.0;
    let counters = vec![
        count_usize("schedules", "Schedules", stats.schedules_active),
        count_usize("subscriptions", "Subscriptions", stats.subscriptions_active),
        count_usize(
            "pending_claims",
            "Pending claims",
            stats.pending_fire_claims,
        ),
        count_usize(
            "pending_ack_retries",
            "Pending ack retries",
            stats.pending_ack_retries,
        ),
        count_u64(
            "notify_failures",
            "Notify failures",
            stats.notify_failures_total,
        ),
        count_u64("ack_failures", "Ack failures", stats.ack_failures_total),
    ];

    add_broker_domain_flow(
        connections,
        "schedule",
        &lane_state,
        activity_per_second,
        counters.clone(),
    );

    if stats.subscriptions_active > 0 {
        connections.push(topology_connection(
            (
                "schedule-subscription-activity",
                domain_node_id("schedule"),
                "consumers:schedule",
            ),
            TopologyConnectionKind::ScheduleSubscriptionActivity,
            "Schedule subscriptions",
            TopologyState::Flowing,
            TopologyScope::default(),
            vec![count_usize(
                "subscriptions",
                "Subscriptions",
                stats.subscriptions_active,
            )],
        ));
    }

    topology_lane(
        ("schedule", "Schedule"),
        lane_state,
        activity_per_second,
        &stats.diagnostics,
        counters,
        (0, stats.subscriptions_active),
        top_schedule_resources(schedules),
    )
}

fn top_schedule_resources(schedules: &[ScheduleInfo]) -> Vec<TopologyScopedResource> {
    let resources = schedules
        .iter()
        .map(|schedule| {
            let counters = vec![
                counter(
                    "enabled",
                    "Enabled",
                    if schedule.enabled { 1.0 } else { 0.0 },
                ),
                count_u64("executions", "Executions", schedule.executions_total),
            ];

            scoped_resource(
                "schedule",
                format!(
                    "{} / {} / {} / {}",
                    schedule.realm, schedule.area, schedule.resource, schedule.operation
                ),
                if schedule.enabled {
                    TopologyState::Flowing
                } else {
                    TopologyState::Quiet
                },
                TopologyScope {
                    operation: Some(schedule.operation.clone()),
                    ..scope_for_resource(&schedule.realm, &schedule.area, &schedule.resource, None)
                },
                counters,
            )
        })
        .collect::<Vec<_>>();

    top_resources(resources)
}
