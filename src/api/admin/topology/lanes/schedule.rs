use crate::api::admin::list::ScheduleInfo;
use crate::api::admin::stats;
use crate::api::admin::topology::helpers::{
    add_broker_domain_flow, counter, domain_node_id, scope_for_resource, scoped_resource,
    top_resources, topology_connection, topology_lane, topology_state,
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
        + stats.notify_failures_total as usize
        + stats.ack_failures_total as usize
        + stats.create_persistence_failures_total as usize
        + stats.upsert_persistence_failures_total as usize
        + stats.cancel_persistence_failures_total as usize;
    let activity = stats.executions_per_minute > 0.0
        || stats.schedules_active > 0
        || stats.subscriptions_active > 0;
    let lane_state = topology_state(&stats.diagnostics, pressure > 0, activity);
    let activity_per_second = stats.executions_per_minute / 60.0;
    let counters = vec![
        counter("schedules", "Schedules", stats.schedules_active as f64),
        counter(
            "subscriptions",
            "Subscriptions",
            stats.subscriptions_active as f64,
        ),
        counter(
            "pending_claims",
            "Pending claims",
            stats.pending_fire_claims as f64,
        ),
        counter(
            "pending_ack_retries",
            "Pending ack retries",
            stats.pending_ack_retries as f64,
        ),
        counter(
            "notify_failures",
            "Notify failures",
            stats.notify_failures_total as f64,
        ),
        counter(
            "ack_failures",
            "Ack failures",
            stats.ack_failures_total as f64,
        ),
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
            vec![counter(
                "subscriptions",
                "Subscriptions",
                stats.subscriptions_active as f64,
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
                counter("executions", "Executions", schedule.executions_total as f64),
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
