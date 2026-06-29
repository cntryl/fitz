use crate::api::admin::list::LeaseInfo;
use crate::api::admin::stats;
use crate::api::admin::topology::helpers::{
    add_broker_domain_flow, counter, domain_node_id, scope_for_resource, scope_with_session,
    scoped_resource, session_node_id, top_resources, topology_connection, topology_lane,
    topology_state,
};
use crate::api::admin::topology::types::{
    TopologyConnectionBuilder, TopologyConnectionKind, TopologyLane, TopologyScopedResource,
    TopologyState,
};

pub(in crate::api::admin::topology) fn lease_lane(
    stats: &stats::LeaseStats,
    leases: &[LeaseInfo],
    connections: &mut TopologyConnectionBuilder,
) -> TopologyLane {
    let pressure =
        stats.waiter_depth + stats.acquire_timeouts_total as usize + stats.failure_total as usize;
    let activity = stats.operations_per_second > 0.0 || stats.leases_active > 0;
    let lane_state = topology_state(&stats.diagnostics, pressure > 0, activity);
    let counters = vec![
        counter("leases", "Leases", stats.leases_active as f64),
        counter("waiters", "Waiters", stats.waiter_depth as f64),
        counter("timeouts", "Timeouts", stats.acquire_timeouts_total as f64),
        counter(
            "forced_releases",
            "Forced releases",
            stats.forced_releases_total as f64,
        ),
        counter(
            "oldest_lease_age_seconds",
            "Oldest lease age",
            stats.oldest_lease_age_seconds as f64,
        ),
    ];

    add_broker_domain_flow(
        connections,
        "lease",
        &lane_state,
        stats.operations_per_second,
        counters.clone(),
    );

    for lease in leases {
        connections.push(topology_connection(
            (
                format!(
                    "lease-owner:{}:{}:{}",
                    lease.realm, lease.area, lease.resource
                ),
                domain_node_id("lease"),
                session_node_id(&lease.owner_session_id),
            ),
            TopologyConnectionKind::LeaseOwner,
            format!("{} / {} owner", lease.area, lease.resource),
            TopologyState::Flowing,
            scope_with_session(
                scope_for_resource(&lease.realm, &lease.area, &lease.resource, None),
                lease.owner_session_id.clone(),
            ),
            vec![
                counter("renewals", "Renewals", lease.renewals as f64),
                counter("fencing_token", "Fencing token", lease.fencing_token as f64),
            ],
        ));
    }

    topology_lane(
        ("lease", "Lease"),
        lane_state,
        stats.operations_per_second,
        &stats.diagnostics,
        counters,
        (stats.leases_active, stats.waiter_depth),
        top_lease_resources(leases),
    )
}

fn top_lease_resources(leases: &[LeaseInfo]) -> Vec<TopologyScopedResource> {
    let resources = leases
        .iter()
        .map(|lease| {
            let counters = vec![
                counter("renewals", "Renewals", lease.renewals as f64),
                counter("fencing_token", "Fencing token", lease.fencing_token as f64),
            ];
            scoped_resource(
                "lease",
                format!("{} / {} / {}", lease.realm, lease.area, lease.resource),
                TopologyState::Flowing,
                scope_with_session(
                    scope_for_resource(&lease.realm, &lease.area, &lease.resource, None),
                    lease.owner_session_id.clone(),
                ),
                counters,
            )
        })
        .collect::<Vec<_>>();

    top_resources(resources)
}
