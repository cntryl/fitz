use crate::api::admin::list::NoticeSubscription;
use crate::api::admin::stats;
use crate::api::admin::topology::helpers::{
    add_broker_domain_flow, counter, domain_node_id, scope_for_pattern, scoped_resource,
    session_node_id, top_resources, topology_connection, topology_lane, topology_state,
};
use crate::api::admin::topology::types::{
    TopologyConnectionBuilder, TopologyConnectionKind, TopologyLane, TopologyScopedResource,
    TopologyState,
};
use std::collections::BTreeMap;

pub(in crate::api::admin::topology) fn notice_lane(
    stats: &stats::NoticeStats,
    subscriptions: &[NoticeSubscription],
    connections: &mut TopologyConnectionBuilder,
) -> TopologyLane {
    let pressure = stats.delivery_drops_total
        + stats.failure_total
        + stats.wildcard_limit_rejects_total
        + stats.unsubscribes_total;
    let activity = stats.publishes_per_second > 0.0 || stats.subscriptions_active > 0;
    let lane_state = topology_state(&stats.diagnostics, pressure > 0, activity);
    let counters = vec![
        counter(
            "subscriptions",
            "Subscriptions",
            stats.subscriptions_active as f64,
        ),
        counter("routes", "Routes", stats.routes_active as f64),
        counter(
            "max_route_subscribers",
            "Max route subscribers",
            stats.max_route_subscribers as f64,
        ),
        counter("delivery_drops", "Drops", stats.delivery_drops_total as f64),
        counter(
            "wildcard_rejects",
            "Wildcard rejects",
            stats.wildcard_limit_rejects_total as f64,
        ),
    ];

    add_broker_domain_flow(
        connections,
        "notice",
        &lane_state,
        stats.publishes_per_second,
        counters.clone(),
    );

    for subscription in subscriptions {
        connections.push(topology_connection(
            (
                format!(
                    "notice-subscription:{}:{}",
                    subscription.session_id, subscription.subscription_id
                ),
                session_node_id(&subscription.session_id),
                domain_node_id("notice"),
            ),
            TopologyConnectionKind::NoticeSubscription,
            subscription.pattern.clone(),
            TopologyState::Flowing,
            scope_for_pattern(
                &subscription.pattern,
                &subscription.realm,
                Some(subscription.session_id.clone()),
            ),
            vec![counter(
                "notifications_received",
                "Notifications",
                subscription.notifications_received as f64,
            )],
        ));
    }

    topology_lane(
        ("notice", "Notice"),
        lane_state,
        stats.publishes_per_second,
        &stats.diagnostics,
        counters,
        (0, stats.subscriptions_active),
        top_notice_resources(subscriptions),
    )
}

fn top_notice_resources(subscriptions: &[NoticeSubscription]) -> Vec<TopologyScopedResource> {
    #[derive(Default)]
    struct Rollup {
        realm: String,
        pattern: String,
        subscriptions: usize,
        notifications: u64,
    }

    let mut rollups: BTreeMap<String, Rollup> = BTreeMap::new();
    for subscription in subscriptions {
        let rollup = rollups.entry(subscription.pattern.clone()).or_default();
        rollup.realm = subscription.realm.clone();
        rollup.pattern = subscription.pattern.clone();
        rollup.subscriptions += 1;
        rollup.notifications += subscription.notifications_received;
    }

    let resources = rollups
        .into_values()
        .map(|rollup| {
            let counters = vec![
                counter(
                    "subscriptions",
                    "Subscriptions",
                    rollup.subscriptions as f64,
                ),
                counter(
                    "notifications",
                    "Notifications",
                    rollup.notifications as f64,
                ),
            ];
            scoped_resource(
                "notice",
                rollup.pattern.clone(),
                TopologyState::Flowing,
                scope_for_pattern(&rollup.pattern, &rollup.realm, None),
                counters,
            )
        })
        .collect::<Vec<_>>();

    top_resources(resources)
}
