use crate::api::admin::list::NoticeSubscription;
use crate::api::admin::stats;
use crate::api::admin::topology::helpers::{
    add_broker_domain_flow, count_u64, count_usize, domain_node_id, scope_for_pattern,
    scoped_resource, session_node_id, top_resources, topology_connection, topology_lane,
    topology_state,
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
        count_usize("subscriptions", "Subscriptions", stats.subscriptions_active),
        count_usize("routes", "Routes", stats.routes_active),
        count_usize(
            "max_route_subscribers",
            "Max route subscribers",
            stats.max_route_subscribers,
        ),
        count_u64("delivery_drops", "Drops", stats.delivery_drops_total),
        count_u64(
            "wildcard_rejects",
            "Wildcard rejects",
            stats.wildcard_limit_rejects_total,
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
            vec![count_u64(
                "notifications_received",
                "Notifications",
                subscription.notifications_received,
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
        rollup.realm.clone_from(&subscription.realm);
        rollup.pattern.clone_from(&subscription.pattern);
        rollup.subscriptions += 1;
        rollup.notifications += subscription.notifications_received;
    }

    let resources = rollups
        .into_values()
        .map(|rollup| {
            let counters = vec![
                count_usize("subscriptions", "Subscriptions", rollup.subscriptions),
                count_u64("notifications", "Notifications", rollup.notifications),
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
