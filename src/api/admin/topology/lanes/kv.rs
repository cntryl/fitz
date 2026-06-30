use crate::api::admin::list::KvTransaction;
use crate::api::admin::stats;
use crate::api::admin::topology::helpers::{
    add_broker_domain_flow, count_u64, count_usize, domain_node_id, scope_for_resource,
    scope_with_session, scoped_resource, session_node_id, top_resources, topology_connection,
    topology_lane, topology_state,
};
use crate::api::admin::topology::types::{
    TopologyConnectionBuilder, TopologyConnectionKind, TopologyLane, TopologyScopedResource,
    TopologyState,
};
use std::collections::BTreeMap;

pub(in crate::api::admin::topology) fn kv_lane(
    stats: &stats::KvStats,
    transactions: &[KvTransaction],
    connections: &mut TopologyConnectionBuilder,
) -> TopologyLane {
    let pressure = stats.commits_failed_total + stats.invalid_transaction_rejects_total;
    let activity =
        stats.operations_per_second > 0.0 || stats.transactions_active > 0 || stats.keys_total > 0;
    let lane_state = topology_state(&stats.diagnostics, pressure > 0, activity);
    let counters = vec![
        count_usize("keys", "Keys", stats.keys_total),
        count_usize("transactions", "Transactions", stats.transactions_active),
        count_u64(
            "failed_commits",
            "Failed commits",
            stats.commits_failed_total,
        ),
        count_u64(
            "invalid_rejects",
            "Invalid rejects",
            stats.invalid_transaction_rejects_total,
        ),
    ];

    add_broker_domain_flow(
        connections,
        "kv",
        &lane_state,
        stats.operations_per_second,
        counters.clone(),
    );

    for transaction in transactions {
        let session_id = session_id_from_transaction_mode(&transaction.mode);
        let target = session_id.as_deref().map_or_else(
            || format!("kv-transaction:{}", transaction.tx_id),
            session_node_id,
        );
        let scope = if let Some(session_id) = session_id {
            scope_with_session(
                scope_for_resource(
                    &transaction.realm,
                    &transaction.area,
                    &transaction.resource,
                    None,
                ),
                session_id,
            )
        } else {
            scope_for_resource(
                &transaction.realm,
                &transaction.area,
                &transaction.resource,
                None,
            )
        };

        connections.push(topology_connection(
            (
                format!("kv-transaction:{}", transaction.tx_id),
                domain_node_id("kv"),
                target,
            ),
            TopologyConnectionKind::KvTransactionActivity,
            format!(
                "{} / {} transaction",
                transaction.area, transaction.resource
            ),
            TopologyState::Flowing,
            scope,
            vec![
                count_usize("operations", "Operations", transaction.operations_count),
                count_u64("idle_seconds", "Idle", transaction.idle_seconds),
            ],
        ));
    }

    topology_lane(
        ("kv", "KV"),
        lane_state,
        stats.operations_per_second,
        &stats.diagnostics,
        counters,
        (stats.transactions_active, 0),
        top_kv_resources(transactions),
    )
}

fn session_id_from_transaction_mode(mode: &str) -> Option<String> {
    let mut parts = mode.split(':');
    if parts.next()? != "session" {
        return None;
    }
    parts.next().map(str::to_string)
}

fn top_kv_resources(transactions: &[KvTransaction]) -> Vec<TopologyScopedResource> {
    #[derive(Default)]
    struct Rollup {
        realm: String,
        area: String,
        resource: String,
        transactions: usize,
        operations: usize,
        max_idle_seconds: u64,
    }

    let mut rollups: BTreeMap<(String, String, String), Rollup> = BTreeMap::new();
    for transaction in transactions {
        let key = (
            transaction.realm.clone(),
            transaction.area.clone(),
            transaction.resource.clone(),
        );
        let rollup = rollups.entry(key).or_default();
        rollup.realm.clone_from(&transaction.realm);
        rollup.area.clone_from(&transaction.area);
        rollup.resource.clone_from(&transaction.resource);
        rollup.transactions += 1;
        rollup.operations += transaction.operations_count;
        rollup.max_idle_seconds = rollup.max_idle_seconds.max(transaction.idle_seconds);
    }

    let resources = rollups
        .into_values()
        .map(|rollup| {
            let counters = vec![
                count_usize("transactions", "Transactions", rollup.transactions),
                count_usize("operations", "Operations", rollup.operations),
                count_u64("max_idle_seconds", "Max idle", rollup.max_idle_seconds),
            ];
            scoped_resource(
                "kv",
                format!("{} / {} / {}", rollup.realm, rollup.area, rollup.resource),
                TopologyState::Flowing,
                scope_for_resource(&rollup.realm, &rollup.area, &rollup.resource, None),
                counters,
            )
        })
        .collect::<Vec<_>>();

    top_resources(resources)
}
