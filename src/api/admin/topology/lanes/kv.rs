use crate::api::admin::list::KvTransaction;
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
        counter("keys", "Keys", stats.keys_total as f64),
        counter(
            "transactions",
            "Transactions",
            stats.transactions_active as f64,
        ),
        counter(
            "failed_commits",
            "Failed commits",
            stats.commits_failed_total as f64,
        ),
        counter(
            "invalid_rejects",
            "Invalid rejects",
            stats.invalid_transaction_rejects_total as f64,
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
                counter(
                    "operations",
                    "Operations",
                    transaction.operations_count as f64,
                ),
                counter("idle_seconds", "Idle", transaction.idle_seconds as f64),
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
        rollup.realm = transaction.realm.clone();
        rollup.area = transaction.area.clone();
        rollup.resource = transaction.resource.clone();
        rollup.transactions += 1;
        rollup.operations += transaction.operations_count;
        rollup.max_idle_seconds = rollup.max_idle_seconds.max(transaction.idle_seconds);
    }

    let resources = rollups
        .into_values()
        .map(|rollup| {
            let counters = vec![
                counter("transactions", "Transactions", rollup.transactions as f64),
                counter("operations", "Operations", rollup.operations as f64),
                counter(
                    "max_idle_seconds",
                    "Max idle",
                    rollup.max_idle_seconds as f64,
                ),
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
