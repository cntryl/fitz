use crate::api::admin::list::{RpcPendingRequest, RpcWorker};
use crate::api::admin::stats;
use crate::api::admin::topology::helpers::{
    add_broker_domain_flow, counter, domain_node_id, scope_for_route, scoped_resource,
    scoped_state, session_node_id, top_resources, topology_connection, topology_lane,
    topology_state,
};
use crate::api::admin::topology::types::{
    TopologyConnectionBuilder, TopologyConnectionKind, TopologyLane, TopologyScopedResource,
    TopologyState,
};
use std::collections::BTreeMap;

pub(in crate::api::admin::topology) fn rpc_lane(
    stats: &stats::RpcStats,
    workers: &[RpcWorker],
    pending: &[RpcPendingRequest],
    connections: &mut TopologyConnectionBuilder,
) -> TopologyLane {
    let pressure = stats.requests_pending
        + stats.request_timeouts_total as usize
        + stats.backpressure_rejects_total as usize
        + stats.failure_total as usize
        + stats.responses_missing_pending_total as usize;
    let activity = stats.operations_per_second > 0.0 || stats.workers_registered > 0;
    let lane_state = topology_state(&stats.diagnostics, pressure > 0, activity);
    let counters = vec![
        counter("workers", "Workers", stats.workers_registered as f64),
        counter("pending", "Pending", stats.requests_pending as f64),
        counter("timeouts", "Timeouts", stats.request_timeouts_total as f64),
        counter(
            "backpressure",
            "Backpressure",
            stats.backpressure_rejects_total as f64,
        ),
        counter(
            "slowest_worker_average_latency_ms",
            "Slowest worker avg latency",
            stats.slowest_worker_average_latency_ms,
        ),
    ];

    add_broker_domain_flow(
        connections,
        "rpc",
        &lane_state,
        stats.operations_per_second,
        counters.clone(),
    );

    for worker in workers {
        connections.push(topology_connection(
            (
                format!("rpc-worker:{}:{}", worker.session_id, worker.route),
                session_node_id(&worker.session_id),
                domain_node_id("rpc"),
            ),
            TopologyConnectionKind::RpcWorker,
            worker.route.clone(),
            TopologyState::Flowing,
            scope_for_route(&worker.route, Some(worker.session_id.clone())),
            vec![
                counter(
                    "requests_handled",
                    "Handled",
                    worker.requests_handled as f64,
                ),
                counter(
                    "average_latency_ms",
                    "Avg latency",
                    worker.average_latency_ms,
                ),
            ],
        ));
    }

    for request in pending {
        let target = request.worker_session_id.as_deref().map_or_else(
            || format!("rpc-pending:{}", request.correlation_id),
            session_node_id,
        );
        let request_state = if request.worker_session_id.is_some() {
            TopologyState::Flowing
        } else {
            TopologyState::Pressure
        };

        connections.push(topology_connection(
            (
                format!("rpc-pending:{}", request.correlation_id),
                domain_node_id("rpc"),
                target,
            ),
            TopologyConnectionKind::RpcPendingAssignment,
            request.route.clone(),
            request_state,
            scope_for_route(&request.route, request.worker_session_id.clone()),
            vec![counter("age_seconds", "Age", request.age_seconds as f64)],
        ));
    }

    topology_lane(
        ("rpc", "RPC"),
        lane_state,
        stats.operations_per_second,
        &stats.diagnostics,
        counters,
        (stats.workers_registered, 0),
        top_rpc_resources(workers, pending),
    )
}

fn top_rpc_resources(
    workers: &[RpcWorker],
    pending: &[RpcPendingRequest],
) -> Vec<TopologyScopedResource> {
    #[derive(Default)]
    struct Rollup {
        route: String,
        workers: usize,
        pending: usize,
        handled: u64,
        slowest_latency_ms: f64,
        oldest_pending_age_seconds: u64,
    }

    let mut rollups: BTreeMap<String, Rollup> = BTreeMap::new();
    for worker in workers {
        let rollup = rollups.entry(worker.route.clone()).or_default();
        rollup.route = worker.route.clone();
        rollup.workers += 1;
        rollup.handled += worker.requests_handled;
        rollup.slowest_latency_ms = rollup.slowest_latency_ms.max(worker.average_latency_ms);
    }

    for request in pending {
        let rollup = rollups.entry(request.route.clone()).or_default();
        rollup.route = request.route.clone();
        rollup.pending += 1;
        rollup.oldest_pending_age_seconds =
            rollup.oldest_pending_age_seconds.max(request.age_seconds);
    }

    let resources = rollups
        .into_values()
        .map(|rollup| {
            let counters = vec![
                counter("workers", "Workers", rollup.workers as f64),
                counter("pending", "Pending", rollup.pending as f64),
                counter("handled", "Handled", rollup.handled as f64),
                counter(
                    "slowest_latency_ms",
                    "Slowest latency",
                    rollup.slowest_latency_ms,
                ),
                counter(
                    "oldest_pending_age_seconds",
                    "Oldest pending age",
                    rollup.oldest_pending_age_seconds as f64,
                ),
            ];
            let state = scoped_state(
                rollup.pending > 0 && rollup.workers == 0,
                rollup.pending > 0,
                rollup.workers > 0 || rollup.handled > 0,
            );

            scoped_resource(
                "rpc",
                rollup.route.clone(),
                state,
                scope_for_route(&rollup.route, None),
                counters,
            )
        })
        .collect::<Vec<_>>();

    top_resources(resources)
}
