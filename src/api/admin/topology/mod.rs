//! Messaging topology endpoint.
//!
//! The topology snapshot is descriptive observability only. It composes the
//! existing admin read models into a bounded graph-shaped response for the UI.

mod helpers;
mod lanes;
mod sessions;
mod types;
pub(crate) use types::MessagingTopology;

use crate::api::http::Response;
use crate::boot::Runtime;
use chrono::Utc;
use types::TopologyConnectionBuilder;

const CONNECTION_LIMIT: usize = 250;

fn build_messaging_topology(runtime: &Runtime) -> MessagingTopology {
    let global_stats = super::stats::build_global_stats(runtime);
    let sessions = runtime.list_sessions();
    let queues = runtime.queue_list_queues(None);
    let queue_inflight = runtime.queue_list_inflight(None);
    let kv_transactions = runtime.kv_list_transactions(None);
    let streams = runtime.stream_list_streams(None);
    let notice_subscriptions = runtime.notice_list_subscriptions(None, None);
    let rpc_workers = runtime.rpc_list_workers(None);
    let rpc_pending = runtime.rpc_list_pending(None);
    let leases = runtime.lease_list_leases(None);
    let schedules = runtime.schedule_list_schedules(None);

    let mut connections = TopologyConnectionBuilder::new(CONNECTION_LIMIT);
    let domains = &global_stats.domains;
    let lanes = vec![
        lanes::queue_lane(&domains.queue, &queues, &queue_inflight, &mut connections),
        lanes::rpc_lane(
            &domains.rpc,
            &rpc_workers,
            &rpc_pending,
            false,
            &mut connections,
        ),
        lanes::notice_lane(
            &domains.notice,
            &notice_subscriptions,
            false,
            &mut connections,
        ),
        lanes::schedule_lane(&domains.schedule, &schedules, &mut connections),
        lanes::stream_lane(&domains.stream, &streams, &mut connections),
        lanes::lease_lane(&domains.lease, &leases, &mut connections),
        lanes::kv_lane(&domains.kv, &kv_transactions, &mut connections),
    ];

    MessagingTopology {
        generated_at: Utc::now().to_rfc3339(),
        broker: global_stats.broker,
        diagnostics: global_stats.diagnostics,
        session_groups: sessions::session_groups(sessions),
        lanes,
        connections: connections.finish(),
    }
}

pub fn handle_topology(runtime: &Runtime) -> Response {
    super::json_response(build_messaging_topology(runtime))
}

/// Return only topology records attributable to one authorized family.
pub fn handle_family_topology(runtime: &Runtime, family: u64) -> Response {
    super::json_response(build_family_topology(runtime, family))
}

pub(crate) fn mcp_topology_value(runtime: &Runtime, family: Option<u64>) -> serde_json::Value {
    let snapshot = runtime.admin_read_model().bounded_snapshot(family, 128);
    let topology = build_snapshot_topology(&snapshot, family);
    let mut value = serde_json::to_value(topology).expect("topology DTO serializes");
    value["_meta"] = serde_json::json!({
        "observed_at": Utc::now().to_rfc3339(),
        "partial": snapshot.truncated,
        "collection_limit": snapshot.limit_per_collection,
        "unavailable": ["unattributed broker counters and domain latency histories"],
    });
    value
}

fn build_family_topology(runtime: &Runtime, family: u64) -> MessagingTopology {
    let snapshot = runtime.refreshed_admin_snapshot(Some(family), usize::MAX);
    build_snapshot_topology(&snapshot, Some(family))
}

fn build_snapshot_topology(
    snapshot: &crate::control::admin::read_model::AdminSnapshot,
    family: Option<u64>,
) -> MessagingTopology {
    let stats = super::stats::build_snapshot_stats(snapshot);
    let domains = &stats.domains;
    let mut connections = TopologyConnectionBuilder::new(CONNECTION_LIMIT);
    let lanes = vec![
        lanes::queue_lane(
            &domains.queue,
            &snapshot.queues,
            &snapshot.queue_inflight,
            &mut connections,
        ),
        lanes::rpc_lane(
            &domains.rpc,
            &snapshot.rpc_workers,
            &snapshot.rpc_pending,
            true,
            &mut connections,
        ),
        lanes::notice_lane(
            &domains.notice,
            &snapshot.notice_subscriptions,
            true,
            &mut connections,
        ),
        lanes::schedule_lane(&domains.schedule, &snapshot.schedules, &mut connections),
        lanes::stream_lane(&domains.stream, &snapshot.streams, &mut connections),
        lanes::lease_lane(&domains.lease, &snapshot.leases, &mut connections),
        lanes::kv_lane(&domains.kv, &snapshot.kv_transactions, &mut connections),
    ];
    let mut connections = connections.finish();
    if let Some(family) = family {
        // Synthetic broker/domain edges have no family; retained facts were selected before collection.
        connections
            .items
            .retain(|item| item.scope.route_family == Some(family));
        if !connections.truncated {
            connections.total = connections.items.len();
        }
    }
    connections.truncated |= snapshot.truncated;
    MessagingTopology {
        generated_at: Utc::now().to_rfc3339(),
        broker: stats.broker,
        diagnostics: stats.diagnostics,
        session_groups: sessions::session_groups(snapshot.sessions.clone()),
        lanes,
        connections,
    }
}

#[cfg(test)]
mod tests;
