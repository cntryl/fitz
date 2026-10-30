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
const SNAPSHOT_COLLECTION_LIMIT: usize = 512;

fn build_messaging_topology(runtime: &Runtime) -> MessagingTopology {
    let snapshot = runtime.refreshed_admin_snapshot(None, SNAPSHOT_COLLECTION_LIMIT);
    build_snapshot_topology(runtime, &snapshot, None, false)
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
    let topology = build_snapshot_topology(runtime, &snapshot, family, true);
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
    let snapshot = family_topology_snapshot(runtime, family);
    build_snapshot_topology(runtime, &snapshot, Some(family), true)
}

fn family_topology_snapshot(
    runtime: &Runtime,
    family: u64,
) -> crate::control::admin::read_model::AdminSnapshot {
    runtime.refreshed_admin_snapshot(Some(family), SNAPSHOT_COLLECTION_LIMIT)
}

fn build_snapshot_topology(
    runtime: &Runtime,
    snapshot: &crate::control::admin::read_model::AdminSnapshot,
    family: Option<u64>,
    include_route_family: bool,
) -> MessagingTopology {
    let stats = super::stats::build_bounded_global_stats(runtime, snapshot);
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
            include_route_family,
            &mut connections,
        ),
        lanes::notice_lane(
            &domains.notice,
            &snapshot.notice_subscriptions,
            include_route_family,
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
    let sessions = if family.is_none() {
        let (sessions, truncated) = runtime.list_sessions_bounded(SNAPSHOT_COLLECTION_LIMIT);
        connections.truncated |= truncated;
        sessions
    } else {
        snapshot.sessions.clone()
    };
    MessagingTopology {
        generated_at: Utc::now().to_rfc3339(),
        broker: stats.broker,
        diagnostics: stats.diagnostics,
        session_groups: sessions::session_groups(sessions),
        lanes,
        connections,
    }
}

#[cfg(test)]
mod tests;
