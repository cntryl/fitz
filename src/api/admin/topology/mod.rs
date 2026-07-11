//! Messaging topology endpoint.
//!
//! The topology snapshot is descriptive observability only. It composes the
//! existing admin read models into a bounded graph-shaped response for the UI.

mod helpers;
mod lanes;
mod sessions;
mod types;

use crate::api::http::Response;
use crate::boot::Runtime;
use chrono::Utc;
use types::{MessagingTopology, TopologyConnectionBuilder};

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
        lanes::rpc_lane(&domains.rpc, &rpc_workers, &rpc_pending, &mut connections),
        lanes::notice_lane(&domains.notice, &notice_subscriptions, &mut connections),
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
    let mut topology = build_messaging_topology(runtime);
    topology
        .session_groups
        .retain(|group| group.route_family == family);
    topology.broker.sessions = topology
        .session_groups
        .iter()
        .map(|group| group.sessions)
        .sum();
    topology.broker.connections = topology.broker.sessions;
    topology.broker.uptime_seconds = 0;
    topology.broker.realms.clear();
    topology.broker.messages_per_second = 0.0;
    topology.broker.router_backpressure_total = 0;
    topology.broker.router_high_lane_backpressure_total = 0;
    topology.diagnostics = super::troubleshooting::healthy_global_diagnostics();
    topology.lanes.iter_mut().for_each(|lane| {
        lane.top_scoped_resources
            .retain(|resource| resource.scope.route_family == Some(family));
        lane.diagnostics = super::troubleshooting::healthy_domain_diagnostics().snapshot;
        lane.consumers = 0;
        lane.observers = 0;
        lane.activity_per_second = 0.0;
        lane.counters.clear();
    });
    topology
        .connections
        .items
        .retain(|connection| connection.scope.route_family == Some(family));
    topology.connections.total = topology.connections.items.len();
    topology.connections.truncated = false;
    super::json_response(topology)
}
