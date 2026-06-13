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
use std::convert::Infallible;
use std::sync::Arc;
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

pub async fn handle_topology(runtime: Arc<Runtime>) -> Result<Response, Infallible> {
    super::json_response(build_messaging_topology(runtime.as_ref()))
}
