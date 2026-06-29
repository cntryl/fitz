mod kv;
mod lease;
mod notice;
mod queue;
mod rpc;
mod schedule;
mod stream;

pub use kv::kv_events_for_resource;
pub use lease::lease_events_for_resource;
pub(crate) use lease::lease_search;
pub use notice::{
    notice_delivery_observations, notice_events_for_resource, notice_subscriptions_for_resource,
};
pub use queue::{
    queue_dead_letters_for_resource, queue_events_for_resource, queue_inflight_for_resource,
};
pub(crate) use rpc::rpc_call_observations;
pub use rpc::{rpc_events_for_resource, rpc_pending, rpc_workers_for_operation};
pub(crate) use schedule::schedule_missed_observations;
pub use schedule::{schedule_events_for_resource, schedule_executions_for_resource};
pub(crate) use stream::stream_search;
pub use stream::{stream_events_for_resource, stream_records_for_resource};

fn timestamp_ms_to_rfc3339(timestamp_ms: u64) -> String {
    chrono::DateTime::<chrono::Utc>::from_timestamp_millis(timestamp_ms.cast_signed())
        .map(|timestamp| timestamp.to_rfc3339())
        .unwrap_or_default()
}
