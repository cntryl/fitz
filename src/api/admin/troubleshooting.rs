mod analysis_kv_stream_notice;
mod analysis_lease_schedule;
mod analysis_queue_rpc;
mod comparison;
mod model;
mod resource_diagnostics;
mod resource_timelines_core;
mod resource_timelines_domains;
mod runtime_snapshot;

use crate::api::admin::list::{
    KvTransaction, LeaseInfo, NoticeRouteInfo, NoticeSubscription, QueueAgeBuckets,
    QueueDeadLetter, QueueInflight, QueueInfo, RpcLatencyBuckets, RpcPendingRequest, RpcWorker,
    ScheduleInfo, ScheduleLatencyBuckets, StreamInfo, StreamLatencyBuckets,
};
use crate::api::admin::ResourcePath;
use crate::runtime::routing::{route_quad, route_triplet};
use num_traits::ToPrimitive;

pub(crate) use analysis_kv_stream_notice::{analyze_kv, analyze_notice, analyze_stream};
pub(crate) use analysis_lease_schedule::{
    age_seconds_since, analyze_lease, analyze_schedule, is_recent, parse_rfc3339, rfc3339,
    trend_from_pressure,
};
pub(crate) use analysis_queue_rpc::{analyze_queue, analyze_rpc, summarize_rpc_worker_latency};
pub(crate) use comparison::{
    compare_resource_sides, DomainAnalysis, ScoredHotspot, TimelineCandidate,
};
pub use comparison::{
    ResourceComparison, ResourceComparisonMetrics, ResourceComparisonScope, ResourceComparisonSide,
    TroubleshootingSnapshot,
};
pub(crate) use model::RECENT_WINDOW_SECS;
pub use model::{
    DiagnosisLabel, DiagnosticHotspot, DiagnosticSeverity, DiagnosticSnapshot, DiagnosticTrend,
    DomainDiagnostics, GlobalTroubleshootingDiagnostics, IncidentStatus, IncidentSummary,
    ResourceTimeline, ResourceTimelineEvent, ResourceTimelineKind, RuntimeDiagnostics,
};
pub(crate) use resource_diagnostics::{
    kv_resource_diagnostics, lease_resource_diagnostics, notice_resource_diagnostics,
    queue_resource_diagnostics, rpc_operation_diagnostics, schedule_resource_diagnostics,
    stream_resource_diagnostics,
};
pub(crate) use resource_timelines_core::{
    build_resource_timeline, kv_resource_timeline, matches_resource_path, matches_resource_route,
    parse_rpc_operation, timeline_candidate,
};
pub(crate) use resource_timelines_domains::{
    lease_resource_timeline, notice_resource_timeline, queue_resource_timeline,
    rpc_resource_timeline, schedule_resource_timeline, stream_resource_timeline,
};
pub use runtime_snapshot::build_troubleshooting_snapshot;

#[cfg(test)]
pub(crate) use runtime_snapshot::summarize_incident;

#[cfg(test)]
mod tests;

pub(crate) fn score_usize(value: usize) -> f64 {
    value.to_f64().unwrap_or(f64::MAX)
}

pub(crate) fn score_u64(value: u64) -> f64 {
    value.to_f64().unwrap_or(f64::MAX)
}

pub(crate) fn saturating_usize(value: u64) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX)
}
