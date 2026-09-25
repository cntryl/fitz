//! Troubleshooting diagnostics scoped to one route family.
//!
//! Only state that carries its own route family is analyzed. Broker-wide
//! counters (RPC timeouts and rejects, Queue and Schedule failure totals,
//! latency histograms, router saturation) are not attributable to a family,
//! so they are excluded rather than copied into a narrower authorization
//! scope. A family without hotspots is therefore reported as healthy with
//! reduced confidence instead of the full confidence of a broker-wide check.

use chrono::Utc;
use std::collections::BTreeMap;

use super::runtime_snapshot::{compare_scored_hotspots, summarize_incident};
use super::{
    analyze_kv, analyze_lease, analyze_notice, analyze_queue, analyze_rpc, analyze_schedule,
    analyze_stream, parse_rpc_operation, rfc3339, DiagnosticSeverity, DomainAnalysis,
    GlobalTroubleshootingDiagnostics, IncidentStatus, IncidentSummary, ScheduleInfo,
};
use crate::api::admin::list::{ScheduleLatencyBuckets, StreamLatencyBuckets};
use crate::boot::Runtime;
use crate::runtime::routing::RouteFamily;

/// Confidence reported when no family-attributable hotspot is found.
///
/// Every state-derived signal was checked, but broker-wide failure counters
/// were not, so the absence of pressure is only partially established.
const FAMILY_HEALTHY_CONFIDENCE: f64 = 0.5;

/// Build troubleshooting diagnostics from state attributable to `family`.
pub(crate) fn build_family_troubleshooting(
    runtime: &Runtime,
    family: u64,
) -> GlobalTroubleshootingDiagnostics {
    let analyses = collect_family_domain_analyses(runtime, family);
    let mut hotspots = analyses
        .iter()
        .flat_map(|analysis| analysis.hotspots.iter().cloned())
        .collect::<Vec<_>>();
    hotspots.sort_by(compare_scored_hotspots);
    hotspots.truncate(5);

    let top_bottleneck = hotspots.first().map(|candidate| candidate.hotspot.clone());
    let last_significant_transition_at = hotspots
        .iter()
        .filter_map(|candidate| candidate.last_changed_at)
        .max()
        .or_else(|| {
            analyses
                .iter()
                .filter_map(|analysis| analysis.last_changed_at)
                .max()
        });
    let incident_summary = match top_bottleneck.as_ref() {
        Some(top) => summarize_incident(Some(top)),
        None => family_healthy_summary(),
    };

    GlobalTroubleshootingDiagnostics {
        incident_summary,
        top_bottleneck,
        last_significant_transition_at: last_significant_transition_at.map(rfc3339),
        hotspots: hotspots
            .into_iter()
            .map(|candidate| candidate.hotspot)
            .collect(),
    }
}

fn family_healthy_summary() -> IncidentSummary {
    IncidentSummary {
        status: IncidentStatus::Healthy,
        title: "No family-attributable pressure detected".to_string(),
        likely_bottleneck: None,
        severity: DiagnosticSeverity::Informational,
        confidence: FAMILY_HEALTHY_CONFIDENCE,
        explanation: "No backlog, contention, or failure pressure is visible in state attributable to this route family. Broker-wide failure counters, latency histograms, and router saturation are not attributable to a family and were not evaluated.".to_string(),
        recommended_next_query: None,
        suggested_next_queries: Vec::new(),
    }
}

fn collect_family_domain_analyses(runtime: &Runtime, family: u64) -> [DomainAnalysis; 7] {
    let now = Utc::now();
    let mut kv_transactions = runtime.kv_list_transactions(None);
    kv_transactions.retain(|item| item.route_family == family);
    let mut streams = runtime.stream_list_streams(None);
    streams.retain(|item| item.route_family == family);
    let mut notice_subscriptions = runtime.notice_list_subscriptions(None, None);
    notice_subscriptions.retain(|item| item.route_family == family);
    let mut notice_routes = runtime.notice_list_routes(None);
    notice_routes.retain(|item| item.route_family == family);
    let mut queues = runtime.queue_list_queues(None);
    queues.retain(|item| item.family == family);
    let mut queue_inflight = runtime.queue_list_inflight(None);
    queue_inflight.retain(|item| item.family == family);
    let mut queue_dead_letters = runtime.queue_list_dead_letters(None);
    queue_dead_letters.retain(|item| item.family == family);
    let mut rpc_workers = runtime.rpc_list_workers(None);
    rpc_workers.retain(|item| item.route_family == family);
    let mut rpc_pending = runtime.rpc_list_pending(None);
    rpc_pending.retain(|item| item.route_family == family);
    let mut leases = runtime.lease_list_leases(None);
    leases.retain(|item| item.route_family == family);
    let mut schedules = runtime.schedule_list_schedules(None);
    schedules.retain(|item| item.route_family == family);

    [
        analyze_kv(&kv_transactions, now),
        analyze_stream(&streams, StreamLatencyBuckets::default(), now),
        analyze_notice(&notice_subscriptions, &notice_routes, now),
        analyze_queue(&queues, &queue_inflight, &queue_dead_letters, 0, 0, now),
        analyze_rpc(&rpc_workers, &rpc_pending, 0, 0, 0, 0, 0, 0, now),
        analyze_lease(&leases, now),
        analyze_family_schedules(runtime, family, schedules, now),
    ]
}

fn analyze_family_schedules(
    runtime: &Runtime,
    family: u64,
    schedules: Vec<ScheduleInfo>,
    now: chrono::DateTime<Utc>,
) -> DomainAnalysis {
    type OperationKey = (String, String, String, String);
    let mut claims_by_operation = BTreeMap::<OperationKey, usize>::new();
    if let Ok(family) = RouteFamily::try_from(family) {
        for claim in runtime.schedule_list_pending_claims(family) {
            if let Some(route) = parse_rpc_operation(&claim.route) {
                let key = (route.realm, route.area, route.resource, route.operation);
                let count = claims_by_operation.entry(key).or_default();
                *count = count.saturating_add(1);
            }
        }
    }

    let mut schedules_by_operation = BTreeMap::<OperationKey, Vec<ScheduleInfo>>::new();
    for schedule in schedules {
        let key = (
            schedule.realm.clone(),
            schedule.area.clone(),
            schedule.resource.clone(),
            schedule.operation.clone(),
        );
        schedules_by_operation
            .entry(key)
            .or_default()
            .push(schedule);
    }

    let mut hotspots = Vec::new();
    for (key, schedules) in schedules_by_operation {
        let claim_count = claims_by_operation.get(&key).copied().unwrap_or_default();
        hotspots.extend(
            analyze_schedule(
                &schedules,
                claim_count,
                0,
                0,
                ScheduleLatencyBuckets::default(),
                0,
                0,
                0,
                now,
            )
            .hotspots,
        );
    }
    DomainAnalysis::from_hotspots(hotspots)
}
