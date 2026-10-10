//! Troubleshooting diagnostics scoped to one route family.
//!
//! Only state that carries its own route family is analyzed. Broker-wide
//! counters (RPC timeouts and rejects, Queue and Schedule failure totals,
//! latency histograms, router saturation) are not attributable to a family,
//! so they are excluded rather than copied into a narrower authorization
//! scope. A family without hotspots is therefore reported as healthy with
//! reduced confidence instead of the full confidence of a broker-wide check.

use chrono::Utc;

use super::runtime_snapshot::{compare_scored_hotspots, summarize_incident};
use super::{
    analyze_kv, analyze_lease, analyze_notice, analyze_queue, analyze_rpc, analyze_schedule,
    analyze_stream, rfc3339, DiagnosticSeverity, DomainAnalysis, GlobalTroubleshootingDiagnostics,
    IncidentStatus, IncidentSummary,
};
use crate::api::admin::list::{ScheduleLatencyBuckets, StreamLatencyBuckets};
use crate::boot::Runtime;

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
    let snapshot = runtime.refreshed_admin_snapshot(Some(family), usize::MAX);
    build_projection_troubleshooting(&snapshot)
}

pub(crate) fn build_projection_troubleshooting(
    snapshot: &crate::control::admin::read_model::AdminSnapshot,
) -> GlobalTroubleshootingDiagnostics {
    let analyses = collect_projection_domain_analyses(snapshot);
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

fn collect_projection_domain_analyses(
    snapshot: &crate::control::admin::read_model::AdminSnapshot,
) -> [DomainAnalysis; 7] {
    let now = Utc::now();
    let kv_transactions = &snapshot.kv_transactions;
    let streams = &snapshot.streams;
    let notice_subscriptions = &snapshot.notice_subscriptions;
    let notice_routes = &snapshot.notice_routes;
    let queues = &snapshot.queues;
    let queue_inflight = &snapshot.queue_inflight;
    let queue_dead_letters = &snapshot.queue_dead_letters;
    let rpc_workers = &snapshot.rpc_workers;
    let rpc_pending = &snapshot.rpc_pending;
    let leases = &snapshot.leases;
    let schedules = &snapshot.schedules;
    let pending_fire_claims = snapshot.pending_fire_claims;
    [
        analyze_kv(kv_transactions, now),
        analyze_stream(streams, StreamLatencyBuckets::default(), now),
        analyze_notice(notice_subscriptions, notice_routes, now),
        analyze_queue(queues, queue_inflight, queue_dead_letters, 0, 0, now),
        analyze_rpc(rpc_workers, rpc_pending, 0, 0, 0, 0, 0, 0, now),
        analyze_lease(leases, now),
        analyze_schedule(
            schedules,
            pending_fire_claims,
            0,
            0,
            ScheduleLatencyBuckets::default(),
            0,
            0,
            0,
            now,
        ),
    ]
}
