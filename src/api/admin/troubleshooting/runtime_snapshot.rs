use std::cmp::Ordering;

use super::model::DiagnosticSnapshotInput;
use super::{
    analyze_kv, analyze_lease, analyze_notice, analyze_queue, analyze_rpc, analyze_schedule,
    analyze_stream, score_u64, DiagnosisLabel, DiagnosticHotspot, DiagnosticSeverity,
    DiagnosticSnapshot, DiagnosticTrend, DomainAnalysis, GlobalTroubleshootingDiagnostics,
    IncidentStatus, IncidentSummary, RuntimeDiagnostics, ScoredHotspot,
};
use crate::api::admin::troubleshooting::{rfc3339, TroubleshootingSnapshot};
use crate::boot::Runtime;
use chrono::{DateTime, Utc};

pub fn build_troubleshooting_snapshot(runtime: &Runtime) -> TroubleshootingSnapshot {
    build_runtime_diagnostics(runtime)
}

pub fn build_runtime_diagnostics(runtime: &Runtime) -> RuntimeDiagnostics {
    let snapshot = runtime.refreshed_admin_snapshot(None, usize::MAX);
    build_runtime_diagnostics_with_schedule(
        runtime,
        &snapshot,
        ScheduleDiagnosticCounters::from_runtime(runtime),
    )
}

pub(crate) fn build_bounded_runtime_diagnostics(
    runtime: &Runtime,
    snapshot: &crate::control::admin::read_model::AdminSnapshot,
) -> RuntimeDiagnostics {
    // Broker/family totals cannot identify the affected Schedule resource.
    // The bounded path never asks actors for an unbounded live-count scan.
    let mut diagnostics = build_runtime_diagnostics_with_schedule(
        runtime,
        snapshot,
        ScheduleDiagnosticCounters::default(),
    );
    diagnostics.global.incident_summary =
        bounded_incident_summary(diagnostics.global.incident_summary);
    diagnostics
}

fn bounded_incident_summary(mut summary: IncidentSummary) -> IncidentSummary {
    if summary.status == IncidentStatus::Healthy {
        summary.status = IncidentStatus::Unknown;
        summary.title = "Health is unknown from bounded cached evidence".into();
        summary.confidence = summary.confidence.min(0.5);
        summary.explanation = "No elevated pressure was established from the bounded cached projection rows and fixed broker counters. Source publication age and unobserved Schedule runtime pressure are unknown; this evidence cannot prove health or ongoing domain progress.".into();
    }
    summary
}

#[derive(Clone, Copy, Default)]
struct ScheduleDiagnosticCounters {
    pending_fire_claims: usize,
    pending_ack_retries: usize,
    oldest_pending_claim_age_seconds: u64,
    notify_failures: u64,
    ack_failures: u64,
    overdue_normalizations: u64,
}

impl ScheduleDiagnosticCounters {
    fn from_runtime(runtime: &Runtime) -> Self {
        Self {
            pending_fire_claims: runtime.schedule_pending_fire_claims(),
            pending_ack_retries: runtime.schedule_pending_ack_retries(),
            oldest_pending_claim_age_seconds: runtime.schedule_oldest_pending_claim_age_seconds(),
            notify_failures: runtime.schedule_notify_failures(),
            ack_failures: runtime.schedule_ack_failures(),
            overdue_normalizations: runtime.schedule_overdue_normalizations(),
        }
    }
}

fn build_runtime_diagnostics_with_schedule(
    runtime: &Runtime,
    snapshot: &crate::control::admin::read_model::AdminSnapshot,
    schedule_counters: ScheduleDiagnosticCounters,
) -> RuntimeDiagnostics {
    let now = Utc::now();
    let analyses = collect_runtime_domain_analyses(runtime, snapshot, &schedule_counters, now);
    let mut all_hotspots = collect_runtime_hotspots(runtime, &analyses);
    all_hotspots.sort_by(compare_scored_hotspots);
    all_hotspots.truncate(5);

    let top_bottleneck = all_hotspots
        .first()
        .map(|candidate| candidate.hotspot.clone());
    let last_significant_transition_at =
        last_significant_runtime_transition(&all_hotspots, &analyses);
    let incident_summary = summarize_incident(top_bottleneck.as_ref());

    RuntimeDiagnostics {
        global: GlobalTroubleshootingDiagnostics {
            incident_summary,
            top_bottleneck,
            last_significant_transition_at: last_significant_transition_at.map(rfc3339),
            hotspots: all_hotspots
                .into_iter()
                .map(|candidate| candidate.hotspot)
                .collect(),
        },
        kv: analyses.kv.diagnostics,
        stream: analyses.stream.diagnostics,
        notice: analyses.notice.diagnostics,
        queue: analyses.queue.diagnostics,
        rpc: analyses.rpc.diagnostics,
        lease: analyses.lease.diagnostics,
        schedule: analyses.schedule.diagnostics,
    }
}

pub(crate) fn compare_scored_hotspots(left: &ScoredHotspot, right: &ScoredHotspot) -> Ordering {
    right
        .score
        .partial_cmp(&left.score)
        .unwrap_or(Ordering::Equal)
        .then_with(|| left.hotspot.path().cmp(&right.hotspot.path()))
}

pub(crate) fn broker_router_hotspot(
    router_backpressure_total: u64,
    router_high_lane_backpressure_total: u64,
) -> Option<ScoredHotspot> {
    if router_backpressure_total == 0 && router_high_lane_backpressure_total == 0 {
        return None;
    }

    let likely_bottleneck = if router_high_lane_backpressure_total > 0 {
        "router control-plane saturation".to_string()
    } else {
        "router saturation".to_string()
    };
    let severity = if router_high_lane_backpressure_total > 0 {
        DiagnosticSeverity::High
    } else {
        DiagnosticSeverity::Medium
    };
    let mut hints = Vec::new();
    if router_backpressure_total > 0 {
        hints.push(format!(
            "{router_backpressure_total} router mailbox saturation event(s)"
        ));
    }
    if router_high_lane_backpressure_total > 0 {
        hints.push(format!(
            "{router_high_lane_backpressure_total} router high-lane saturation event(s)"
        ));
    }

    let snapshot = DiagnosticSnapshot::with_stage(DiagnosticSnapshotInput {
        current_stage: DiagnosisLabel::Throughput,
        trend: DiagnosticTrend::Growing,
        severity,
        likely_bottleneck: Some(likely_bottleneck.clone()),
        last_changed_at: None,
        last_success_at: None,
        last_failure_at: None,
        age_seconds: None,
        recent_transition_count: 0,
        failure_count: 0,
        contention_count: 0,
        waiter_count: 0,
        explanation_hints: hints,
    });

    Some(ScoredHotspot {
        score: score_u64(router_backpressure_total) * 3.0
            + score_u64(router_high_lane_backpressure_total) * 12.0,
        hotspot: DiagnosticHotspot {
            domain: "broker".to_string(),
            realm: None,
            area: None,
            resource: None,
            operation: None,
            family: None,
            backlog: None,
            inflight: None,
            ready: None,
            delayed: None,
            dead_letters: None,
            workers: None,
            subscriptions: None,
            owner_session: None,
            worker_session: None,
            snapshot,
        },
        last_changed_at: None,
    })
}

struct RuntimeDomainSnapshot {
    kv: DomainAnalysis,
    stream: DomainAnalysis,
    notice: DomainAnalysis,
    queue: DomainAnalysis,
    rpc: DomainAnalysis,
    lease: DomainAnalysis,
    schedule: DomainAnalysis,
}

fn collect_runtime_domain_analyses(
    runtime: &Runtime,
    snapshot: &crate::control::admin::read_model::AdminSnapshot,
    schedule_counters: &ScheduleDiagnosticCounters,
    now: DateTime<Utc>,
) -> RuntimeDomainSnapshot {
    RuntimeDomainSnapshot {
        kv: analyze_kv(&snapshot.kv_transactions, now),
        stream: analyze_stream(
            &snapshot.streams,
            runtime.stream_request_latency_buckets(),
            now,
        ),
        notice: analyze_notice(&snapshot.notice_subscriptions, &snapshot.notice_routes, now),
        queue: analyze_queue(
            &snapshot.queues,
            &snapshot.queue_inflight,
            &snapshot.queue_dead_letters,
            runtime.queue_dead_letter_transitions_total(),
            runtime.queue_complete_rejected_total(),
            now,
        ),
        rpc: analyze_rpc(
            &snapshot.rpc_workers,
            &snapshot.rpc_pending,
            runtime.rpc_request_timeouts_total(),
            runtime.rpc_backpressure_rejects_total(),
            runtime.rpc_duplicate_correlation_rejects_total(),
            runtime.rpc_wrong_worker_rejects_total(),
            runtime.rpc_responses_dropped_closed_caller_total(),
            runtime.rpc_responses_missing_pending_total(),
            now,
        ),
        lease: analyze_lease(&snapshot.leases, now),
        schedule: analyze_schedule(
            &snapshot.schedules,
            schedule_counters.pending_fire_claims,
            schedule_counters.pending_ack_retries,
            schedule_counters.oldest_pending_claim_age_seconds,
            runtime.schedule_request_latency_buckets(),
            schedule_counters.notify_failures,
            schedule_counters.ack_failures,
            schedule_counters.overdue_normalizations,
            now,
        ),
    }
}

fn collect_runtime_hotspots(
    runtime: &Runtime,
    analyses: &RuntimeDomainSnapshot,
) -> Vec<ScoredHotspot> {
    let mut all_hotspots = Vec::new();
    all_hotspots.extend(analyses.kv.hotspots.iter().cloned());
    all_hotspots.extend(analyses.stream.hotspots.iter().cloned());
    all_hotspots.extend(analyses.notice.hotspots.iter().cloned());
    all_hotspots.extend(analyses.queue.hotspots.iter().cloned());
    all_hotspots.extend(analyses.rpc.hotspots.iter().cloned());
    all_hotspots.extend(analyses.lease.hotspots.iter().cloned());
    all_hotspots.extend(analyses.schedule.hotspots.iter().cloned());

    if let Some(router_hotspot) = broker_router_hotspot(
        runtime.router_backpressure_total(),
        runtime.router_high_lane_backpressure_total(),
    ) {
        all_hotspots.push(router_hotspot);
    }

    all_hotspots
}

fn last_significant_runtime_transition(
    all_hotspots: &[ScoredHotspot],
    analyses: &RuntimeDomainSnapshot,
) -> Option<DateTime<Utc>> {
    all_hotspots
        .iter()
        .filter_map(|candidate| candidate.last_changed_at)
        .max()
        .or_else(|| {
            [
                analyses.kv.last_changed_at,
                analyses.stream.last_changed_at,
                analyses.notice.last_changed_at,
                analyses.queue.last_changed_at,
                analyses.rpc.last_changed_at,
                analyses.lease.last_changed_at,
                analyses.schedule.last_changed_at,
            ]
            .into_iter()
            .flatten()
            .max()
        })
}

pub(crate) fn summarize_incident(top_bottleneck: Option<&DiagnosticHotspot>) -> IncidentSummary {
    let Some(top) = top_bottleneck else {
        return IncidentSummary {
            status: IncidentStatus::Healthy,
            title: "Broker is healthy".to_string(),
            likely_bottleneck: None,
            severity: DiagnosticSeverity::Informational,
            confidence: 1.0,
            explanation:
                "No domain is currently showing elevated backlog, contention, or failure pressure."
                    .to_string(),
            recommended_next_query: None,
            suggested_next_queries: Vec::new(),
        };
    };

    let label = top.snapshot.diagnosis_label();
    let status = match top.snapshot.severity {
        DiagnosticSeverity::High | DiagnosticSeverity::Critical => IncidentStatus::Stalled,
        DiagnosticSeverity::Medium | DiagnosticSeverity::Low => IncidentStatus::Degraded,
        DiagnosticSeverity::Informational => IncidentStatus::Healthy,
    };

    let title = format!(
        "{} hotspot in {}",
        label.display_name(),
        if top.domain == "broker" {
            "broker scope".to_string()
        } else {
            top.path().unwrap_or_else(|| "unknown scope".to_string())
        }
    );
    let explanation = if top.snapshot.explanation_hints.is_empty() {
        label.explanation_hint().to_string()
    } else {
        top.snapshot.explanation_hints.join("; ")
    };
    let suggested_next_queries = top.suggested_queries();

    IncidentSummary {
        status,
        title,
        likely_bottleneck: top.snapshot.likely_bottleneck.clone(),
        severity: top.snapshot.severity.clone(),
        confidence: top.snapshot.confidence,
        explanation,
        recommended_next_query: suggested_next_queries
            .first()
            .map(|query| query.endpoint.clone()),
        suggested_next_queries,
    }
}

#[cfg(test)]
mod tests;
