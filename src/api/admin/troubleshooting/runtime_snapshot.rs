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
    let now = Utc::now();
    let analyses = collect_runtime_domain_analyses(runtime, now);
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

fn collect_runtime_domain_analyses(runtime: &Runtime, now: DateTime<Utc>) -> RuntimeDomainSnapshot {
    let read_model = runtime.admin_read_model();
    RuntimeDomainSnapshot {
        kv: analyze_kv(&read_model.kv_transactions(None), now),
        stream: analyze_stream(
            &read_model.streams(None),
            runtime.stream_request_latency_buckets(),
            now,
        ),
        notice: analyze_notice(
            &read_model.notice_subscriptions(None, None),
            &read_model.notice_routes(None),
            now,
        ),
        queue: analyze_queue(
            &read_model.queues(None),
            &read_model.queue_inflight(None),
            &read_model.queue_dead_letters(None),
            runtime.queue_dead_letter_transitions_total(),
            runtime.queue_complete_rejected_total(),
            now,
        ),
        rpc: analyze_rpc(
            &read_model.rpc_workers(None),
            &read_model.rpc_pending(None),
            runtime.rpc_request_timeouts_total(),
            runtime.rpc_backpressure_rejects_total(),
            runtime.rpc_duplicate_correlation_rejects_total(),
            runtime.rpc_wrong_worker_rejects_total(),
            runtime.rpc_responses_dropped_closed_caller_total(),
            runtime.rpc_responses_missing_pending_total(),
            now,
        ),
        lease: analyze_lease(&read_model.leases(None), now),
        schedule: analyze_schedule(
            &read_model.schedules(None),
            runtime.schedule_pending_fire_claims(),
            runtime.schedule_pending_ack_retries(),
            runtime.schedule_oldest_pending_claim_age_seconds(),
            runtime.schedule_request_latency_buckets(),
            runtime.schedule_notify_failures(),
            runtime.schedule_ack_failures(),
            runtime.schedule_overdue_normalizations(),
            runtime.schedule_pending_claims_expired_total(),
            runtime.schedule_pending_claim_cleanup_failures_total(),
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
