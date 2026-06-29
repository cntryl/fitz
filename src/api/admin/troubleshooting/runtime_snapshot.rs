use super::*;

pub fn build_troubleshooting_snapshot(runtime: &Runtime) -> TroubleshootingSnapshot {
    build_runtime_diagnostics(runtime)
}

pub fn build_runtime_diagnostics(runtime: &Runtime) -> RuntimeDiagnostics {
    let now = Utc::now();
    let read_model = runtime.admin_read_model();

    let kv = analyze_kv(&read_model.kv_transactions(None), now);
    let stream = analyze_stream(
        &read_model.streams(None),
        runtime.stream_request_latency_buckets(),
        now,
    );
    let notice = analyze_notice(
        &read_model.notice_subscriptions(None, None),
        &read_model.notice_routes(None),
        now,
    );
    let queue = analyze_queue(
        &read_model.queues(None),
        &read_model.queue_inflight(None),
        &read_model.queue_dead_letters(None),
        runtime.queue_dead_letter_transitions_total(),
        runtime.queue_complete_rejected_total(),
        now,
    );
    let rpc = analyze_rpc(
        &read_model.rpc_workers(None),
        &read_model.rpc_pending(None),
        runtime.rpc_request_timeouts_total(),
        runtime.rpc_backpressure_rejects_total(),
        runtime.rpc_duplicate_correlation_rejects_total(),
        runtime.rpc_wrong_worker_rejects_total(),
        runtime.rpc_responses_dropped_closed_caller_total(),
        runtime.rpc_responses_missing_pending_total(),
        runtime.rpc_acks_rejected_wrong_worker_total(),
        now,
    );
    let lease = analyze_lease(&read_model.leases(None), now);
    let schedule = analyze_schedule(
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
    );

    let mut all_hotspots = Vec::new();
    all_hotspots.extend(kv.hotspots.iter().cloned());
    all_hotspots.extend(stream.hotspots.iter().cloned());
    all_hotspots.extend(notice.hotspots.iter().cloned());
    all_hotspots.extend(queue.hotspots.iter().cloned());
    all_hotspots.extend(rpc.hotspots.iter().cloned());
    all_hotspots.extend(lease.hotspots.iter().cloned());
    all_hotspots.extend(schedule.hotspots.iter().cloned());
    if let Some(router_hotspot) = broker_router_hotspot(
        runtime.router_backpressure_total(),
        runtime.router_high_lane_backpressure_total(),
    ) {
        all_hotspots.push(router_hotspot);
    }

    all_hotspots.sort_by(compare_scored_hotspots);
    all_hotspots.truncate(5);

    let top_bottleneck = all_hotspots
        .first()
        .map(|candidate| candidate.hotspot.clone());
    let last_significant_transition_at = all_hotspots
        .iter()
        .filter_map(|candidate| candidate.last_changed_at)
        .max()
        .or_else(|| {
            [
                kv.last_changed_at,
                stream.last_changed_at,
                notice.last_changed_at,
                queue.last_changed_at,
                rpc.last_changed_at,
                lease.last_changed_at,
                schedule.last_changed_at,
            ]
            .into_iter()
            .flatten()
            .max()
        });

    let incident_summary = summarize_incident(&top_bottleneck);

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
        kv: kv.diagnostics,
        stream: stream.diagnostics,
        notice: notice.diagnostics,
        queue: queue.diagnostics,
        rpc: rpc.diagnostics,
        lease: lease.diagnostics,
        schedule: schedule.diagnostics,
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

    let snapshot = DiagnosticSnapshot::with_stage(
        DiagnosisLabel::Throughput,
        DiagnosticTrend::Growing,
        severity,
        Some(likely_bottleneck.clone()),
        None,
        None,
        None,
        None,
        0,
        0,
        0,
        0,
        hints,
    );

    Some(ScoredHotspot {
        score: router_backpressure_total as f64 * 3.0
            + router_high_lane_backpressure_total as f64 * 12.0,
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

pub(crate) fn summarize_incident(top_bottleneck: &Option<DiagnosticHotspot>) -> IncidentSummary {
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
