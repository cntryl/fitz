use super::model::DiagnosticSnapshotInput;
use super::{
    analyze_lease, analyze_notice, analyze_queue, analyze_rpc, analyze_schedule, analyze_stream,
    kv_resource_diagnostics, lease_resource_diagnostics, queue_resource_diagnostics,
    rpc_operation_diagnostics, schedule_resource_diagnostics, summarize_incident,
    summarize_rpc_worker_latency, DiagnosisLabel, DiagnosticHotspot, DiagnosticSeverity,
    DiagnosticSnapshot, DiagnosticTrend, IncidentStatus,
};
use crate::api::admin::list::{
    LeaseInfo, NoticeRouteInfo, NoticeSubscription, QueueDeadLetter, QueueInfo, RpcWorker,
    ScheduleInfo, ScheduleLatencyBuckets, StreamInfo, StreamLatencyBuckets,
};
use crate::api::admin::QueueAgeBuckets;
use chrono::{Duration, Utc};

#[test]
fn should_mark_empty_snapshot_healthy() {
    // Arrange
    let snapshot = DiagnosticSnapshot::healthy();

    // Act
    let diagnosis_label = snapshot.diagnosis_label();

    // Assert
    assert!(snapshot.is_healthy());
    assert_eq!(snapshot.current_stage, "healthy");
    assert_eq!(diagnosis_label, DiagnosisLabel::Healthy);
}

#[test]
fn should_derive_confidence_from_supporting_signals() {
    // Arrange
    let snapshot = queue_resource_diagnostics(4, 2, 1, 0, 45, QueueAgeBuckets::default());

    // Act
    let justification = snapshot
        .confidence_justification
        .as_ref()
        .expect("confidence justification");

    // Assert
    assert!(snapshot.confidence >= 0.85);
    assert!(justification
        .signals_matched
        .iter()
        .any(|signal| signal == "backlog_age_or_waiters_present"));
    assert!(justification
        .signals_matched
        .iter()
        .any(|signal| signal == "fresh_telemetry"));
    assert!(justification
        .signals_matched
        .iter()
        .any(|signal| signal == "bottleneck_identified"));
}

#[test]
fn should_mark_missing_freshness_when_signal_context_is_sparse() {
    // Arrange
    let snapshot = kv_resource_diagnostics(2);

    // Act
    let justification = snapshot
        .confidence_justification
        .as_ref()
        .expect("confidence justification");

    // Assert
    assert!(snapshot.confidence < 0.85);
    assert!(justification
        .signals_missing
        .iter()
        .any(|signal| signal == "fresh_telemetry"));
}

#[test]
fn should_prioritize_current_snapshot_for_queue_backlog_growth() {
    // Arrange
    let hotspot = DiagnosticHotspot {
        domain: "queue".to_string(),
        realm: Some("prod".to_string()),
        area: Some("jobs".to_string()),
        resource: Some("worker".to_string()),
        operation: None,
        family: Some(1),
        backlog: Some(6),
        inflight: Some(2),
        ready: Some(4),
        delayed: Some(2),
        dead_letters: Some(0),
        workers: None,
        subscriptions: None,
        owner_session: None,
        worker_session: None,
        snapshot: queue_resource_diagnostics(4, 2, 1, 0, 45, QueueAgeBuckets::default()),
    };

    // Act
    let suggestions = hotspot.suggested_queries();

    // Assert
    assert_eq!(suggestions.len(), 2);
    assert_eq!(suggestions[0].priority, 1);
    assert_eq!(suggestions[0].title, "Inspect current resource snapshot");
    assert!(suggestions[0]
        .endpoint
        .contains("/api/v1/queue/realms/prod/areas/jobs/resources/worker?family=1"));
    assert!(suggestions[0].remediation.contains("backlog"));
    assert_eq!(suggestions[1].priority, 2);
    assert_eq!(suggestions[1].title, "Inspect recent transitions");
    assert!(suggestions[1].endpoint.contains("/events?family=1"));
}

#[test]
fn should_round_trip_canonical_diagnosis_labels() {
    // Arrange
    let data_loss_stage = "data_loss_risk";
    let backlog_stage = "backlog_growth";

    // Act
    let data_loss_label = DiagnosisLabel::from_stage(data_loss_stage);
    let backlog_label = DiagnosisLabel::from_stage(backlog_stage);

    // Assert
    assert_eq!(data_loss_label, Some(DiagnosisLabel::DataLossRisk));
    assert_eq!(backlog_label, Some(DiagnosisLabel::BacklogGrowth));
}

#[test]
fn should_classify_queue_backlog_growth() {
    // Arrange
    let snapshot = queue_resource_diagnostics(4, 2, 1, 0, 45, QueueAgeBuckets::default());

    // Act
    let diagnosis_label = snapshot.diagnosis_label();

    // Assert
    assert_eq!(snapshot.current_stage, "backlog_growth");
    assert_eq!(diagnosis_label, DiagnosisLabel::BacklogGrowth);
    assert_eq!(snapshot.severity, DiagnosticSeverity::Medium);
    assert!(snapshot
        .explanation_hints
        .iter()
        .any(|hint| hint.contains("Backlog is growing")));
}

#[test]
fn should_classify_queue_dead_letter_pressure_with_transition_counts() {
    // Arrange
    let now = Utc::now();
    let queues = vec![QueueInfo {
        family: 1,
        realm: "prod".to_string(),
        area: "jobs".to_string(),
        resource: "worker".to_string(),
        subscriptions_active: 0,
        messages_ready: 0,
        messages_delayed: 0,
        messages_inflight: 0,
        messages_dead_lettered: 2,
        messages_total: 2,
        oldest_message_age_seconds: 0,
        oldest_backlog_age_seconds: 0,
        backlog_age_buckets: QueueAgeBuckets::default(),
        delay_age_buckets: QueueAgeBuckets::default(),
        enqueue_success_total: 0,
        complete_success_total: 0,
        in_rate_per_second: 0.0,
        out_rate_per_second: 0.0,
        status: "backlogged".to_string(),
    }];
    let dead_letters = vec![QueueDeadLetter {
        message_id: 42,
        family: 1,
        realm: "prod".to_string(),
        area: "jobs".to_string(),
        resource: "worker".to_string(),
        dead_lettered_at: (now - Duration::seconds(15)).to_rfc3339(),
        attempts: 3,
        reason: "max_attempts_exceeded".to_string(),
    }];

    // Act
    let analysis = analyze_queue(&queues, &[], &dead_letters, 7, 2, now);
    let hotspot = analysis.hotspots.first().expect("queue hotspot");

    // Assert
    assert_eq!(
        hotspot.hotspot.snapshot.current_stage,
        "dead_letter_pressure"
    );
    assert_eq!(
        hotspot.hotspot.snapshot.diagnosis_label(),
        DiagnosisLabel::DeadLetterPressure
    );
    assert!(hotspot
        .hotspot
        .snapshot
        .explanation_hints
        .iter()
        .any(|hint| hint.contains("dead-letter transition")));
    assert!(hotspot
        .hotspot
        .snapshot
        .explanation_hints
        .iter()
        .any(|hint| hint.contains("queue complete rejection")));
}

#[test]
fn should_not_classify_stream_latency_as_pressure_when_watermarks_are_caught_up() {
    // Arrange
    let streams = vec![StreamInfo {
        route_family: 1,
        realm: "prod".to_string(),
        area: "events".to_string(),
        resource: "orders".to_string(),
        offset: 1223,
        watermark: 1223,
        size_bytes: 8192,
        sessions_active: 0,
    }];
    let latency_buckets = StreamLatencyBuckets {
        under_100ms: 1650,
        under_500ms: 810,
        ..StreamLatencyBuckets::default()
    };

    // Act
    let analysis = analyze_stream(&streams, latency_buckets, Utc::now());

    // Assert
    assert!(analysis.hotspots.is_empty());
    assert!(analysis.diagnostics.snapshot.is_healthy());
    assert_eq!(
        analysis.diagnostics.snapshot.severity,
        DiagnosticSeverity::Informational
    );
    assert_eq!(analysis.diagnostics.snapshot.likely_bottleneck, None);
}

#[test]
fn should_classify_rpc_worker_starvation() {
    // Arrange
    let snapshot = rpc_operation_diagnostics(0, 3, None);

    // Act
    let diagnosis_label = snapshot.diagnosis_label();

    // Assert
    assert_eq!(snapshot.current_stage, "worker_starvation");
    assert_eq!(diagnosis_label, DiagnosisLabel::WorkerStarvation);
    assert_eq!(snapshot.severity, DiagnosticSeverity::High);
}

#[test]
fn should_summarize_rpc_worker_latency_buckets() {
    // Arrange
    let workers = [
        RpcWorker {
            route_family: 1,
            session_id: "9001".to_string(),
            realm: "prod".to_string(),
            route: "rpc://prod/api/users/get".to_string(),
            registered_at: "2026-03-14T12:00:00Z".to_string(),
            requests_handled: 12,
            average_latency_ms: 4.5,
        },
        RpcWorker {
            route_family: 1,
            session_id: "9002".to_string(),
            realm: "prod".to_string(),
            route: "rpc://prod/api/users/get".to_string(),
            registered_at: "2026-03-14T12:00:01Z".to_string(),
            requests_handled: 13,
            average_latency_ms: 22.0,
        },
        RpcWorker {
            route_family: 1,
            session_id: "9003".to_string(),
            realm: "prod".to_string(),
            route: "rpc://prod/api/users/get".to_string(),
            registered_at: "2026-03-14T12:00:02Z".to_string(),
            requests_handled: 14,
            average_latency_ms: 63.0,
        },
        RpcWorker {
            route_family: 1,
            session_id: "9004".to_string(),
            realm: "prod".to_string(),
            route: "rpc://prod/api/users/get".to_string(),
            registered_at: "2026-03-14T12:00:03Z".to_string(),
            requests_handled: 15,
            average_latency_ms: 125.0,
        },
    ];

    // Act
    let summary = summarize_rpc_worker_latency(workers.iter());

    // Assert
    assert!((summary.slowest_worker_average_latency_ms - 125.0).abs() < f64::EPSILON);
    assert_eq!(summary.worker_latency_buckets.under_5ms, 1);
    assert_eq!(summary.worker_latency_buckets.under_25ms, 1);
    assert_eq!(summary.worker_latency_buckets.under_100ms, 1);
    assert_eq!(summary.worker_latency_buckets.over_100ms, 1);
}

#[test]
fn should_classify_rpc_worker_latency_pressure() {
    // Arrange
    let snapshot = rpc_operation_diagnostics(3, 0, Some(180.0));

    // Act
    let diagnosis_label = snapshot.diagnosis_label();

    // Assert
    assert_eq!(snapshot.current_stage, "throughput");
    assert_eq!(diagnosis_label, DiagnosisLabel::Throughput);
    assert_eq!(snapshot.severity, DiagnosticSeverity::Medium);
    assert_eq!(
        snapshot.likely_bottleneck.as_deref(),
        Some("slow worker latency")
    );
    assert!(snapshot
        .explanation_hints
        .iter()
        .any(|hint| hint.contains("slowest worker average latency is 180.0ms")));
}

#[test]
fn should_classify_rpc_data_loss_risk() {
    // Arrange
    let now = Utc::now();

    // Act
    let analysis = analyze_rpc(&[], &[], 0, 0, 0, 0, 3, 2, now);
    let hotspot = analysis.hotspots.first().expect("rpc hotspot");

    // Assert
    assert_eq!(hotspot.hotspot.snapshot.current_stage, "data_loss_risk");
    assert_eq!(
        hotspot.hotspot.snapshot.diagnosis_label(),
        DiagnosisLabel::DataLossRisk
    );
    assert_eq!(
        hotspot.hotspot.snapshot.likely_bottleneck.as_deref(),
        Some("missing pending response")
    );
    assert_eq!(hotspot.hotspot.snapshot.severity, DiagnosticSeverity::High);
    assert!(hotspot
        .hotspot
        .snapshot
        .explanation_hints
        .iter()
        .any(|hint| hint.contains("no pending request state")));
}

#[test]
fn should_not_classify_expected_closed_caller_responses_as_data_loss() {
    // Arrange
    let now = Utc::now();

    // Act
    let analysis = analyze_rpc(&[], &[], 0, 0, 0, 0, 3, 0, now);

    // Assert
    assert!(analysis.hotspots.is_empty());
}

#[test]
fn should_classify_lease_contention() {
    // Arrange
    let snapshot = lease_resource_diagnostics(1, Some(120), 0);

    // Act
    let diagnosis_label = snapshot.diagnosis_label();

    // Assert
    assert_eq!(snapshot.current_stage, "contention");
    assert_eq!(diagnosis_label, DiagnosisLabel::Contention);
    assert_eq!(snapshot.severity, DiagnosticSeverity::Low);
}

#[test]
fn should_classify_lease_churn() {
    // Arrange
    let snapshot = lease_resource_diagnostics(1, Some(120), 3);

    // Act
    let diagnosis_label = snapshot.diagnosis_label();

    // Assert
    assert_eq!(snapshot.current_stage, "contention");
    assert_eq!(diagnosis_label, DiagnosisLabel::Contention);
    assert_eq!(snapshot.severity, DiagnosticSeverity::Medium);
    assert_eq!(
        snapshot.likely_bottleneck.as_deref(),
        Some("lease ownership churn")
    );
    assert_eq!(snapshot.trend, DiagnosticTrend::Growing);
    assert!(snapshot
        .explanation_hints
        .iter()
        .any(|hint| hint.contains("renewals recorded")));
}

#[test]
fn should_rank_lease_hotspot_as_churn_when_renewals_present() {
    // Arrange
    let now = Utc::now();
    let leases = vec![LeaseInfo {
        route_family: 1,
        realm: "prod".to_string(),
        area: "locks".to_string(),
        resource: "cache".to_string(),
        owner_session_id: "session-1".to_string(),
        acquired_at: (now - Duration::seconds(40)).to_rfc3339(),
        expires_at: (now + Duration::seconds(50)).to_rfc3339(),
        renewals: 4,
        fencing_token: 11,
    }];

    // Act
    let analysis = analyze_lease(&leases, now);
    let hotspot = analysis.hotspots.first().expect("lease hotspot");

    // Assert
    assert_eq!(analysis.diagnostics.snapshot.current_stage, "contention");
    assert_eq!(
        hotspot.hotspot.snapshot.likely_bottleneck.as_deref(),
        Some("lease ownership churn")
    );
    assert_eq!(hotspot.hotspot.snapshot.trend, DiagnosticTrend::Growing);
    assert_eq!(
        hotspot.hotspot.snapshot.severity,
        DiagnosticSeverity::Medium
    );
}

#[test]
fn should_classify_schedule_stale_handoff() {
    // Arrange
    let now = Utc::now();
    let snapshot = schedule_resource_diagnostics(
        true,
        Some(&(now - Duration::seconds(45)).to_rfc3339()),
        Some(&(now - Duration::seconds(90)).to_rfc3339()),
        4,
    );

    // Act
    let diagnosis_label = snapshot.diagnosis_label();

    // Assert
    assert_eq!(snapshot.current_stage, "stale_handoff");
    assert_eq!(diagnosis_label, DiagnosisLabel::StaleHandoff);
    assert_eq!(snapshot.severity, DiagnosticSeverity::High);
}

#[test]
fn should_classify_notice_route_concentration() {
    // Arrange
    let now = Utc::now();
    let subscriptions = vec![
        NoticeSubscription {
            route_family: 1,
            subscription_id: 1,
            session_id: "sub-1".to_string(),
            realm: "prod".to_string(),
            pattern: "notice://prod/events/orders/created".to_string(),
            created_at: (now - Duration::seconds(5)).to_rfc3339(),
            notifications_received: 3,
        },
        NoticeSubscription {
            route_family: 1,
            subscription_id: 2,
            session_id: "sub-2".to_string(),
            realm: "prod".to_string(),
            pattern: "notice://prod/events/orders/created".to_string(),
            created_at: (now - Duration::seconds(3)).to_rfc3339(),
            notifications_received: 1,
        },
        NoticeSubscription {
            route_family: 1,
            subscription_id: 3,
            session_id: "sub-3".to_string(),
            realm: "prod".to_string(),
            pattern: "notice://prod/events/orders/updated".to_string(),
            created_at: (now - Duration::seconds(2)).to_rfc3339(),
            notifications_received: 0,
        },
    ];
    let routes = vec![
        NoticeRouteInfo {
            route_family: 1,
            route: "notice://prod/events/orders/created".to_string(),
            subscribers: 2,
            publishes_total: 0,
            publishes_per_minute: 0.0,
        },
        NoticeRouteInfo {
            route_family: 1,
            route: "notice://prod/events/orders/updated".to_string(),
            subscribers: 1,
            publishes_total: 0,
            publishes_per_minute: 0.0,
        },
    ];

    // Act
    let analysis = analyze_notice(&subscriptions, &routes, now);

    // Assert
    assert_eq!(analysis.diagnostics.snapshot.current_stage, "throughput");
    assert_eq!(
        analysis.diagnostics.snapshot.likely_bottleneck.as_deref(),
        Some("route concentration")
    );
    assert_eq!(
        analysis.diagnostics.snapshot.severity,
        DiagnosticSeverity::Medium
    );
    assert_eq!(analysis.hotspots.len(), 1);
}

#[test]
fn should_rank_schedule_with_pending_ack_retry_pressure() {
    // Arrange
    let now = Utc::now();
    let schedules = vec![ScheduleInfo {
        route_family: 1,
        realm: "prod".to_string(),
        area: "jobs".to_string(),
        resource: "billing".to_string(),
        operation: "send".to_string(),
        cron: "0 * * * *".to_string(),
        delivery_mode: crate::domains::schedule::ScheduleDeliveryMode::Broadcast,
        next_run: (now - Duration::seconds(45)).to_rfc3339(),
        last_run: Some((now - Duration::seconds(90)).to_rfc3339()),
        executions_total: 7,
        enabled: true,
    }];

    // Act
    let analysis = analyze_schedule(
        &schedules,
        2,
        1,
        45,
        ScheduleLatencyBuckets::default(),
        0,
        1,
        0,
        now,
    );
    let hotspot = analysis.hotspots.first().expect("schedule hotspot");

    // Assert
    assert_eq!(hotspot.hotspot.snapshot.current_stage, "stale_handoff");
    assert_eq!(
        hotspot.hotspot.snapshot.diagnosis_label(),
        DiagnosisLabel::StaleHandoff
    );
    assert_eq!(hotspot.hotspot.backlog, Some(3));
    assert_eq!(hotspot.hotspot.snapshot.age_seconds, Some(45));
    assert!(hotspot
        .hotspot
        .snapshot
        .explanation_hints
        .iter()
        .any(|hint| hint.contains("pending ack retry")));
}

#[test]
fn should_classify_schedule_latency_pressure() {
    // Arrange
    let now = Utc::now();
    let schedules = vec![ScheduleInfo {
        route_family: 1,
        realm: "prod".to_string(),
        area: "jobs".to_string(),
        resource: "billing".to_string(),
        operation: "send".to_string(),
        cron: "0 * * * *".to_string(),
        delivery_mode: crate::domains::schedule::ScheduleDeliveryMode::Broadcast,
        next_run: (now + Duration::seconds(30)).to_rfc3339(),
        last_run: Some((now - Duration::seconds(60)).to_rfc3339()),
        executions_total: 7,
        enabled: true,
    }];
    let request_latency_buckets = ScheduleLatencyBuckets {
        under_1ms: 10,
        under_500ms: 40,
        ..Default::default()
    };

    // Act
    let analysis = analyze_schedule(&schedules, 0, 0, 0, request_latency_buckets, 0, 0, 0, now);
    let hotspot = analysis.hotspots.first().expect("schedule hotspot");

    // Assert
    assert_eq!(hotspot.hotspot.snapshot.current_stage, "throughput");
    assert_eq!(
        hotspot.hotspot.snapshot.likely_bottleneck.as_deref(),
        Some("schedule latency")
    );
    assert_eq!(hotspot.hotspot.snapshot.severity, DiagnosticSeverity::High);
    assert!(hotspot
        .hotspot
        .snapshot
        .explanation_hints
        .iter()
        .any(|hint| hint.contains("schedule request latency tail")));
}

#[test]
fn should_summarize_incident_given_hotspot() {
    // Arrange
    let hotspot = DiagnosticHotspot {
        domain: "queue".to_string(),
        realm: Some("prod".to_string()),
        area: Some("jobs".to_string()),
        resource: Some("worker".to_string()),
        operation: None,
        family: Some(1),
        backlog: Some(6),
        inflight: Some(2),
        ready: Some(4),
        delayed: Some(2),
        dead_letters: Some(0),
        workers: None,
        subscriptions: None,
        owner_session: None,
        worker_session: None,
        snapshot: queue_resource_diagnostics(4, 2, 1, 0, 45, QueueAgeBuckets::default()),
    };

    // Act
    let summary = summarize_incident(Some(&hotspot));

    // Assert
    assert_eq!(summary.status, IncidentStatus::Degraded);
    assert!(summary.title.contains("backlog growth"));
    assert!(summary.explanation.contains("Backlog is growing"));
    assert_eq!(
        summary.recommended_next_query.as_deref(),
        Some("inspect /api/v1/queue/realms/prod/areas/jobs/resources/worker?family=1")
    );
}

#[test]
fn should_summarize_incident_given_broker_hotspot() {
    // Arrange
    let hotspot = DiagnosticHotspot {
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
        snapshot: DiagnosticSnapshot::with_stage(DiagnosticSnapshotInput {
            current_stage: DiagnosisLabel::Throughput,
            trend: DiagnosticTrend::Growing,
            severity: DiagnosticSeverity::Medium,
            likely_bottleneck: Some("router saturation".to_string()),
            last_changed_at: None,
            last_success_at: None,
            last_failure_at: None,
            age_seconds: None,
            recent_transition_count: 0,
            failure_count: 0,
            contention_count: 0,
            waiter_count: 0,
            explanation_hints: vec!["5 router mailbox saturation event(s)".to_string()],
        }),
    };

    // Act
    let summary = summarize_incident(Some(&hotspot));

    // Assert
    assert_eq!(summary.status, IncidentStatus::Degraded);
    assert!(summary.title.contains("broker scope"));
    assert_eq!(
        summary.likely_bottleneck.as_deref(),
        Some("router saturation")
    );
    assert_eq!(
        summary.recommended_next_query.as_deref(),
        Some("inspect /api/v1/stats")
    );
    assert_eq!(summary.suggested_next_queries.len(), 2);
    assert_eq!(
        summary.suggested_next_queries[1].endpoint,
        "inspect /api/v1/all/metrics"
    );
}

#[test]
fn should_report_rpc_response_loss_as_ephemeral_not_durable() {
    // Arrange
    // RPC holds no durable state: a lost response is in-flight work dropped
    // under transport backpressure. Calling it a "durability gap" and warning
    // about "durable-state loss" sends an operator hunting for storage
    // corruption that cannot exist.
    let label = DiagnosisLabel::DataLossRisk;

    // Act
    let hints = crate::api::admin::troubleshooting::model::canonical_explanation_hints(
        label,
        vec![super::analysis_queue_rpc::RPC_RESPONSE_LOSS_HINT.to_string()],
    );

    // Assert
    let joined = hints.join(" | ");
    assert!(
        hints
            .iter()
            .any(|hint| hint.contains("Ephemeral RPC response loss")),
        "expected an ephemeral RPC explanation, got {joined}"
    );
    assert!(
        !joined.contains("durable-state loss"),
        "must not claim durable-state loss for an ephemeral domain: {joined}"
    );
}

#[test]
fn should_classify_rpc_transport_backpressure() {
    // Arrange
    // request_timeouts_total + backpressure_rejects_total was computed as
    // `transport_pressure` and then used only in a hint string, so a broker
    // shedding load under transport saturation still reported healthy with a
    // zero failure count.
    let now = Utc::now();

    // Act
    let analysis = analyze_rpc(&[], &[], 4, 7, 0, 0, 0, 0, now);
    let hotspot = analysis.hotspots.first().expect("rpc hotspot");

    // Assert
    assert_eq!(
        hotspot.hotspot.snapshot.diagnosis_label(),
        DiagnosisLabel::TransportBackpressure,
        "transport pressure must drive the diagnosis, not just a hint"
    );
    assert_eq!(
        hotspot.hotspot.snapshot.failure_count, 11,
        "timeouts and backpressure rejections must both be counted"
    );
    assert_ne!(
        hotspot.hotspot.snapshot.severity,
        DiagnosticSeverity::Informational
    );
}
