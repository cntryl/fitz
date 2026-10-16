use super::{
    bounded_incident_summary, build_bounded_runtime_diagnostics,
    build_runtime_diagnostics_with_schedule, summarize_incident, IncidentStatus,
    ScheduleDiagnosticCounters,
};
use crate::boot::Runtime;
use crate::control::admin::read_model::AdminSnapshot;
use crate::control::admin::ScheduleInfo;
use crate::runtime::Router;
use std::sync::Arc;

#[test]
fn should_report_unknown_health_from_absence_in_a_bounded_cached_projection() {
    // Arrange
    let summary = summarize_incident(None);

    // Act
    let summary = bounded_incident_summary(summary);

    // Assert
    assert_eq!(summary.status, IncidentStatus::Unknown);
    assert!(summary.confidence <= 0.5);
    assert!(summary.explanation.contains("cached"));
}

#[test]
fn should_preserve_observed_pressure_in_a_bounded_incident_summary() {
    // Arrange
    let mut summary = summarize_incident(None);
    summary.status = IncidentStatus::Degraded;
    summary.confidence = 0.8;
    summary.explanation = "observed backlog pressure".into();

    // Act
    let summary = bounded_incident_summary(summary);

    // Assert
    assert_eq!(summary.status, IncidentStatus::Degraded);
    assert!((summary.confidence - 0.8).abs() < f64::EPSILON);
    assert_eq!(summary.explanation, "observed backlog pressure");
}

#[test]
fn should_use_explicit_live_schedule_inputs_without_attributing_cached_totals() {
    // Arrange
    let runtime = Runtime::new(Arc::new(Router::new()));
    let snapshot = AdminSnapshot {
        schedules: vec![ScheduleInfo::enabled_snapshot(
            1,
            "acme".into(),
            "jobs".into(),
            "alpha".into(),
            "run".into(),
            "0 * * * *".into(),
            "2999-01-01T00:00:00Z",
        )],
        pending_fire_claims: 99,
        ..AdminSnapshot::default()
    };
    let counters = ScheduleDiagnosticCounters {
        pending_fire_claims: 7,
        ..ScheduleDiagnosticCounters::default()
    };

    // Act
    let live = build_runtime_diagnostics_with_schedule(&runtime, &snapshot, counters);
    let bounded = build_bounded_runtime_diagnostics(&runtime, &snapshot);

    // Assert
    let schedule_pressure = |value: &super::RuntimeDiagnostics| {
        value
            .global
            .hotspots
            .iter()
            .find(|hotspot| hotspot.domain == "schedule")
            .unwrap()
            .backlog
    };
    assert_eq!(schedule_pressure(&live), Some(7));
    assert_eq!(schedule_pressure(&bounded), Some(0));
}
