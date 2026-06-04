use crate::api::admin::list::{
    KvTransaction, LeaseInfo, NoticeRouteInfo, NoticeSubscription, QueueAgeBuckets,
    QueueDeadLetter, QueueInflight, QueueInfo, RpcLatencyBuckets, RpcPendingRequest, RpcWorker,
    ScheduleInfo, ScheduleLatencyBuckets, StreamInfo, StreamLatencyBuckets,
};
use crate::api::admin::ResourcePath;
use crate::boot::Runtime;
use crate::runtime::routing::{route_quad, route_triplet};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap};

const RECENT_WINDOW_SECS: i64 = 300;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticTrend {
    Growing,
    Shrinking,
    Steady,
    Stalled,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticSeverity {
    Informational,
    Low,
    Medium,
    High,
    Critical,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IncidentStatus {
    Healthy,
    Degraded,
    Stalled,
    Recovering,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiagnosisLabel {
    Healthy,
    Throughput,
    Contention,
    BacklogGrowth,
    StaleHandoff,
    DeadLetterPressure,
    WorkerStarvation,
    DataLossRisk,
}

impl DiagnosisLabel {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Throughput => "throughput",
            Self::Contention => "contention",
            Self::BacklogGrowth => "backlog_growth",
            Self::StaleHandoff => "stale_handoff",
            Self::DeadLetterPressure => "dead_letter_pressure",
            Self::WorkerStarvation => "worker_starvation",
            Self::DataLossRisk => "data_loss_risk",
        }
    }

    const fn display_name(self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Throughput => "throughput",
            Self::Contention => "contention",
            Self::BacklogGrowth => "backlog growth",
            Self::StaleHandoff => "stale handoff",
            Self::DeadLetterPressure => "dead-letter pressure",
            Self::WorkerStarvation => "worker starvation",
            Self::DataLossRisk => "data-loss risk",
        }
    }

    const fn explanation_hint(self) -> &'static str {
        match self {
            Self::Healthy => "No active pressure detected",
            Self::Throughput => "Work is moving, but the surface is lagging demand",
            Self::Contention => "Multiple actors are contending for the same resource",
            Self::BacklogGrowth => "Backlog is growing faster than it drains",
            Self::StaleHandoff => "A durable handoff is overdue",
            Self::DeadLetterPressure => "Dead letters are accumulating",
            Self::WorkerStarvation => "Work is waiting for workers or owners",
            Self::DataLossRisk => "The control plane sees a durability gap",
        }
    }

    const fn durability_hint(self) -> Option<&'static str> {
        match self {
            Self::Healthy => None,
            Self::Throughput => Some("Mostly live activity with a durable backlog overlay"),
            Self::Contention => {
                Some("Usually live coordination state; durable state may still be intact")
            }
            Self::BacklogGrowth => Some("Durable backlog with live processing lag"),
            Self::StaleHandoff => Some("Durable ownership or schedule state with live lateness"),
            Self::DeadLetterPressure => Some("Durable failure state plus live retry pressure"),
            Self::WorkerStarvation => Some("Mostly live capacity pressure"),
            Self::DataLossRisk => Some("Potential durable-state loss; treat this as critical"),
        }
    }

    fn from_stage(stage: &str) -> Option<Self> {
        Some(match stage {
            "healthy" => Self::Healthy,
            "throughput" => Self::Throughput,
            "contention" => Self::Contention,
            "backlog_growth" => Self::BacklogGrowth,
            "stale_handoff" => Self::StaleHandoff,
            "dead_letter_pressure" => Self::DeadLetterPressure,
            "worker_starvation" => Self::WorkerStarvation,
            "data_loss_risk" => Self::DataLossRisk,
            _ => return None,
        })
    }
}

fn canonical_explanation_hints(
    label: DiagnosisLabel,
    explanation_hints: Vec<String>,
) -> Vec<String> {
    let mut hints = Vec::with_capacity(2 + explanation_hints.len());
    hints.push(label.explanation_hint().to_string());
    if let Some(durability_hint) = label.durability_hint() {
        hints.push(durability_hint.to_string());
    }
    for hint in explanation_hints {
        if !hints.iter().any(|existing| existing == &hint) {
            hints.push(hint);
        }
    }
    hints
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfidenceJustification {
    pub signals_matched: Vec<String>,
    pub signals_missing: Vec<String>,
    pub rationale: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SuggestedQuery {
    pub priority: u8,
    pub title: String,
    pub endpoint: String,
    pub rationale: String,
    pub remediation: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiagnosticSnapshot {
    pub current_stage: String,
    pub trend: DiagnosticTrend,
    pub severity: DiagnosticSeverity,
    pub likely_bottleneck: Option<String>,
    pub last_changed_at: Option<String>,
    pub last_success_at: Option<String>,
    pub last_failure_at: Option<String>,
    pub age_seconds: Option<u64>,
    pub recent_transition_count: u64,
    pub failure_count: u64,
    pub contention_count: u64,
    pub waiter_count: usize,
    pub confidence: f64,
    pub confidence_justification: Option<ConfidenceJustification>,
    pub explanation_hints: Vec<String>,
    pub delta_5m: Option<i64>,
    pub delta_1h: Option<i64>,
}

impl DiagnosticSnapshot {
    fn healthy() -> Self {
        Self {
            current_stage: DiagnosisLabel::Healthy.as_str().to_string(),
            trend: DiagnosticTrend::Steady,
            severity: DiagnosticSeverity::Informational,
            likely_bottleneck: None,
            last_changed_at: None,
            last_success_at: None,
            last_failure_at: None,
            age_seconds: None,
            recent_transition_count: 0,
            failure_count: 0,
            contention_count: 0,
            waiter_count: 0,
            confidence: 1.0,
            confidence_justification: Some(ConfidenceJustification {
                signals_matched: vec!["no_active_pressure".to_string()],
                signals_missing: Vec::new(),
                rationale: "Healthy snapshots default to full confidence because no backlog, contention, or failure signal is active.".to_string(),
            }),
            explanation_hints: canonical_explanation_hints(DiagnosisLabel::Healthy, Vec::new()),
            delta_5m: None,
            delta_1h: None,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn with_stage(
        current_stage: DiagnosisLabel,
        trend: DiagnosticTrend,
        severity: DiagnosticSeverity,
        likely_bottleneck: Option<String>,
        last_changed_at: Option<DateTime<Utc>>,
        last_success_at: Option<DateTime<Utc>>,
        last_failure_at: Option<DateTime<Utc>>,
        age_seconds: Option<u64>,
        recent_transition_count: u64,
        failure_count: u64,
        contention_count: u64,
        waiter_count: usize,
        explanation_hints: Vec<String>,
    ) -> Self {
        let (confidence, confidence_justification) = calculate_confidence(
            current_stage,
            likely_bottleneck.as_deref(),
            last_changed_at,
            last_success_at,
            last_failure_at,
            age_seconds,
            recent_transition_count,
            failure_count,
            contention_count,
            waiter_count,
        );

        Self {
            current_stage: current_stage.as_str().to_string(),
            trend,
            severity,
            likely_bottleneck,
            last_changed_at: last_changed_at.map(rfc3339),
            last_success_at: last_success_at.map(rfc3339),
            last_failure_at: last_failure_at.map(rfc3339),
            age_seconds,
            recent_transition_count,
            failure_count,
            contention_count,
            waiter_count,
            confidence,
            confidence_justification: Some(confidence_justification),
            explanation_hints: canonical_explanation_hints(current_stage, explanation_hints),
            delta_5m: None,
            delta_1h: None,
        }
    }

    fn diagnosis_label(&self) -> DiagnosisLabel {
        DiagnosisLabel::from_stage(&self.current_stage).unwrap_or(DiagnosisLabel::Healthy)
    }

    #[cfg(test)]
    fn is_healthy(&self) -> bool {
        matches!(self.severity, DiagnosticSeverity::Informational)
            && self.likely_bottleneck.is_none()
            && self.diagnosis_label() == DiagnosisLabel::Healthy
    }
}

#[allow(clippy::too_many_arguments)]
fn calculate_confidence(
    current_stage: DiagnosisLabel,
    likely_bottleneck: Option<&str>,
    last_changed_at: Option<DateTime<Utc>>,
    last_success_at: Option<DateTime<Utc>>,
    last_failure_at: Option<DateTime<Utc>>,
    age_seconds: Option<u64>,
    recent_transition_count: u64,
    failure_count: u64,
    contention_count: u64,
    waiter_count: usize,
) -> (f64, ConfidenceJustification) {
    let failure_signal = failure_count > 0 || last_failure_at.is_some();
    let contention_signal = contention_count > 0 || waiter_count > 0;
    let age_signal = age_seconds.unwrap_or(0) > 0;
    let transition_signal = recent_transition_count > 0 || last_changed_at.is_some();
    let bottleneck_signal = likely_bottleneck.is_some();
    let now = Utc::now();
    let freshness_signal = [last_changed_at, last_success_at, last_failure_at]
        .into_iter()
        .flatten()
        .any(|timestamp| is_recent(timestamp, now))
        || age_seconds
            .map(|age| age <= RECENT_WINDOW_SECS as u64)
            .unwrap_or(false)
        || recent_transition_count > 0;

    let (primary_signal_name, primary_signal, coverage_target) = match current_stage {
        DiagnosisLabel::Healthy => (
            "no_active_pressure",
            !failure_signal && !contention_signal && !bottleneck_signal,
            0,
        ),
        DiagnosisLabel::Contention => ("contention_or_waiters_present", contention_signal, 2),
        DiagnosisLabel::WorkerStarvation => (
            "waiters_without_capacity",
            contention_signal || age_signal,
            2,
        ),
        DiagnosisLabel::BacklogGrowth => (
            "backlog_age_or_waiters_present",
            contention_signal || age_signal,
            2,
        ),
        DiagnosisLabel::DeadLetterPressure => ("failure_signal_present", failure_signal, 2),
        DiagnosisLabel::DataLossRisk => ("failure_signal_present", failure_signal, 2),
        DiagnosisLabel::StaleHandoff => (
            "staleness_signal_present",
            age_signal || transition_signal,
            2,
        ),
        DiagnosisLabel::Throughput => (
            "throughput_pressure_present",
            age_signal || contention_signal || transition_signal || bottleneck_signal,
            1,
        ),
    };

    let observed_support = [
        failure_signal,
        contention_signal,
        age_signal,
        transition_signal,
        bottleneck_signal,
    ]
    .into_iter()
    .filter(|signal| *signal)
    .count();
    let coverage_ratio = if coverage_target == 0 {
        1.0
    } else {
        (observed_support as f64 / coverage_target as f64).min(1.0)
    };
    let coverage_signal = coverage_target == 0 || observed_support >= coverage_target;
    let freshness_score = if freshness_signal {
        1.0
    } else if transition_signal
        || last_success_at.is_some()
        || last_failure_at.is_some()
        || age_signal
    {
        0.6
    } else {
        0.35
    };

    let mut signals_matched = Vec::new();
    let mut signals_missing = Vec::new();

    if primary_signal {
        signals_matched.push(primary_signal_name.to_string());
    } else {
        signals_missing.push(primary_signal_name.to_string());
    }

    if freshness_signal {
        signals_matched.push("fresh_telemetry".to_string());
    } else {
        signals_missing.push("fresh_telemetry".to_string());
    }

    if coverage_signal {
        signals_matched.push(format!(
            "rule_coverage_{observed_support}_of_{coverage_target}"
        ));
    } else {
        signals_missing.push(format!(
            "rule_coverage_{observed_support}_of_{coverage_target}"
        ));
    }

    if bottleneck_signal {
        signals_matched.push("bottleneck_identified".to_string());
    } else if !matches!(current_stage, DiagnosisLabel::Healthy) {
        signals_missing.push("bottleneck_identified".to_string());
    }

    let confidence = if matches!(current_stage, DiagnosisLabel::Healthy) && primary_signal {
        if freshness_signal {
            0.92
        } else {
            0.82
        }
    } else {
        let primary_score = if primary_signal { 1.0 } else { 0.35 };
        let bottleneck_score = if bottleneck_signal { 1.0 } else { 0.0 };
        (0.25
            + 0.30 * primary_score
            + 0.20 * coverage_ratio
            + 0.10 * freshness_score
            + 0.05 * bottleneck_score)
            .min(0.90)
    };

    let matched_summary = if signals_matched.is_empty() {
        "none".to_string()
    } else {
        signals_matched.join(", ")
    };
    let missing_summary = if signals_missing.is_empty() {
        "none".to_string()
    } else {
        signals_missing.join(", ")
    };

    (
        confidence,
        ConfidenceJustification {
            signals_matched,
            signals_missing,
            rationale: format!(
                "{} confidence is derived from observed signals, telemetry freshness, and rule coverage. Matched: {}. Missing: {}.",
                current_stage.display_name(),
                matched_summary,
                missing_summary,
            ),
        },
    )
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DomainDiagnostics {
    #[serde(flatten)]
    pub snapshot: DiagnosticSnapshot,
}

impl DomainDiagnostics {
    fn healthy() -> Self {
        Self {
            snapshot: DiagnosticSnapshot::healthy(),
        }
    }

    fn from_snapshot(snapshot: DiagnosticSnapshot) -> Self {
        Self { snapshot }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiagnosticHotspot {
    pub domain: String,
    pub realm: Option<String>,
    pub area: Option<String>,
    pub resource: Option<String>,
    pub operation: Option<String>,
    pub family: Option<u64>,
    pub backlog: Option<usize>,
    pub inflight: Option<usize>,
    pub ready: Option<usize>,
    pub delayed: Option<usize>,
    pub dead_letters: Option<usize>,
    pub workers: Option<usize>,
    pub subscriptions: Option<usize>,
    pub owner_session: Option<String>,
    pub worker_session: Option<String>,
    #[serde(flatten)]
    pub snapshot: DiagnosticSnapshot,
}

impl DiagnosticHotspot {
    fn path(&self) -> Option<String> {
        let realm = self.realm.as_ref()?;
        let area = self.area.as_ref()?;
        let resource = self.resource.as_ref()?;
        if let Some(operation) = &self.operation {
            Some(format!("{}/{}/{}/{}", realm, area, resource, operation))
        } else {
            Some(format!("{}/{}/{}", realm, area, resource))
        }
    }

    fn events_query(&self) -> Option<String> {
        let realm = self.realm.as_ref()?;
        let area = self.area.as_ref()?;
        let resource = self.resource.as_ref()?;
        if let Some(operation) = &self.operation {
            Some(format!(
                "inspect /api/v1/{}/realms/{}/areas/{}/resources/{}/operations/{}/events",
                self.domain, realm, area, resource, operation
            ))
        } else if let Some(family) = self.family {
            Some(format!(
                "inspect /api/v1/{}/realms/{}/areas/{}/resources/{}/events?family={}",
                self.domain, realm, area, resource, family
            ))
        } else {
            Some(format!(
                "inspect /api/v1/{}/realms/{}/areas/{}/resources/{}/events",
                self.domain, realm, area, resource
            ))
        }
    }

    fn resource_query(&self) -> Option<String> {
        let realm = self.realm.as_ref()?;
        let area = self.area.as_ref()?;
        let resource = self.resource.as_ref()?;
        if self.domain == "queue" {
            self.family
                .map(|family| {
                    format!(
                        "inspect /api/v1/{}/realms/{}/areas/{}/resources/{}?family={}",
                        self.domain, realm, area, resource, family
                    )
                })
                .or_else(|| {
                    Some(format!(
                        "inspect /api/v1/{}/realms/{}/areas/{}/resources/{}",
                        self.domain, realm, area, resource
                    ))
                })
        } else {
            Some(format!(
                "inspect /api/v1/{}/realms/{}/areas/{}/resources/{}",
                self.domain, realm, area, resource
            ))
        }
    }

    fn suggested_query(
        priority: u8,
        title: &str,
        endpoint: String,
        rationale: &str,
        remediation: &str,
    ) -> SuggestedQuery {
        SuggestedQuery {
            priority,
            title: title.to_string(),
            endpoint,
            rationale: rationale.to_string(),
            remediation: remediation.to_string(),
        }
    }

    fn suggested_queries(&self) -> Vec<SuggestedQuery> {
        if self.domain == "broker" {
            return vec![
                Self::suggested_query(
                    1,
                    "Inspect broker stats",
                    "inspect /api/v1/stats".to_string(),
                    "Broker-level stats are the bounded next step for confirming whether overload is confined to router saturation or reflected in domain diagnostics.",
                    "Compare broker router counters against domain diagnostics before treating the overload as domain-local pressure.",
                ),
                Self::suggested_query(
                    2,
                    "Inspect broker metrics",
                    "inspect /metrics".to_string(),
                    "Prometheus counters are the bounded next step for checking whether saturation is accumulating in the normal lane or the control-plane high lane.",
                    "Use the metrics view to separate data-plane mailbox pressure from control-plane saturation before changing overload policy.",
                ),
            ];
        }

        let Some(events_endpoint) = self.events_query() else {
            return Vec::new();
        };

        let resource_endpoint = self.resource_query();
        let label = self.snapshot.diagnosis_label();

        let timeline_first = matches!(
            label,
            DiagnosisLabel::DeadLetterPressure
                | DiagnosisLabel::DataLossRisk
                | DiagnosisLabel::StaleHandoff
        );

        let mut queries = Vec::new();

        let push_timeline_query = |queries: &mut Vec<SuggestedQuery>, priority: u8| {
            queries.push(Self::suggested_query(
                priority,
                "Inspect recent transitions",
                events_endpoint.clone(),
                if matches!(
                    label,
                    DiagnosisLabel::DeadLetterPressure | DiagnosisLabel::DataLossRisk
                ) {
                    "Recent event timelines are the bounded next step for confirming the failure path or dead-letter trigger."
                } else if matches!(label, DiagnosisLabel::StaleHandoff) {
                    "Recent event timelines are the bounded next step for confirming the handoff or ownership change."
                } else {
                    "Recent event timelines are the bounded next step for confirming why this hotspot changed state."
                },
                if matches!(
                    label,
                    DiagnosisLabel::DeadLetterPressure | DiagnosisLabel::DataLossRisk
                ) {
                    "Use the transition history to isolate the failure reason or retry pattern before taking any follow-up action."
                } else if matches!(label, DiagnosisLabel::StaleHandoff) {
                    "Use the transition history to confirm the latest ownership flip or overdue handoff."
                } else {
                    "Use the timeline to confirm the latest state flip before widening the investigation."
                },
            ));
        };

        let push_resource_query = |queries: &mut Vec<SuggestedQuery>, priority: u8| {
            if let Some(endpoint) = resource_endpoint.clone() {
                queries.push(Self::suggested_query(
                    priority,
                    "Inspect current resource snapshot",
                    endpoint,
                    if matches!(
                        label,
                        DiagnosisLabel::WorkerStarvation
                            | DiagnosisLabel::BacklogGrowth
                            | DiagnosisLabel::Contention
                    ) {
                        "Current counters are the bounded next step for confirming active pressure, backlog, or wait depth."
                    } else {
                        "Current counters are the bounded next step for checking whether the hotspot is still active."
                    },
                    if matches!(
                        label,
                        DiagnosisLabel::WorkerStarvation
                            | DiagnosisLabel::BacklogGrowth
                            | DiagnosisLabel::Contention
                    ) {
                        "Check backlog, inflight work, waiters, and ownership before following the timeline."
                    } else {
                        "Check the current snapshot to verify the hotspot is still present before following the timeline."
                    },
                ));
            }
        };

        if timeline_first {
            push_timeline_query(&mut queries, 1);
            push_resource_query(&mut queries, 2);
        } else {
            push_resource_query(&mut queries, 1);
            push_timeline_query(&mut queries, 2);
        }

        queries
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IncidentSummary {
    pub status: IncidentStatus,
    pub title: String,
    pub likely_bottleneck: Option<String>,
    pub severity: DiagnosticSeverity,
    pub confidence: f64,
    pub explanation: String,
    pub recommended_next_query: Option<String>,
    pub suggested_next_queries: Vec<SuggestedQuery>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalTroubleshootingDiagnostics {
    pub incident_summary: IncidentSummary,
    pub top_bottleneck: Option<DiagnosticHotspot>,
    pub last_significant_transition_at: Option<String>,
    pub hotspots: Vec<DiagnosticHotspot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeDiagnostics {
    pub global: GlobalTroubleshootingDiagnostics,
    pub kv: DomainDiagnostics,
    pub stream: DomainDiagnostics,
    pub notice: DomainDiagnostics,
    pub queue: DomainDiagnostics,
    pub rpc: DomainDiagnostics,
    pub lease: DomainDiagnostics,
    pub schedule: DomainDiagnostics,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ResourceTimelineKind {
    Observation,
    Transition,
    Failure,
    Retry,
    OwnershipChange,
    StateFlip,
    Registration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceTimelineEvent {
    pub domain: String,
    pub kind: ResourceTimelineKind,
    pub observed_at: String,
    pub summary: String,
    pub realm: String,
    pub area: String,
    pub resource: String,
    pub operation: Option<String>,
    pub family: Option<u64>,
    pub age_seconds: Option<u64>,
    pub owner_session: Option<String>,
    pub worker_session: Option<String>,
    pub correlation_id: Option<String>,
    pub message_id: Option<u64>,
    pub attempts: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceTimeline {
    pub domain: String,
    pub realm: String,
    pub area: String,
    pub resource: String,
    pub family: Option<u64>,
    pub derived: bool,
    pub limit: usize,
    pub diagnostics: DiagnosticSnapshot,
    pub events: Vec<ResourceTimelineEvent>,
}

impl ResourceTimelineEvent {
    #[allow(clippy::too_many_arguments)]
    fn new(
        domain: &str,
        kind: ResourceTimelineKind,
        observed_at: DateTime<Utc>,
        summary: impl Into<String>,
        path: &ResourcePath<'_>,
        family: Option<u64>,
        operation: Option<String>,
        age_seconds: Option<u64>,
        owner_session: Option<String>,
        worker_session: Option<String>,
        correlation_id: Option<String>,
        message_id: Option<u64>,
        attempts: Option<usize>,
    ) -> Self {
        Self {
            domain: domain.to_string(),
            kind,
            observed_at: rfc3339(observed_at),
            summary: summary.into(),
            realm: path.realm.to_string(),
            area: path.area.to_string(),
            resource: path.resource.to_string(),
            operation,
            family,
            age_seconds,
            owner_session,
            worker_session,
            correlation_id,
            message_id,
            attempts,
        }
    }
}

impl ResourceTimeline {
    #[allow(clippy::too_many_arguments)]
    fn new(
        domain: &str,
        path: &ResourcePath<'_>,
        family: Option<u64>,
        diagnostics: DiagnosticSnapshot,
        limit: usize,
        events: Vec<ResourceTimelineEvent>,
    ) -> Self {
        Self {
            domain: domain.to_string(),
            realm: path.realm.to_string(),
            area: path.area.to_string(),
            resource: path.resource.to_string(),
            family,
            derived: true,
            limit,
            diagnostics,
            events,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceComparisonScope {
    pub realm: String,
    pub area: String,
    pub resource: String,
    pub family: Option<u64>,
}

impl ResourceComparisonScope {
    pub(crate) fn new(path: &ResourcePath<'_>, family: Option<u64>) -> Self {
        Self {
            realm: path.realm.to_string(),
            area: path.area.to_string(),
            resource: path.resource.to_string(),
            family,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ResourceComparisonMetrics {
    pub backlog: Option<usize>,
    pub inflight: Option<usize>,
    pub ready: Option<usize>,
    pub delayed: Option<usize>,
    pub dead_letters: Option<usize>,
    pub workers: Option<usize>,
    pub subscriptions: Option<usize>,
    pub waiters: Option<usize>,
    pub age_seconds: Option<u64>,
    pub recent_transition_count: Option<u64>,
    pub failure_count: Option<u64>,
    pub contention_count: Option<u64>,
    pub operations_total: Option<u64>,
}

impl ResourceComparisonMetrics {
    fn pressure_score(&self, diagnostics: &DiagnosticSnapshot) -> f64 {
        let severity_score = match diagnostics.severity {
            DiagnosticSeverity::Informational => 0.0,
            DiagnosticSeverity::Low => 1.0,
            DiagnosticSeverity::Medium => 2.0,
            DiagnosticSeverity::High => 3.0,
            DiagnosticSeverity::Critical => 4.0,
        };
        let trend_score = match diagnostics.trend {
            DiagnosticTrend::Growing => 1.0,
            DiagnosticTrend::Stalled => 2.0,
            DiagnosticTrend::Steady => 0.5,
            DiagnosticTrend::Shrinking => 0.0,
            DiagnosticTrend::Unknown => 0.0,
        };

        severity_score
            + trend_score
            + self.backlog.unwrap_or(0) as f64 * 2.5
            + self.inflight.unwrap_or(0) as f64 * 0.5
            + self.dead_letters.unwrap_or(0) as f64 * 5.0
            + self.workers.unwrap_or(0) as f64 * 0.25
            + self.subscriptions.unwrap_or(0) as f64 * 0.2
            + self.waiters.unwrap_or(0) as f64 * 1.5
            + self.failure_count.unwrap_or(0) as f64 * 3.0
            + self.contention_count.unwrap_or(0) as f64 * 2.0
            + self.age_seconds.unwrap_or(0) as f64 / 30.0
            + self.recent_transition_count.unwrap_or(0) as f64 * 0.25
            + self.operations_total.unwrap_or(0) as f64 * 0.0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceComparisonSide {
    pub scope: ResourceComparisonScope,
    pub diagnostics: DiagnosticSnapshot,
    pub metrics: ResourceComparisonMetrics,
}

impl ResourceComparisonSide {
    fn pressure_score(&self) -> f64 {
        self.metrics.pressure_score(&self.diagnostics)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ResourceComparisonDelta {
    pub backlog: Option<i64>,
    pub inflight: Option<i64>,
    pub ready: Option<i64>,
    pub delayed: Option<i64>,
    pub dead_letters: Option<i64>,
    pub workers: Option<i64>,
    pub subscriptions: Option<i64>,
    pub waiters: Option<i64>,
    pub age_seconds: Option<i64>,
    pub recent_transition_count: Option<i64>,
    pub failure_count: Option<i64>,
    pub contention_count: Option<i64>,
    pub operations_total: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceComparison {
    pub domain: String,
    pub comparison_mode: String,
    pub derived: bool,
    pub left: ResourceComparisonSide,
    pub right: ResourceComparisonSide,
    pub delta: ResourceComparisonDelta,
    pub summary: String,
}

impl ResourceComparisonDelta {
    fn from_sides(left: &ResourceComparisonMetrics, right: &ResourceComparisonMetrics) -> Self {
        Self {
            backlog: diff_usize(left.backlog, right.backlog),
            inflight: diff_usize(left.inflight, right.inflight),
            ready: diff_usize(left.ready, right.ready),
            delayed: diff_usize(left.delayed, right.delayed),
            dead_letters: diff_usize(left.dead_letters, right.dead_letters),
            workers: diff_usize(left.workers, right.workers),
            subscriptions: diff_usize(left.subscriptions, right.subscriptions),
            waiters: diff_usize(left.waiters, right.waiters),
            age_seconds: diff_u64(left.age_seconds, right.age_seconds),
            recent_transition_count: diff_u64(
                left.recent_transition_count,
                right.recent_transition_count,
            ),
            failure_count: diff_u64(left.failure_count, right.failure_count),
            contention_count: diff_u64(left.contention_count, right.contention_count),
            operations_total: diff_u64(left.operations_total, right.operations_total),
        }
    }
}

pub(crate) fn compare_resource_sides(
    domain: &str,
    left: ResourceComparisonSide,
    right: ResourceComparisonSide,
) -> ResourceComparison {
    let delta = ResourceComparisonDelta::from_sides(&left.metrics, &right.metrics);
    let summary = summarize_comparison(&left, &right, &delta);

    ResourceComparison {
        domain: domain.to_string(),
        comparison_mode: "snapshot_vs_snapshot".to_string(),
        derived: true,
        left,
        right,
        delta,
        summary,
    }
}

fn summarize_comparison(
    left: &ResourceComparisonSide,
    right: &ResourceComparisonSide,
    delta: &ResourceComparisonDelta,
) -> String {
    let left_score = left.pressure_score();
    let right_score = right.pressure_score();
    let (dominant_label, dominant, follower_label, _follower) =
        if (left_score - right_score).abs() <= 0.25 {
            ("left and right", left, "right and left", right)
        } else if left_score > right_score {
            ("left", left, "right", right)
        } else {
            ("right", right, "left", left)
        };

    let mut summary = if dominant_label == "left and right" {
        format!(
            "left and right look similar ({} vs {})",
            left.diagnostics.current_stage, right.diagnostics.current_stage
        )
    } else {
        let bottleneck = dominant
            .diagnostics
            .likely_bottleneck
            .as_deref()
            .unwrap_or(dominant.diagnostics.current_stage.as_str());
        format!(
            "{} side is under more pressure than {} side ({bottleneck})",
            dominant_label, follower_label
        )
    };

    let mut notes = Vec::new();
    append_delta_note(&mut notes, "backlog", delta.backlog);
    append_delta_note(&mut notes, "inflight", delta.inflight);
    append_delta_note(&mut notes, "ready", delta.ready);
    append_delta_note(&mut notes, "delayed", delta.delayed);
    append_delta_note(&mut notes, "dead_letters", delta.dead_letters);
    append_delta_note(&mut notes, "workers", delta.workers);
    append_delta_note(&mut notes, "subscriptions", delta.subscriptions);
    append_delta_note(&mut notes, "waiters", delta.waiters);
    append_delta_note(&mut notes, "age_seconds", delta.age_seconds);
    append_delta_note(&mut notes, "failures", delta.failure_count);
    append_delta_note(&mut notes, "contention", delta.contention_count);
    append_delta_note(&mut notes, "transitions", delta.recent_transition_count);
    append_delta_note(&mut notes, "operations_total", delta.operations_total);

    if !notes.is_empty() {
        summary.push_str("; ");
        summary.push_str(&notes.into_iter().take(3).collect::<Vec<_>>().join(", "));
    }

    summary
}

fn append_delta_note(notes: &mut Vec<String>, name: &str, value: Option<i64>) {
    if let Some(value) = value {
        if value != 0 {
            notes.push(format!("{name} {value:+}"));
        }
    }
}

fn diff_usize(left: Option<usize>, right: Option<usize>) -> Option<i64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left as i64 - right as i64),
        _ => None,
    }
}

fn diff_u64(left: Option<u64>, right: Option<u64>) -> Option<i64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left as i64 - right as i64),
        _ => None,
    }
}

#[derive(Clone)]
struct TimelineCandidate {
    observed_at: DateTime<Utc>,
    priority: u8,
    event: ResourceTimelineEvent,
}

#[derive(Clone)]
struct ScoredHotspot {
    score: f64,
    hotspot: DiagnosticHotspot,
    last_changed_at: Option<DateTime<Utc>>,
}

struct DomainAnalysis {
    diagnostics: DomainDiagnostics,
    hotspots: Vec<ScoredHotspot>,
    last_changed_at: Option<DateTime<Utc>>,
}

impl DomainAnalysis {
    fn healthy() -> Self {
        Self {
            diagnostics: DomainDiagnostics::healthy(),
            hotspots: Vec::new(),
            last_changed_at: None,
        }
    }

    fn from_hotspots(mut hotspots: Vec<ScoredHotspot>) -> Self {
        hotspots.sort_by(compare_scored_hotspots);
        let last_changed_at = hotspots
            .iter()
            .filter_map(|candidate| candidate.last_changed_at)
            .max();
        let diagnostics = hotspots
            .first()
            .map(|candidate| DomainDiagnostics::from_snapshot(candidate.hotspot.snapshot.clone()))
            .unwrap_or_else(DomainDiagnostics::healthy);

        Self {
            diagnostics,
            hotspots,
            last_changed_at,
        }
    }
}

pub type TroubleshootingSnapshot = RuntimeDiagnostics;

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

fn compare_scored_hotspots(left: &ScoredHotspot, right: &ScoredHotspot) -> Ordering {
    right
        .score
        .partial_cmp(&left.score)
        .unwrap_or(Ordering::Equal)
        .then_with(|| left.hotspot.path().cmp(&right.hotspot.path()))
}

fn broker_router_hotspot(
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

fn summarize_incident(top_bottleneck: &Option<DiagnosticHotspot>) -> IncidentSummary {
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

fn analyze_kv(transactions: &[KvTransaction], now: DateTime<Utc>) -> DomainAnalysis {
    let mut grouped: HashMap<(String, String, String), Vec<&KvTransaction>> = HashMap::new();
    for tx in transactions {
        grouped
            .entry((tx.realm.clone(), tx.area.clone(), tx.resource.clone()))
            .or_default()
            .push(tx);
    }

    let mut hotspots = Vec::new();
    let mut last_changed_at: Option<DateTime<Utc>> = None;

    for ((realm, area, resource), txs) in grouped {
        let waiter_count = txs.len();
        let age_seconds = txs.iter().map(|tx| tx.idle_seconds).max();
        let last_change = txs
            .iter()
            .filter_map(|tx| parse_rfc3339(&tx.started_at))
            .max();
        let recent_transition_count = txs
            .iter()
            .filter(|tx| {
                parse_rfc3339(&tx.started_at).is_some_and(|started| is_recent(started, now))
            })
            .count() as u64;
        let failure_count = 0;
        let contention_count = waiter_count as u64;
        let label = if waiter_count > 0 {
            DiagnosisLabel::Contention
        } else {
            DiagnosisLabel::Healthy
        };
        let trend = trend_from_pressure(waiter_count, age_seconds.unwrap_or(0));
        let bottleneck = if waiter_count > 0 {
            Some("transaction coordination".to_string())
        } else {
            None
        };
        let mut hints = vec![];
        if let Some(age) = age_seconds {
            hints.push(format!("oldest transaction idle for {age}s"));
        }
        if waiter_count > 0 {
            hints.push(format!("{waiter_count} open transaction(s)"));
        }

        let snapshot = DiagnosticSnapshot::with_stage(
            label,
            trend,
            if waiter_count > 0 {
                DiagnosticSeverity::Medium
            } else {
                DiagnosticSeverity::Informational
            },
            bottleneck,
            last_change,
            None,
            None,
            age_seconds,
            recent_transition_count,
            failure_count,
            contention_count,
            waiter_count,
            hints,
        );

        if let Some(candidate_changed_at) = last_change {
            let previous = last_changed_at;
            last_changed_at = Some(match previous {
                Some(current) => current.max(candidate_changed_at),
                None => candidate_changed_at,
            });
        }

        hotspots.push(ScoredHotspot {
            score: waiter_count as f64 * 3.0 + age_seconds.unwrap_or(0) as f64 / 15.0,
            hotspot: DiagnosticHotspot {
                domain: "kv".to_string(),
                realm: Some(realm),
                area: Some(area),
                resource: Some(resource),
                operation: None,
                family: None,
                backlog: Some(waiter_count),
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
            last_changed_at: last_change,
        });
    }

    if hotspots.is_empty() {
        DomainAnalysis::healthy()
    } else {
        DomainAnalysis::from_hotspots(hotspots)
    }
}

fn analyze_stream(
    streams: &[StreamInfo],
    request_latency_buckets: StreamLatencyBuckets,
    _now: DateTime<Utc>,
) -> DomainAnalysis {
    let mut grouped: HashMap<(String, String, String), Vec<&StreamInfo>> = HashMap::new();
    for stream in streams {
        grouped
            .entry((
                stream.realm.clone(),
                stream.area.clone(),
                stream.resource.clone(),
            ))
            .or_default()
            .push(stream);
    }

    let mut hotspots = Vec::new();
    let latency_total = request_latency_buckets.total();
    let latency_tail_count = request_latency_buckets.slow_tail_count();
    let latency_tail_ratio = request_latency_buckets.slow_tail_ratio();
    let latency_pressure = latency_total > 0
        && latency_tail_count > 0
        && (latency_tail_ratio >= 0.25 || latency_tail_count >= 3);

    for ((realm, area, resource), streams) in grouped {
        let backlog = streams
            .iter()
            .map(|stream| stream.offset.saturating_sub(stream.watermark) as usize)
            .sum::<usize>();
        let workers = streams
            .iter()
            .map(|stream| stream.sessions_active)
            .sum::<usize>();
        let max_lag = streams
            .iter()
            .map(|stream| stream.offset.saturating_sub(stream.watermark))
            .max()
            .unwrap_or(0);
        let label = if backlog > 0 || latency_pressure || workers > 0 {
            DiagnosisLabel::Throughput
        } else {
            DiagnosisLabel::Healthy
        };
        let trend = if backlog > 0 {
            DiagnosticTrend::Stalled
        } else if latency_pressure {
            if latency_tail_ratio >= 0.5 {
                DiagnosticTrend::Stalled
            } else {
                DiagnosticTrend::Steady
            }
        } else if workers > 0 {
            DiagnosticTrend::Steady
        } else {
            DiagnosticTrend::Unknown
        };
        let severity = if backlog > 0 {
            DiagnosticSeverity::Medium
        } else if latency_pressure {
            if latency_tail_ratio >= 0.5 {
                DiagnosticSeverity::High
            } else {
                DiagnosticSeverity::Medium
            }
        } else if workers > 0 {
            DiagnosticSeverity::Low
        } else {
            DiagnosticSeverity::Informational
        };
        let mut hints = vec![];
        if backlog > 0 {
            hints.push(format!("stream lag is {max_lag} event(s)"));
        }
        if latency_pressure {
            hints.push(format!(
                "stream request latency tail is {latency_tail_count} of {latency_total} observation(s) over 100ms"
            ));
        }
        if workers > 0 {
            hints.push(format!("{workers} live append session(s)"));
        }

        let snapshot = DiagnosticSnapshot::with_stage(
            label,
            trend,
            severity,
            if backlog > 0 {
                Some("append lag".to_string())
            } else if latency_pressure {
                Some("stream latency".to_string())
            } else if workers > 0 {
                Some("append throughput".to_string())
            } else {
                None
            },
            None,
            None,
            None,
            None,
            0,
            0,
            backlog as u64 + latency_tail_count as u64,
            workers,
            hints,
        );

        hotspots.push(ScoredHotspot {
            score: backlog as f64 * 2.0 + workers as f64 * 0.5 + latency_tail_ratio * 20.0,
            hotspot: DiagnosticHotspot {
                domain: "stream".to_string(),
                realm: Some(realm),
                area: Some(area),
                resource: Some(resource),
                operation: None,
                family: None,
                backlog: Some(backlog),
                inflight: Some(workers),
                ready: None,
                delayed: None,
                dead_letters: None,
                workers: Some(workers),
                subscriptions: None,
                owner_session: None,
                worker_session: None,
                snapshot,
            },
            last_changed_at: None,
        });
    }

    if hotspots.is_empty() {
        DomainAnalysis::healthy()
    } else {
        DomainAnalysis::from_hotspots(hotspots)
    }
}

fn analyze_notice(
    subscriptions: &[NoticeSubscription],
    routes: &[NoticeRouteInfo],
    now: DateTime<Utc>,
) -> DomainAnalysis {
    let mut route_map: HashMap<(String, String, String), Vec<&NoticeRouteInfo>> = HashMap::new();
    for route_info in routes {
        if let Some(route) = route_triplet(&route_info.route) {
            route_map
                .entry((
                    route.realm.to_string(),
                    route.area.to_string(),
                    route.resource.to_string(),
                ))
                .or_default()
                .push(route_info);
        }
    }

    let mut subscriptions_by_route: HashMap<(String, String, String), usize> = HashMap::new();
    for subscription in subscriptions {
        if let Some(route) = route_triplet(&subscription.pattern) {
            *subscriptions_by_route
                .entry((
                    route.realm.to_string(),
                    route.area.to_string(),
                    route.resource.to_string(),
                ))
                .or_default() += 1;
        }
    }

    let mut hotspots = Vec::new();
    let mut last_changed_at: Option<DateTime<Utc>> = None;

    for ((realm, area, resource), route_items) in route_map {
        let route_key = (realm.clone(), area.clone(), resource.clone());
        let subscribers = route_items
            .iter()
            .map(|route| route.subscribers)
            .sum::<usize>();
        let routes_active = route_items.len();
        let max_route_subscribers = route_items
            .iter()
            .map(|route| route.subscribers)
            .max()
            .unwrap_or(0);
        let publishes_per_minute = route_items
            .iter()
            .map(|route| route.publishes_per_minute)
            .fold(0.0_f64, f64::max);
        let waiter_count = subscribers;
        let label = if subscribers > 0 {
            DiagnosisLabel::Throughput
        } else {
            DiagnosisLabel::Healthy
        };
        let trend = if subscribers > 0 {
            DiagnosticTrend::Steady
        } else {
            DiagnosticTrend::Unknown
        };
        let concentration = routes_active > 1
            && max_route_subscribers > 0
            && max_route_subscribers * 2 >= subscribers.max(1);
        let severity = if subscribers > 25 {
            DiagnosticSeverity::High
        } else if concentration {
            DiagnosticSeverity::Medium
        } else if subscribers > 0 {
            DiagnosticSeverity::Low
        } else {
            DiagnosticSeverity::Informational
        };
        let mut hints = vec![];
        if subscribers > 0 {
            hints.push(format!("{subscribers} subscriber(s) on route"));
        }
        if concentration {
            hints.push(format!(
                "route concentration: {max_route_subscribers} subscriber(s) on one route across {routes_active} route(s)"
            ));
        }
        if publishes_per_minute > 0.0 {
            hints.push(format!("{publishes_per_minute:.1} publish(es)/min"));
        }

        let last_change = subscriptions
            .iter()
            .filter_map(|subscription| parse_rfc3339(&subscription.created_at))
            .filter(|created_at| is_recent(*created_at, now))
            .max();
        if let Some(candidate_changed_at) = last_change {
            let previous = last_changed_at;
            last_changed_at = Some(match previous {
                Some(current) => current.max(candidate_changed_at),
                None => candidate_changed_at,
            });
        }

        let snapshot = DiagnosticSnapshot::with_stage(
            label,
            trend,
            severity,
            if concentration {
                Some("route concentration".to_string())
            } else if subscribers > 0 {
                Some("subscription fanout".to_string())
            } else {
                None
            },
            last_change,
            None,
            None,
            None,
            subscriptions
                .iter()
                .filter_map(|subscription| parse_rfc3339(&subscription.created_at))
                .filter(|created_at| is_recent(*created_at, now))
                .count() as u64,
            0,
            subscribers as u64,
            waiter_count,
            hints,
        );

        hotspots.push(ScoredHotspot {
            score: subscribers as f64 * 2.0
                + publishes_per_minute
                + if concentration { 1.0 } else { 0.0 },
            hotspot: DiagnosticHotspot {
                domain: "notice".to_string(),
                realm: Some(realm),
                area: Some(area),
                resource: Some(resource),
                operation: None,
                family: None,
                backlog: Some(subscribers),
                inflight: None,
                ready: None,
                delayed: None,
                dead_letters: None,
                workers: None,
                subscriptions: Some(subscriptions_by_route.get(&route_key).copied().unwrap_or(0)),
                owner_session: None,
                worker_session: None,
                snapshot,
            },
            last_changed_at: last_change,
        });
    }

    if hotspots.is_empty() {
        DomainAnalysis::healthy()
    } else {
        DomainAnalysis::from_hotspots(hotspots)
    }
}

fn analyze_queue(
    queues: &[QueueInfo],
    inflight: &[QueueInflight],
    dead_letters: &[QueueDeadLetter],
    dead_letter_transitions_total: u64,
    complete_rejected_total: u64,
    now: DateTime<Utc>,
) -> DomainAnalysis {
    let mut inflight_by_resource: HashMap<(u64, String, String, String), Vec<&QueueInflight>> =
        HashMap::new();
    for item in inflight {
        inflight_by_resource
            .entry((
                item.family,
                item.realm.clone(),
                item.area.clone(),
                item.resource.clone(),
            ))
            .or_default()
            .push(item);
    }

    let mut dead_letters_by_resource: HashMap<
        (u64, String, String, String),
        Vec<&QueueDeadLetter>,
    > = HashMap::new();
    for item in dead_letters {
        dead_letters_by_resource
            .entry((
                item.family,
                item.realm.clone(),
                item.area.clone(),
                item.resource.clone(),
            ))
            .or_default()
            .push(item);
    }

    let mut hotspots = Vec::new();
    let mut last_changed_at: Option<DateTime<Utc>> = None;

    for queue in queues {
        let key = (
            queue.family,
            queue.realm.clone(),
            queue.area.clone(),
            queue.resource.clone(),
        );
        let queue_inflight = inflight_by_resource.get(&key).cloned().unwrap_or_default();
        let queue_dead_letters = dead_letters_by_resource
            .get(&key)
            .cloned()
            .unwrap_or_default();
        let backlog = queue.messages_ready + queue.messages_delayed;
        let waiters = backlog;
        let dead_letter_count = queue.messages_dead_lettered.max(queue_dead_letters.len());
        let inflight_count = queue.messages_inflight.max(queue_inflight.len());
        let last_failure_at = queue_dead_letters
            .iter()
            .filter_map(|item| parse_rfc3339(&item.dead_lettered_at))
            .max();
        let last_change_from_age = if queue.oldest_backlog_age_seconds > 0 {
            Some(now - Duration::seconds(queue.oldest_backlog_age_seconds as i64))
        } else {
            None
        };
        let last_change_from_inflight = queue_inflight
            .iter()
            .filter_map(|item| parse_rfc3339(&item.expires_at))
            .max();
        let last_changed = [
            last_failure_at,
            last_change_from_age,
            last_change_from_inflight,
        ]
        .into_iter()
        .flatten()
        .max();
        let recent_transition_count = queue_dead_letters
            .iter()
            .filter_map(|item| parse_rfc3339(&item.dead_lettered_at))
            .filter(|ts| is_recent(*ts, now))
            .count() as u64
            + queue_inflight
                .iter()
                .filter_map(|item| parse_rfc3339(&item.expires_at))
                .filter(|ts| is_recent(*ts, now))
                .count() as u64;
        let contention_count = if backlog > 0 {
            backlog as u64
        } else {
            dead_letter_count as u64
        };
        let (label, trend, severity, bottleneck) = if dead_letter_count > 0 {
            (
                DiagnosisLabel::DeadLetterPressure,
                DiagnosticTrend::Stalled,
                DiagnosticSeverity::High,
                Some("dead-letter pressure".to_string()),
            )
        } else if backlog > 0 && inflight_count == 0 {
            (
                DiagnosisLabel::WorkerStarvation,
                DiagnosticTrend::Growing,
                DiagnosticSeverity::High,
                Some("worker starvation".to_string()),
            )
        } else if backlog > 0
            && (queue.messages_delayed > 0 || queue.oldest_backlog_age_seconds >= 30)
        {
            (
                DiagnosisLabel::BacklogGrowth,
                DiagnosticTrend::Growing,
                DiagnosticSeverity::Medium,
                Some("backlog growth".to_string()),
            )
        } else if backlog > 0 {
            (
                DiagnosisLabel::Throughput,
                DiagnosticTrend::Steady,
                DiagnosticSeverity::Low,
                Some("queue throughput".to_string()),
            )
        } else if inflight_count > 0 {
            (
                DiagnosisLabel::Throughput,
                DiagnosticTrend::Steady,
                DiagnosticSeverity::Informational,
                Some("queue throughput".to_string()),
            )
        } else {
            (
                DiagnosisLabel::Healthy,
                DiagnosticTrend::Steady,
                DiagnosticSeverity::Informational,
                None,
            )
        };
        let mut hints = vec![];
        if backlog > 0 {
            hints.push(format!("{backlog} message(s) waiting"));
        }
        if queue.messages_delayed > 0 {
            hints.push(format!("{} delayed message(s)", queue.messages_delayed));
        }
        if queue.delay_age_buckets.over_15m > 0 {
            hints.push(format!(
                "{} delayed message(s) are 15m+ old",
                queue.delay_age_buckets.over_15m
            ));
        }
        if dead_letter_count > 0 {
            hints.push(format!("{dead_letter_count} dead-lettered message(s)"));
        }
        if dead_letter_count > 0 && dead_letter_transitions_total > 0 {
            hints.push(format!(
                "{dead_letter_transitions_total} dead-letter transition(s) recorded"
            ));
        }
        if complete_rejected_total > 0 {
            hints.push(format!(
                "{complete_rejected_total} queue complete rejection(s)"
            ));
        }
        if queue.oldest_backlog_age_seconds > 0 {
            hints.push(format!(
                "oldest backlog message is {}s old",
                queue.oldest_backlog_age_seconds
            ));
        }

        let snapshot = DiagnosticSnapshot::with_stage(
            label,
            trend,
            severity,
            bottleneck.clone(),
            last_changed,
            None,
            last_failure_at,
            Some(queue.oldest_backlog_age_seconds),
            recent_transition_count,
            dead_letter_count as u64,
            contention_count,
            waiters,
            hints,
        );

        if let Some(candidate_changed_at) = last_changed {
            let previous = last_changed_at;
            last_changed_at = Some(match previous {
                Some(current) => current.max(candidate_changed_at),
                None => candidate_changed_at,
            });
        }

        hotspots.push(ScoredHotspot {
            score: backlog as f64 * 4.0
                + dead_letter_count as f64 * 8.0
                + inflight_count as f64 * 1.5
                + queue.delay_age_buckets.over_15m as f64 * 2.0
                + queue.oldest_backlog_age_seconds as f64 / 12.0,
            hotspot: DiagnosticHotspot {
                domain: "queue".to_string(),
                realm: Some(queue.realm.clone()),
                area: Some(queue.area.clone()),
                resource: Some(queue.resource.clone()),
                operation: None,
                family: Some(queue.family),
                backlog: Some(backlog),
                inflight: Some(inflight_count),
                ready: Some(queue.messages_ready),
                delayed: Some(queue.messages_delayed),
                dead_letters: Some(dead_letter_count),
                workers: None,
                subscriptions: None,
                owner_session: queue_inflight.first().map(|item| item.session_id.clone()),
                worker_session: None,
                snapshot,
            },
            last_changed_at: last_changed,
        });
    }

    if hotspots.is_empty() {
        DomainAnalysis::healthy()
    } else {
        DomainAnalysis::from_hotspots(hotspots)
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct RpcLatencySummary {
    pub worker_latency_buckets: RpcLatencyBuckets,
    pub slowest_worker_average_latency_ms: f64,
}

pub(crate) fn summarize_rpc_worker_latency<'a, I>(workers: I) -> RpcLatencySummary
where
    I: IntoIterator<Item = &'a RpcWorker>,
{
    let mut summary = RpcLatencySummary::default();

    for worker in workers {
        if worker.average_latency_ms.is_finite() {
            let latency_ms = worker.average_latency_ms.max(0.0);
            let mut latency_bucket = RpcLatencyBuckets::default();
            latency_bucket.record_latency_ms(latency_ms);
            summary.worker_latency_buckets.merge(latency_bucket);
            summary.slowest_worker_average_latency_ms =
                summary.slowest_worker_average_latency_ms.max(latency_ms);
        }
    }

    summary
}

#[allow(clippy::too_many_arguments)]
fn analyze_rpc(
    workers: &[RpcWorker],
    pending: &[RpcPendingRequest],
    request_timeouts_total: u64,
    backpressure_rejects_total: u64,
    duplicate_correlation_rejects_total: u64,
    wrong_worker_rejects_total: u64,
    responses_dropped_closed_caller_total: u64,
    responses_missing_pending_total: u64,
    acks_rejected_wrong_worker_total: u64,
    now: DateTime<Utc>,
) -> DomainAnalysis {
    let mut worker_by_route: HashMap<(String, String, String, String), Vec<&RpcWorker>> =
        HashMap::new();
    for worker in workers {
        if let Some(route) = route_quad(&worker.route) {
            worker_by_route
                .entry((
                    route.realm.to_string(),
                    route.area.to_string(),
                    route.resource.to_string(),
                    route.operation.to_string(),
                ))
                .or_default()
                .push(worker);
        }
    }

    let mut pending_by_route: HashMap<(String, String, String, String), Vec<&RpcPendingRequest>> =
        HashMap::new();
    for item in pending {
        if let Some(route) = route_quad(&item.route) {
            pending_by_route
                .entry((
                    route.realm.to_string(),
                    route.area.to_string(),
                    route.resource.to_string(),
                    route.operation.to_string(),
                ))
                .or_default()
                .push(item);
        }
    }

    let mut hotspots = Vec::new();
    let mut last_changed_at: Option<DateTime<Utc>> = None;
    let late_response_pressure =
        responses_dropped_closed_caller_total + responses_missing_pending_total;
    let correlation_pressure = duplicate_correlation_rejects_total
        + wrong_worker_rejects_total
        + acks_rejected_wrong_worker_total;
    let transport_pressure = request_timeouts_total + backpressure_rejects_total;
    let overall_latency_summary = summarize_rpc_worker_latency(workers.iter());

    for (key, pending_items) in pending_by_route {
        let workers = worker_by_route.get(&key).cloned().unwrap_or_default();
        let pending_count = pending_items.len();
        let worker_count = workers.len();
        let age_seconds = pending_items.iter().map(|item| item.age_seconds).max();
        let last_failure_at = pending_items
            .iter()
            .filter_map(|item| parse_rfc3339(&item.submitted_at))
            .max();
        let last_change = pending_items
            .iter()
            .filter_map(|item| parse_rfc3339(&item.submitted_at))
            .chain(
                workers
                    .iter()
                    .filter_map(|item| parse_rfc3339(&item.registered_at)),
            )
            .max();
        let latency_summary = summarize_rpc_worker_latency(workers.iter().copied());
        let slowest_worker_average_latency_ms = latency_summary.slowest_worker_average_latency_ms;
        let recent_transition_count = pending_items
            .iter()
            .filter_map(|item| parse_rfc3339(&item.submitted_at))
            .filter(|ts| is_recent(*ts, now))
            .count() as u64
            + workers
                .iter()
                .filter_map(|item| parse_rfc3339(&item.registered_at))
                .filter(|ts| is_recent(*ts, now))
                .count() as u64;
        let contention_count = pending_count.saturating_sub(worker_count) as u64;
        let (label, trend, severity, bottleneck) = if worker_count == 0 && pending_count > 0 {
            (
                DiagnosisLabel::WorkerStarvation,
                DiagnosticTrend::Growing,
                DiagnosticSeverity::High,
                Some("worker starvation".to_string()),
            )
        } else if late_response_pressure > 0 && pending_count == 0 {
            (
                DiagnosisLabel::DataLossRisk,
                DiagnosticTrend::Stalled,
                DiagnosticSeverity::High,
                Some("late response drop".to_string()),
            )
        } else if correlation_pressure > 0 && pending_count == 0 {
            (
                DiagnosisLabel::DataLossRisk,
                DiagnosticTrend::Stalled,
                DiagnosticSeverity::High,
                Some("correlation mismatch".to_string()),
            )
        } else if pending_count > worker_count
            && (age_seconds.unwrap_or(0) >= 30 || pending_count >= worker_count.saturating_mul(2))
        {
            (
                DiagnosisLabel::BacklogGrowth,
                if age_seconds.unwrap_or(0) >= 60 {
                    DiagnosticTrend::Stalled
                } else {
                    DiagnosticTrend::Growing
                },
                DiagnosticSeverity::High,
                Some("route backlog".to_string()),
            )
        } else if pending_count > worker_count {
            (
                DiagnosisLabel::Contention,
                DiagnosticTrend::Growing,
                DiagnosticSeverity::Medium,
                Some("route contention".to_string()),
            )
        } else if slowest_worker_average_latency_ms >= 100.0 {
            (
                DiagnosisLabel::Throughput,
                DiagnosticTrend::Steady,
                if slowest_worker_average_latency_ms >= 250.0 {
                    DiagnosticSeverity::High
                } else {
                    DiagnosticSeverity::Medium
                },
                Some("slow worker latency".to_string()),
            )
        } else if pending_count > 0 {
            (
                DiagnosisLabel::Throughput,
                DiagnosticTrend::Steady,
                DiagnosticSeverity::Low,
                Some("route throughput".to_string()),
            )
        } else {
            (
                DiagnosisLabel::Healthy,
                DiagnosticTrend::Unknown,
                DiagnosticSeverity::Informational,
                None,
            )
        };
        let failure_count = if matches!(label, DiagnosisLabel::DataLossRisk) {
            if late_response_pressure > 0 {
                late_response_pressure
            } else if correlation_pressure > 0 {
                correlation_pressure
            } else {
                0
            }
        } else {
            0
        };
        let mut hints = vec![];
        if pending_count > 0 {
            hints.push(format!("{pending_count} pending request(s)"));
        }
        if worker_count > 0 {
            hints.push(format!("{worker_count} registered worker(s)"));
        }
        if slowest_worker_average_latency_ms > 0.0 {
            hints.push(format!(
                "slowest worker average latency is {:.1}ms",
                slowest_worker_average_latency_ms
            ));
        }
        if let Some(age) = age_seconds {
            hints.push(format!("oldest request is {age}s old"));
        }
        if duplicate_correlation_rejects_total > 0 {
            hints.push(format!(
                "{duplicate_correlation_rejects_total} duplicate correlation rejection(s)"
            ));
        }
        if wrong_worker_rejects_total > 0 {
            hints.push(format!(
                "{wrong_worker_rejects_total} wrong worker rejection(s)"
            ));
        }
        if late_response_pressure > 0 {
            hints.push(format!("{late_response_pressure} late response drop(s)"));
        }
        if transport_pressure > 0 {
            hints.push(format!(
                "{transport_pressure} timeout/backpressure rejection(s)"
            ));
        }

        let snapshot = DiagnosticSnapshot::with_stage(
            label,
            trend,
            severity,
            bottleneck.clone(),
            last_change,
            None,
            last_failure_at,
            age_seconds,
            recent_transition_count,
            failure_count,
            contention_count,
            pending_count,
            hints,
        );

        if let Some(candidate_changed_at) = last_change {
            let previous = last_changed_at;
            last_changed_at = Some(match previous {
                Some(current) => current.max(candidate_changed_at),
                None => candidate_changed_at,
            });
        }

        hotspots.push(ScoredHotspot {
            score: pending_count as f64 * 5.0
                + contention_count as f64 * 4.0
                + age_seconds.unwrap_or(0) as f64 / 10.0
                + slowest_worker_average_latency_ms / 10.0,
            hotspot: DiagnosticHotspot {
                domain: "rpc".to_string(),
                realm: Some(key.0),
                area: Some(key.1),
                resource: Some(key.2),
                operation: Some(key.3),
                family: None,
                backlog: Some(pending_count),
                inflight: Some(pending_count),
                ready: None,
                delayed: None,
                dead_letters: None,
                workers: Some(worker_count),
                subscriptions: None,
                owner_session: None,
                worker_session: workers.first().map(|worker| worker.session_id.clone()),
                snapshot,
            },
            last_changed_at: last_change,
        });
    }

    let has_route_hotspots = !hotspots.is_empty();
    if !has_route_hotspots {
        let data_loss_pressure = late_response_pressure + correlation_pressure;
        if data_loss_pressure > 0 || transport_pressure > 0 {
            let bottleneck = if late_response_pressure > 0 {
                "late response drop"
            } else if correlation_pressure > 0 {
                "correlation mismatch"
            } else {
                "rpc backpressure"
            };
            let label = if late_response_pressure > 0 || correlation_pressure > 0 {
                DiagnosisLabel::DataLossRisk
            } else {
                DiagnosisLabel::Throughput
            };
            let trend = if late_response_pressure > 0 || correlation_pressure > 0 {
                DiagnosticTrend::Stalled
            } else {
                DiagnosticTrend::Growing
            };
            let severity = if late_response_pressure > 0 || correlation_pressure > 0 {
                DiagnosticSeverity::High
            } else {
                DiagnosticSeverity::Medium
            };
            let mut hints = vec![];
            if request_timeouts_total > 0 {
                hints.push(format!("{request_timeouts_total} request timeout(s)"));
            }
            if backpressure_rejects_total > 0 {
                hints.push(format!(
                    "{backpressure_rejects_total} backpressure reject(s)"
                ));
            }
            if duplicate_correlation_rejects_total > 0 {
                hints.push(format!(
                    "{duplicate_correlation_rejects_total} duplicate correlation rejection(s)"
                ));
            }
            if wrong_worker_rejects_total > 0 {
                hints.push(format!(
                    "{wrong_worker_rejects_total} wrong worker rejection(s)"
                ));
            }
            if late_response_pressure > 0 {
                hints.push(format!("{late_response_pressure} late response drop(s)"));
            }
            if correlation_pressure > 0 {
                hints.push(format!(
                    "{correlation_pressure} correlation mismatch event(s)"
                ));
            }
            hotspots.push(ScoredHotspot {
                score: late_response_pressure as f64 * 12.0
                    + correlation_pressure as f64 * 6.0
                    + transport_pressure as f64 * 2.0,
                hotspot: DiagnosticHotspot {
                    domain: "rpc".to_string(),
                    realm: None,
                    area: None,
                    resource: None,
                    operation: None,
                    family: None,
                    backlog: Some(pending.len()),
                    inflight: Some(pending.len()),
                    ready: None,
                    delayed: None,
                    dead_letters: None,
                    workers: Some(workers.len()),
                    subscriptions: None,
                    owner_session: None,
                    worker_session: None,
                    snapshot: DiagnosticSnapshot::with_stage(
                        label,
                        trend,
                        severity,
                        Some(bottleneck.to_string()),
                        None,
                        None,
                        None,
                        Some(
                            pending
                                .iter()
                                .map(|item| item.age_seconds)
                                .max()
                                .unwrap_or(0),
                        ),
                        data_loss_pressure,
                        data_loss_pressure,
                        correlation_pressure,
                        pending.len(),
                        hints,
                    ),
                },
                last_changed_at: pending
                    .iter()
                    .filter_map(|item| parse_rfc3339(&item.submitted_at))
                    .max(),
            });
        } else if overall_latency_summary.slowest_worker_average_latency_ms >= 100.0 {
            let slowest_latency_ms = overall_latency_summary.slowest_worker_average_latency_ms;
            let severity = if slowest_latency_ms >= 250.0 {
                DiagnosticSeverity::High
            } else {
                DiagnosticSeverity::Medium
            };
            let registered_at_times: Vec<_> = workers
                .iter()
                .filter_map(|worker| parse_rfc3339(&worker.registered_at))
                .collect();
            let mut hints = vec![];
            if !registered_at_times.is_empty() {
                hints.push(format!("{} registered worker(s)", workers.len()));
            }
            hints.push(format!(
                "slowest worker average latency is {:.1}ms",
                slowest_latency_ms
            ));
            hotspots.push(ScoredHotspot {
                score: slowest_latency_ms / 10.0 + workers.len() as f64,
                hotspot: DiagnosticHotspot {
                    domain: "rpc".to_string(),
                    realm: None,
                    area: None,
                    resource: None,
                    operation: None,
                    family: None,
                    backlog: Some(pending.len()),
                    inflight: Some(pending.len()),
                    ready: None,
                    delayed: None,
                    dead_letters: None,
                    workers: Some(workers.len()),
                    subscriptions: None,
                    owner_session: None,
                    worker_session: workers.first().map(|worker| worker.session_id.clone()),
                    snapshot: DiagnosticSnapshot::with_stage(
                        DiagnosisLabel::Throughput,
                        DiagnosticTrend::Steady,
                        severity,
                        Some("slow worker latency".to_string()),
                        registered_at_times.iter().copied().max(),
                        None,
                        None,
                        None,
                        registered_at_times
                            .iter()
                            .filter(|ts| is_recent(**ts, now))
                            .count() as u64,
                        0,
                        0,
                        pending.len(),
                        hints,
                    ),
                },
                last_changed_at: registered_at_times.iter().copied().max(),
            });
        }
    }

    if hotspots.is_empty() {
        DomainAnalysis::healthy()
    } else {
        DomainAnalysis::from_hotspots(hotspots)
    }
}

fn analyze_lease(leases: &[LeaseInfo], now: DateTime<Utc>) -> DomainAnalysis {
    let mut grouped: HashMap<(String, String, String), Vec<&LeaseInfo>> = HashMap::new();
    for lease in leases {
        grouped
            .entry((
                lease.realm.clone(),
                lease.area.clone(),
                lease.resource.clone(),
            ))
            .or_default()
            .push(lease);
    }

    let mut hotspots = Vec::new();
    let mut last_changed_at: Option<DateTime<Utc>> = None;

    for ((realm, area, resource), items) in grouped {
        let active_leases = items.len();
        let renewals = items.iter().map(|lease| lease.renewals).sum::<usize>();
        let last_change = items
            .iter()
            .filter_map(|lease| parse_rfc3339(&lease.acquired_at))
            .max();
        let age_seconds = last_change.map(|changed| (now - changed).num_seconds().max(0) as u64);
        let remaining_seconds = items
            .iter()
            .filter_map(|lease| parse_rfc3339(&lease.expires_at))
            .map(|expires| (expires - now).num_seconds().max(0) as u64)
            .min();
        let churn_pressure = renewals > 0;
        let (label, trend, severity, bottleneck) =
            if remaining_seconds.unwrap_or(0) <= 30 && active_leases > 0 {
                (
                    DiagnosisLabel::StaleHandoff,
                    DiagnosticTrend::Stalled,
                    DiagnosticSeverity::High,
                    Some("lease ownership".to_string()),
                )
            } else if active_leases > 0 {
                (
                    DiagnosisLabel::Contention,
                    if churn_pressure {
                        DiagnosticTrend::Growing
                    } else {
                        DiagnosticTrend::Steady
                    },
                    if churn_pressure {
                        DiagnosticSeverity::Medium
                    } else {
                        DiagnosticSeverity::Low
                    },
                    Some(if churn_pressure {
                        "lease ownership churn".to_string()
                    } else {
                        "lease ownership".to_string()
                    }),
                )
            } else {
                (
                    DiagnosisLabel::Healthy,
                    DiagnosticTrend::Steady,
                    DiagnosticSeverity::Informational,
                    None,
                )
            };
        let mut hints = vec![];
        if active_leases > 0 {
            hints.push(format!("{active_leases} active lease(s)"));
        }
        if renewals > 0 {
            hints.push(format!("{renewals} renewals recorded"));
        }
        if let Some(remaining) = remaining_seconds {
            hints.push(format!("{remaining}s until next expiry"));
        }

        let snapshot = DiagnosticSnapshot::with_stage(
            label,
            trend,
            severity,
            bottleneck.clone(),
            last_change,
            last_change,
            None,
            age_seconds,
            if active_leases > 0 { 1 } else { 0 },
            0,
            renewals as u64,
            active_leases,
            hints,
        );

        if let Some(candidate_changed_at) = last_change {
            let previous = last_changed_at;
            last_changed_at = Some(match previous {
                Some(current) => current.max(candidate_changed_at),
                None => candidate_changed_at,
            });
        }

        hotspots.push(ScoredHotspot {
            score: active_leases as f64 * 3.0 + renewals as f64 * 1.5,
            hotspot: DiagnosticHotspot {
                domain: "lease".to_string(),
                realm: Some(realm),
                area: Some(area),
                resource: Some(resource),
                operation: None,
                family: None,
                backlog: Some(active_leases),
                inflight: None,
                ready: None,
                delayed: None,
                dead_letters: None,
                workers: None,
                subscriptions: None,
                owner_session: items.first().map(|lease| lease.owner_session_id.clone()),
                worker_session: None,
                snapshot,
            },
            last_changed_at: last_change,
        });
    }

    if hotspots.is_empty() {
        DomainAnalysis::healthy()
    } else {
        DomainAnalysis::from_hotspots(hotspots)
    }
}

#[allow(clippy::too_many_arguments)]
fn analyze_schedule(
    schedules: &[ScheduleInfo],
    pending_fire_claims: usize,
    pending_ack_retries: usize,
    oldest_pending_claim_age_seconds: u64,
    request_latency_buckets: ScheduleLatencyBuckets,
    notify_failures: u64,
    ack_failures: u64,
    overdue_normalizations: u64,
    pending_claims_expired_total: u64,
    pending_claim_cleanup_failures_total: u64,
    now: DateTime<Utc>,
) -> DomainAnalysis {
    let mut grouped: HashMap<(String, String, String, String), Vec<&ScheduleInfo>> = HashMap::new();
    for schedule in schedules {
        grouped
            .entry((
                schedule.realm.clone(),
                schedule.area.clone(),
                schedule.resource.clone(),
                schedule.operation.clone(),
            ))
            .or_default()
            .push(schedule);
    }

    let mut hotspots = Vec::new();
    let mut last_changed_at: Option<DateTime<Utc>> = None;
    let latency_total = request_latency_buckets.total();
    let latency_tail_count = request_latency_buckets.slow_tail_count();
    let latency_tail_ratio = request_latency_buckets.slow_tail_ratio();
    let latency_pressure = latency_total > 0
        && latency_tail_count > 0
        && (latency_tail_ratio >= 0.25 || latency_tail_count >= 3);

    for ((realm, area, resource, operation), items) in grouped {
        let enabled = items.iter().any(|schedule| schedule.enabled);
        let next_run = items
            .iter()
            .filter_map(|schedule| parse_rfc3339(&schedule.next_run))
            .min();
        let last_run = items
            .iter()
            .filter_map(|schedule| schedule.last_run.as_deref().and_then(parse_rfc3339))
            .max();
        let age_seconds = next_run
            .and_then(|due| {
                if due <= now {
                    Some((now - due).num_seconds().max(0) as u64)
                } else {
                    last_run.map(|run| (now - run).num_seconds().max(0) as u64)
                }
            })
            .or_else(|| last_run.map(|run| (now - run).num_seconds().max(0) as u64));
        let age_seconds = match (oldest_pending_claim_age_seconds, age_seconds) {
            (0, age_seconds) => age_seconds,
            (pending_age, _) => Some(pending_age),
        };
        let recent_transition_count = items
            .iter()
            .filter_map(|schedule| schedule.last_run.as_deref().and_then(parse_rfc3339))
            .filter(|ts| is_recent(*ts, now))
            .count() as u64;
        let contention_count = pending_fire_claims
            .saturating_add(pending_ack_retries)
            .saturating_add(overdue_normalizations as usize)
            .saturating_add(pending_claims_expired_total as usize)
            .saturating_add(pending_claim_cleanup_failures_total as usize)
            .saturating_add(latency_tail_count) as u64;
        let failure_count = notify_failures + ack_failures + pending_claim_cleanup_failures_total;
        let handoff_pressure =
            pending_fire_claims > 0 || pending_ack_retries > 0 || overdue_normalizations > 0;
        let cleanup_pressure =
            pending_claims_expired_total > 0 || pending_claim_cleanup_failures_total > 0;
        let (label, trend, severity, bottleneck) = if handoff_pressure {
            (
                DiagnosisLabel::StaleHandoff,
                DiagnosticTrend::Stalled,
                DiagnosticSeverity::High,
                Some("durable handoff".to_string()),
            )
        } else if cleanup_pressure {
            (
                DiagnosisLabel::StaleHandoff,
                DiagnosticTrend::Stalled,
                if pending_claim_cleanup_failures_total > 0 {
                    DiagnosticSeverity::High
                } else {
                    DiagnosticSeverity::Medium
                },
                Some("claim cleanup".to_string()),
            )
        } else if latency_pressure {
            (
                DiagnosisLabel::Throughput,
                if latency_tail_ratio >= 0.5 {
                    DiagnosticTrend::Stalled
                } else {
                    DiagnosticTrend::Steady
                },
                if latency_tail_ratio >= 0.5 {
                    DiagnosticSeverity::High
                } else {
                    DiagnosticSeverity::Medium
                },
                Some("schedule latency".to_string()),
            )
        } else if failure_count > 0 {
            (
                DiagnosisLabel::Throughput,
                DiagnosticTrend::Steady,
                DiagnosticSeverity::Medium,
                Some("schedule failure".to_string()),
            )
        } else if enabled {
            (
                DiagnosisLabel::Healthy,
                DiagnosticTrend::Steady,
                DiagnosticSeverity::Informational,
                None,
            )
        } else {
            (
                DiagnosisLabel::Healthy,
                DiagnosticTrend::Unknown,
                DiagnosticSeverity::Informational,
                None,
            )
        };
        let mut hints = vec![];
        if let Some(run) = last_run {
            hints.push(format!("last run at {}", rfc3339(run)));
        }
        if let Some(due) = next_run {
            hints.push(format!("next run at {}", rfc3339(due)));
        }
        if pending_fire_claims > 0 {
            hints.push(format!("{pending_fire_claims} pending fire claim(s)"));
        }
        if pending_ack_retries > 0 {
            hints.push(format!("{pending_ack_retries} pending ack retry(s)"));
        }
        if oldest_pending_claim_age_seconds > 0 {
            hints.push(format!(
                "oldest pending claim is {oldest_pending_claim_age_seconds}s old"
            ));
        }
        if latency_pressure {
            hints.push(format!(
                "schedule request latency tail is {latency_tail_count} of {latency_total} observation(s) over 100ms"
            ));
        }
        if overdue_normalizations > 0 {
            hints.push(format!("{overdue_normalizations} overdue normalization(s)"));
        }
        if pending_claims_expired_total > 0 {
            hints.push(format!(
                "{pending_claims_expired_total} expired pending claim(s)"
            ));
        }
        if pending_claim_cleanup_failures_total > 0 {
            hints.push(format!(
                "{pending_claim_cleanup_failures_total} pending claim cleanup failure(s)"
            ));
        }

        let last_change = last_run.or(next_run);
        let snapshot = DiagnosticSnapshot::with_stage(
            label,
            trend,
            severity,
            bottleneck.clone(),
            last_change,
            last_run,
            None,
            age_seconds,
            recent_transition_count,
            failure_count,
            contention_count,
            pending_fire_claims,
            hints,
        );

        if let Some(candidate_changed_at) = last_change {
            let previous = last_changed_at;
            last_changed_at = Some(match previous {
                Some(current) => current.max(candidate_changed_at),
                None => candidate_changed_at,
            });
        }

        hotspots.push(ScoredHotspot {
            score: pending_fire_claims as f64 * 5.0
                + pending_ack_retries as f64 * 3.5
                + overdue_normalizations as f64 * 4.0
                + pending_claims_expired_total as f64 * 2.5
                + pending_claim_cleanup_failures_total as f64 * 5.0
                + failure_count as f64 * 1.25
                + age_seconds.unwrap_or(0) as f64 / 20.0,
            hotspot: DiagnosticHotspot {
                domain: "schedule".to_string(),
                realm: Some(realm),
                area: Some(area),
                resource: Some(resource),
                operation: Some(operation),
                family: None,
                backlog: Some(pending_fire_claims.saturating_add(pending_ack_retries)),
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
            last_changed_at: last_change,
        });
    }

    if hotspots.is_empty() {
        DomainAnalysis::healthy()
    } else {
        DomainAnalysis::from_hotspots(hotspots)
    }
}

fn trend_from_pressure(waiter_count: usize, age_seconds: u64) -> DiagnosticTrend {
    if waiter_count == 0 {
        DiagnosticTrend::Steady
    } else if age_seconds >= 60 {
        DiagnosticTrend::Stalled
    } else if age_seconds >= 15 {
        DiagnosticTrend::Growing
    } else {
        DiagnosticTrend::Steady
    }
}

fn is_recent(timestamp: DateTime<Utc>, now: DateTime<Utc>) -> bool {
    now.signed_duration_since(timestamp) <= Duration::seconds(RECENT_WINDOW_SECS)
}

fn parse_rfc3339(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|timestamp| timestamp.with_timezone(&Utc))
}

pub(crate) fn age_seconds_since(timestamp: &str) -> Option<u64> {
    let timestamp = parse_rfc3339(timestamp)?;
    Some(
        Utc::now()
            .signed_duration_since(timestamp)
            .num_seconds()
            .max(0) as u64,
    )
}

fn rfc3339(timestamp: DateTime<Utc>) -> String {
    timestamp.to_rfc3339()
}

pub(crate) fn kv_resource_diagnostics(transactions_active: usize) -> DiagnosticSnapshot {
    if transactions_active == 0 {
        return DiagnosticSnapshot::healthy();
    }

    DiagnosticSnapshot::with_stage(
        DiagnosisLabel::Contention,
        if transactions_active > 1 {
            DiagnosticTrend::Growing
        } else {
            DiagnosticTrend::Steady
        },
        DiagnosticSeverity::Medium,
        Some("transaction coordination".to_string()),
        None,
        None,
        None,
        None,
        0,
        0,
        transactions_active as u64,
        transactions_active,
        vec![format!("{transactions_active} open transaction(s)")],
    )
}

pub(crate) fn queue_resource_diagnostics(
    messages_ready: usize,
    messages_delayed: usize,
    messages_inflight: usize,
    messages_dead_lettered: usize,
    oldest_backlog_age_seconds: u64,
    delay_age_buckets: QueueAgeBuckets,
) -> DiagnosticSnapshot {
    let backlog = messages_ready + messages_delayed;
    let has_dead_letters = messages_dead_lettered > 0;
    let has_backlog = backlog > 0;
    let has_inflight = messages_inflight > 0;

    let (label, trend, severity, bottleneck) = if has_dead_letters {
        (
            DiagnosisLabel::DeadLetterPressure,
            DiagnosticTrend::Stalled,
            DiagnosticSeverity::High,
            Some("dead-letter pressure".to_string()),
        )
    } else if has_backlog && !has_inflight {
        (
            DiagnosisLabel::WorkerStarvation,
            if oldest_backlog_age_seconds >= 30 {
                DiagnosticTrend::Growing
            } else {
                DiagnosticTrend::Steady
            },
            DiagnosticSeverity::High,
            Some("worker starvation".to_string()),
        )
    } else if has_backlog && oldest_backlog_age_seconds >= 30 {
        (
            DiagnosisLabel::BacklogGrowth,
            DiagnosticTrend::Growing,
            DiagnosticSeverity::Medium,
            Some("backlog growth".to_string()),
        )
    } else if has_inflight {
        (
            DiagnosisLabel::Throughput,
            DiagnosticTrend::Steady,
            DiagnosticSeverity::Low,
            Some("queue throughput".to_string()),
        )
    } else {
        (
            DiagnosisLabel::Healthy,
            DiagnosticTrend::Steady,
            DiagnosticSeverity::Informational,
            None,
        )
    };

    DiagnosticSnapshot::with_stage(
        label,
        trend,
        severity,
        bottleneck,
        None,
        None,
        None,
        if oldest_backlog_age_seconds > 0 {
            Some(oldest_backlog_age_seconds)
        } else {
            None
        },
        0,
        messages_dead_lettered as u64,
        backlog as u64,
        backlog,
        {
            let mut hints = Vec::new();
            if has_backlog {
                hints.push(format!("{backlog} message(s) waiting"));
            }
            if messages_delayed > 0 {
                hints.push(format!("{messages_delayed} delayed message(s)"));
            }
            if messages_dead_lettered > 0 {
                hints.push(format!("{messages_dead_lettered} dead-lettered message(s)"));
            }
            if delay_age_buckets.over_15m > 0 {
                hints.push(format!(
                    "{} delayed message(s) are 15m+ old",
                    delay_age_buckets.over_15m
                ));
            }
            if oldest_backlog_age_seconds > 0 {
                hints.push(format!(
                    "oldest backlog message is {oldest_backlog_age_seconds}s old"
                ));
            }
            hints
        },
    )
}

pub(crate) fn stream_resource_diagnostics(
    offset: u64,
    watermark: u64,
    sessions_active: usize,
) -> DiagnosticSnapshot {
    let lag = offset.saturating_sub(watermark);
    if lag == 0 && sessions_active == 0 {
        return DiagnosticSnapshot::healthy();
    }

    let (label, trend, severity, bottleneck) = if lag > 0 {
        (
            DiagnosisLabel::Throughput,
            DiagnosticTrend::Stalled,
            DiagnosticSeverity::Medium,
            Some("append lag".to_string()),
        )
    } else {
        (
            DiagnosisLabel::Throughput,
            DiagnosticTrend::Steady,
            DiagnosticSeverity::Low,
            Some("append throughput".to_string()),
        )
    };

    DiagnosticSnapshot::with_stage(
        label,
        trend,
        severity,
        bottleneck,
        None,
        None,
        None,
        None,
        0,
        0,
        lag,
        sessions_active,
        {
            let mut hints = Vec::new();
            if lag > 0 {
                hints.push(format!("stream lag is {lag} event(s)"));
            }
            if sessions_active > 0 {
                hints.push(format!("{sessions_active} live append session(s)"));
            }
            hints
        },
    )
}

pub(crate) fn lease_resource_diagnostics(
    active_leases: usize,
    oldest_lease_age_seconds: Option<u64>,
    renewals_total: usize,
) -> DiagnosticSnapshot {
    if active_leases == 0 {
        return DiagnosticSnapshot::healthy();
    }

    let churn_pressure = renewals_total > 0;
    let severity = if churn_pressure {
        if renewals_total > active_leases {
            DiagnosticSeverity::Medium
        } else {
            DiagnosticSeverity::Low
        }
    } else if active_leases > 1 {
        DiagnosticSeverity::Medium
    } else {
        DiagnosticSeverity::Low
    };

    DiagnosticSnapshot::with_stage(
        DiagnosisLabel::Contention,
        if churn_pressure {
            DiagnosticTrend::Growing
        } else {
            DiagnosticTrend::Steady
        },
        severity,
        Some(if churn_pressure {
            "lease ownership churn".to_string()
        } else {
            "lease ownership".to_string()
        }),
        None,
        None,
        None,
        oldest_lease_age_seconds,
        0,
        0,
        renewals_total as u64,
        active_leases,
        {
            let mut hints = vec![format!("{active_leases} active lease(s)")];
            if renewals_total > 0 {
                hints.push(format!("{renewals_total} renewals recorded"));
            }
            hints
        },
    )
}

pub(crate) fn notice_resource_diagnostics(subscriptions_active: usize) -> DiagnosticSnapshot {
    if subscriptions_active == 0 {
        return DiagnosticSnapshot::healthy();
    }

    DiagnosticSnapshot::with_stage(
        DiagnosisLabel::Throughput,
        DiagnosticTrend::Steady,
        if subscriptions_active > 25 {
            DiagnosticSeverity::High
        } else {
            DiagnosticSeverity::Low
        },
        Some("subscription fanout".to_string()),
        None,
        None,
        None,
        None,
        0,
        0,
        subscriptions_active as u64,
        subscriptions_active,
        vec![format!("{subscriptions_active} active subscription(s)")],
    )
}

pub(crate) fn rpc_operation_diagnostics(
    workers_registered: usize,
    requests_pending: usize,
    slowest_worker_average_latency_ms: Option<f64>,
) -> DiagnosticSnapshot {
    if workers_registered == 0 && requests_pending == 0 {
        return DiagnosticSnapshot::healthy();
    }

    let slowest_worker_average_latency_ms = slowest_worker_average_latency_ms.unwrap_or(0.0);
    let (label, trend, severity, bottleneck) = if workers_registered == 0 {
        (
            DiagnosisLabel::WorkerStarvation,
            DiagnosticTrend::Growing,
            DiagnosticSeverity::High,
            Some("worker starvation".to_string()),
        )
    } else if requests_pending > workers_registered && requests_pending >= workers_registered * 2 {
        (
            DiagnosisLabel::BacklogGrowth,
            DiagnosticTrend::Growing,
            DiagnosticSeverity::Medium,
            Some("route backlog".to_string()),
        )
    } else if requests_pending > workers_registered {
        (
            DiagnosisLabel::Contention,
            DiagnosticTrend::Growing,
            DiagnosticSeverity::Medium,
            Some("route contention".to_string()),
        )
    } else if slowest_worker_average_latency_ms >= 100.0 {
        (
            DiagnosisLabel::Throughput,
            DiagnosticTrend::Steady,
            if slowest_worker_average_latency_ms >= 250.0 {
                DiagnosticSeverity::High
            } else {
                DiagnosticSeverity::Medium
            },
            Some("slow worker latency".to_string()),
        )
    } else {
        (
            DiagnosisLabel::Throughput,
            DiagnosticTrend::Steady,
            DiagnosticSeverity::Low,
            Some("route throughput".to_string()),
        )
    };

    DiagnosticSnapshot::with_stage(
        label,
        trend,
        severity,
        bottleneck,
        None,
        None,
        None,
        None,
        0,
        0,
        requests_pending as u64,
        requests_pending,
        {
            let mut hints = Vec::new();
            if workers_registered > 0 {
                hints.push(format!("{workers_registered} registered worker(s)"));
            }
            if requests_pending > 0 {
                hints.push(format!("{requests_pending} pending request(s)"));
            }
            if slowest_worker_average_latency_ms > 0.0 {
                hints.push(format!(
                    "slowest worker average latency is {:.1}ms",
                    slowest_worker_average_latency_ms
                ));
            }
            hints
        },
    )
}

pub(crate) fn schedule_resource_diagnostics(
    enabled: bool,
    next_run: Option<&str>,
    last_run: Option<&str>,
    executions_total: u64,
) -> DiagnosticSnapshot {
    let now = Utc::now();
    let next_run_at = next_run.and_then(parse_rfc3339);
    let last_run_at = last_run.and_then(parse_rfc3339);
    let is_overdue = enabled && next_run_at.map(|next| next <= now).unwrap_or(false);
    let age_seconds = if is_overdue {
        next_run_at.map(|next| (now - next).num_seconds().max(0) as u64)
    } else {
        last_run_at.map(|last| (now - last).num_seconds().max(0) as u64)
    };

    if !enabled && next_run_at.is_none() && last_run_at.is_none() {
        return DiagnosticSnapshot::healthy();
    }

    let (label, trend, severity, bottleneck) = if is_overdue {
        (
            DiagnosisLabel::StaleHandoff,
            DiagnosticTrend::Stalled,
            DiagnosticSeverity::High,
            Some("durable handoff".to_string()),
        )
    } else if enabled && executions_total > 0 {
        (
            DiagnosisLabel::Throughput,
            DiagnosticTrend::Steady,
            DiagnosticSeverity::Medium,
            Some("scheduled work".to_string()),
        )
    } else if enabled {
        (
            DiagnosisLabel::Healthy,
            DiagnosticTrend::Steady,
            DiagnosticSeverity::Informational,
            None,
        )
    } else {
        (
            DiagnosisLabel::Healthy,
            DiagnosticTrend::Unknown,
            DiagnosticSeverity::Informational,
            None,
        )
    };

    let mut hints = Vec::new();
    if let Some(next) = next_run_at {
        hints.push(format!("next run at {}", rfc3339(next)));
    }
    if let Some(last) = last_run_at {
        hints.push(format!("last run at {}", rfc3339(last)));
    }
    if executions_total > 0 {
        hints.push(format!("{executions_total} total execution(s)"));
    }
    if is_overdue {
        hints.push("schedule is overdue".to_string());
    }

    DiagnosticSnapshot::with_stage(
        label,
        trend,
        severity,
        bottleneck,
        last_run_at.or(next_run_at),
        last_run_at,
        None,
        age_seconds,
        if last_run_at
            .map(|last| is_recent(last, now))
            .unwrap_or(false)
        {
            1
        } else {
            0
        },
        0,
        if is_overdue { 1 } else { 0 },
        if is_overdue { 1 } else { 0 },
        hints,
    )
}

fn build_resource_timeline(
    domain: &str,
    path: &ResourcePath<'_>,
    family: Option<u64>,
    diagnostics: DiagnosticSnapshot,
    limit: usize,
    mut candidates: Vec<TimelineCandidate>,
) -> ResourceTimeline {
    candidates.sort_by(|left, right| {
        right
            .observed_at
            .cmp(&left.observed_at)
            .then_with(|| left.priority.cmp(&right.priority))
            .then_with(|| left.event.summary.cmp(&right.event.summary))
    });

    let events = candidates
        .into_iter()
        .take(limit)
        .map(|candidate| candidate.event)
        .collect();

    ResourceTimeline::new(domain, path, family, diagnostics, limit, events)
}

fn timeline_candidate(
    observed_at: DateTime<Utc>,
    priority: u8,
    event: ResourceTimelineEvent,
) -> TimelineCandidate {
    TimelineCandidate {
        observed_at,
        priority,
        event,
    }
}

fn parse_session_from_mode(mode: &str) -> Option<String> {
    mode.split(':').nth(1).map(|value| value.to_string())
}

fn matches_resource_path(path: &ResourcePath<'_>, realm: &str, area: &str, resource: &str) -> bool {
    path.realm == realm && path.area == area && path.resource == resource
}

#[derive(Clone)]
struct OwnedRpcOperation {
    realm: String,
    area: String,
    resource: String,
    operation: String,
}

impl OwnedRpcOperation {
    fn matches_resource_path(&self, path: &ResourcePath<'_>) -> bool {
        self.realm == path.realm && self.area == path.area && self.resource == path.resource
    }
}

fn matches_resource_route(route: &str, path: &ResourcePath<'_>) -> bool {
    route_triplet(route)
        .is_some_and(|parts| matches_resource_path(path, parts.realm, parts.area, parts.resource))
}

fn parse_rpc_operation(route: &str) -> Option<OwnedRpcOperation> {
    route_quad(route).map(|parts| OwnedRpcOperation {
        realm: parts.realm.to_string(),
        area: parts.area.to_string(),
        resource: parts.resource.to_string(),
        operation: parts.operation.to_string(),
    })
}

pub(crate) fn kv_resource_timeline(
    transactions: &[KvTransaction],
    path: &ResourcePath<'_>,
    limit: usize,
) -> ResourceTimeline {
    let now = Utc::now();
    let mut candidates = Vec::new();
    let mut open_transactions = 0usize;
    let mut oldest_idle_seconds = 0u64;
    let mut latest_started_at: Option<DateTime<Utc>> = None;

    for tx in transactions
        .iter()
        .filter(|tx| matches_resource_path(path, &tx.realm, &tx.area, &tx.resource))
    {
        open_transactions += 1;
        oldest_idle_seconds = oldest_idle_seconds.max(tx.idle_seconds);
        let started_at = parse_rfc3339(&tx.started_at).unwrap_or(now);
        latest_started_at = Some(match latest_started_at {
            Some(current) => current.max(started_at),
            None => started_at,
        });
        let owner_session = parse_session_from_mode(&tx.mode);
        candidates.push(timeline_candidate(
            started_at,
            1,
            ResourceTimelineEvent::new(
                "kv",
                ResourceTimelineKind::Transition,
                started_at,
                format!(
                    "Transaction {} open{}",
                    tx.tx_id,
                    if tx.operations_count > 0 {
                        format!(" after {} operation(s)", tx.operations_count)
                    } else {
                        String::new()
                    }
                ),
                path,
                None,
                None,
                Some(tx.idle_seconds),
                owner_session,
                None,
                None,
                None,
                Some(tx.operations_count),
            ),
        ));
    }

    let diagnostics = kv_resource_diagnostics(open_transactions);
    if open_transactions > 0 {
        let summary = match latest_started_at {
            Some(started_at) => format!(
                "{} open transaction(s); oldest idle {}s; latest start {}",
                open_transactions,
                oldest_idle_seconds,
                rfc3339(started_at)
            ),
            None => format!(
                "{} open transaction(s); oldest idle {}s",
                open_transactions, oldest_idle_seconds
            ),
        };
        candidates.push(timeline_candidate(
            now,
            0,
            ResourceTimelineEvent::new(
                "kv",
                ResourceTimelineKind::Observation,
                now,
                summary,
                path,
                None,
                None,
                Some(oldest_idle_seconds),
                transactions
                    .iter()
                    .find(|tx| matches_resource_path(path, &tx.realm, &tx.area, &tx.resource))
                    .and_then(|tx| parse_session_from_mode(&tx.mode)),
                None,
                None,
                None,
                Some(open_transactions),
            ),
        ));
    }

    build_resource_timeline("kv", path, None, diagnostics, limit, candidates)
}

pub(crate) fn queue_resource_timeline(
    queues: &[QueueInfo],
    inflight: &[QueueInflight],
    dead_letters: &[QueueDeadLetter],
    path: &ResourcePath<'_>,
    family: Option<u64>,
    limit: usize,
) -> ResourceTimeline {
    let now = Utc::now();
    let matching_queues: Vec<_> = queues
        .iter()
        .filter(|queue| {
            matches_resource_path(path, &queue.realm, &queue.area, &queue.resource)
                && family.map(|value| queue.family == value).unwrap_or(true)
        })
        .collect();
    if matching_queues.is_empty() {
        return ResourceTimeline::new(
            "queue",
            path,
            family,
            DiagnosticSnapshot::healthy(),
            limit,
            Vec::new(),
        );
    }

    let matching_inflight: Vec<_> = inflight
        .iter()
        .filter(|item| {
            matches_resource_path(path, &item.realm, &item.area, &item.resource)
                && family.map(|value| item.family == value).unwrap_or(true)
        })
        .collect();
    let matching_dead_letters: Vec<_> = dead_letters
        .iter()
        .filter(|item| {
            matches_resource_path(path, &item.realm, &item.area, &item.resource)
                && family.map(|value| item.family == value).unwrap_or(true)
        })
        .collect();

    let mut candidates = Vec::new();
    let mut messages_ready = 0usize;
    let mut messages_delayed = 0usize;
    let mut messages_inflight = 0usize;
    let mut messages_dead_lettered = 0usize;
    let mut oldest_backlog_age_seconds = 0u64;
    let mut delay_age_buckets = QueueAgeBuckets::default();

    for queue in &matching_queues {
        messages_ready += queue.messages_ready;
        messages_delayed += queue.messages_delayed;
        messages_inflight += queue.messages_inflight;
        messages_dead_lettered += queue.messages_dead_lettered;
        oldest_backlog_age_seconds =
            oldest_backlog_age_seconds.max(queue.oldest_backlog_age_seconds);
        delay_age_buckets.merge(queue.delay_age_buckets);
    }

    let owner_sessions = matching_inflight
        .iter()
        .map(|item| item.session_id.clone())
        .filter(|session_id| !session_id.is_empty())
        .collect::<BTreeSet<_>>();
    let owner_session = owner_sessions.iter().next().cloned();
    let dead_letter_count = messages_dead_lettered.max(matching_dead_letters.len());
    let inflight_count = messages_inflight.max(matching_inflight.len());
    let backlog = messages_ready + messages_delayed;
    let diagnostics = queue_resource_diagnostics(
        messages_ready,
        messages_delayed,
        inflight_count,
        dead_letter_count,
        oldest_backlog_age_seconds,
        delay_age_buckets,
    );

    for dead_letter in matching_dead_letters {
        if let Some(observed_at) = parse_rfc3339(&dead_letter.dead_lettered_at) {
            candidates.push(timeline_candidate(
                observed_at,
                0,
                ResourceTimelineEvent::new(
                    "queue",
                    ResourceTimelineKind::Failure,
                    observed_at,
                    format!(
                        "Message {} dead-lettered after {} attempt(s){}",
                        dead_letter.message_id,
                        dead_letter.attempts,
                        if dead_letter.reason.is_empty() {
                            String::new()
                        } else {
                            format!(" ({})", dead_letter.reason)
                        }
                    ),
                    path,
                    Some(dead_letter.family),
                    None,
                    None,
                    None,
                    None,
                    None,
                    Some(dead_letter.message_id),
                    Some(dead_letter.attempts),
                ),
            ));
        }
    }

    if inflight_count > 0 {
        candidates.push(timeline_candidate(
            now,
            1,
            ResourceTimelineEvent::new(
                "queue",
                ResourceTimelineKind::OwnershipChange,
                now,
                if let Some(owner_session) = owner_session.clone() {
                    format!(
                        "{} inflight message(s) owned by session {}",
                        inflight_count, owner_session
                    )
                } else {
                    format!("{} inflight message(s)", inflight_count)
                },
                path,
                family,
                None,
                Some(oldest_backlog_age_seconds),
                owner_session.clone(),
                None,
                None,
                None,
                Some(inflight_count),
            ),
        ));
    }

    if backlog > 0 || dead_letter_count > 0 || inflight_count > 0 {
        candidates.push(timeline_candidate(
            now,
            2,
            ResourceTimelineEvent::new(
                "queue",
                ResourceTimelineKind::Observation,
                now,
                format!(
                    "{} ready, {} delayed, {} inflight, {} dead-lettered; oldest backlog message {}s old",
                    messages_ready,
                    messages_delayed,
                    inflight_count,
                    dead_letter_count,
                    oldest_backlog_age_seconds
                ),
                path,
                family,
                None,
                Some(oldest_backlog_age_seconds),
                owner_session,
                None,
                None,
                None,
                Some(backlog),
            ),
        ));
    }

    if delay_age_buckets.over_15m > 0 {
        candidates.push(timeline_candidate(
            now,
            3,
            ResourceTimelineEvent::new(
                "queue",
                ResourceTimelineKind::Observation,
                now,
                format!(
                    "{} delayed message(s) are 15m+ old",
                    delay_age_buckets.over_15m
                ),
                path,
                family,
                None,
                None,
                None,
                None,
                None,
                None,
                Some(delay_age_buckets.over_15m),
            ),
        ));
    }

    build_resource_timeline("queue", path, family, diagnostics, limit, candidates)
}

pub(crate) fn stream_resource_timeline(
    streams: &[StreamInfo],
    path: &ResourcePath<'_>,
    limit: usize,
) -> ResourceTimeline {
    let now = Utc::now();
    let stream = streams
        .iter()
        .find(|item| matches_resource_path(path, &item.realm, &item.area, &item.resource));
    let Some(stream) = stream else {
        return ResourceTimeline::new(
            "stream",
            path,
            None,
            DiagnosticSnapshot::healthy(),
            limit,
            Vec::new(),
        );
    };

    let diagnostics =
        stream_resource_diagnostics(stream.offset, stream.watermark, stream.sessions_active);
    let lag = stream.offset.saturating_sub(stream.watermark);
    let mut candidates = Vec::new();
    candidates.push(timeline_candidate(
        now,
        0,
        ResourceTimelineEvent::new(
            "stream",
            ResourceTimelineKind::Observation,
            now,
            if lag == 0 {
                format!(
                    "Stream is caught up with {} live append session(s)",
                    stream.sessions_active
                )
            } else {
                format!(
                    "Offset {}, watermark {}, lag {} event(s), {} live append session(s)",
                    stream.offset, stream.watermark, lag, stream.sessions_active
                )
            },
            path,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ),
    ));

    build_resource_timeline("stream", path, None, diagnostics, limit, candidates)
}

pub(crate) fn lease_resource_timeline(
    leases: &[LeaseInfo],
    path: &ResourcePath<'_>,
    limit: usize,
) -> ResourceTimeline {
    let now = Utc::now();
    let matching_leases: Vec<_> = leases
        .iter()
        .filter(|lease| matches_resource_path(path, &lease.realm, &lease.area, &lease.resource))
        .collect();
    if matching_leases.is_empty() {
        return ResourceTimeline::new(
            "lease",
            path,
            None,
            DiagnosticSnapshot::healthy(),
            limit,
            Vec::new(),
        );
    }

    let active_leases = matching_leases.len();
    let renewals_total = matching_leases
        .iter()
        .map(|lease| lease.renewals)
        .sum::<usize>();
    let mut candidates = Vec::new();
    let mut oldest_age_seconds = 0u64;
    let mut next_expiry_seconds: Option<u64> = None;

    for lease in &matching_leases {
        if let Some(acquired_at) = parse_rfc3339(&lease.acquired_at) {
            let age_seconds = (now - acquired_at).num_seconds().max(0) as u64;
            oldest_age_seconds = oldest_age_seconds.max(age_seconds);
            candidates.push(timeline_candidate(
                acquired_at,
                0,
                ResourceTimelineEvent::new(
                    "lease",
                    ResourceTimelineKind::OwnershipChange,
                    acquired_at,
                    format!(
                        "Lease owned by {} until {}",
                        lease.owner_session_id, lease.expires_at
                    ),
                    path,
                    None,
                    None,
                    Some(age_seconds),
                    Some(lease.owner_session_id.clone()),
                    None,
                    None,
                    None,
                    Some(lease.renewals),
                ),
            ));
        }

        if let Some(expires_at) = parse_rfc3339(&lease.expires_at) {
            let remaining_seconds = (expires_at - now).num_seconds().max(0) as u64;
            next_expiry_seconds = Some(match next_expiry_seconds {
                Some(current) => current.min(remaining_seconds),
                None => remaining_seconds,
            });
        }
    }

    let diagnostics =
        lease_resource_diagnostics(active_leases, Some(oldest_age_seconds), renewals_total);
    if active_leases > 0 {
        let summary = match next_expiry_seconds {
            Some(remaining_seconds) => format!(
                "{} active lease(s); next expiry in {}s; {} renewal(s)",
                active_leases, remaining_seconds, renewals_total
            ),
            None => format!(
                "{} active lease(s); {} renewal(s)",
                active_leases, renewals_total
            ),
        };
        candidates.push(timeline_candidate(
            now,
            1,
            ResourceTimelineEvent::new(
                "lease",
                ResourceTimelineKind::Observation,
                now,
                summary,
                path,
                None,
                None,
                Some(oldest_age_seconds),
                matching_leases
                    .first()
                    .map(|lease| lease.owner_session_id.clone()),
                None,
                None,
                None,
                Some(active_leases),
            ),
        ));
    }

    build_resource_timeline("lease", path, None, diagnostics, limit, candidates)
}

pub(crate) fn notice_resource_timeline(
    subscriptions: &[NoticeSubscription],
    routes: &[NoticeRouteInfo],
    path: &ResourcePath<'_>,
    limit: usize,
) -> ResourceTimeline {
    let now = Utc::now();
    let matching_subscriptions: Vec<_> = subscriptions
        .iter()
        .filter(|subscription| matches_resource_route(&subscription.pattern, path))
        .collect();
    let matching_routes: Vec<_> = routes
        .iter()
        .filter(|route| matches_resource_route(&route.route, path))
        .collect();

    if matching_subscriptions.is_empty() && matching_routes.is_empty() {
        return ResourceTimeline::new(
            "notice",
            path,
            None,
            DiagnosticSnapshot::healthy(),
            limit,
            Vec::new(),
        );
    }

    let subscriptions_active = matching_subscriptions.len();
    let diagnostics = notice_resource_diagnostics(subscriptions_active);
    let publishes_per_minute = matching_routes
        .iter()
        .map(|route| route.publishes_per_minute)
        .fold(0.0_f64, f64::max);
    let publishes_total = matching_routes
        .iter()
        .map(|route| route.publishes_total)
        .sum::<u64>();
    let mut candidates = Vec::new();

    for subscription in &matching_subscriptions {
        if let Some(created_at) = parse_rfc3339(&subscription.created_at) {
            let age_seconds = (now - created_at).num_seconds().max(0) as u64;
            candidates.push(timeline_candidate(
                created_at,
                0,
                ResourceTimelineEvent::new(
                    "notice",
                    ResourceTimelineKind::Registration,
                    created_at,
                    format!(
                        "Subscription {} created for {}",
                        subscription.subscription_id, subscription.pattern
                    ),
                    path,
                    None,
                    None,
                    Some(age_seconds),
                    Some(subscription.session_id.clone()),
                    None,
                    None,
                    None,
                    Some(subscription.notifications_received as usize),
                ),
            ));
        }
    }

    if subscriptions_active > 0 || publishes_total > 0 {
        let summary = if publishes_total > 0 {
            format!(
                "{} subscriber(s); {:.1} publish(es)/min; {} total publish(es)",
                subscriptions_active, publishes_per_minute, publishes_total
            )
        } else {
            format!("{} subscriber(s)", subscriptions_active)
        };
        candidates.push(timeline_candidate(
            now,
            1,
            ResourceTimelineEvent::new(
                "notice",
                ResourceTimelineKind::Observation,
                now,
                summary,
                path,
                None,
                None,
                None,
                matching_subscriptions
                    .first()
                    .map(|subscription| subscription.session_id.clone()),
                None,
                None,
                None,
                Some(subscriptions_active),
            ),
        ));
    }

    build_resource_timeline("notice", path, None, diagnostics, limit, candidates)
}

pub(crate) fn rpc_resource_timeline(
    workers: &[RpcWorker],
    pending: &[RpcPendingRequest],
    path: &ResourcePath<'_>,
    limit: usize,
) -> ResourceTimeline {
    let now = Utc::now();
    let matching_workers: Vec<_> = workers
        .iter()
        .filter(|worker| {
            parse_rpc_operation(&worker.route)
                .is_some_and(|parsed| parsed.matches_resource_path(path))
        })
        .collect();
    let matching_pending: Vec<_> = pending
        .iter()
        .filter(|request| {
            parse_rpc_operation(&request.route)
                .is_some_and(|parsed| parsed.matches_resource_path(path))
        })
        .collect();

    if matching_workers.is_empty() && matching_pending.is_empty() {
        return ResourceTimeline::new(
            "rpc",
            path,
            None,
            DiagnosticSnapshot::healthy(),
            limit,
            Vec::new(),
        );
    }

    let workers_registered = matching_workers.len();
    let requests_pending = matching_pending.len();
    let latency_summary = summarize_rpc_worker_latency(matching_workers.iter().copied());
    let diagnostics = rpc_operation_diagnostics(
        workers_registered,
        requests_pending,
        Some(latency_summary.slowest_worker_average_latency_ms),
    );
    let mut candidates = Vec::new();
    let mut oldest_pending_age = 0u64;

    for worker in &matching_workers {
        if let Some(registered_at) = parse_rfc3339(&worker.registered_at) {
            candidates.push(timeline_candidate(
                registered_at,
                0,
                ResourceTimelineEvent::new(
                    "rpc",
                    ResourceTimelineKind::Registration,
                    registered_at,
                    format!(
                        "Worker {} registered on {}",
                        worker.session_id, worker.route
                    ),
                    path,
                    None,
                    parse_rpc_operation(&worker.route).map(|operation| operation.operation),
                    Some(
                        now.signed_duration_since(registered_at)
                            .num_seconds()
                            .max(0) as u64,
                    ),
                    None,
                    Some(worker.session_id.clone()),
                    None,
                    None,
                    Some(worker.requests_handled as usize),
                ),
            ));
        }
    }

    for request in matching_pending {
        if let Some(submitted_at) = parse_rfc3339(&request.submitted_at) {
            let age_seconds = (now - submitted_at).num_seconds().max(0) as u64;
            oldest_pending_age = oldest_pending_age.max(age_seconds);
            candidates.push(timeline_candidate(
                submitted_at,
                1,
                ResourceTimelineEvent::new(
                    "rpc",
                    ResourceTimelineKind::Transition,
                    submitted_at,
                    if let Some(worker_session_id) = &request.worker_session_id {
                        format!(
                            "Request {} pending for {}s; waiting on worker {}",
                            request.correlation_id, age_seconds, worker_session_id
                        )
                    } else {
                        format!(
                            "Request {} pending for {}s",
                            request.correlation_id, age_seconds
                        )
                    },
                    path,
                    None,
                    parse_rpc_operation(&request.route).map(|operation| operation.operation),
                    Some(age_seconds),
                    None,
                    request.worker_session_id.clone(),
                    Some(request.correlation_id.clone()),
                    None,
                    None,
                ),
            ));
        }
    }

    if workers_registered == 0 && requests_pending > 0 {
        candidates.push(timeline_candidate(
            now,
            0,
            ResourceTimelineEvent::new(
                "rpc",
                ResourceTimelineKind::Observation,
                now,
                format!(
                    "{} pending request(s); worker starvation and oldest request {}s old",
                    requests_pending, oldest_pending_age
                ),
                path,
                None,
                None,
                Some(oldest_pending_age),
                None,
                None,
                None,
                None,
                Some(requests_pending),
            ),
        ));
    } else if requests_pending > 0 || workers_registered > 0 {
        let latency_note = if latency_summary.slowest_worker_average_latency_ms > 0.0 {
            format!(
                "; slowest worker avg latency {:.1}ms",
                latency_summary.slowest_worker_average_latency_ms
            )
        } else {
            String::new()
        };
        let summary = if requests_pending > 0 {
            format!(
                "{} worker(s), {} pending request(s); oldest request {}s old{}",
                workers_registered, requests_pending, oldest_pending_age, latency_note
            )
        } else {
            format!(
                "{} worker(s) registered{}",
                workers_registered, latency_note
            )
        };
        candidates.push(timeline_candidate(
            now,
            2,
            ResourceTimelineEvent::new(
                "rpc",
                ResourceTimelineKind::Observation,
                now,
                summary,
                path,
                None,
                None,
                Some(oldest_pending_age),
                matching_workers
                    .first()
                    .map(|worker| worker.session_id.clone()),
                matching_workers
                    .first()
                    .map(|worker| worker.session_id.clone()),
                None,
                None,
                Some(requests_pending),
            ),
        ));
    }

    build_resource_timeline("rpc", path, None, diagnostics, limit, candidates)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn schedule_resource_timeline(
    schedules: &[ScheduleInfo],
    pending_fire_claims: usize,
    pending_ack_retries: usize,
    oldest_pending_claim_age_seconds: u64,
    notify_failures: u64,
    ack_failures: u64,
    overdue_normalizations: u64,
    pending_claims_expired_total: u64,
    pending_claim_cleanup_failures_total: u64,
    path: &ResourcePath<'_>,
    limit: usize,
) -> ResourceTimeline {
    let now = Utc::now();
    let matching_schedules: Vec<_> = schedules
        .iter()
        .filter(|schedule| {
            matches_resource_path(path, &schedule.realm, &schedule.area, &schedule.resource)
        })
        .collect();
    if matching_schedules.is_empty() {
        return ResourceTimeline::new(
            "schedule",
            path,
            None,
            DiagnosticSnapshot::healthy(),
            limit,
            Vec::new(),
        );
    }

    let mut candidates = Vec::new();
    let mut enabled = false;
    let mut latest_run_at: Option<DateTime<Utc>> = None;
    let mut latest_next_run_at: Option<DateTime<Utc>> = None;

    for schedule in &matching_schedules {
        enabled |= schedule.enabled;
        let next_run = parse_rfc3339(&schedule.next_run);
        let last_run = schedule.last_run.as_deref().and_then(parse_rfc3339);

        if let Some(last_run) = last_run {
            latest_run_at = Some(match latest_run_at {
                Some(current) => current.max(last_run),
                None => last_run,
            });
            candidates.push(timeline_candidate(
                last_run,
                0,
                ResourceTimelineEvent::new(
                    "schedule",
                    ResourceTimelineKind::Transition,
                    last_run,
                    format!(
                        "Schedule {} last ran at {}",
                        schedule.operation,
                        schedule.last_run.as_deref().unwrap_or("")
                    ),
                    path,
                    None,
                    Some(schedule.operation.clone()),
                    Some((now - last_run).num_seconds().max(0) as u64),
                    None,
                    None,
                    None,
                    None,
                    Some(schedule.executions_total as usize),
                ),
            ));
        }

        if let Some(next_run) = next_run {
            latest_next_run_at = Some(match latest_next_run_at {
                Some(current) => current.max(next_run),
                None => next_run,
            });
        }
    }

    let latest_next_run = latest_next_run_at.map(rfc3339);
    let latest_last_run = latest_run_at.map(rfc3339);
    let diagnostics = schedule_resource_diagnostics(
        enabled,
        latest_next_run.as_deref(),
        latest_last_run.as_deref(),
        matching_schedules
            .iter()
            .map(|schedule| schedule.executions_total)
            .sum(),
    );
    let age_seconds = diagnostics.age_seconds;
    let overdue = diagnostics.diagnosis_label() == DiagnosisLabel::StaleHandoff;
    if enabled
        || overdue
        || pending_fire_claims > 0
        || notify_failures > 0
        || ack_failures > 0
        || pending_claims_expired_total > 0
        || pending_claim_cleanup_failures_total > 0
    {
        let mut summary = if overdue {
            match latest_next_run_at {
                Some(next_run) => format!(
                    "{} overdue by {}s",
                    matching_schedules
                        .first()
                        .map(|schedule| schedule.operation.as_str())
                        .unwrap_or("schedule"),
                    (now - next_run).num_seconds().max(0)
                ),
                None => format!(
                    "{} is overdue",
                    matching_schedules
                        .first()
                        .map(|schedule| schedule.operation.as_str())
                        .unwrap_or("schedule")
                ),
            }
        } else if let Some(next_run) = latest_next_run_at {
            format!(
                "{} next runs at {}",
                matching_schedules
                    .first()
                    .map(|schedule| schedule.operation.as_str())
                    .unwrap_or("schedule"),
                rfc3339(next_run)
            )
        } else {
            format!(
                "{} enabled with {} execution(s)",
                matching_schedules
                    .first()
                    .map(|schedule| schedule.operation.as_str())
                    .unwrap_or("schedule"),
                matching_schedules
                    .iter()
                    .map(|schedule| schedule.executions_total)
                    .sum::<u64>()
            )
        };

        let mut pressure_notes = Vec::new();
        if pending_fire_claims > 0 {
            pressure_notes.push(format!("{pending_fire_claims} pending fire claim(s)"));
        }
        if pending_ack_retries > 0 {
            pressure_notes.push(format!("{pending_ack_retries} pending ack retry(s)"));
        }
        if oldest_pending_claim_age_seconds > 0 {
            pressure_notes.push(format!(
                "oldest pending claim {}s old",
                oldest_pending_claim_age_seconds
            ));
        }
        if notify_failures > 0 {
            pressure_notes.push(format!("{notify_failures} notify failure(s)"));
        }
        if ack_failures > 0 {
            pressure_notes.push(format!("{ack_failures} ack failure(s)"));
        }
        if overdue_normalizations > 0 {
            pressure_notes.push(format!("{overdue_normalizations} overdue normalization(s)"));
        }
        if pending_claims_expired_total > 0 {
            pressure_notes.push(format!(
                "{pending_claims_expired_total} expired pending claim(s)"
            ));
        }
        if pending_claim_cleanup_failures_total > 0 {
            pressure_notes.push(format!(
                "{pending_claim_cleanup_failures_total} pending claim cleanup failure(s)"
            ));
        }
        if !pressure_notes.is_empty() {
            summary.push_str("; ");
            summary.push_str(&pressure_notes.join(", "));
        }

        candidates.push(timeline_candidate(
            now,
            1,
            ResourceTimelineEvent::new(
                "schedule",
                if overdue {
                    ResourceTimelineKind::StateFlip
                } else {
                    ResourceTimelineKind::Observation
                },
                now,
                summary,
                path,
                None,
                matching_schedules
                    .first()
                    .map(|schedule| schedule.operation.clone()),
                age_seconds,
                None,
                None,
                None,
                None,
                Some(matching_schedules.len()),
            ),
        ));
    }

    build_resource_timeline("schedule", path, None, diagnostics, limit, candidates)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::admin::QueueAgeBuckets;

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
            messages_ready: 0,
            messages_delayed: 0,
            messages_inflight: 0,
            messages_dead_lettered: 2,
            messages_total: 2,
            oldest_message_age_seconds: 0,
            oldest_backlog_age_seconds: 0,
            backlog_age_buckets: QueueAgeBuckets::default(),
            delay_age_buckets: QueueAgeBuckets::default(),
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
                session_id: "9001".to_string(),
                realm: "prod".to_string(),
                route: "rpc://prod/api/users/get".to_string(),
                registered_at: "2026-03-14T12:00:00Z".to_string(),
                requests_handled: 12,
                average_latency_ms: 4.5,
            },
            RpcWorker {
                session_id: "9002".to_string(),
                realm: "prod".to_string(),
                route: "rpc://prod/api/users/get".to_string(),
                registered_at: "2026-03-14T12:00:01Z".to_string(),
                requests_handled: 13,
                average_latency_ms: 22.0,
            },
            RpcWorker {
                session_id: "9003".to_string(),
                realm: "prod".to_string(),
                route: "rpc://prod/api/users/get".to_string(),
                registered_at: "2026-03-14T12:00:02Z".to_string(),
                requests_handled: 14,
                average_latency_ms: 63.0,
            },
            RpcWorker {
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
        assert_eq!(summary.slowest_worker_average_latency_ms, 125.0);
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
        let analysis = analyze_rpc(&[], &[], 0, 0, 0, 0, 3, 2, 0, now);
        let hotspot = analysis.hotspots.first().expect("rpc hotspot");

        // Assert
        assert_eq!(hotspot.hotspot.snapshot.current_stage, "data_loss_risk");
        assert_eq!(
            hotspot.hotspot.snapshot.diagnosis_label(),
            DiagnosisLabel::DataLossRisk
        );
        assert_eq!(
            hotspot.hotspot.snapshot.likely_bottleneck.as_deref(),
            Some("late response drop")
        );
        assert_eq!(hotspot.hotspot.snapshot.severity, DiagnosticSeverity::High);
        assert!(hotspot
            .hotspot
            .snapshot
            .explanation_hints
            .iter()
            .any(|hint| hint.contains("late response drop")));
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
                subscription_id: 1,
                session_id: "sub-1".to_string(),
                realm: "prod".to_string(),
                pattern: "notice://prod/events/orders/created".to_string(),
                created_at: (now - Duration::seconds(5)).to_rfc3339(),
                notifications_received: 3,
            },
            NoticeSubscription {
                subscription_id: 2,
                session_id: "sub-2".to_string(),
                realm: "prod".to_string(),
                pattern: "notice://prod/events/orders/created".to_string(),
                created_at: (now - Duration::seconds(3)).to_rfc3339(),
                notifications_received: 1,
            },
            NoticeSubscription {
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
                route: "notice://prod/events/orders/created".to_string(),
                subscribers: 2,
                publishes_total: 0,
                publishes_per_minute: 0.0,
            },
            NoticeRouteInfo {
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
            realm: "prod".to_string(),
            area: "jobs".to_string(),
            resource: "billing".to_string(),
            operation: "send".to_string(),
            cron: "0 * * * *".to_string(),
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
            0,
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
    fn should_classify_schedule_cleanup_pressure() {
        // Arrange
        let now = Utc::now();
        let schedules = vec![ScheduleInfo {
            realm: "prod".to_string(),
            area: "jobs".to_string(),
            resource: "billing".to_string(),
            operation: "send".to_string(),
            cron: "0 * * * *".to_string(),
            next_run: (now + Duration::seconds(30)).to_rfc3339(),
            last_run: None,
            executions_total: 2,
            enabled: true,
        }];

        // Act
        let analysis = analyze_schedule(
            &schedules,
            0,
            0,
            0,
            ScheduleLatencyBuckets::default(),
            0,
            0,
            0,
            3,
            1,
            now,
        );
        let hotspot = analysis.hotspots.first().expect("schedule hotspot");

        // Assert
        assert_eq!(hotspot.hotspot.snapshot.current_stage, "stale_handoff");
        assert_eq!(
            hotspot.hotspot.snapshot.likely_bottleneck.as_deref(),
            Some("claim cleanup")
        );
        assert_eq!(hotspot.hotspot.snapshot.severity, DiagnosticSeverity::High);
        assert!(hotspot
            .hotspot
            .snapshot
            .explanation_hints
            .iter()
            .any(|hint| hint.contains("expired pending claim")));
        assert!(hotspot
            .hotspot
            .snapshot
            .explanation_hints
            .iter()
            .any(|hint| hint.contains("cleanup failure")));
    }

    #[test]
    fn should_classify_schedule_latency_pressure() {
        // Arrange
        let now = Utc::now();
        let schedules = vec![ScheduleInfo {
            realm: "prod".to_string(),
            area: "jobs".to_string(),
            resource: "billing".to_string(),
            operation: "send".to_string(),
            cron: "0 * * * *".to_string(),
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
        let analysis = analyze_schedule(
            &schedules,
            0,
            0,
            0,
            request_latency_buckets,
            0,
            0,
            0,
            0,
            0,
            now,
        );
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
        let summary = summarize_incident(&Some(hotspot));

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
            snapshot: DiagnosticSnapshot::with_stage(
                DiagnosisLabel::Throughput,
                DiagnosticTrend::Growing,
                DiagnosticSeverity::Medium,
                Some("router saturation".to_string()),
                None,
                None,
                None,
                None,
                0,
                0,
                0,
                0,
                vec!["5 router mailbox saturation event(s)".to_string()],
            ),
        };

        // Act
        let summary = summarize_incident(&Some(hotspot));

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
            "inspect /metrics"
        );
    }
}
