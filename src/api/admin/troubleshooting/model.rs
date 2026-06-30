use crate::api::admin::ResourcePath;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::{is_recent, rfc3339, score_usize};

pub(crate) const RECENT_WINDOW_SECS: i64 = 300;

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
    pub(crate) const fn as_str(self) -> &'static str {
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

    pub(crate) const fn display_name(self) -> &'static str {
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

    pub(crate) const fn explanation_hint(self) -> &'static str {
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

    pub(crate) const fn durability_hint(self) -> Option<&'static str> {
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

    pub(super) fn from_stage(stage: &str) -> Option<Self> {
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

pub(crate) fn canonical_explanation_hints(
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
    pub(super) fn healthy() -> Self {
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

    pub(super) fn with_stage(input: DiagnosticSnapshotInput) -> Self {
        let (confidence, confidence_justification) = calculate_confidence(ConfidenceInput {
            current_stage: input.current_stage,
            likely_bottleneck: input.likely_bottleneck.as_deref(),
            last_changed_at: input.last_changed_at,
            last_success_at: input.last_success_at,
            last_failure_at: input.last_failure_at,
            age_seconds: input.age_seconds,
            recent_transition_count: input.recent_transition_count,
            failure_count: input.failure_count,
            contention_count: input.contention_count,
            waiter_count: input.waiter_count,
        });

        Self {
            current_stage: input.current_stage.as_str().to_string(),
            trend: input.trend,
            severity: input.severity,
            likely_bottleneck: input.likely_bottleneck,
            last_changed_at: input.last_changed_at.map(rfc3339),
            last_success_at: input.last_success_at.map(rfc3339),
            last_failure_at: input.last_failure_at.map(rfc3339),
            age_seconds: input.age_seconds,
            recent_transition_count: input.recent_transition_count,
            failure_count: input.failure_count,
            contention_count: input.contention_count,
            waiter_count: input.waiter_count,
            confidence,
            confidence_justification: Some(confidence_justification),
            explanation_hints: canonical_explanation_hints(
                input.current_stage,
                input.explanation_hints,
            ),
            delta_5m: None,
            delta_1h: None,
        }
    }

    pub(super) fn diagnosis_label(&self) -> DiagnosisLabel {
        DiagnosisLabel::from_stage(&self.current_stage).unwrap_or(DiagnosisLabel::Healthy)
    }

    #[cfg(test)]
    pub(super) fn is_healthy(&self) -> bool {
        matches!(self.severity, DiagnosticSeverity::Informational)
            && self.likely_bottleneck.is_none()
            && self.diagnosis_label() == DiagnosisLabel::Healthy
    }
}

pub(crate) fn calculate_confidence(input: ConfidenceInput<'_>) -> (f64, ConfidenceJustification) {
    let signals = gather_confidence_signals(&ConfidenceSignalInput {
        failure_count: input.failure_count,
        contention_count: input.contention_count,
        waiter_count: input.waiter_count,
        likely_bottleneck: input.likely_bottleneck,
        age_seconds: input.age_seconds,
        recent_transition_count: input.recent_transition_count,
        last_changed_at: input.last_changed_at,
        last_success_at: input.last_success_at,
        last_failure_at: input.last_failure_at,
    });
    let freshness_signal = freshness_signal(
        signals.last_changed_at,
        signals.last_success_at,
        signals.last_failure_at,
        input.age_seconds,
        input.recent_transition_count,
    );
    let (primary_signal_name, primary_signal, coverage_target) =
        primary_signal_for_stage(input.current_stage, &signals);

    let observed_support = count_observed_signals(&signals);
    let coverage_ratio = coverage_signal_ratio(observed_support, coverage_target);
    let coverage_signal = coverage_target == 0 || observed_support >= coverage_target;
    let freshness_score = freshness_score(freshness_signal, &signals, input.age_seconds);

    let (mut signals_matched, mut signals_missing) =
        evaluate_signal_alignment(input.current_stage, primary_signal_name, primary_signal);
    maybe_add_signal(
        &mut signals_matched,
        &mut signals_missing,
        "fresh_telemetry",
        freshness_signal,
    );
    maybe_add_signal(
        &mut signals_matched,
        &mut signals_missing,
        &format!("rule_coverage_{observed_support}_of_{coverage_target}"),
        coverage_signal,
    );
    maybe_add_bottleneck_signal(
        input.current_stage,
        &mut signals_matched,
        &mut signals_missing,
        &signals,
    );

    let confidence = confidence_score(
        input.current_stage,
        primary_signal,
        coverage_ratio,
        freshness_score,
        signals.bottleneck_signal(),
    );
    let matched_summary = summarize_signal_names(&signals_matched);
    let missing_summary = summarize_signal_names(&signals_missing);
    (
        confidence,
        ConfidenceJustification {
            signals_matched,
            signals_missing,
            rationale: format!(
                "{} confidence is derived from observed signals, telemetry freshness, and rule coverage. Matched: {}. Missing: {}.",
                input.current_stage.display_name(),
                matched_summary,
                missing_summary,
            ),
        },
    )
}

#[derive(Clone, Copy)]
pub(crate) struct ConfidenceInput<'a> {
    pub(crate) current_stage: DiagnosisLabel,
    pub(crate) likely_bottleneck: Option<&'a str>,
    pub(crate) last_changed_at: Option<DateTime<Utc>>,
    pub(crate) last_success_at: Option<DateTime<Utc>>,
    pub(crate) last_failure_at: Option<DateTime<Utc>>,
    pub(crate) age_seconds: Option<u64>,
    pub(crate) recent_transition_count: u64,
    pub(crate) failure_count: u64,
    pub(crate) contention_count: u64,
    pub(crate) waiter_count: usize,
}

pub(crate) struct DiagnosticSnapshotInput {
    pub(crate) current_stage: DiagnosisLabel,
    pub(crate) trend: DiagnosticTrend,
    pub(crate) severity: DiagnosticSeverity,
    pub(crate) likely_bottleneck: Option<String>,
    pub(crate) last_changed_at: Option<DateTime<Utc>>,
    pub(crate) last_success_at: Option<DateTime<Utc>>,
    pub(crate) last_failure_at: Option<DateTime<Utc>>,
    pub(crate) age_seconds: Option<u64>,
    pub(crate) recent_transition_count: u64,
    pub(crate) failure_count: u64,
    pub(crate) contention_count: u64,
    pub(crate) waiter_count: usize,
    pub(crate) explanation_hints: Vec<String>,
}

struct ConfidenceSignals {
    signal_bits: u8,
    last_changed_at: Option<DateTime<Utc>>,
    last_success_at: Option<DateTime<Utc>>,
    last_failure_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Copy)]
struct ConfidenceSignalInput<'a> {
    failure_count: u64,
    contention_count: u64,
    waiter_count: usize,
    likely_bottleneck: Option<&'a str>,
    age_seconds: Option<u64>,
    recent_transition_count: u64,
    last_changed_at: Option<DateTime<Utc>>,
    last_success_at: Option<DateTime<Utc>>,
    last_failure_at: Option<DateTime<Utc>>,
}

fn gather_confidence_signals(input: &ConfidenceSignalInput<'_>) -> ConfidenceSignals {
    let mut signal_bits = 0u8;
    if input.failure_count > 0 || input.last_failure_at.is_some() {
        signal_bits |= 0b0001;
    }
    if input.contention_count > 0 || input.waiter_count > 0 {
        signal_bits |= 0b0010;
    }
    if input.age_seconds.unwrap_or(0) > 0 {
        signal_bits |= 0b0100;
    }
    if input.recent_transition_count > 0 || input.last_changed_at.is_some() {
        signal_bits |= 0b1000;
    }
    if input.likely_bottleneck.is_some() {
        signal_bits |= 0b1_0000;
    }
    ConfidenceSignals {
        signal_bits,
        last_changed_at: input.last_changed_at,
        last_success_at: input.last_success_at,
        last_failure_at: input.last_failure_at,
    }
}

impl ConfidenceSignals {
    fn failure_signal(&self) -> bool {
        self.signal_bits & 0b0001 != 0
    }

    fn contention_signal(&self) -> bool {
        self.signal_bits & 0b0010 != 0
    }

    fn age_signal(&self) -> bool {
        self.signal_bits & 0b0100 != 0
    }

    fn transition_signal(&self) -> bool {
        self.signal_bits & 0b1000 != 0
    }

    fn bottleneck_signal(&self) -> bool {
        self.signal_bits & 0b1_0000 != 0
    }
}

fn freshness_signal(
    last_changed_at: Option<DateTime<Utc>>,
    last_success_at: Option<DateTime<Utc>>,
    last_failure_at: Option<DateTime<Utc>>,
    age_seconds: Option<u64>,
    recent_transition_count: u64,
) -> bool {
    let now = Utc::now();
    [last_changed_at, last_success_at, last_failure_at]
        .into_iter()
        .flatten()
        .any(|timestamp| is_recent(timestamp, now))
        || age_seconds
            .is_some_and(|age| age <= u64::try_from(RECENT_WINDOW_SECS).unwrap_or(u64::MAX))
        || recent_transition_count > 0
}

fn primary_signal_for_stage(
    current_stage: DiagnosisLabel,
    signals: &ConfidenceSignals,
) -> (&'static str, bool, usize) {
    match current_stage {
        DiagnosisLabel::Healthy => (
            "no_active_pressure",
            !signals.failure_signal() && !signals.contention_signal() && !signals.bottleneck_signal(),
            0,
        ),
        DiagnosisLabel::Contention => (
            "contention_or_waiters_present",
            signals.contention_signal(),
            2,
        ),
        DiagnosisLabel::WorkerStarvation => (
            "waiters_without_capacity",
            signals.contention_signal() || signals.age_signal(),
            2,
        ),
        DiagnosisLabel::BacklogGrowth => (
            "backlog_age_or_waiters_present",
            signals.contention_signal() || signals.age_signal(),
            2,
        ),
        DiagnosisLabel::DeadLetterPressure | DiagnosisLabel::DataLossRisk => {
            ("failure_signal_present", signals.failure_signal(), 2)
        }
        DiagnosisLabel::StaleHandoff => (
            "staleness_signal_present",
            signals.age_signal() || signals.transition_signal(),
            2,
        ),
        DiagnosisLabel::Throughput => (
            "throughput_pressure_present",
            signals.age_signal()
                || signals.contention_signal()
                || signals.transition_signal()
                || signals.bottleneck_signal(),
            1,
        ),
    }
}

fn count_observed_signals(signals: &ConfidenceSignals) -> usize {
    [
        signals.failure_signal(),
        signals.contention_signal(),
        signals.age_signal(),
        signals.transition_signal(),
        signals.bottleneck_signal(),
    ]
    .into_iter()
    .filter(|signal| *signal)
    .count()
}

fn coverage_signal_ratio(observed_support: usize, coverage_target: usize) -> f64 {
    if coverage_target == 0 {
        1.0
    } else {
        (score_usize(observed_support) / score_usize(coverage_target)).min(1.0)
    }
}

fn freshness_score(
    freshness_signal: bool,
    signals: &ConfidenceSignals,
    age_seconds: Option<u64>,
) -> f64 {
    if freshness_signal {
        1.0
    } else if signals.transition_signal()
        || signals.last_success_at.is_some()
        || signals.last_failure_at.is_some()
        || age_seconds.unwrap_or(0) > 0
    {
        0.6
    } else {
        0.35
    }
}

fn evaluate_signal_alignment(
    current_stage: DiagnosisLabel,
    primary_signal_name: &'static str,
    primary_signal: bool,
) -> (Vec<String>, Vec<String>) {
    let mut signals_matched = Vec::new();
    let mut signals_missing = Vec::new();

    if primary_signal {
        signals_matched.push(primary_signal_name.to_string());
    } else {
        signals_missing.push(primary_signal_name.to_string());
    }

    if current_stage == DiagnosisLabel::Healthy {
        return (signals_matched, signals_missing);
    }
    (signals_matched, signals_missing)
}

fn maybe_add_signal(
    matched: &mut Vec<String>,
    missing: &mut Vec<String>,
    signal_name: &str,
    signal_present: bool,
) {
    if signal_name.is_empty() {
        return;
    }
    if signal_present {
        matched.push(signal_name.to_string());
    } else {
        missing.push(signal_name.to_string());
    }
}

fn maybe_add_bottleneck_signal(
    current_stage: DiagnosisLabel,
    matched: &mut Vec<String>,
    missing: &mut Vec<String>,
    signals: &ConfidenceSignals,
) {
    if signals.bottleneck_signal() {
        matched.push("bottleneck_identified".to_string());
        return;
    }
    if !matches!(current_stage, DiagnosisLabel::Healthy) {
        missing.push("bottleneck_identified".to_string());
    }
}

fn confidence_score(
    current_stage: DiagnosisLabel,
    primary_signal: bool,
    coverage_ratio: f64,
    freshness_score: f64,
    bottleneck_signal: bool,
) -> f64 {
    if current_stage == DiagnosisLabel::Healthy && primary_signal {
        if freshness_score >= 0.999_999 {
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
    }
}

fn summarize_signal_names(signals: &[String]) -> String {
    if signals.is_empty() {
        "none".to_string()
    } else {
        signals.join(", ")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DomainDiagnostics {
    #[serde(flatten)]
    pub snapshot: DiagnosticSnapshot,
}

impl DomainDiagnostics {
    pub(super) fn healthy() -> Self {
        Self {
            snapshot: DiagnosticSnapshot::healthy(),
        }
    }

    pub(super) fn from_snapshot(snapshot: DiagnosticSnapshot) -> Self {
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
    pub(super) fn path(&self) -> Option<String> {
        let realm = self.realm.as_ref()?;
        let area = self.area.as_ref()?;
        let resource = self.resource.as_ref()?;
        if let Some(operation) = &self.operation {
            Some(format!("{realm}/{area}/{resource}/{operation}"))
        } else {
            Some(format!("{realm}/{area}/{resource}"))
        }
    }

    pub(super) fn events_query(&self) -> Option<String> {
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

    pub(super) fn resource_query(&self) -> Option<String> {
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

    pub(super) fn suggested_query(
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

    pub(super) fn suggested_queries(&self) -> Vec<SuggestedQuery> {
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
    pub(super) fn new(
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
    pub(super) fn new(
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
