use super::*;

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
    pub(crate) fn pressure_score(&self, diagnostics: &DiagnosticSnapshot) -> f64 {
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
    pub(crate) fn pressure_score(&self) -> f64 {
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
    pub(crate) fn from_sides(
        left: &ResourceComparisonMetrics,
        right: &ResourceComparisonMetrics,
    ) -> Self {
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

pub(crate) fn summarize_comparison(
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

pub(crate) fn append_delta_note(notes: &mut Vec<String>, name: &str, value: Option<i64>) {
    if let Some(value) = value {
        if value != 0 {
            notes.push(format!("{name} {value:+}"));
        }
    }
}

pub(crate) fn diff_usize(left: Option<usize>, right: Option<usize>) -> Option<i64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left as i64 - right as i64),
        _ => None,
    }
}

pub(crate) fn diff_u64(left: Option<u64>, right: Option<u64>) -> Option<i64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left as i64 - right as i64),
        _ => None,
    }
}

#[derive(Clone)]
pub(crate) struct TimelineCandidate {
    pub(crate) observed_at: DateTime<Utc>,
    pub(crate) priority: u8,
    pub(crate) event: ResourceTimelineEvent,
}

#[derive(Clone)]
pub(crate) struct ScoredHotspot {
    pub(crate) score: f64,
    pub(crate) hotspot: DiagnosticHotspot,
    pub(crate) last_changed_at: Option<DateTime<Utc>>,
}

pub(crate) struct DomainAnalysis {
    pub(crate) diagnostics: DomainDiagnostics,
    pub(crate) hotspots: Vec<ScoredHotspot>,
    pub(crate) last_changed_at: Option<DateTime<Utc>>,
}

impl DomainAnalysis {
    pub(crate) fn healthy() -> Self {
        Self {
            diagnostics: DomainDiagnostics::healthy(),
            hotspots: Vec::new(),
            last_changed_at: None,
        }
    }

    pub(crate) fn from_hotspots(mut hotspots: Vec<ScoredHotspot>) -> Self {
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
