use super::model::DiagnosticSnapshotInput;
use chrono::{DateTime, Duration, Utc};
use std::collections::HashMap;

use super::{
    score_u64, score_usize, DiagnosisLabel, DiagnosticHotspot, DiagnosticSeverity,
    DiagnosticSnapshot, DiagnosticTrend, DomainAnalysis, LeaseInfo, ScheduleInfo,
    ScheduleLatencyBuckets, ScoredHotspot, RECENT_WINDOW_SECS,
};

pub(crate) fn analyze_lease(leases: &[LeaseInfo], now: DateTime<Utc>) -> DomainAnalysis {
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
        let (hotspot, last_change) = build_lease_hotspot(&realm, &area, &resource, &items, now);
        update_last_changed(&mut last_changed_at, last_change);
        hotspots.push(hotspot);
    }

    if hotspots.is_empty() {
        DomainAnalysis::healthy()
    } else {
        DomainAnalysis::from_hotspots(hotspots)
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn analyze_schedule(
    schedules: &[ScheduleInfo],
    pending_fire_claims: usize,
    pending_ack_retries: usize,
    oldest_pending_claim_age_seconds: u64,
    request_latency_buckets: ScheduleLatencyBuckets,
    notify_failures: u64,
    ack_failures: u64,
    overdue_normalizations: u64,
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
        let context = ScheduleHotspotContext {
            oldest_pending_claim_age_seconds,
            pending_fire_claims,
            pending_ack_retries,
            notify_failures,
            ack_failures,
            overdue_normalizations,
            latency_tail_count,
            latency_total,
            latency_tail_ratio,
            latency_pressure,
        };
        let (hotspot, last_change) =
            build_schedule_hotspot(&realm, &area, &resource, &operation, &items, now, &context);
        update_last_changed(&mut last_changed_at, last_change);
        hotspots.push(hotspot);
    }

    if hotspots.is_empty() {
        DomainAnalysis::healthy()
    } else {
        DomainAnalysis::from_hotspots(hotspots)
    }
}

pub(crate) fn trend_from_pressure(waiter_count: usize, age_seconds: u64) -> DiagnosticTrend {
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

pub(crate) fn is_recent(timestamp: DateTime<Utc>, now: DateTime<Utc>) -> bool {
    now.signed_duration_since(timestamp) <= Duration::seconds(RECENT_WINDOW_SECS)
}

pub(crate) fn parse_rfc3339(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|timestamp| timestamp.with_timezone(&Utc))
}

pub(crate) fn age_seconds_since(timestamp: &str) -> Option<u64> {
    let timestamp = parse_rfc3339(timestamp)?;
    Some(seconds_to_u64(
        Utc::now()
            .signed_duration_since(timestamp)
            .num_seconds()
            .max(0),
    ))
}

pub(crate) fn rfc3339(timestamp: DateTime<Utc>) -> String {
    timestamp.to_rfc3339()
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn seconds_to_u64(seconds: i64) -> u64 {
    u64::try_from(seconds).unwrap_or(0)
}

fn update_last_changed(
    current_last_changed: &mut Option<DateTime<Utc>>,
    candidate: Option<DateTime<Utc>>,
) {
    if let Some(candidate_changed_at) = candidate {
        let merged = match *current_last_changed {
            Some(current) => current.max(candidate_changed_at),
            None => candidate_changed_at,
        };
        *current_last_changed = Some(merged);
    }
}

#[derive(Clone, Copy)]
struct ScheduleHotspotContext {
    oldest_pending_claim_age_seconds: u64,
    pending_fire_claims: usize,
    pending_ack_retries: usize,
    notify_failures: u64,
    ack_failures: u64,
    overdue_normalizations: u64,
    latency_tail_count: usize,
    latency_total: usize,
    latency_tail_ratio: f64,
    latency_pressure: bool,
}

fn build_lease_hotspot(
    realm: &str,
    area: &str,
    resource: &str,
    items: &[&LeaseInfo],
    now: DateTime<Utc>,
) -> (ScoredHotspot, Option<DateTime<Utc>>) {
    let active_leases = items.len();
    let renewals = items.iter().map(|lease| lease.renewals).sum::<usize>();
    let last_change = items
        .iter()
        .filter_map(|lease| parse_rfc3339(&lease.acquired_at))
        .max();
    let age_seconds =
        last_change.map(|changed| seconds_to_u64((now - changed).num_seconds().max(0)));
    let remaining_seconds = items
        .iter()
        .filter_map(|lease| parse_rfc3339(&lease.expires_at))
        .map(|expires| seconds_to_u64((expires - now).num_seconds().max(0)))
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
    let mut hints = Vec::new();
    if active_leases > 0 {
        hints.push(format!("{active_leases} active lease(s)"));
    }
    if renewals > 0 {
        hints.push(format!("{renewals} renewals recorded"));
    }
    if let Some(remaining) = remaining_seconds {
        hints.push(format!("{remaining}s until next expiry"));
    }

    let snapshot = DiagnosticSnapshot::with_stage(DiagnosticSnapshotInput {
        current_stage: label,
        trend,
        severity,
        likely_bottleneck: bottleneck,
        last_changed_at: last_change,
        last_success_at: last_change,
        last_failure_at: None,
        age_seconds,
        recent_transition_count: u64::from(active_leases > 0),
        failure_count: 0,
        contention_count: usize_to_u64(renewals),
        waiter_count: active_leases,
        explanation_hints: hints,
    });

    (
        ScoredHotspot {
            score: score_usize(active_leases) * 3.0 + score_usize(renewals) * 1.5,
            hotspot: DiagnosticHotspot {
                domain: "lease".to_string(),
                realm: Some(realm.to_string()),
                area: Some(area.to_string()),
                resource: Some(resource.to_string()),
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
        },
        last_change,
    )
}

#[allow(clippy::too_many_lines)]
fn build_schedule_hotspot(
    realm: &str,
    area: &str,
    resource: &str,
    operation: &str,
    items: &[&ScheduleInfo],
    now: DateTime<Utc>,
    context: &ScheduleHotspotContext,
) -> (ScoredHotspot, Option<DateTime<Utc>>) {
    let ScheduleHotspotContext {
        oldest_pending_claim_age_seconds,
        pending_fire_claims,
        pending_ack_retries,
        notify_failures,
        ack_failures,
        overdue_normalizations,
        latency_tail_count,
        latency_total,
        latency_tail_ratio,
        latency_pressure,
    } = *context;

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
                Some(seconds_to_u64((now - due).num_seconds().max(0)))
            } else {
                last_run.map(|run| seconds_to_u64((now - run).num_seconds().max(0)))
            }
        })
        .or_else(|| last_run.map(|run| seconds_to_u64((now - run).num_seconds().max(0))));
    let age_seconds = match (oldest_pending_claim_age_seconds, age_seconds) {
        (0, age_seconds) => age_seconds,
        (pending_age, _) => Some(pending_age),
    };
    let recent_transition_count = u64::try_from(
        items
            .iter()
            .filter_map(|schedule| schedule.last_run.as_deref().and_then(parse_rfc3339))
            .filter(|ts| is_recent(*ts, now))
            .count(),
    )
    .unwrap_or(u64::MAX);
    let contention_count = saturating_sum_u64([
        usize_to_u64(pending_fire_claims),
        usize_to_u64(pending_ack_retries),
        usize_to_u64(latency_tail_count),
    ]);
    let failure_count = notify_failures + ack_failures;
    let handoff_pressure =
        pending_fire_claims > 0 || pending_ack_retries > 0 || overdue_normalizations > 0;
    let (label, trend, severity, bottleneck) = if handoff_pressure {
        (
            DiagnosisLabel::StaleHandoff,
            DiagnosticTrend::Stalled,
            DiagnosticSeverity::High,
            Some("durable handoff".to_string()),
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
    let mut hints = Vec::new();
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
    if notify_failures > 0 {
        hints.push(format!("{notify_failures} notify failure(s)"));
    }
    if ack_failures > 0 {
        hints.push(format!("{ack_failures} ack failure(s)"));
    }

    let last_change = last_run.or(next_run);
    let snapshot = DiagnosticSnapshot::with_stage(DiagnosticSnapshotInput {
        current_stage: label,
        trend,
        severity,
        likely_bottleneck: bottleneck,
        last_changed_at: last_change,
        last_success_at: last_run,
        last_failure_at: None,
        age_seconds,
        recent_transition_count,
        failure_count,
        contention_count,
        waiter_count: pending_fire_claims,
        explanation_hints: hints,
    });

    let score = score_usize(pending_fire_claims) * 5.0
        + score_usize(pending_ack_retries) * 3.5
        + score_u64(overdue_normalizations) * 4.0
        + score_u64(failure_count) * 1.25
        + score_u64(age_seconds.unwrap_or(0)) / 20.0;
    let backlog = pending_fire_claims.saturating_add(pending_ack_retries);
    (
        ScoredHotspot {
            score,
            hotspot: DiagnosticHotspot {
                domain: "schedule".to_string(),
                realm: Some(realm.to_string()),
                area: Some(area.to_string()),
                resource: Some(resource.to_string()),
                operation: Some(operation.to_string()),
                family: None,
                backlog: Some(backlog),
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
        },
        last_change,
    )
}

fn saturating_sum_u64(values: impl IntoIterator<Item = u64>) -> u64 {
    values.into_iter().fold(0_u64, u64::saturating_add)
}
