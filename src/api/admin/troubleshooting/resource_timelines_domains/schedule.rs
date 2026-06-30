use super::super::{
    build_resource_timeline, matches_resource_path, parse_rfc3339, rfc3339,
    schedule_resource_diagnostics, timeline_candidate, DiagnosisLabel, DiagnosticSnapshot,
    ResourcePath, ResourceTimeline, ResourceTimelineEvent, ResourceTimelineKind,
};
use crate::api::admin::list::ScheduleInfo;
use chrono::{DateTime, Utc};

#[inline]
fn i64_to_u64_non_negative(seconds: i64) -> u64 {
    u64::try_from(seconds).unwrap_or(0)
}

#[inline]
fn u64_to_usize_non_negative(value: u64) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX)
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
                    Some(i64_to_u64_non_negative((now - last_run).num_seconds())),
                    None,
                    None,
                    None,
                    None,
                    Some(u64_to_usize_non_negative(schedule.executions_total)),
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
                        .map_or("schedule", |schedule| schedule.operation.as_str()),
                    (now - next_run).num_seconds().max(0)
                ),
                None => format!(
                    "{} is overdue",
                    matching_schedules
                        .first()
                        .map_or("schedule", |schedule| schedule.operation.as_str())
                ),
            }
        } else if let Some(next_run) = latest_next_run_at {
            format!(
                "{} next runs at {}",
                matching_schedules
                    .first()
                    .map_or("schedule", |schedule| schedule.operation.as_str()),
                rfc3339(next_run)
            )
        } else {
            format!(
                "{} enabled with {} execution(s)",
                matching_schedules
                    .first()
                    .map_or("schedule", |schedule| schedule.operation.as_str()),
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
                "oldest pending claim {oldest_pending_claim_age_seconds}s old"
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
