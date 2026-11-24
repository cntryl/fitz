use super::super::{
    build_resource_timeline, matches_resource_path, queue_resource_diagnostics, timeline_candidate,
    DiagnosticSnapshot, QueueAgeBuckets, ResourcePath, ResourceTimeline, ResourceTimelineEvent,
    ResourceTimelineKind, TimelineCandidate,
};
use crate::api::admin::list::{QueueDeadLetter, QueueInflight, QueueInfo};
use chrono::Utc;

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
        .filter(|queue| matches_queue(path, family, queue))
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
            matches_queue_member(
                path,
                family,
                &item.realm,
                &item.area,
                &item.resource,
                item.family,
            )
        })
        .collect();
    let matching_dead_letters: Vec<_> = dead_letters
        .iter()
        .filter(|item| {
            matches_queue_member(
                path,
                family,
                &item.realm,
                &item.area,
                &item.resource,
                item.family,
            )
        })
        .collect();
    let queue_summary =
        summarize_queue_timeline(&matching_queues, &matching_inflight, &matching_dead_letters);
    let diagnostics = queue_resource_diagnostics(
        queue_summary.messages_ready,
        queue_summary.messages_delayed,
        queue_summary.inflight_count,
        queue_summary.dead_letter_count,
        queue_summary.oldest_backlog_age_seconds,
        queue_summary.delay_age_buckets,
    );
    let candidates = build_queue_timeline_events(path, family, now, &queue_summary);

    build_resource_timeline("queue", path, family, diagnostics, limit, candidates)
}

fn matches_queue(path: &ResourcePath<'_>, family: Option<u64>, queue: &QueueInfo) -> bool {
    matches_resource_path(path, &queue.realm, &queue.area, &queue.resource)
        && family.is_none_or(|value| queue.family == value)
}

fn matches_queue_member(
    path: &ResourcePath<'_>,
    family: Option<u64>,
    realm: &str,
    area: &str,
    resource: &str,
    item_family: u64,
) -> bool {
    matches_resource_path(path, realm, area, resource)
        && family.is_none_or(|value| item_family == value)
}

struct QueueTimelineSummary {
    messages_ready: usize,
    messages_delayed: usize,
    inflight_count: usize,
    dead_letter_count: usize,
    oldest_backlog_age_seconds: u64,
    delay_age_buckets: QueueAgeBuckets,
    owner_session: Option<String>,
    backlog: usize,
}

fn summarize_queue_timeline(
    matching_queues: &[&QueueInfo],
    matching_inflight: &[&QueueInflight],
    matching_dead_letters: &[&QueueDeadLetter],
) -> QueueTimelineSummary {
    let mut summary = QueueTimelineSummary {
        messages_ready: 0,
        messages_delayed: 0,
        inflight_count: matching_inflight.len(),
        dead_letter_count: matching_dead_letters.len(),
        oldest_backlog_age_seconds: 0,
        delay_age_buckets: QueueAgeBuckets::default(),
        owner_session: matching_inflight
            .iter()
            .map(|item| item.session_id.clone())
            .find(|value| !value.is_empty()),
        backlog: 0,
    };
    for queue in matching_queues {
        summary.messages_ready += queue.messages_ready;
        summary.messages_delayed += queue.messages_delayed;
        summary.oldest_backlog_age_seconds = summary
            .oldest_backlog_age_seconds
            .max(queue.oldest_backlog_age_seconds);
        summary.delay_age_buckets.merge(queue.delay_age_buckets);
    }
    summary.backlog = summary.messages_ready + summary.messages_delayed;
    summary.dead_letter_count = summary.dead_letter_count.max(
        summary
            .messages_delayed
            .saturating_add(matching_dead_letters.len()),
    );
    summary.inflight_count = summary.inflight_count.max(matching_inflight.len());
    summary
}

fn build_queue_timeline_events(
    path: &ResourcePath<'_>,
    family: Option<u64>,
    now: chrono::DateTime<chrono::Utc>,
    summary: &QueueTimelineSummary,
) -> Vec<TimelineCandidate> {
    let mut candidates = Vec::new();
    if summary.inflight_count > 0 {
        candidates.push(timeline_candidate(
            now,
            1,
            ResourceTimelineEvent::new(
                "queue",
                ResourceTimelineKind::OwnershipChange,
                now,
                if let Some(owner_session) = &summary.owner_session {
                    format!(
                        "{} inflight message(s) owned by session {owner_session}",
                        summary.inflight_count
                    )
                } else {
                    format!("{} inflight message(s)", summary.inflight_count)
                },
                path,
                family,
                None,
                Some(summary.oldest_backlog_age_seconds),
                summary.owner_session.clone(),
                None,
                None,
                None,
                Some(summary.inflight_count),
            ),
        ));
    }
    if summary.backlog > 0 || summary.dead_letter_count > 0 || summary.inflight_count > 0 {
        candidates.push(timeline_candidate(
            now,
            2,
            ResourceTimelineEvent::new(
                "queue",
                ResourceTimelineKind::Observation,
                now,
                format!(
                    "{} ready, {} delayed, {} inflight, {} dead-lettered; oldest backlog message {}s old",
                    summary.messages_ready, summary.messages_delayed, summary.inflight_count, summary.dead_letter_count, summary.oldest_backlog_age_seconds
                ),
                path,
                family,
                None,
                Some(summary.oldest_backlog_age_seconds),
                summary.owner_session.clone(),
                None,
                None,
                None,
                Some(summary.backlog),
            ),
        ));
    }
    if summary.delay_age_buckets.over_15m > 0 {
        candidates.push(timeline_candidate(
            now,
            3,
            ResourceTimelineEvent::new(
                "queue",
                ResourceTimelineKind::Observation,
                now,
                format!(
                    "{} delayed message(s) are 15m+ old",
                    summary.delay_age_buckets.over_15m
                ),
                path,
                family,
                None,
                None,
                None,
                None,
                None,
                None,
                Some(summary.delay_age_buckets.over_15m),
            ),
        ));
    }
    candidates
}
