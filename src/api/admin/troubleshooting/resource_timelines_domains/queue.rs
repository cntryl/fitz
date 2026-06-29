use super::super::*;

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
                && family.is_none_or(|value| queue.family == value)
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
                && family.is_none_or(|value| item.family == value)
        })
        .collect();
    let matching_dead_letters: Vec<_> = dead_letters
        .iter()
        .filter(|item| {
            matches_resource_path(path, &item.realm, &item.area, &item.resource)
                && family.is_none_or(|value| item.family == value)
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
                    format!("{inflight_count} inflight message(s) owned by session {owner_session}")
                } else {
                    format!("{inflight_count} inflight message(s)")
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
                    "{messages_ready} ready, {messages_delayed} delayed, {inflight_count} inflight, {dead_letter_count} dead-lettered; oldest backlog message {oldest_backlog_age_seconds}s old"
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
