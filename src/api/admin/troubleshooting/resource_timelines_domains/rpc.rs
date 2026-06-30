use super::super::{
    build_resource_timeline, parse_rfc3339, parse_rpc_operation, rpc_operation_diagnostics,
    summarize_rpc_worker_latency, timeline_candidate, DiagnosticSnapshot, ResourcePath,
    ResourceTimeline, ResourceTimelineEvent, ResourceTimelineKind, TimelineCandidate,
};
use crate::api::admin::list::{RpcPendingRequest, RpcWorker};
use chrono::Utc;

#[inline]
fn i64_to_u64_non_negative(seconds: i64) -> u64 {
    u64::try_from(seconds).unwrap_or(0)
}

#[inline]
fn u64_to_usize_non_negative(value: u64) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX)
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
        .filter(|worker| matches_rpc(path, &worker.route))
        .collect();
    let matching_pending: Vec<_> = pending
        .iter()
        .filter(|request| matches_rpc(path, &request.route))
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
    let mut oldest_pending_age = 0u64;
    let mut candidates = build_rpc_timeline_candidates(
        path,
        now,
        &matching_workers,
        &matching_pending,
        &mut oldest_pending_age,
    );

    if workers_registered == 0 && requests_pending > 0 {
        candidates.push(timeline_candidate(
            now,
            0,
            ResourceTimelineEvent::new(
                "rpc",
                ResourceTimelineKind::Observation,
                now,
                format!(
                    "{requests_pending} pending request(s); worker starvation and oldest request {oldest_pending_age}s old"
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
                "{workers_registered} worker(s), {requests_pending} pending request(s); oldest request {oldest_pending_age}s old{latency_note}"
            )
        } else {
            format!("{workers_registered} worker(s) registered{latency_note}")
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

fn matches_rpc(path: &ResourcePath<'_>, route: &str) -> bool {
    parse_rpc_operation(route).is_some_and(|parsed| parsed.matches_resource_path(path))
}

fn build_rpc_timeline_candidates(
    path: &ResourcePath<'_>,
    now: chrono::DateTime<chrono::Utc>,
    matching_workers: &[&RpcWorker],
    matching_pending: &[&RpcPendingRequest],
    oldest_pending_age: &mut u64,
) -> Vec<TimelineCandidate> {
    let mut candidates = Vec::new();
    for worker in matching_workers {
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
                    Some(i64_to_u64_non_negative(
                        now.signed_duration_since(registered_at).num_seconds(),
                    )),
                    None,
                    Some(worker.session_id.clone()),
                    None,
                    None,
                    Some(u64_to_usize_non_negative(worker.requests_handled)),
                ),
            ));
        }
    }
    for request in matching_pending {
        if let Some(submitted_at) = parse_rfc3339(&request.submitted_at) {
            let age_seconds = i64_to_u64_non_negative((now - submitted_at).num_seconds());
            *oldest_pending_age = (*oldest_pending_age).max(age_seconds);
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
    candidates
}
