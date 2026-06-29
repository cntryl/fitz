use super::*;

pub(crate) fn build_resource_timeline(
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

pub(crate) fn timeline_candidate(
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

pub(crate) fn parse_session_from_mode(mode: &str) -> Option<String> {
    mode.split(':').nth(1).map(|value| value.to_string())
}

pub(crate) fn matches_resource_path(
    path: &ResourcePath<'_>,
    realm: &str,
    area: &str,
    resource: &str,
) -> bool {
    path.realm == realm && path.area == area && path.resource == resource
}

#[derive(Clone)]
pub(crate) struct OwnedRpcOperation {
    pub(super) realm: String,
    pub(super) area: String,
    pub(super) resource: String,
    pub(super) operation: String,
}

impl OwnedRpcOperation {
    pub(super) fn matches_resource_path(&self, path: &ResourcePath<'_>) -> bool {
        self.realm == path.realm && self.area == path.area && self.resource == path.resource
    }
}

pub(crate) fn matches_resource_route(route: &str, path: &ResourcePath<'_>) -> bool {
    route_triplet(route)
        .is_some_and(|parts| matches_resource_path(path, parts.realm, parts.area, parts.resource))
}

pub(crate) fn parse_rpc_operation(route: &str) -> Option<OwnedRpcOperation> {
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
                "{open_transactions} open transaction(s); oldest idle {oldest_idle_seconds}s"
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
