use super::super::{
    build_resource_timeline, lease_resource_diagnostics, matches_resource_path, parse_rfc3339,
    timeline_candidate, DiagnosticSnapshot, ResourcePath, ResourceTimeline, ResourceTimelineEvent,
    ResourceTimelineKind,
};
use crate::api::admin::list::LeaseInfo;
use chrono::Utc;

#[inline]
fn i64_to_u64_non_negative(seconds: i64) -> u64 {
    u64::try_from(seconds).unwrap_or(0)
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
            let age_seconds = i64_to_u64_non_negative((now - acquired_at).num_seconds());
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
            let remaining_seconds = i64_to_u64_non_negative((expires_at - now).num_seconds());
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
                "{active_leases} active lease(s); next expiry in {remaining_seconds}s; {renewals_total} renewal(s)"
            ),
            None => format!(
                "{active_leases} active lease(s); {renewals_total} renewal(s)"
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
