use super::super::{
    build_resource_timeline, matches_resource_route, notice_resource_diagnostics, parse_rfc3339,
    timeline_candidate, DiagnosticSnapshot, ResourcePath, ResourceTimeline, ResourceTimelineEvent,
    ResourceTimelineKind,
};
use crate::api::admin::list::{NoticeRouteInfo, NoticeSubscription};
use chrono::Utc;

#[inline]
fn i64_to_u64_non_negative(seconds: i64) -> u64 {
    u64::try_from(seconds).unwrap_or(0)
}

#[inline]
fn u64_to_usize_non_negative(value: u64) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX)
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
            let age_seconds = i64_to_u64_non_negative((now - created_at).num_seconds());
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
                    Some(u64_to_usize_non_negative(
                        subscription.notifications_received,
                    )),
                ),
            ));
        }
    }

    if subscriptions_active > 0 || publishes_total > 0 {
        let summary = if publishes_total > 0 {
            format!(
                "{subscriptions_active} subscriber(s); {publishes_per_minute:.1} publish(es)/min; {publishes_total} total publish(es)"
            )
        } else {
            format!("{subscriptions_active} subscriber(s)")
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
