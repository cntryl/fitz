use super::model::DiagnosticSnapshotInput;
use chrono::{DateTime, Utc};
use std::collections::HashMap;

use super::{
    is_recent, parse_rfc3339, route_triplet, saturating_usize, score_u64, score_usize,
    trend_from_pressure, DiagnosisLabel, DiagnosticHotspot, DiagnosticSeverity, DiagnosticSnapshot,
    DiagnosticTrend, DomainAnalysis, KvTransaction, NoticeRouteInfo, NoticeSubscription,
    ScoredHotspot, StreamInfo, StreamLatencyBuckets,
};

pub(crate) fn analyze_kv(transactions: &[KvTransaction], now: DateTime<Utc>) -> DomainAnalysis {
    let mut grouped: HashMap<(String, String, String), Vec<&KvTransaction>> = HashMap::new();
    for tx in transactions {
        grouped
            .entry((tx.realm.clone(), tx.area.clone(), tx.resource.clone()))
            .or_default()
            .push(tx);
    }

    let mut hotspots = Vec::new();
    let mut last_changed_at: Option<DateTime<Utc>> = None;

    for ((realm, area, resource), txs) in grouped {
        let waiter_count = txs.len();
        let age_seconds = txs.iter().map(|tx| tx.idle_seconds).max();
        let last_change = txs
            .iter()
            .filter_map(|tx| parse_rfc3339(&tx.started_at))
            .max();
        let recent_transition_count = u64::try_from(
            txs.iter()
                .filter(|tx| {
                    parse_rfc3339(&tx.started_at).is_some_and(|started| is_recent(started, now))
                })
                .count(),
        )
        .unwrap_or(u64::MAX);
        let failure_count = 0;
        let contention_count = u64::try_from(waiter_count).unwrap_or(u64::MAX);
        let label = if waiter_count > 0 {
            DiagnosisLabel::Contention
        } else {
            DiagnosisLabel::Healthy
        };
        let trend = trend_from_pressure(waiter_count, age_seconds.unwrap_or(0));
        let bottleneck = if waiter_count > 0 {
            Some("transaction coordination".to_string())
        } else {
            None
        };
        let mut hints = vec![];
        if let Some(age) = age_seconds {
            hints.push(format!("oldest transaction idle for {age}s"));
        }
        if waiter_count > 0 {
            hints.push(format!("{waiter_count} open transaction(s)"));
        }

        let snapshot = DiagnosticSnapshot::with_stage(DiagnosticSnapshotInput {
            current_stage: label,
            trend,
            severity: if waiter_count > 0 {
                DiagnosticSeverity::Medium
            } else {
                DiagnosticSeverity::Informational
            },
            likely_bottleneck: bottleneck,
            last_changed_at: last_change,
            last_success_at: None,
            last_failure_at: None,
            age_seconds,
            recent_transition_count,
            failure_count,
            contention_count,
            waiter_count,
            explanation_hints: hints,
        });

        if let Some(candidate_changed_at) = last_change {
            let previous = last_changed_at;
            last_changed_at = Some(match previous {
                Some(current) => current.max(candidate_changed_at),
                None => candidate_changed_at,
            });
        }

        hotspots.push(ScoredHotspot {
            score: score_usize(waiter_count) * 3.0 + score_u64(age_seconds.unwrap_or(0)) / 15.0,
            hotspot: DiagnosticHotspot {
                domain: "kv".to_string(),
                realm: Some(realm),
                area: Some(area),
                resource: Some(resource),
                operation: None,
                family: None,
                backlog: Some(waiter_count),
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
        });
    }

    if hotspots.is_empty() {
        DomainAnalysis::healthy()
    } else {
        DomainAnalysis::from_hotspots(hotspots)
    }
}

pub(crate) fn analyze_stream(
    streams: &[StreamInfo],
    request_latency_buckets: StreamLatencyBuckets,
    _now: DateTime<Utc>,
) -> DomainAnalysis {
    let mut grouped: HashMap<(String, String, String), Vec<&StreamInfo>> = HashMap::new();
    for stream in streams {
        grouped
            .entry((
                stream.realm.clone(),
                stream.area.clone(),
                stream.resource.clone(),
            ))
            .or_default()
            .push(stream);
    }

    let mut hotspots = Vec::new();
    let latency_pressure = stream_latency_pressure(request_latency_buckets);

    for ((realm, area, resource), streams) in grouped {
        if let Some(hotspot) =
            build_stream_hotspot(realm, area, resource, &streams, &latency_pressure)
        {
            hotspots.push(hotspot);
        }
    }

    if hotspots.is_empty() {
        DomainAnalysis::healthy()
    } else {
        DomainAnalysis::from_hotspots(hotspots)
    }
}

#[derive(Clone, Copy)]
struct StreamLatencyPressure {
    total: usize,
    tail_count: usize,
    active: bool,
}

fn stream_latency_pressure(buckets: StreamLatencyBuckets) -> StreamLatencyPressure {
    let total = buckets.total();
    let tail_count = buckets.slow_tail_count();
    let tail_ratio = buckets.slow_tail_ratio();

    StreamLatencyPressure {
        total,
        tail_count,
        active: total > 0 && tail_count > 0 && (tail_ratio >= 0.25 || tail_count >= 3),
    }
}

fn build_stream_hotspot(
    realm: String,
    area: String,
    resource: String,
    streams: &[&StreamInfo],
    latency_pressure: &StreamLatencyPressure,
) -> Option<ScoredHotspot> {
    let backlog = streams
        .iter()
        .map(|stream| saturating_usize(stream.offset.saturating_sub(stream.watermark)))
        .sum::<usize>();
    let workers = streams
        .iter()
        .map(|stream| stream.sessions_active)
        .sum::<usize>();
    let max_lag = streams
        .iter()
        .map(|stream| stream.offset.saturating_sub(stream.watermark))
        .max()
        .unwrap_or(0);

    let (label, trend, severity, _contention_count) = if backlog > 0 || workers > 0 {
        let backlog_u64 = u64::try_from(backlog).unwrap_or(u64::MAX);
        let tail_count_u64 = u64::try_from(latency_pressure.tail_count).unwrap_or(u64::MAX);
        (
            DiagnosisLabel::Throughput,
            if backlog > 0 {
                DiagnosticTrend::Stalled
            } else {
                DiagnosticTrend::Steady
            },
            if backlog > 0 {
                DiagnosticSeverity::Medium
            } else {
                DiagnosticSeverity::Low
            },
            backlog_u64.saturating_add(tail_count_u64),
        )
    } else {
        (
            DiagnosisLabel::Healthy,
            DiagnosticTrend::Unknown,
            DiagnosticSeverity::Informational,
            0,
        )
    };

    let mut hints = Vec::new();
    if backlog > 0 {
        hints.push(format!("stream lag is {max_lag} event(s)"));
    }
    if latency_pressure.active {
        hints.push(format!(
            "stream request latency tail is {} of {} observation(s) over 100ms",
            latency_pressure.tail_count, latency_pressure.total
        ));
    }
    if workers > 0 {
        hints.push(format!("{workers} live append session(s)"));
    }

    let snapshot = DiagnosticSnapshot::with_stage(DiagnosticSnapshotInput {
        current_stage: label,
        trend,
        severity,
        likely_bottleneck: stream_bottleneck(backlog, workers),
        last_changed_at: None,
        last_success_at: None,
        last_failure_at: None,
        age_seconds: None,
        recent_transition_count: 0,
        failure_count: 0,
        contention_count: u64::try_from(backlog)
            .unwrap_or(u64::MAX)
            .saturating_add(u64::try_from(latency_pressure.tail_count).unwrap_or(u64::MAX)),
        waiter_count: workers,
        explanation_hints: hints,
    });

    (!matches!(snapshot.severity, DiagnosticSeverity::Informational)).then_some(ScoredHotspot {
        score: score_usize(backlog) * 2.0 + score_usize(workers) * 0.5,
        hotspot: DiagnosticHotspot {
            domain: "stream".to_string(),
            realm: Some(realm),
            area: Some(area),
            resource: Some(resource),
            operation: None,
            family: None,
            backlog: Some(backlog),
            inflight: Some(workers),
            ready: None,
            delayed: None,
            dead_letters: None,
            workers: Some(workers),
            subscriptions: None,
            owner_session: None,
            worker_session: None,
            snapshot,
        },
        last_changed_at: None,
    })
}

fn stream_bottleneck(backlog: usize, workers: usize) -> Option<String> {
    if backlog > 0 {
        Some("append lag".to_string())
    } else if workers > 0 {
        Some("append throughput".to_string())
    } else {
        None
    }
}

pub(crate) fn analyze_notice(
    subscriptions: &[NoticeSubscription],
    routes: &[NoticeRouteInfo],
    now: DateTime<Utc>,
) -> DomainAnalysis {
    let route_map = group_notice_routes(routes);
    let subscriptions_by_route = count_notice_subscriptions(subscriptions);
    let recent_subscription_count = u64::try_from(
        subscriptions
            .iter()
            .filter_map(|subscription| parse_rfc3339(&subscription.created_at))
            .filter(|created_at| is_recent(*created_at, now))
            .count(),
    )
    .unwrap_or(u64::MAX);

    let mut hotspots = Vec::new();
    let mut last_changed_at: Option<DateTime<Utc>> = None;

    for ((realm, area, resource), route_items) in route_map {
        let route_key = (realm.clone(), area.clone(), resource.clone());
        let last_change = notice_last_change(subscriptions, now);
        if let Some(candidate_changed_at) = last_change {
            let previous = last_changed_at;
            last_changed_at = Some(match previous {
                Some(current) => current.max(candidate_changed_at),
                None => candidate_changed_at,
            });
        }
        hotspots.push(build_notice_hotspot(
            (&realm, &area, &resource),
            &route_items,
            subscriptions_by_route.get(&route_key).copied().unwrap_or(0),
            recent_subscription_count,
            last_change,
        ));
    }

    if hotspots.is_empty() {
        DomainAnalysis::healthy()
    } else {
        DomainAnalysis::from_hotspots(hotspots)
    }
}

fn group_notice_routes(
    routes: &[NoticeRouteInfo],
) -> HashMap<(String, String, String), Vec<&NoticeRouteInfo>> {
    let mut route_map: HashMap<(String, String, String), Vec<&NoticeRouteInfo>> = HashMap::new();
    for route_info in routes {
        if let Some(route) = route_triplet(&route_info.route) {
            route_map
                .entry((
                    route.realm.to_string(),
                    route.area.to_string(),
                    route.resource.to_string(),
                ))
                .or_default()
                .push(route_info);
        }
    }
    route_map
}

fn count_notice_subscriptions(
    subscriptions: &[NoticeSubscription],
) -> HashMap<(String, String, String), usize> {
    let mut subscriptions_by_route = HashMap::new();
    for subscription in subscriptions {
        if let Some(route) = route_triplet(&subscription.pattern) {
            *subscriptions_by_route
                .entry((
                    route.realm.to_string(),
                    route.area.to_string(),
                    route.resource.to_string(),
                ))
                .or_default() += 1;
        }
    }
    subscriptions_by_route
}

fn notice_last_change(
    subscriptions: &[NoticeSubscription],
    now: DateTime<Utc>,
) -> Option<DateTime<Utc>> {
    subscriptions
        .iter()
        .filter_map(|subscription| parse_rfc3339(&subscription.created_at))
        .filter(|created_at| is_recent(*created_at, now))
        .max()
}

fn build_notice_hotspot(
    route: (&str, &str, &str),
    route_items: &[&NoticeRouteInfo],
    subscription_count: usize,
    recent_subscription_count: u64,
    last_change: Option<DateTime<Utc>>,
) -> ScoredHotspot {
    let (subscribers, routes_active, max_route_subscribers, publishes_per_minute, concentration) =
        notice_route_metrics(route_items);
    let hints = notice_hotspot_hints(
        subscribers,
        routes_active,
        max_route_subscribers,
        concentration,
        publishes_per_minute,
    );
    let snapshot = build_notice_snapshot(
        subscribers,
        concentration,
        recent_subscription_count,
        last_change,
        hints,
    );

    ScoredHotspot {
        score: score_usize(subscribers) * 2.0
            + publishes_per_minute
            + if concentration { 1.0 } else { 0.0 },
        hotspot: DiagnosticHotspot {
            domain: "notice".to_string(),
            realm: Some(route.0.to_string()),
            area: Some(route.1.to_string()),
            resource: Some(route.2.to_string()),
            operation: None,
            family: None,
            backlog: Some(subscribers),
            inflight: None,
            ready: None,
            delayed: None,
            dead_letters: None,
            workers: None,
            subscriptions: Some(subscription_count),
            owner_session: None,
            worker_session: None,
            snapshot,
        },
        last_changed_at: last_change,
    }
}

fn notice_route_metrics(route_items: &[&NoticeRouteInfo]) -> (usize, usize, usize, f64, bool) {
    let subscribers = route_items
        .iter()
        .map(|route| route.subscribers)
        .sum::<usize>();
    let routes_active = route_items.len();
    let max_route_subscribers = route_items
        .iter()
        .map(|route| route.subscribers)
        .max()
        .unwrap_or(0);
    let publishes_per_minute = route_items
        .iter()
        .map(|route| route.publishes_per_minute)
        .fold(0.0_f64, f64::max);
    let concentration = routes_active > 1
        && max_route_subscribers > 0
        && max_route_subscribers * 2 >= subscribers.max(1);

    (
        subscribers,
        routes_active,
        max_route_subscribers,
        publishes_per_minute,
        concentration,
    )
}

fn notice_hotspot_hints(
    subscribers: usize,
    routes_active: usize,
    max_route_subscribers: usize,
    concentration: bool,
    publishes_per_minute: f64,
) -> Vec<String> {
    let mut hints = vec![];
    if subscribers > 0 {
        hints.push(format!("{subscribers} subscriber(s) on route"));
    }
    if concentration {
        hints.push(format!(
            "route concentration: {max_route_subscribers} subscriber(s) on one route across {routes_active} route(s)"
        ));
    }
    if publishes_per_minute > 0.0 {
        hints.push(format!("{publishes_per_minute:.1} publish(es)/min"));
    }
    hints
}

fn build_notice_snapshot(
    subscribers: usize,
    concentration: bool,
    recent_subscription_count: u64,
    last_change: Option<DateTime<Utc>>,
    hints: Vec<String>,
) -> DiagnosticSnapshot {
    DiagnosticSnapshot::with_stage(DiagnosticSnapshotInput {
        current_stage: if subscribers > 0 {
            DiagnosisLabel::Throughput
        } else {
            DiagnosisLabel::Healthy
        },
        trend: if subscribers > 0 {
            DiagnosticTrend::Steady
        } else {
            DiagnosticTrend::Unknown
        },
        severity: if subscribers > 25 {
            DiagnosticSeverity::High
        } else if concentration {
            DiagnosticSeverity::Medium
        } else if subscribers > 0 {
            DiagnosticSeverity::Low
        } else {
            DiagnosticSeverity::Informational
        },
        likely_bottleneck: if concentration {
            Some("route concentration".to_string())
        } else if subscribers > 0 {
            Some("subscription fanout".to_string())
        } else {
            None
        },
        last_changed_at: last_change,
        last_success_at: None,
        last_failure_at: None,
        age_seconds: None,
        recent_transition_count: recent_subscription_count,
        failure_count: 0,
        contention_count: u64::try_from(subscribers).unwrap_or(u64::MAX),
        waiter_count: subscribers,
        explanation_hints: hints,
    })
}
