use super::*;

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
        let recent_transition_count = txs
            .iter()
            .filter(|tx| {
                parse_rfc3339(&tx.started_at).is_some_and(|started| is_recent(started, now))
            })
            .count() as u64;
        let failure_count = 0;
        let contention_count = waiter_count as u64;
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

        let snapshot = DiagnosticSnapshot::with_stage(
            label,
            trend,
            if waiter_count > 0 {
                DiagnosticSeverity::Medium
            } else {
                DiagnosticSeverity::Informational
            },
            bottleneck,
            last_change,
            None,
            None,
            age_seconds,
            recent_transition_count,
            failure_count,
            contention_count,
            waiter_count,
            hints,
        );

        if let Some(candidate_changed_at) = last_change {
            let previous = last_changed_at;
            last_changed_at = Some(match previous {
                Some(current) => current.max(candidate_changed_at),
                None => candidate_changed_at,
            });
        }

        hotspots.push(ScoredHotspot {
            score: waiter_count as f64 * 3.0 + age_seconds.unwrap_or(0) as f64 / 15.0,
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
    let latency_total = request_latency_buckets.total();
    let latency_tail_count = request_latency_buckets.slow_tail_count();
    let latency_tail_ratio = request_latency_buckets.slow_tail_ratio();
    let latency_pressure = latency_total > 0
        && latency_tail_count > 0
        && (latency_tail_ratio >= 0.25 || latency_tail_count >= 3);

    for ((realm, area, resource), streams) in grouped {
        let backlog = streams
            .iter()
            .map(|stream| stream.offset.saturating_sub(stream.watermark) as usize)
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
        let label = if backlog > 0 || workers > 0 {
            DiagnosisLabel::Throughput
        } else {
            DiagnosisLabel::Healthy
        };
        let trend = if backlog > 0 {
            DiagnosticTrend::Stalled
        } else if workers > 0 {
            DiagnosticTrend::Steady
        } else {
            DiagnosticTrend::Unknown
        };
        let severity = if backlog > 0 {
            DiagnosticSeverity::Medium
        } else if workers > 0 {
            DiagnosticSeverity::Low
        } else {
            DiagnosticSeverity::Informational
        };
        let mut hints = vec![];
        if backlog > 0 {
            hints.push(format!("stream lag is {max_lag} event(s)"));
        }
        if latency_pressure {
            hints.push(format!(
                "stream request latency tail is {latency_tail_count} of {latency_total} observation(s) over 100ms"
            ));
        }
        if workers > 0 {
            hints.push(format!("{workers} live append session(s)"));
        }

        let snapshot = DiagnosticSnapshot::with_stage(
            label,
            trend,
            severity,
            if backlog > 0 {
                Some("append lag".to_string())
            } else if workers > 0 {
                Some("append throughput".to_string())
            } else {
                None
            },
            None,
            None,
            None,
            None,
            0,
            0,
            backlog as u64 + latency_tail_count as u64,
            workers,
            hints,
        );

        if !matches!(snapshot.severity, DiagnosticSeverity::Informational) {
            hotspots.push(ScoredHotspot {
                score: backlog as f64 * 2.0 + workers as f64 * 0.5,
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
            });
        }
    }

    if hotspots.is_empty() {
        DomainAnalysis::healthy()
    } else {
        DomainAnalysis::from_hotspots(hotspots)
    }
}

pub(crate) fn analyze_notice(
    subscriptions: &[NoticeSubscription],
    routes: &[NoticeRouteInfo],
    now: DateTime<Utc>,
) -> DomainAnalysis {
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

    let mut subscriptions_by_route: HashMap<(String, String, String), usize> = HashMap::new();
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

    let mut hotspots = Vec::new();
    let mut last_changed_at: Option<DateTime<Utc>> = None;

    for ((realm, area, resource), route_items) in route_map {
        let route_key = (realm.clone(), area.clone(), resource.clone());
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
        let waiter_count = subscribers;
        let label = if subscribers > 0 {
            DiagnosisLabel::Throughput
        } else {
            DiagnosisLabel::Healthy
        };
        let trend = if subscribers > 0 {
            DiagnosticTrend::Steady
        } else {
            DiagnosticTrend::Unknown
        };
        let concentration = routes_active > 1
            && max_route_subscribers > 0
            && max_route_subscribers * 2 >= subscribers.max(1);
        let severity = if subscribers > 25 {
            DiagnosticSeverity::High
        } else if concentration {
            DiagnosticSeverity::Medium
        } else if subscribers > 0 {
            DiagnosticSeverity::Low
        } else {
            DiagnosticSeverity::Informational
        };
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

        let last_change = subscriptions
            .iter()
            .filter_map(|subscription| parse_rfc3339(&subscription.created_at))
            .filter(|created_at| is_recent(*created_at, now))
            .max();
        if let Some(candidate_changed_at) = last_change {
            let previous = last_changed_at;
            last_changed_at = Some(match previous {
                Some(current) => current.max(candidate_changed_at),
                None => candidate_changed_at,
            });
        }

        let snapshot = DiagnosticSnapshot::with_stage(
            label,
            trend,
            severity,
            if concentration {
                Some("route concentration".to_string())
            } else if subscribers > 0 {
                Some("subscription fanout".to_string())
            } else {
                None
            },
            last_change,
            None,
            None,
            None,
            subscriptions
                .iter()
                .filter_map(|subscription| parse_rfc3339(&subscription.created_at))
                .filter(|created_at| is_recent(*created_at, now))
                .count() as u64,
            0,
            subscribers as u64,
            waiter_count,
            hints,
        );

        hotspots.push(ScoredHotspot {
            score: subscribers as f64 * 2.0
                + publishes_per_minute
                + if concentration { 1.0 } else { 0.0 },
            hotspot: DiagnosticHotspot {
                domain: "notice".to_string(),
                realm: Some(realm),
                area: Some(area),
                resource: Some(resource),
                operation: None,
                family: None,
                backlog: Some(subscribers),
                inflight: None,
                ready: None,
                delayed: None,
                dead_letters: None,
                workers: None,
                subscriptions: Some(subscriptions_by_route.get(&route_key).copied().unwrap_or(0)),
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
