use super::*;

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
        let active_leases = items.len();
        let renewals = items.iter().map(|lease| lease.renewals).sum::<usize>();
        let last_change = items
            .iter()
            .filter_map(|lease| parse_rfc3339(&lease.acquired_at))
            .max();
        let age_seconds = last_change.map(|changed| (now - changed).num_seconds().max(0) as u64);
        let remaining_seconds = items
            .iter()
            .filter_map(|lease| parse_rfc3339(&lease.expires_at))
            .map(|expires| (expires - now).num_seconds().max(0) as u64)
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
        let mut hints = vec![];
        if active_leases > 0 {
            hints.push(format!("{active_leases} active lease(s)"));
        }
        if renewals > 0 {
            hints.push(format!("{renewals} renewals recorded"));
        }
        if let Some(remaining) = remaining_seconds {
            hints.push(format!("{remaining}s until next expiry"));
        }

        let snapshot = DiagnosticSnapshot::with_stage(
            label,
            trend,
            severity,
            bottleneck.clone(),
            last_change,
            last_change,
            None,
            age_seconds,
            if active_leases > 0 { 1 } else { 0 },
            0,
            renewals as u64,
            active_leases,
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
            score: active_leases as f64 * 3.0 + renewals as f64 * 1.5,
            hotspot: DiagnosticHotspot {
                domain: "lease".to_string(),
                realm: Some(realm),
                area: Some(area),
                resource: Some(resource),
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
        });
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
    pending_claims_expired_total: u64,
    pending_claim_cleanup_failures_total: u64,
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
                    Some((now - due).num_seconds().max(0) as u64)
                } else {
                    last_run.map(|run| (now - run).num_seconds().max(0) as u64)
                }
            })
            .or_else(|| last_run.map(|run| (now - run).num_seconds().max(0) as u64));
        let age_seconds = match (oldest_pending_claim_age_seconds, age_seconds) {
            (0, age_seconds) => age_seconds,
            (pending_age, _) => Some(pending_age),
        };
        let recent_transition_count = items
            .iter()
            .filter_map(|schedule| schedule.last_run.as_deref().and_then(parse_rfc3339))
            .filter(|ts| is_recent(*ts, now))
            .count() as u64;
        let contention_count = pending_fire_claims
            .saturating_add(pending_ack_retries)
            .saturating_add(overdue_normalizations as usize)
            .saturating_add(pending_claims_expired_total as usize)
            .saturating_add(pending_claim_cleanup_failures_total as usize)
            .saturating_add(latency_tail_count) as u64;
        let failure_count = notify_failures + ack_failures + pending_claim_cleanup_failures_total;
        let handoff_pressure =
            pending_fire_claims > 0 || pending_ack_retries > 0 || overdue_normalizations > 0;
        let cleanup_pressure =
            pending_claims_expired_total > 0 || pending_claim_cleanup_failures_total > 0;
        let (label, trend, severity, bottleneck) = if handoff_pressure {
            (
                DiagnosisLabel::StaleHandoff,
                DiagnosticTrend::Stalled,
                DiagnosticSeverity::High,
                Some("durable handoff".to_string()),
            )
        } else if cleanup_pressure {
            (
                DiagnosisLabel::StaleHandoff,
                DiagnosticTrend::Stalled,
                if pending_claim_cleanup_failures_total > 0 {
                    DiagnosticSeverity::High
                } else {
                    DiagnosticSeverity::Medium
                },
                Some("claim cleanup".to_string()),
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
        let mut hints = vec![];
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
        if pending_claims_expired_total > 0 {
            hints.push(format!(
                "{pending_claims_expired_total} expired pending claim(s)"
            ));
        }
        if pending_claim_cleanup_failures_total > 0 {
            hints.push(format!(
                "{pending_claim_cleanup_failures_total} pending claim cleanup failure(s)"
            ));
        }

        let last_change = last_run.or(next_run);
        let snapshot = DiagnosticSnapshot::with_stage(
            label,
            trend,
            severity,
            bottleneck.clone(),
            last_change,
            last_run,
            None,
            age_seconds,
            recent_transition_count,
            failure_count,
            contention_count,
            pending_fire_claims,
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
            score: pending_fire_claims as f64 * 5.0
                + pending_ack_retries as f64 * 3.5
                + overdue_normalizations as f64 * 4.0
                + pending_claims_expired_total as f64 * 2.5
                + pending_claim_cleanup_failures_total as f64 * 5.0
                + failure_count as f64 * 1.25
                + age_seconds.unwrap_or(0) as f64 / 20.0,
            hotspot: DiagnosticHotspot {
                domain: "schedule".to_string(),
                realm: Some(realm),
                area: Some(area),
                resource: Some(resource),
                operation: Some(operation),
                family: None,
                backlog: Some(pending_fire_claims.saturating_add(pending_ack_retries)),
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
    Some(
        Utc::now()
            .signed_duration_since(timestamp)
            .num_seconds()
            .max(0) as u64,
    )
}

pub(crate) fn rfc3339(timestamp: DateTime<Utc>) -> String {
    timestamp.to_rfc3339()
}
