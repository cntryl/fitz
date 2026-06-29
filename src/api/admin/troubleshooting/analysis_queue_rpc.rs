use super::*;

pub(crate) fn analyze_queue(
    queues: &[QueueInfo],
    inflight: &[QueueInflight],
    dead_letters: &[QueueDeadLetter],
    dead_letter_transitions_total: u64,
    complete_rejected_total: u64,
    now: DateTime<Utc>,
) -> DomainAnalysis {
    let mut inflight_by_resource: HashMap<(u64, String, String, String), Vec<&QueueInflight>> =
        HashMap::new();
    for item in inflight {
        inflight_by_resource
            .entry((
                item.family,
                item.realm.clone(),
                item.area.clone(),
                item.resource.clone(),
            ))
            .or_default()
            .push(item);
    }

    let mut dead_letters_by_resource: HashMap<
        (u64, String, String, String),
        Vec<&QueueDeadLetter>,
    > = HashMap::new();
    for item in dead_letters {
        dead_letters_by_resource
            .entry((
                item.family,
                item.realm.clone(),
                item.area.clone(),
                item.resource.clone(),
            ))
            .or_default()
            .push(item);
    }

    let mut hotspots = Vec::new();
    let mut last_changed_at: Option<DateTime<Utc>> = None;

    for queue in queues {
        let key = (
            queue.family,
            queue.realm.clone(),
            queue.area.clone(),
            queue.resource.clone(),
        );
        let queue_inflight = inflight_by_resource.get(&key).cloned().unwrap_or_default();
        let queue_dead_letters = dead_letters_by_resource
            .get(&key)
            .cloned()
            .unwrap_or_default();
        let backlog = queue.messages_ready + queue.messages_delayed;
        let waiters = backlog;
        let dead_letter_count = queue.messages_dead_lettered.max(queue_dead_letters.len());
        let inflight_count = queue.messages_inflight.max(queue_inflight.len());
        let last_failure_at = queue_dead_letters
            .iter()
            .filter_map(|item| parse_rfc3339(&item.dead_lettered_at))
            .max();
        let last_change_from_age = if queue.oldest_backlog_age_seconds > 0 {
            Some(now - Duration::seconds(queue.oldest_backlog_age_seconds as i64))
        } else {
            None
        };
        let last_change_from_inflight = queue_inflight
            .iter()
            .filter_map(|item| parse_rfc3339(&item.expires_at))
            .max();
        let last_changed = [
            last_failure_at,
            last_change_from_age,
            last_change_from_inflight,
        ]
        .into_iter()
        .flatten()
        .max();
        let recent_transition_count = queue_dead_letters
            .iter()
            .filter_map(|item| parse_rfc3339(&item.dead_lettered_at))
            .filter(|ts| is_recent(*ts, now))
            .count() as u64
            + queue_inflight
                .iter()
                .filter_map(|item| parse_rfc3339(&item.expires_at))
                .filter(|ts| is_recent(*ts, now))
                .count() as u64;
        let contention_count = if backlog > 0 {
            backlog as u64
        } else {
            dead_letter_count as u64
        };
        let (label, trend, severity, bottleneck) = if dead_letter_count > 0 {
            (
                DiagnosisLabel::DeadLetterPressure,
                DiagnosticTrend::Stalled,
                DiagnosticSeverity::High,
                Some("dead-letter pressure".to_string()),
            )
        } else if backlog > 0 && inflight_count == 0 {
            (
                DiagnosisLabel::WorkerStarvation,
                DiagnosticTrend::Growing,
                DiagnosticSeverity::High,
                Some("worker starvation".to_string()),
            )
        } else if backlog > 0
            && (queue.messages_delayed > 0 || queue.oldest_backlog_age_seconds >= 30)
        {
            (
                DiagnosisLabel::BacklogGrowth,
                DiagnosticTrend::Growing,
                DiagnosticSeverity::Medium,
                Some("backlog growth".to_string()),
            )
        } else if backlog > 0 {
            (
                DiagnosisLabel::Throughput,
                DiagnosticTrend::Steady,
                DiagnosticSeverity::Low,
                Some("queue throughput".to_string()),
            )
        } else if inflight_count > 0 {
            (
                DiagnosisLabel::Throughput,
                DiagnosticTrend::Steady,
                DiagnosticSeverity::Informational,
                Some("queue throughput".to_string()),
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
        if backlog > 0 {
            hints.push(format!("{backlog} message(s) waiting"));
        }
        if queue.messages_delayed > 0 {
            hints.push(format!("{} delayed message(s)", queue.messages_delayed));
        }
        if queue.delay_age_buckets.over_15m > 0 {
            hints.push(format!(
                "{} delayed message(s) are 15m+ old",
                queue.delay_age_buckets.over_15m
            ));
        }
        if dead_letter_count > 0 {
            hints.push(format!("{dead_letter_count} dead-lettered message(s)"));
        }
        if dead_letter_count > 0 && dead_letter_transitions_total > 0 {
            hints.push(format!(
                "{dead_letter_transitions_total} dead-letter transition(s) recorded"
            ));
        }
        if complete_rejected_total > 0 {
            hints.push(format!(
                "{complete_rejected_total} queue complete rejection(s)"
            ));
        }
        if queue.oldest_backlog_age_seconds > 0 {
            hints.push(format!(
                "oldest backlog message is {}s old",
                queue.oldest_backlog_age_seconds
            ));
        }

        let snapshot = DiagnosticSnapshot::with_stage(
            label,
            trend,
            severity,
            bottleneck.clone(),
            last_changed,
            None,
            last_failure_at,
            Some(queue.oldest_backlog_age_seconds),
            recent_transition_count,
            dead_letter_count as u64,
            contention_count,
            waiters,
            hints,
        );

        if let Some(candidate_changed_at) = last_changed {
            let previous = last_changed_at;
            last_changed_at = Some(match previous {
                Some(current) => current.max(candidate_changed_at),
                None => candidate_changed_at,
            });
        }

        hotspots.push(ScoredHotspot {
            score: backlog as f64 * 4.0
                + dead_letter_count as f64 * 8.0
                + inflight_count as f64 * 1.5
                + queue.delay_age_buckets.over_15m as f64 * 2.0
                + queue.oldest_backlog_age_seconds as f64 / 12.0,
            hotspot: DiagnosticHotspot {
                domain: "queue".to_string(),
                realm: Some(queue.realm.clone()),
                area: Some(queue.area.clone()),
                resource: Some(queue.resource.clone()),
                operation: None,
                family: Some(queue.family),
                backlog: Some(backlog),
                inflight: Some(inflight_count),
                ready: Some(queue.messages_ready),
                delayed: Some(queue.messages_delayed),
                dead_letters: Some(dead_letter_count),
                workers: None,
                subscriptions: None,
                owner_session: queue_inflight.first().map(|item| item.session_id.clone()),
                worker_session: None,
                snapshot,
            },
            last_changed_at: last_changed,
        });
    }

    if hotspots.is_empty() {
        DomainAnalysis::healthy()
    } else {
        DomainAnalysis::from_hotspots(hotspots)
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct RpcLatencySummary {
    pub worker_latency_buckets: RpcLatencyBuckets,
    pub slowest_worker_average_latency_ms: f64,
}

pub(crate) fn summarize_rpc_worker_latency<'a, I>(workers: I) -> RpcLatencySummary
where
    I: IntoIterator<Item = &'a RpcWorker>,
{
    let mut summary = RpcLatencySummary::default();

    for worker in workers {
        if worker.average_latency_ms.is_finite() {
            let latency_ms = worker.average_latency_ms.max(0.0);
            let mut latency_bucket = RpcLatencyBuckets::default();
            latency_bucket.record_latency_ms(latency_ms);
            summary.worker_latency_buckets.merge(latency_bucket);
            summary.slowest_worker_average_latency_ms =
                summary.slowest_worker_average_latency_ms.max(latency_ms);
        }
    }

    summary
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn analyze_rpc(
    workers: &[RpcWorker],
    pending: &[RpcPendingRequest],
    request_timeouts_total: u64,
    backpressure_rejects_total: u64,
    duplicate_correlation_rejects_total: u64,
    wrong_worker_rejects_total: u64,
    responses_dropped_closed_caller_total: u64,
    responses_missing_pending_total: u64,
    acks_rejected_wrong_worker_total: u64,
    now: DateTime<Utc>,
) -> DomainAnalysis {
    let mut worker_by_route: HashMap<(String, String, String, String), Vec<&RpcWorker>> =
        HashMap::new();
    for worker in workers {
        if let Some(route) = route_quad(&worker.route) {
            worker_by_route
                .entry((
                    route.realm.to_string(),
                    route.area.to_string(),
                    route.resource.to_string(),
                    route.operation.to_string(),
                ))
                .or_default()
                .push(worker);
        }
    }

    let mut pending_by_route: HashMap<(String, String, String, String), Vec<&RpcPendingRequest>> =
        HashMap::new();
    for item in pending {
        if let Some(route) = route_quad(&item.route) {
            pending_by_route
                .entry((
                    route.realm.to_string(),
                    route.area.to_string(),
                    route.resource.to_string(),
                    route.operation.to_string(),
                ))
                .or_default()
                .push(item);
        }
    }

    let mut hotspots = Vec::new();
    let mut last_changed_at: Option<DateTime<Utc>> = None;
    let late_response_pressure =
        responses_dropped_closed_caller_total + responses_missing_pending_total;
    let correlation_pressure = duplicate_correlation_rejects_total
        + wrong_worker_rejects_total
        + acks_rejected_wrong_worker_total;
    let transport_pressure = request_timeouts_total + backpressure_rejects_total;
    let overall_latency_summary = summarize_rpc_worker_latency(workers.iter());

    for (key, pending_items) in pending_by_route {
        let workers = worker_by_route.get(&key).cloned().unwrap_or_default();
        let pending_count = pending_items.len();
        let worker_count = workers.len();
        let age_seconds = pending_items.iter().map(|item| item.age_seconds).max();
        let last_failure_at = pending_items
            .iter()
            .filter_map(|item| parse_rfc3339(&item.submitted_at))
            .max();
        let last_change = pending_items
            .iter()
            .filter_map(|item| parse_rfc3339(&item.submitted_at))
            .chain(
                workers
                    .iter()
                    .filter_map(|item| parse_rfc3339(&item.registered_at)),
            )
            .max();
        let latency_summary = summarize_rpc_worker_latency(workers.iter().copied());
        let slowest_worker_average_latency_ms = latency_summary.slowest_worker_average_latency_ms;
        let recent_transition_count = pending_items
            .iter()
            .filter_map(|item| parse_rfc3339(&item.submitted_at))
            .filter(|ts| is_recent(*ts, now))
            .count() as u64
            + workers
                .iter()
                .filter_map(|item| parse_rfc3339(&item.registered_at))
                .filter(|ts| is_recent(*ts, now))
                .count() as u64;
        let contention_count = pending_count.saturating_sub(worker_count) as u64;
        let (label, trend, severity, bottleneck) = if worker_count == 0 && pending_count > 0 {
            (
                DiagnosisLabel::WorkerStarvation,
                DiagnosticTrend::Growing,
                DiagnosticSeverity::High,
                Some("worker starvation".to_string()),
            )
        } else if late_response_pressure > 0 && pending_count == 0 {
            (
                DiagnosisLabel::DataLossRisk,
                DiagnosticTrend::Stalled,
                DiagnosticSeverity::High,
                Some("late response drop".to_string()),
            )
        } else if correlation_pressure > 0 && pending_count == 0 {
            (
                DiagnosisLabel::DataLossRisk,
                DiagnosticTrend::Stalled,
                DiagnosticSeverity::High,
                Some("correlation mismatch".to_string()),
            )
        } else if pending_count > worker_count
            && (age_seconds.unwrap_or(0) >= 30 || pending_count >= worker_count.saturating_mul(2))
        {
            (
                DiagnosisLabel::BacklogGrowth,
                if age_seconds.unwrap_or(0) >= 60 {
                    DiagnosticTrend::Stalled
                } else {
                    DiagnosticTrend::Growing
                },
                DiagnosticSeverity::High,
                Some("route backlog".to_string()),
            )
        } else if pending_count > worker_count {
            (
                DiagnosisLabel::Contention,
                DiagnosticTrend::Growing,
                DiagnosticSeverity::Medium,
                Some("route contention".to_string()),
            )
        } else if slowest_worker_average_latency_ms >= 100.0 {
            (
                DiagnosisLabel::Throughput,
                DiagnosticTrend::Steady,
                if slowest_worker_average_latency_ms >= 250.0 {
                    DiagnosticSeverity::High
                } else {
                    DiagnosticSeverity::Medium
                },
                Some("slow worker latency".to_string()),
            )
        } else if pending_count > 0 {
            (
                DiagnosisLabel::Throughput,
                DiagnosticTrend::Steady,
                DiagnosticSeverity::Low,
                Some("route throughput".to_string()),
            )
        } else {
            (
                DiagnosisLabel::Healthy,
                DiagnosticTrend::Unknown,
                DiagnosticSeverity::Informational,
                None,
            )
        };
        let failure_count = if matches!(label, DiagnosisLabel::DataLossRisk) {
            if late_response_pressure > 0 {
                late_response_pressure
            } else if correlation_pressure > 0 {
                correlation_pressure
            } else {
                0
            }
        } else {
            0
        };
        let mut hints = vec![];
        if pending_count > 0 {
            hints.push(format!("{pending_count} pending request(s)"));
        }
        if worker_count > 0 {
            hints.push(format!("{worker_count} registered worker(s)"));
        }
        if slowest_worker_average_latency_ms > 0.0 {
            hints.push(format!(
                "slowest worker average latency is {:.1}ms",
                slowest_worker_average_latency_ms
            ));
        }
        if let Some(age) = age_seconds {
            hints.push(format!("oldest request is {age}s old"));
        }
        if duplicate_correlation_rejects_total > 0 {
            hints.push(format!(
                "{duplicate_correlation_rejects_total} duplicate correlation rejection(s)"
            ));
        }
        if wrong_worker_rejects_total > 0 {
            hints.push(format!(
                "{wrong_worker_rejects_total} wrong worker rejection(s)"
            ));
        }
        if late_response_pressure > 0 {
            hints.push(format!("{late_response_pressure} late response drop(s)"));
        }
        if transport_pressure > 0 {
            hints.push(format!(
                "{transport_pressure} timeout/backpressure rejection(s)"
            ));
        }

        let snapshot = DiagnosticSnapshot::with_stage(
            label,
            trend,
            severity,
            bottleneck.clone(),
            last_change,
            None,
            last_failure_at,
            age_seconds,
            recent_transition_count,
            failure_count,
            contention_count,
            pending_count,
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
            score: pending_count as f64 * 5.0
                + contention_count as f64 * 4.0
                + age_seconds.unwrap_or(0) as f64 / 10.0
                + slowest_worker_average_latency_ms / 10.0,
            hotspot: DiagnosticHotspot {
                domain: "rpc".to_string(),
                realm: Some(key.0),
                area: Some(key.1),
                resource: Some(key.2),
                operation: Some(key.3),
                family: None,
                backlog: Some(pending_count),
                inflight: Some(pending_count),
                ready: None,
                delayed: None,
                dead_letters: None,
                workers: Some(worker_count),
                subscriptions: None,
                owner_session: None,
                worker_session: workers.first().map(|worker| worker.session_id.clone()),
                snapshot,
            },
            last_changed_at: last_change,
        });
    }

    let has_route_hotspots = !hotspots.is_empty();
    if !has_route_hotspots {
        let data_loss_pressure = late_response_pressure + correlation_pressure;
        if data_loss_pressure > 0 || transport_pressure > 0 {
            let bottleneck = if late_response_pressure > 0 {
                "late response drop"
            } else if correlation_pressure > 0 {
                "correlation mismatch"
            } else {
                "rpc backpressure"
            };
            let label = if late_response_pressure > 0 || correlation_pressure > 0 {
                DiagnosisLabel::DataLossRisk
            } else {
                DiagnosisLabel::Throughput
            };
            let trend = if late_response_pressure > 0 || correlation_pressure > 0 {
                DiagnosticTrend::Stalled
            } else {
                DiagnosticTrend::Growing
            };
            let severity = if late_response_pressure > 0 || correlation_pressure > 0 {
                DiagnosticSeverity::High
            } else {
                DiagnosticSeverity::Medium
            };
            let mut hints = vec![];
            if request_timeouts_total > 0 {
                hints.push(format!("{request_timeouts_total} request timeout(s)"));
            }
            if backpressure_rejects_total > 0 {
                hints.push(format!(
                    "{backpressure_rejects_total} backpressure reject(s)"
                ));
            }
            if duplicate_correlation_rejects_total > 0 {
                hints.push(format!(
                    "{duplicate_correlation_rejects_total} duplicate correlation rejection(s)"
                ));
            }
            if wrong_worker_rejects_total > 0 {
                hints.push(format!(
                    "{wrong_worker_rejects_total} wrong worker rejection(s)"
                ));
            }
            if late_response_pressure > 0 {
                hints.push(format!("{late_response_pressure} late response drop(s)"));
            }
            if correlation_pressure > 0 {
                hints.push(format!(
                    "{correlation_pressure} correlation mismatch event(s)"
                ));
            }
            hotspots.push(ScoredHotspot {
                score: late_response_pressure as f64 * 12.0
                    + correlation_pressure as f64 * 6.0
                    + transport_pressure as f64 * 2.0,
                hotspot: DiagnosticHotspot {
                    domain: "rpc".to_string(),
                    realm: None,
                    area: None,
                    resource: None,
                    operation: None,
                    family: None,
                    backlog: Some(pending.len()),
                    inflight: Some(pending.len()),
                    ready: None,
                    delayed: None,
                    dead_letters: None,
                    workers: Some(workers.len()),
                    subscriptions: None,
                    owner_session: None,
                    worker_session: None,
                    snapshot: DiagnosticSnapshot::with_stage(
                        label,
                        trend,
                        severity,
                        Some(bottleneck.to_string()),
                        None,
                        None,
                        None,
                        Some(
                            pending
                                .iter()
                                .map(|item| item.age_seconds)
                                .max()
                                .unwrap_or(0),
                        ),
                        data_loss_pressure,
                        data_loss_pressure,
                        correlation_pressure,
                        pending.len(),
                        hints,
                    ),
                },
                last_changed_at: pending
                    .iter()
                    .filter_map(|item| parse_rfc3339(&item.submitted_at))
                    .max(),
            });
        } else if overall_latency_summary.slowest_worker_average_latency_ms >= 100.0 {
            let slowest_latency_ms = overall_latency_summary.slowest_worker_average_latency_ms;
            let severity = if slowest_latency_ms >= 250.0 {
                DiagnosticSeverity::High
            } else {
                DiagnosticSeverity::Medium
            };
            let registered_at_times: Vec<_> = workers
                .iter()
                .filter_map(|worker| parse_rfc3339(&worker.registered_at))
                .collect();
            let mut hints = vec![];
            if !registered_at_times.is_empty() {
                hints.push(format!("{} registered worker(s)", workers.len()));
            }
            hints.push(format!(
                "slowest worker average latency is {:.1}ms",
                slowest_latency_ms
            ));
            hotspots.push(ScoredHotspot {
                score: slowest_latency_ms / 10.0 + workers.len() as f64,
                hotspot: DiagnosticHotspot {
                    domain: "rpc".to_string(),
                    realm: None,
                    area: None,
                    resource: None,
                    operation: None,
                    family: None,
                    backlog: Some(pending.len()),
                    inflight: Some(pending.len()),
                    ready: None,
                    delayed: None,
                    dead_letters: None,
                    workers: Some(workers.len()),
                    subscriptions: None,
                    owner_session: None,
                    worker_session: workers.first().map(|worker| worker.session_id.clone()),
                    snapshot: DiagnosticSnapshot::with_stage(
                        DiagnosisLabel::Throughput,
                        DiagnosticTrend::Steady,
                        severity,
                        Some("slow worker latency".to_string()),
                        registered_at_times.iter().copied().max(),
                        None,
                        None,
                        None,
                        registered_at_times
                            .iter()
                            .filter(|ts| is_recent(**ts, now))
                            .count() as u64,
                        0,
                        0,
                        pending.len(),
                        hints,
                    ),
                },
                last_changed_at: registered_at_times.iter().copied().max(),
            });
        }
    }

    if hotspots.is_empty() {
        DomainAnalysis::healthy()
    } else {
        DomainAnalysis::from_hotspots(hotspots)
    }
}
