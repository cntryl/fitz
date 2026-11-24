use super::model::DiagnosticSnapshotInput;
use chrono::{DateTime, Duration, Utc};
use std::collections::HashMap;

use super::{
    is_recent, parse_rfc3339, route_quad, score_u64, score_usize, DiagnosisLabel,
    DiagnosticHotspot, DiagnosticSeverity, DiagnosticSnapshot, DiagnosticTrend, DomainAnalysis,
    QueueDeadLetter, QueueInflight, QueueInfo, RpcLatencyBuckets, RpcPendingRequest, RpcWorker,
    ScoredHotspot,
};

/// Explanation for RPC entries labelled `DataLossRisk`.
///
/// RPC holds no durable state: a lost response is in-flight work dropped
/// under transport backpressure. Saying "durability gap" here sends an
/// operator hunting for storage corruption that cannot exist.
pub(super) const RPC_RESPONSE_LOSS_HINT: &str =
    "Ephemeral RPC response loss caused by transport backpressure; no durable state is affected";

fn i64_from_u64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn u64_from_usize(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

pub(crate) fn analyze_queue(
    queues: &[QueueInfo],
    inflight: &[QueueInflight],
    dead_letters: &[QueueDeadLetter],
    dead_letter_transitions_total: u64,
    complete_rejected_total: u64,
    now: DateTime<Utc>,
) -> DomainAnalysis {
    let inflight_by_resource = group_queue_inflight_by_resource(inflight);
    let dead_letters_by_resource = group_queue_dead_letters_by_resource(dead_letters);
    let mut hotspots = Vec::new();
    let mut last_changed_at: Option<DateTime<Utc>> = None;

    for queue in queues {
        let resource_key = queue_key(queue.family, &queue.realm, &queue.area, &queue.resource);
        hotspots.push(score_queue_hotspot(
            queue,
            inflight_by_resource
                .get(&resource_key)
                .map_or(&[][..], Vec::as_slice),
            dead_letters_by_resource
                .get(&resource_key)
                .map_or(&[][..], Vec::as_slice),
            dead_letter_transitions_total,
            complete_rejected_total,
            now,
            &mut last_changed_at,
        ));
    }

    if hotspots.is_empty() {
        DomainAnalysis::healthy()
    } else {
        DomainAnalysis::from_hotspots(hotspots)
    }
}

fn queue_key(
    family: u64,
    realm: &str,
    area: &str,
    resource: &str,
) -> (u64, String, String, String) {
    (
        family,
        realm.to_string(),
        area.to_string(),
        resource.to_string(),
    )
}

fn group_queue_inflight_by_resource(
    inflight: &[QueueInflight],
) -> HashMap<(u64, String, String, String), Vec<&QueueInflight>> {
    let mut grouped: HashMap<(u64, String, String, String), Vec<&QueueInflight>> = HashMap::new();
    for item in inflight {
        grouped
            .entry(queue_key(
                item.family,
                &item.realm,
                &item.area,
                &item.resource,
            ))
            .or_default()
            .push(item);
    }
    grouped
}

fn group_queue_dead_letters_by_resource(
    dead_letters: &[QueueDeadLetter],
) -> HashMap<(u64, String, String, String), Vec<&QueueDeadLetter>> {
    let mut grouped: HashMap<(u64, String, String, String), Vec<&QueueDeadLetter>> = HashMap::new();
    for item in dead_letters {
        grouped
            .entry(queue_key(
                item.family,
                &item.realm,
                &item.area,
                &item.resource,
            ))
            .or_default()
            .push(item);
    }
    grouped
}

fn score_queue_hotspot(
    queue: &QueueInfo,
    queue_inflight: &[&QueueInflight],
    queue_dead_letters: &[&QueueDeadLetter],
    dead_letter_transitions_total: u64,
    complete_rejected_total: u64,
    now: DateTime<Utc>,
    last_changed_at: &mut Option<DateTime<Utc>>,
) -> ScoredHotspot {
    let backlog = queue.messages_ready + queue.messages_delayed;
    let waiters = backlog;
    let dead_letter_count = queue.messages_dead_lettered.max(queue_dead_letters.len());
    let inflight_count = queue.messages_inflight.max(queue_inflight.len());
    let last_failure_at = queue_dead_letters
        .iter()
        .filter_map(|item| parse_rfc3339(&item.dead_lettered_at))
        .max();
    let last_change_from_age = if queue.oldest_backlog_age_seconds > 0 {
        Some(now - Duration::seconds(i64_from_u64(queue.oldest_backlog_age_seconds)))
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
    let recent_transition_count =
        count_recent_queue_transitions(queue_inflight, queue_dead_letters, now);
    let contention_count = if backlog > 0 {
        u64_from_usize(backlog)
    } else {
        u64_from_usize(dead_letter_count)
    };
    let (label, trend, severity, bottleneck) =
        queue_hotspot_stage(queue, backlog, inflight_count, dead_letter_count);
    let hints = build_queue_hotspot_hints(
        queue,
        backlog,
        dead_letter_count,
        dead_letter_transitions_total,
        complete_rejected_total,
    );

    let snapshot = DiagnosticSnapshot::with_stage(DiagnosticSnapshotInput {
        current_stage: label,
        trend,
        severity,
        likely_bottleneck: bottleneck.clone(),
        last_changed_at: last_changed,
        last_success_at: None,
        last_failure_at,
        age_seconds: Some(queue.oldest_backlog_age_seconds),
        recent_transition_count,
        failure_count: u64_from_usize(dead_letter_count),
        contention_count,
        waiter_count: waiters,
        explanation_hints: hints,
    });

    if let Some(candidate_changed_at) = last_changed {
        *last_changed_at = Some((*last_changed_at).map_or(candidate_changed_at, |current| {
            current.max(candidate_changed_at)
        }));
    }

    ScoredHotspot {
        score: score_usize(backlog) * 4.0
            + score_usize(dead_letter_count) * 8.0
            + score_usize(inflight_count) * 1.5
            + score_usize(queue.delay_age_buckets.over_15m) * 2.0
            + score_u64(queue.oldest_backlog_age_seconds) / 12.0,
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
    }
}

fn count_recent_queue_transitions(
    queue_inflight: &[&QueueInflight],
    queue_dead_letters: &[&QueueDeadLetter],
    now: DateTime<Utc>,
) -> u64 {
    let dead_letter_transitions = u64_from_usize(
        queue_dead_letters
            .iter()
            .filter_map(|item| parse_rfc3339(&item.dead_lettered_at))
            .filter(|ts| is_recent(*ts, now))
            .count(),
    );
    let inflight_transitions = u64_from_usize(
        queue_inflight
            .iter()
            .filter_map(|item| parse_rfc3339(&item.expires_at))
            .filter(|ts| is_recent(*ts, now))
            .count(),
    );
    dead_letter_transitions.saturating_add(inflight_transitions)
}

fn queue_hotspot_stage(
    queue: &QueueInfo,
    backlog: usize,
    inflight_count: usize,
    dead_letter_count: usize,
) -> (
    DiagnosisLabel,
    DiagnosticTrend,
    DiagnosticSeverity,
    Option<String>,
) {
    if dead_letter_count > 0 {
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
    } else if backlog > 0 && (queue.messages_delayed > 0 || queue.oldest_backlog_age_seconds >= 30)
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
    }
}

fn build_queue_hotspot_hints(
    queue: &QueueInfo,
    backlog: usize,
    dead_letter_count: usize,
    dead_letter_transitions_total: u64,
    complete_rejected_total: u64,
) -> Vec<String> {
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
    hints
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

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub(crate) fn analyze_rpc(
    workers: &[RpcWorker],
    pending: &[RpcPendingRequest],
    request_timeouts_total: u64,
    backpressure_rejects_total: u64,
    duplicate_correlation_rejects_total: u64,
    wrong_worker_rejects_total: u64,
    responses_dropped_closed_caller_total: u64,
    responses_missing_pending_total: u64,
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
    let response_loss_pressure = responses_missing_pending_total;
    let correlation_pressure = duplicate_correlation_rejects_total + wrong_worker_rejects_total;
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
        let recent_transition_count = {
            let pending_transitions = u64_from_usize(
                pending_items
                    .iter()
                    .filter_map(|item| parse_rfc3339(&item.submitted_at))
                    .filter(|ts| is_recent(*ts, now))
                    .count(),
            );
            let worker_transitions = u64_from_usize(
                workers
                    .iter()
                    .filter_map(|item| parse_rfc3339(&item.registered_at))
                    .filter(|ts| is_recent(*ts, now))
                    .count(),
            );
            pending_transitions.saturating_add(worker_transitions)
        };
        let contention_count = u64_from_usize(pending_count.saturating_sub(worker_count));
        let (label, trend, severity, bottleneck) = if worker_count == 0 && pending_count > 0 {
            (
                DiagnosisLabel::WorkerStarvation,
                DiagnosticTrend::Growing,
                DiagnosticSeverity::High,
                Some("worker starvation".to_string()),
            )
        } else if response_loss_pressure > 0 && pending_count == 0 {
            (
                DiagnosisLabel::DataLossRisk,
                DiagnosticTrend::Stalled,
                DiagnosticSeverity::High,
                Some("missing pending response".to_string()),
            )
        } else if correlation_pressure > 0 && pending_count == 0 {
            (
                DiagnosisLabel::DataLossRisk,
                DiagnosticTrend::Stalled,
                DiagnosticSeverity::High,
                Some("correlation mismatch".to_string()),
            )
        } else if transport_pressure > 0 {
            // The backlog is not the problem; the path to the client is.
            (
                DiagnosisLabel::TransportBackpressure,
                DiagnosticTrend::Growing,
                DiagnosticSeverity::High,
                Some("transport backpressure".to_string()),
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
        let failure_count = match label {
            DiagnosisLabel::DataLossRisk => {
                if response_loss_pressure > 0 {
                    response_loss_pressure
                } else {
                    correlation_pressure
                }
            }
            // Shed work is failed work: counting it as zero is what let a
            // saturated broker report success totals with no failures.
            DiagnosisLabel::TransportBackpressure => transport_pressure,
            _ => 0,
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
                "slowest worker average latency is {slowest_worker_average_latency_ms:.1}ms"
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
        if responses_dropped_closed_caller_total > 0 {
            hints.push(format!(
                "{responses_dropped_closed_caller_total} response(s) arrived after caller closure"
            ));
        }
        if response_loss_pressure > 0 {
            hints.push(format!(
                "{response_loss_pressure} response(s) had no pending request state"
            ));
        }
        if matches!(label, DiagnosisLabel::DataLossRisk) {
            hints.push(RPC_RESPONSE_LOSS_HINT.to_string());
        }
        if transport_pressure > 0 {
            hints.push(format!(
                "{transport_pressure} timeout/backpressure rejection(s)"
            ));
        }

        let snapshot = DiagnosticSnapshot::with_stage(DiagnosticSnapshotInput {
            current_stage: label,
            trend,
            severity,
            likely_bottleneck: bottleneck.clone(),
            last_changed_at: last_change,
            last_success_at: None,
            last_failure_at,
            age_seconds,
            recent_transition_count,
            failure_count,
            contention_count,
            waiter_count: pending_count,
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
            score: score_usize(pending_count) * 5.0
                + score_u64(contention_count) * 4.0
                + score_u64(age_seconds.unwrap_or(0)) / 10.0
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
        let data_loss_pressure = response_loss_pressure + correlation_pressure;
        if data_loss_pressure > 0 || transport_pressure > 0 {
            let bottleneck = if response_loss_pressure > 0 {
                "missing pending response"
            } else if correlation_pressure > 0 {
                "correlation mismatch"
            } else {
                "rpc backpressure"
            };
            let label = if response_loss_pressure > 0 || correlation_pressure > 0 {
                DiagnosisLabel::DataLossRisk
            } else {
                // Not throughput: nothing is flowing slowly, work is being
                // shed because the transport cannot carry it.
                DiagnosisLabel::TransportBackpressure
            };
            let trend = if response_loss_pressure > 0 || correlation_pressure > 0 {
                DiagnosticTrend::Stalled
            } else {
                DiagnosticTrend::Growing
            };
            let severity = DiagnosticSeverity::High;
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
            if responses_dropped_closed_caller_total > 0 {
                hints.push(format!(
                    "{responses_dropped_closed_caller_total} response(s) arrived after caller closure"
                ));
            }
            if response_loss_pressure > 0 {
                hints.push(format!(
                    "{response_loss_pressure} response(s) had no pending request state"
                ));
            }
            if correlation_pressure > 0 {
                hints.push(format!(
                    "{correlation_pressure} correlation mismatch event(s)"
                ));
            }
            hotspots.push(ScoredHotspot {
                score: score_u64(response_loss_pressure) * 12.0
                    + score_u64(correlation_pressure) * 6.0
                    + score_u64(transport_pressure) * 2.0,
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
                    snapshot: DiagnosticSnapshot::with_stage(DiagnosticSnapshotInput {
                        current_stage: label,
                        trend,
                        severity,
                        likely_bottleneck: Some(bottleneck.to_string()),
                        last_changed_at: None,
                        last_success_at: None,
                        last_failure_at: None,
                        age_seconds: Some(
                            pending
                                .iter()
                                .map(|item| item.age_seconds)
                                .max()
                                .unwrap_or(0),
                        ),
                        recent_transition_count: data_loss_pressure,
                        failure_count: if data_loss_pressure > 0 {
                            data_loss_pressure
                        } else {
                            transport_pressure
                        },
                        contention_count: correlation_pressure,
                        waiter_count: pending.len(),
                        explanation_hints: hints,
                    }),
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
                "slowest worker average latency is {slowest_latency_ms:.1}ms"
            ));
            let recent_worker_transitions = u64_from_usize(
                registered_at_times
                    .iter()
                    .filter(|ts| is_recent(**ts, now))
                    .count(),
            );
            hotspots.push(ScoredHotspot {
                score: slowest_latency_ms / 10.0 + score_usize(workers.len()),
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
                    snapshot: DiagnosticSnapshot::with_stage(DiagnosticSnapshotInput {
                        current_stage: DiagnosisLabel::Throughput,
                        trend: DiagnosticTrend::Steady,
                        severity,
                        likely_bottleneck: Some("slow worker latency".to_string()),
                        last_changed_at: registered_at_times.iter().copied().max(),
                        last_success_at: None,
                        last_failure_at: None,
                        age_seconds: None,
                        recent_transition_count: recent_worker_transitions,
                        failure_count: 0,
                        contention_count: 0,
                        waiter_count: pending.len(),
                        explanation_hints: hints,
                    }),
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
