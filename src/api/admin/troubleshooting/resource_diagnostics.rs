use super::*;

pub(crate) fn kv_resource_diagnostics(transactions_active: usize) -> DiagnosticSnapshot {
    if transactions_active == 0 {
        return DiagnosticSnapshot::healthy();
    }

    DiagnosticSnapshot::with_stage(
        DiagnosisLabel::Contention,
        if transactions_active > 1 {
            DiagnosticTrend::Growing
        } else {
            DiagnosticTrend::Steady
        },
        DiagnosticSeverity::Medium,
        Some("transaction coordination".to_string()),
        None,
        None,
        None,
        None,
        0,
        0,
        transactions_active as u64,
        transactions_active,
        vec![format!("{transactions_active} open transaction(s)")],
    )
}

pub(crate) fn queue_resource_diagnostics(
    messages_ready: usize,
    messages_delayed: usize,
    messages_inflight: usize,
    messages_dead_lettered: usize,
    oldest_backlog_age_seconds: u64,
    delay_age_buckets: QueueAgeBuckets,
) -> DiagnosticSnapshot {
    let backlog = messages_ready + messages_delayed;
    let has_dead_letters = messages_dead_lettered > 0;
    let has_backlog = backlog > 0;
    let has_inflight = messages_inflight > 0;

    let (label, trend, severity, bottleneck) = if has_dead_letters {
        (
            DiagnosisLabel::DeadLetterPressure,
            DiagnosticTrend::Stalled,
            DiagnosticSeverity::High,
            Some("dead-letter pressure".to_string()),
        )
    } else if has_backlog && !has_inflight {
        (
            DiagnosisLabel::WorkerStarvation,
            if oldest_backlog_age_seconds >= 30 {
                DiagnosticTrend::Growing
            } else {
                DiagnosticTrend::Steady
            },
            DiagnosticSeverity::High,
            Some("worker starvation".to_string()),
        )
    } else if has_backlog && oldest_backlog_age_seconds >= 30 {
        (
            DiagnosisLabel::BacklogGrowth,
            DiagnosticTrend::Growing,
            DiagnosticSeverity::Medium,
            Some("backlog growth".to_string()),
        )
    } else if has_inflight {
        (
            DiagnosisLabel::Throughput,
            DiagnosticTrend::Steady,
            DiagnosticSeverity::Low,
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

    DiagnosticSnapshot::with_stage(
        label,
        trend,
        severity,
        bottleneck,
        None,
        None,
        None,
        if oldest_backlog_age_seconds > 0 {
            Some(oldest_backlog_age_seconds)
        } else {
            None
        },
        0,
        messages_dead_lettered as u64,
        backlog as u64,
        backlog,
        {
            let mut hints = Vec::new();
            if has_backlog {
                hints.push(format!("{backlog} message(s) waiting"));
            }
            if messages_delayed > 0 {
                hints.push(format!("{messages_delayed} delayed message(s)"));
            }
            if messages_dead_lettered > 0 {
                hints.push(format!("{messages_dead_lettered} dead-lettered message(s)"));
            }
            if delay_age_buckets.over_15m > 0 {
                hints.push(format!(
                    "{} delayed message(s) are 15m+ old",
                    delay_age_buckets.over_15m
                ));
            }
            if oldest_backlog_age_seconds > 0 {
                hints.push(format!(
                    "oldest backlog message is {oldest_backlog_age_seconds}s old"
                ));
            }
            hints
        },
    )
}

pub(crate) fn stream_resource_diagnostics(
    offset: u64,
    watermark: u64,
    sessions_active: usize,
) -> DiagnosticSnapshot {
    let lag = offset.saturating_sub(watermark);
    if lag == 0 && sessions_active == 0 {
        return DiagnosticSnapshot::healthy();
    }

    let (label, trend, severity, bottleneck) = if lag > 0 {
        (
            DiagnosisLabel::Throughput,
            DiagnosticTrend::Stalled,
            DiagnosticSeverity::Medium,
            Some("append lag".to_string()),
        )
    } else {
        (
            DiagnosisLabel::Throughput,
            DiagnosticTrend::Steady,
            DiagnosticSeverity::Low,
            Some("append throughput".to_string()),
        )
    };

    DiagnosticSnapshot::with_stage(
        label,
        trend,
        severity,
        bottleneck,
        None,
        None,
        None,
        None,
        0,
        0,
        lag,
        sessions_active,
        {
            let mut hints = Vec::new();
            if lag > 0 {
                hints.push(format!("stream lag is {lag} event(s)"));
            }
            if sessions_active > 0 {
                hints.push(format!("{sessions_active} live append session(s)"));
            }
            hints
        },
    )
}

pub(crate) fn lease_resource_diagnostics(
    active_leases: usize,
    oldest_lease_age_seconds: Option<u64>,
    renewals_total: usize,
) -> DiagnosticSnapshot {
    if active_leases == 0 {
        return DiagnosticSnapshot::healthy();
    }

    let churn_pressure = renewals_total > 0;
    let severity = if churn_pressure {
        if renewals_total > active_leases {
            DiagnosticSeverity::Medium
        } else {
            DiagnosticSeverity::Low
        }
    } else if active_leases > 1 {
        DiagnosticSeverity::Medium
    } else {
        DiagnosticSeverity::Low
    };

    DiagnosticSnapshot::with_stage(
        DiagnosisLabel::Contention,
        if churn_pressure {
            DiagnosticTrend::Growing
        } else {
            DiagnosticTrend::Steady
        },
        severity,
        Some(if churn_pressure {
            "lease ownership churn".to_string()
        } else {
            "lease ownership".to_string()
        }),
        None,
        None,
        None,
        oldest_lease_age_seconds,
        0,
        0,
        renewals_total as u64,
        active_leases,
        {
            let mut hints = vec![format!("{active_leases} active lease(s)")];
            if renewals_total > 0 {
                hints.push(format!("{renewals_total} renewals recorded"));
            }
            hints
        },
    )
}

pub(crate) fn notice_resource_diagnostics(subscriptions_active: usize) -> DiagnosticSnapshot {
    if subscriptions_active == 0 {
        return DiagnosticSnapshot::healthy();
    }

    DiagnosticSnapshot::with_stage(
        DiagnosisLabel::Throughput,
        DiagnosticTrend::Steady,
        if subscriptions_active > 25 {
            DiagnosticSeverity::High
        } else {
            DiagnosticSeverity::Low
        },
        Some("subscription fanout".to_string()),
        None,
        None,
        None,
        None,
        0,
        0,
        subscriptions_active as u64,
        subscriptions_active,
        vec![format!("{subscriptions_active} active subscription(s)")],
    )
}

pub(crate) fn rpc_operation_diagnostics(
    workers_registered: usize,
    requests_pending: usize,
    slowest_worker_average_latency_ms: Option<f64>,
) -> DiagnosticSnapshot {
    if workers_registered == 0 && requests_pending == 0 {
        return DiagnosticSnapshot::healthy();
    }

    let slowest_worker_average_latency_ms = slowest_worker_average_latency_ms.unwrap_or(0.0);
    let (label, trend, severity, bottleneck) = if workers_registered == 0 {
        (
            DiagnosisLabel::WorkerStarvation,
            DiagnosticTrend::Growing,
            DiagnosticSeverity::High,
            Some("worker starvation".to_string()),
        )
    } else if requests_pending > workers_registered && requests_pending >= workers_registered * 2 {
        (
            DiagnosisLabel::BacklogGrowth,
            DiagnosticTrend::Growing,
            DiagnosticSeverity::Medium,
            Some("route backlog".to_string()),
        )
    } else if requests_pending > workers_registered {
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
    } else {
        (
            DiagnosisLabel::Throughput,
            DiagnosticTrend::Steady,
            DiagnosticSeverity::Low,
            Some("route throughput".to_string()),
        )
    };

    DiagnosticSnapshot::with_stage(
        label,
        trend,
        severity,
        bottleneck,
        None,
        None,
        None,
        None,
        0,
        0,
        requests_pending as u64,
        requests_pending,
        {
            let mut hints = Vec::new();
            if workers_registered > 0 {
                hints.push(format!("{workers_registered} registered worker(s)"));
            }
            if requests_pending > 0 {
                hints.push(format!("{requests_pending} pending request(s)"));
            }
            if slowest_worker_average_latency_ms > 0.0 {
                hints.push(format!(
                    "slowest worker average latency is {:.1}ms",
                    slowest_worker_average_latency_ms
                ));
            }
            hints
        },
    )
}

pub(crate) fn schedule_resource_diagnostics(
    enabled: bool,
    next_run: Option<&str>,
    last_run: Option<&str>,
    executions_total: u64,
) -> DiagnosticSnapshot {
    let now = Utc::now();
    let next_run_at = next_run.and_then(parse_rfc3339);
    let last_run_at = last_run.and_then(parse_rfc3339);
    let is_overdue = enabled && next_run_at.map(|next| next <= now).unwrap_or(false);
    let age_seconds = if is_overdue {
        next_run_at.map(|next| (now - next).num_seconds().max(0) as u64)
    } else {
        last_run_at.map(|last| (now - last).num_seconds().max(0) as u64)
    };

    if !enabled && next_run_at.is_none() && last_run_at.is_none() {
        return DiagnosticSnapshot::healthy();
    }

    let (label, trend, severity, bottleneck) = if is_overdue {
        (
            DiagnosisLabel::StaleHandoff,
            DiagnosticTrend::Stalled,
            DiagnosticSeverity::High,
            Some("durable handoff".to_string()),
        )
    } else if enabled && executions_total > 0 {
        (
            DiagnosisLabel::Throughput,
            DiagnosticTrend::Steady,
            DiagnosticSeverity::Medium,
            Some("scheduled work".to_string()),
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

    let mut hints = Vec::new();
    if let Some(next) = next_run_at {
        hints.push(format!("next run at {}", rfc3339(next)));
    }
    if let Some(last) = last_run_at {
        hints.push(format!("last run at {}", rfc3339(last)));
    }
    if executions_total > 0 {
        hints.push(format!("{executions_total} total execution(s)"));
    }
    if is_overdue {
        hints.push("schedule is overdue".to_string());
    }

    DiagnosticSnapshot::with_stage(
        label,
        trend,
        severity,
        bottleneck,
        last_run_at.or(next_run_at),
        last_run_at,
        None,
        age_seconds,
        if last_run_at
            .map(|last| is_recent(last, now))
            .unwrap_or(false)
        {
            1
        } else {
            0
        },
        0,
        if is_overdue { 1 } else { 0 },
        if is_overdue { 1 } else { 0 },
        hints,
    )
}
