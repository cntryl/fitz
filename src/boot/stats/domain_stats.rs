use super::Runtime;
use crate::domains::queue::metrics::{
    METRIC_COMPLETE_TOTAL, METRIC_ENQUEUE_TOTAL, METRIC_EXTEND_TOTAL, METRIC_FAILURE_TOTAL,
    METRIC_RELEASE_TOTAL, METRIC_REQUESTS_TOTAL, METRIC_RESERVE_TOTAL, METRIC_SUCCESS_TOTAL,
};
use crate::domains::schedule::metrics::{
    METRIC_CANCEL_PERSISTENCE_FAILURES_TOTAL, METRIC_CREATE_PERSISTENCE_FAILURES_TOTAL,
    METRIC_UPSERT_PERSISTENCE_FAILURES_TOTAL,
};
use std::collections::HashSet;

fn metric_counter(name: &str) -> u64 {
    crate::observability::metrics().counter_get(name)
}

fn metric_gauge(name: &str) -> u64 {
    crate::observability::metrics().gauge_get(name)
}

fn u64_to_f64(value: u64) -> f64 {
    let high = u32::try_from(value >> 32).unwrap_or(u32::MAX);
    let low = u32::try_from(value & u64::from(u32::MAX)).unwrap_or(u32::MAX);
    f64::from(high) * 4_294_967_296.0 + f64::from(low)
}

fn u64_to_usize_saturating(value: u64) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX)
}

impl Runtime {
    #[must_use]
    pub fn queue_messages_ready(&self) -> usize {
        self.domains.read().as_ref().map_or_else(
            || {
                self.admin_read_model
                    .queues(None)
                    .into_iter()
                    .map(|queue| queue.messages_ready)
                    .sum()
            },
            |domains| domains.queue_ready_message_count(),
        )
    }

    #[must_use]
    pub fn queue_messages_delayed(&self) -> usize {
        self.domains.read().as_ref().map_or_else(
            || {
                self.admin_read_model
                    .queues(None)
                    .into_iter()
                    .map(|queue| queue.messages_delayed)
                    .sum()
            },
            |domains| domains.queue_delayed_message_count(),
        )
    }

    #[must_use]
    pub fn kv_transactions_active(&self) -> usize {
        self.domains.read().as_ref().map_or_else(
            || self.admin_read_model.kv_transactions(None).len(),
            |domains| domains.kv_active_transaction_count(),
        )
    }

    #[must_use]
    pub fn kv_keys_total(&self) -> usize {
        0
    }

    #[must_use]
    pub fn kv_commits_failed_total(&self) -> u64 {
        metric_counter("fitz_kv_commits_failed_total")
    }

    #[must_use]
    pub fn kv_rollbacks_total(&self) -> u64 {
        metric_counter("fitz_kv_rollbacks_total")
    }

    #[must_use]
    pub fn kv_invalid_transaction_rejects_total(&self) -> u64 {
        metric_counter("fitz_kv_invalid_transaction_rejects_total")
    }

    #[must_use]
    pub fn kv_notify_drops_total(&self) -> u64 {
        metric_counter(crate::domains::kv::metrics::METRIC_NOTIFY_DROPS_TOTAL)
    }

    #[must_use]
    pub fn notice_subscriptions_active(&self) -> usize {
        self.admin_read_model.notice_subscriptions(None, None).len()
    }

    #[must_use]
    pub fn notice_routes_active(&self) -> usize {
        self.admin_read_model.notice_routes(None).len()
    }

    #[must_use]
    pub fn notice_max_route_subscribers(&self) -> usize {
        self.admin_read_model
            .notice_routes(None)
            .into_iter()
            .map(|route| route.subscribers)
            .max()
            .unwrap_or(0)
    }

    #[must_use]
    pub fn queue_messages_pending(&self) -> usize {
        self.domains.read().as_ref().map_or_else(
            || {
                self.admin_read_model
                    .queues(None)
                    .into_iter()
                    .map(|queue| queue.messages_ready + queue.messages_delayed)
                    .sum()
            },
            |domains| domains.queue_pending_message_count(),
        )
    }

    #[must_use]
    pub fn queue_messages_dead_lettered(&self) -> usize {
        self.domains.read().as_ref().map_or_else(
            || {
                self.admin_read_model
                    .queues(None)
                    .into_iter()
                    .map(|queue| queue.messages_dead_lettered)
                    .sum()
            },
            |domains| domains.queue_dead_letter_count(),
        )
    }

    #[must_use]
    pub fn queue_requests_total(&self) -> u64 {
        metric_counter(METRIC_REQUESTS_TOTAL)
    }

    #[must_use]
    pub fn queue_success_total(&self) -> u64 {
        metric_counter(METRIC_SUCCESS_TOTAL)
    }

    #[must_use]
    pub fn queue_failure_total(&self) -> u64 {
        metric_counter(METRIC_FAILURE_TOTAL)
    }

    #[must_use]
    pub fn queue_enqueues_total(&self) -> u64 {
        metric_counter(METRIC_ENQUEUE_TOTAL)
    }

    #[must_use]
    pub fn queue_reserves_total(&self) -> u64 {
        metric_counter(METRIC_RESERVE_TOTAL)
    }

    #[must_use]
    pub fn queue_completes_total(&self) -> u64 {
        metric_counter(METRIC_COMPLETE_TOTAL)
    }

    #[must_use]
    pub fn queue_releases_total(&self) -> u64 {
        metric_counter(METRIC_RELEASE_TOTAL)
    }

    #[must_use]
    pub fn queue_extends_total(&self) -> u64 {
        metric_counter(METRIC_EXTEND_TOTAL)
    }

    #[must_use]
    pub fn queue_notify_drops_total(&self) -> u64 {
        metric_counter(crate::domains::queue::metrics::METRIC_NOTIFY_DROPS_TOTAL)
    }

    #[must_use]
    pub fn queue_redeliveries_total(&self) -> u64 {
        metric_counter("fitz_queue_redeliveries_total")
    }

    #[must_use]
    pub fn queue_dead_letter_transitions_total(&self) -> u64 {
        metric_counter("fitz_queue_dlq_transitions_total")
    }

    #[must_use]
    pub fn queue_complete_rejected_total(&self) -> u64 {
        metric_counter("fitz_queue_complete_rejected_total")
    }

    #[must_use]
    pub fn queue_inflight_active(&self) -> usize {
        self.domains.read().as_ref().map_or_else(
            || self.admin_read_model.queue_inflight(None).len(),
            |domains| domains.queue_active_inflight_count(),
        )
    }

    #[must_use]
    pub fn queue_oldest_message_age_seconds(&self) -> u64 {
        self.admin_read_model
            .queues(None)
            .into_iter()
            .map(|queue| queue.oldest_message_age_seconds)
            .max()
            .unwrap_or(0)
    }

    #[must_use]
    pub fn queue_oldest_backlog_age_seconds(&self) -> u64 {
        self.admin_read_model
            .queues(None)
            .into_iter()
            .map(|queue| queue.oldest_backlog_age_seconds)
            .max()
            .unwrap_or(0)
    }

    #[must_use]
    pub fn queue_backlog_age_buckets(&self) -> crate::control::admin::QueueAgeBuckets {
        self.admin_read_model.queues(None).into_iter().fold(
            crate::control::admin::QueueAgeBuckets::default(),
            |mut buckets, queue| {
                buckets.merge(queue.backlog_age_buckets);
                buckets
            },
        )
    }

    #[must_use]
    pub fn queue_delay_age_buckets(&self) -> crate::control::admin::QueueAgeBuckets {
        self.admin_read_model.queues(None).into_iter().fold(
            crate::control::admin::QueueAgeBuckets::default(),
            |mut buckets, queue| {
                buckets.merge(queue.delay_age_buckets);
                buckets
            },
        )
    }

    #[must_use]
    pub fn router_backpressure_total(&self) -> u64 {
        metric_counter("fitz_router_backpressure_total")
    }

    #[must_use]
    pub fn router_high_lane_backpressure_total(&self) -> u64 {
        metric_counter("fitz_router_high_lane_backpressure_total")
    }

    #[must_use]
    pub fn rpc_workers_registered(&self) -> usize {
        self.domains.read().as_ref().map_or_else(
            || self.admin_read_model.rpc_workers(None).len(),
            |domains| domains.rpc_worker_count(),
        )
    }

    #[must_use]
    pub fn rpc_requests_pending(&self) -> usize {
        self.domains.read().as_ref().map_or_else(
            || self.rpc_pending_snapshot().len(),
            |domains| domains.rpc_pending_request_count(),
        )
    }

    #[must_use]
    pub fn rpc_oldest_pending_request_age_seconds(&self) -> u64 {
        self.rpc_pending_snapshot()
            .into_iter()
            .map(|request| request.age_seconds)
            .max()
            .unwrap_or(0)
    }

    #[must_use]
    pub fn rpc_pending_routes_active(&self) -> usize {
        self.rpc_pending_snapshot()
            .into_iter()
            .map(|request| request.route)
            .collect::<HashSet<_>>()
            .len()
    }

    #[must_use]
    pub fn rpc_worker_latency_buckets(&self) -> crate::control::admin::RpcLatencyBuckets {
        let workers = self.admin_read_model.rpc_workers(None);
        crate::api::admin::troubleshooting::summarize_rpc_worker_latency(workers.iter())
            .worker_latency_buckets
    }

    #[must_use]
    pub fn rpc_slowest_worker_average_latency_ms(&self) -> f64 {
        let workers = self.admin_read_model.rpc_workers(None);
        crate::api::admin::troubleshooting::summarize_rpc_worker_latency(workers.iter())
            .slowest_worker_average_latency_ms
    }

    #[must_use]
    pub fn lease_active(&self) -> usize {
        self.domains.read().as_ref().map_or_else(
            || self.admin_read_model.leases(None).len(),
            |domains| domains.lease_count(),
        )
    }

    #[must_use]
    pub fn stream_active(&self) -> usize {
        self.domains.read().as_ref().map_or_else(
            || self.admin_read_model.streams(None).len(),
            |domains| domains.stream_count(),
        )
    }

    #[must_use]
    pub fn stream_append_sessions_active(&self) -> usize {
        self.domains.read().as_ref().map_or_else(
            || {
                self.admin_read_model
                    .streams(None)
                    .into_iter()
                    .map(|stream| stream.sessions_active)
                    .sum()
            },
            |domains| domains.stream_append_session_count(),
        )
    }

    #[must_use]
    pub fn kv_operations_per_second(&self) -> f64 {
        0.0
    }

    #[must_use]
    pub fn stream_events_total(&self) -> usize {
        self.refresh_stream_admin_snapshot();
        self.admin_read_model.stream_events_total()
    }

    #[must_use]
    pub fn stream_requests_total(&self) -> u64 {
        metric_counter("fitz_stream_requests_total")
    }

    #[must_use]
    pub fn stream_success_total(&self) -> u64 {
        metric_counter("fitz_stream_success_total")
    }

    #[must_use]
    pub fn stream_failure_total(&self) -> u64 {
        metric_counter("fitz_stream_failure_total")
    }

    #[must_use]
    pub fn stream_request_latency_buckets(&self) -> crate::control::admin::StreamLatencyBuckets {
        crate::control::admin::StreamLatencyBuckets::from_histogram(
            crate::observability::metrics()
                .histogram_get_buckets("fitz_stream_latency_ms")
                .unwrap_or([0; 9]),
        )
    }

    #[must_use]
    pub fn stream_append_conflicts_total(&self) -> u64 {
        metric_counter("fitz_stream_append_conflicts_total")
    }

    #[must_use]
    pub fn stream_notify_drops_total(&self) -> u64 {
        metric_counter(crate::domains::stream::metrics::METRIC_NOTIFY_DROPS_TOTAL)
    }

    #[must_use]
    pub fn stream_response_drops_total(&self) -> u64 {
        metric_counter(crate::domains::stream::metrics::METRIC_RESPONSE_DROPS_TOTAL)
    }

    #[must_use]
    pub fn stream_append_sessions_started_total(&self) -> u64 {
        metric_counter("fitz_stream_append_sessions_started_total")
    }

    #[must_use]
    pub fn stream_append_sessions_ended_total(&self) -> u64 {
        metric_counter("fitz_stream_append_sessions_ended_total")
    }

    #[must_use]
    pub fn stream_operations_per_second(&self) -> f64 {
        let uptime_secs = self.uptime().as_secs_f64();
        if uptime_secs < 0.001 {
            return 0.0;
        }

        let total_operations =
            crate::observability::metrics().counter_get("fitz_stream_operations_total");
        u64_to_f64(total_operations) / uptime_secs
    }

    #[must_use]
    pub fn stream_subscriptions_active(&self) -> usize {
        self.domains
            .read()
            .as_ref()
            .map_or(0, |domains| domains.stream_subscription_count())
    }

    #[must_use]
    pub fn stream_watermark_lag_buckets(&self) -> crate::control::admin::StreamLagBuckets {
        self.admin_read_model
            .stream_area_watermarks()
            .into_iter()
            .fold(
                crate::control::admin::StreamLagBuckets::default(),
                |mut buckets, detail| {
                    let max_watermark = detail
                        .family_watermarks
                        .iter()
                        .map(|watermark| watermark.watermark)
                        .max()
                        .unwrap_or(0);
                    let mut area_buckets = crate::control::admin::StreamLagBuckets::default();
                    for watermark in detail.family_watermarks {
                        area_buckets
                            .record_lag_events(max_watermark.saturating_sub(watermark.watermark));
                    }
                    buckets.merge(area_buckets);
                    buckets
                },
            )
    }

    #[must_use]
    pub fn notice_publishes_per_second(&self) -> f64 {
        let uptime_secs = self.uptime().as_secs_f64();
        if uptime_secs < 0.001 {
            return 0.0;
        }

        u64_to_f64(self.notice_requests_total()) / uptime_secs
    }

    #[must_use]
    pub fn notice_requests_total(&self) -> u64 {
        metric_counter("fitz_notice_requests_total")
    }

    #[must_use]
    pub fn notice_success_total(&self) -> u64 {
        metric_counter("fitz_notice_success_total")
    }

    #[must_use]
    pub fn notice_failure_total(&self) -> u64 {
        metric_counter("fitz_notice_failure_total")
    }

    #[must_use]
    pub fn notice_delivery_drops_total(&self) -> u64 {
        metric_counter(crate::domains::notice::metrics::METRIC_DELIVERY_DROPS_TOTAL)
    }

    #[must_use]
    pub fn notice_response_drops_total(&self) -> u64 {
        metric_counter(crate::domains::notice::metrics::METRIC_RESPONSE_DROPS_TOTAL)
    }

    #[must_use]
    pub fn notice_unsubscribes_total(&self) -> u64 {
        metric_counter("fitz_notice_unsubscribes_total")
    }

    #[must_use]
    pub fn notice_wildcard_limit_rejects_total(&self) -> u64 {
        metric_counter("fitz_notice_wildcard_limit_rejects_total")
    }

    #[must_use]
    pub fn queue_operations_per_second(&self) -> f64 {
        let uptime_secs = self.uptime().as_secs_f64();
        if uptime_secs < 0.001 {
            return 0.0;
        }

        u64_to_f64(self.queue_requests_total()) / uptime_secs
    }

    #[must_use]
    pub fn rpc_operations_per_second(&self) -> f64 {
        let uptime_secs = self.uptime().as_secs_f64();
        if uptime_secs < 0.001 {
            return 0.0;
        }

        u64_to_f64(self.rpc_requests_total()) / uptime_secs
    }

    #[must_use]
    pub fn rpc_requests_total(&self) -> u64 {
        metric_counter("fitz_rpc_requests_total")
    }

    #[must_use]
    pub fn rpc_success_total(&self) -> u64 {
        metric_counter("fitz_rpc_success_total")
    }

    #[must_use]
    pub fn rpc_failure_total(&self) -> u64 {
        metric_counter("fitz_rpc_failure_total")
    }

    #[must_use]
    pub fn rpc_response_drops_total(&self) -> u64 {
        metric_counter(crate::domains::rpc::metrics::METRIC_RESPONSE_DROPS_TOTAL)
    }

    #[must_use]
    pub fn rpc_request_timeouts_total(&self) -> u64 {
        metric_counter("rpc_request_timeouts_total")
    }

    #[must_use]
    pub fn rpc_backpressure_rejects_total(&self) -> u64 {
        metric_counter("rpc_backpressure_rejects_total")
    }

    #[must_use]
    pub fn rpc_duplicate_correlation_rejects_total(&self) -> u64 {
        metric_counter("rpc_requests_rejected_duplicate_correlation_total")
    }

    #[must_use]
    pub fn rpc_wrong_worker_rejects_total(&self) -> u64 {
        metric_counter("rpc_responses_rejected_wrong_worker_total")
    }

    #[must_use]
    pub fn rpc_responses_dropped_closed_caller_total(&self) -> u64 {
        metric_counter("rpc_responses_dropped_closed_caller_total")
    }

    #[must_use]
    pub fn rpc_responses_missing_pending_total(&self) -> u64 {
        metric_counter("rpc_responses_missing_pending_total")
    }

    #[must_use]
    pub fn rpc_invalid_sequence_responses_total(&self) -> u64 {
        metric_counter("rpc_response_invalid_sequence_total")
    }

    #[must_use]
    pub fn rpc_invalid_sequence_errors_forwarded_total(&self) -> u64 {
        metric_counter("rpc_invalid_sequence_errors_forwarded_total")
    }

    #[must_use]
    pub fn rpc_invalid_sequence_errors_dropped_total(&self) -> u64 {
        metric_counter("rpc_invalid_sequence_errors_dropped_total")
    }

    #[must_use]
    pub fn lease_operations_per_second(&self) -> f64 {
        let uptime_secs = self.uptime().as_secs_f64();
        if uptime_secs < 0.001 {
            return 0.0;
        }

        u64_to_f64(self.lease_requests_total()) / uptime_secs
    }

    #[must_use]
    pub fn lease_requests_total(&self) -> u64 {
        metric_counter("fitz_lease_requests_total")
    }

    #[must_use]
    pub fn lease_success_total(&self) -> u64 {
        metric_counter("fitz_lease_success_total")
    }

    #[must_use]
    pub fn lease_failure_total(&self) -> u64 {
        metric_counter("fitz_lease_failure_total")
    }

    #[must_use]
    pub fn lease_waiter_depth(&self) -> usize {
        let direct_depth = metric_gauge("fitz_lease_waiters_gauge");
        let legacy_depth = metric_gauge("fitz_lease_waiter_depth");
        u64_to_usize_saturating(direct_depth.max(legacy_depth))
    }

    #[must_use]
    pub fn lease_acquire_timeouts_total(&self) -> u64 {
        metric_counter("fitz_lease_acquire_timeouts_total")
    }

    #[must_use]
    pub fn lease_forced_releases_total(&self) -> u64 {
        metric_counter("fitz_lease_forced_releases_total")
    }

    #[must_use]
    pub fn lease_invalid_token_rejects_total(&self) -> u64 {
        metric_counter("fitz_lease_invalid_token_rejects_total")
    }

    #[must_use]
    pub fn lease_response_drops_total(&self) -> u64 {
        metric_counter(crate::domains::lease::metrics::METRIC_RESPONSE_DROPS_TOTAL)
    }

    #[must_use]
    pub fn lease_notify_drops_total(&self) -> u64 {
        metric_counter(crate::domains::lease::metrics::METRIC_NOTIFY_DROPS_TOTAL)
    }

    #[must_use]
    pub fn lease_ownership_churn_total(&self) -> u64 {
        metric_counter("fitz_lease_ownership_churn_total")
    }

    #[must_use]
    pub fn lease_oldest_lease_age_seconds(&self) -> u64 {
        self.admin_read_model
            .leases(None)
            .into_iter()
            .filter_map(|lease| {
                crate::api::admin::troubleshooting::age_seconds_since(&lease.acquired_at)
            })
            .max()
            .unwrap_or(0)
    }

    #[must_use]
    pub fn schedule_active(&self) -> usize {
        self.domains.read().as_ref().map_or_else(
            || self.admin_read_model.schedules(None).len(),
            |domains| domains.schedule_count(),
        )
    }

    #[must_use]
    pub fn schedule_executions_per_minute(&self) -> f64 {
        self.domains
            .read()
            .as_ref()
            .map_or(0.0, |domains| domains.schedule_executions_per_minute())
    }

    #[must_use]
    pub fn schedule_subscriptions_active(&self) -> usize {
        self.domains
            .read()
            .as_ref()
            .map_or(0, |domains| domains.schedule_subscription_count())
    }

    #[must_use]
    pub fn schedule_pending_fire_claims(&self) -> usize {
        self.domains
            .read()
            .as_ref()
            .map_or(0, |domains| domains.schedule_pending_fire_count())
    }

    #[must_use]
    pub fn schedule_pending_ack_retries(&self) -> usize {
        self.domains
            .read()
            .as_ref()
            .map_or(0, |domains| domains.schedule_pending_ack_retry_count())
    }

    #[must_use]
    pub fn schedule_request_latency_buckets(
        &self,
    ) -> crate::control::admin::ScheduleLatencyBuckets {
        crate::control::admin::ScheduleLatencyBuckets::from_histogram(
            crate::observability::metrics()
                .histogram_get_buckets("fitz_schedule_latency_ms")
                .unwrap_or([0; 9]),
        )
    }

    #[must_use]
    pub fn schedule_oldest_pending_claim_age_seconds(&self) -> u64 {
        self.domains.read().as_ref().map_or(0, |domains| {
            domains.schedule_oldest_pending_claim_age_seconds()
        })
    }

    #[must_use]
    pub fn schedule_notify_failures(&self) -> u64 {
        self.domains
            .read()
            .as_ref()
            .map_or(0, |domains| domains.schedule_notify_failure_count())
    }

    #[must_use]
    pub fn schedule_response_drops_total(&self) -> u64 {
        metric_counter(crate::domains::schedule::metrics::METRIC_RESPONSE_DROPS_TOTAL)
    }

    #[must_use]
    pub fn schedule_ack_failures(&self) -> u64 {
        self.domains
            .read()
            .as_ref()
            .map_or(0, |domains| domains.schedule_ack_failure_count())
    }

    #[must_use]
    pub fn schedule_overdue_normalizations(&self) -> u64 {
        self.domains
            .read()
            .as_ref()
            .map_or(0, |domains| domains.schedule_overdue_normalization_count())
    }

    #[must_use]
    pub fn schedule_create_persistence_failures_total(&self) -> u64 {
        metric_counter(METRIC_CREATE_PERSISTENCE_FAILURES_TOTAL)
    }

    #[must_use]
    pub fn schedule_upsert_persistence_failures_total(&self) -> u64 {
        metric_counter(METRIC_UPSERT_PERSISTENCE_FAILURES_TOTAL)
    }

    #[must_use]
    pub fn schedule_cancel_persistence_failures_total(&self) -> u64 {
        metric_counter(METRIC_CANCEL_PERSISTENCE_FAILURES_TOTAL)
    }
}
