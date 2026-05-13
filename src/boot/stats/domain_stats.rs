use super::Runtime;
use crate::domains::queue::metrics::{
    METRIC_COMPLETE_TOTAL, METRIC_ENQUEUE_TOTAL, METRIC_EXTEND_TOTAL, METRIC_FAILURE_TOTAL,
    METRIC_RELEASE_TOTAL, METRIC_REQUESTS_TOTAL, METRIC_RESERVE_TOTAL, METRIC_SUCCESS_TOTAL,
};
use std::collections::HashSet;

fn metric_counter(name: &str) -> u64 {
    crate::boot::observability::metrics().counter_get(name)
}

fn metric_gauge(name: &str) -> u64 {
    crate::boot::observability::metrics().gauge_get(name)
}

impl Runtime {
    pub fn queue_messages_ready(&self) -> usize {
        self.domains
            .read()
            .as_ref()
            .map(|domains| domains.queue.ready_message_count())
            .unwrap_or_else(|| {
                self.admin_read_model
                    .queues(None)
                    .into_iter()
                    .map(|queue| queue.messages_ready)
                    .sum()
            })
    }

    pub fn queue_messages_delayed(&self) -> usize {
        self.domains
            .read()
            .as_ref()
            .map(|domains| domains.queue.delayed_message_count())
            .unwrap_or_else(|| {
                self.admin_read_model
                    .queues(None)
                    .into_iter()
                    .map(|queue| queue.messages_delayed)
                    .sum()
            })
    }

    pub fn kv_transactions_active(&self) -> usize {
        self.domains
            .read()
            .as_ref()
            .map(|domains| domains.kv.active_transaction_count())
            .unwrap_or_else(|| self.admin_read_model.kv_transactions(None).len())
    }

    pub fn kv_keys_total(&self) -> usize {
        0
    }

    pub fn notice_subscriptions_active(&self) -> usize {
        self.admin_read_model.notice_subscriptions(None, None).len()
    }

    pub fn notice_routes_active(&self) -> usize {
        self.admin_read_model.notice_routes(None).len()
    }

    pub fn notice_max_route_subscribers(&self) -> usize {
        self.admin_read_model
            .notice_routes(None)
            .into_iter()
            .map(|route| route.subscribers)
            .max()
            .unwrap_or(0)
    }

    pub fn queue_messages_pending(&self) -> usize {
        self.domains
            .read()
            .as_ref()
            .map(|domains| domains.queue.pending_message_count())
            .unwrap_or_else(|| {
                self.admin_read_model
                    .queues(None)
                    .into_iter()
                    .map(|queue| queue.messages_ready + queue.messages_delayed)
                    .sum()
            })
    }

    pub fn queue_messages_dead_lettered(&self) -> usize {
        self.domains
            .read()
            .as_ref()
            .map(|domains| domains.queue.dead_letter_count())
            .unwrap_or_else(|| {
                self.admin_read_model
                    .queues(None)
                    .into_iter()
                    .map(|queue| queue.messages_dead_lettered)
                    .sum()
            })
    }

    pub fn queue_requests_total(&self) -> u64 {
        metric_counter(METRIC_REQUESTS_TOTAL)
    }

    pub fn queue_success_total(&self) -> u64 {
        metric_counter(METRIC_SUCCESS_TOTAL)
    }

    pub fn queue_failure_total(&self) -> u64 {
        metric_counter(METRIC_FAILURE_TOTAL)
    }

    pub fn queue_enqueues_total(&self) -> u64 {
        metric_counter(METRIC_ENQUEUE_TOTAL)
    }

    pub fn queue_reserves_total(&self) -> u64 {
        metric_counter(METRIC_RESERVE_TOTAL)
    }

    pub fn queue_completes_total(&self) -> u64 {
        metric_counter(METRIC_COMPLETE_TOTAL)
    }

    pub fn queue_releases_total(&self) -> u64 {
        metric_counter(METRIC_RELEASE_TOTAL)
    }

    pub fn queue_extends_total(&self) -> u64 {
        metric_counter(METRIC_EXTEND_TOTAL)
    }

    pub fn queue_notify_drops_total(&self) -> u64 {
        metric_counter("fitz_queue_notify_drops_total")
    }

    pub fn queue_redeliveries_total(&self) -> u64 {
        metric_counter("fitz_queue_redeliveries_total")
    }

    pub fn queue_inflight_active(&self) -> usize {
        self.domains
            .read()
            .as_ref()
            .map(|domains| domains.queue.active_inflight_count())
            .unwrap_or_else(|| self.admin_read_model.queue_inflight(None).len())
    }

    pub fn queue_oldest_message_age_seconds(&self) -> u64 {
        self.admin_read_model
            .queues(None)
            .into_iter()
            .map(|queue| queue.oldest_message_age_seconds)
            .max()
            .unwrap_or(0)
    }

    pub fn queue_oldest_backlog_age_seconds(&self) -> u64 {
        self.admin_read_model
            .queues(None)
            .into_iter()
            .map(|queue| queue.oldest_backlog_age_seconds)
            .max()
            .unwrap_or(0)
    }

    pub fn queue_backlog_age_buckets(&self) -> crate::api::admin::QueueAgeBuckets {
        self.admin_read_model.queues(None).into_iter().fold(
            crate::api::admin::QueueAgeBuckets::default(),
            |mut buckets, queue| {
                buckets.merge(queue.backlog_age_buckets);
                buckets
            },
        )
    }

    pub fn rpc_workers_registered(&self) -> usize {
        self.domains
            .read()
            .as_ref()
            .map(|domains| domains.rpc.worker_count())
            .unwrap_or_else(|| self.admin_read_model.rpc_workers(None).len())
    }

    pub fn rpc_requests_pending(&self) -> usize {
        self.rpc_pending_snapshot().len()
    }

    pub fn rpc_oldest_pending_request_age_seconds(&self) -> u64 {
        self.rpc_pending_snapshot()
            .into_iter()
            .map(|request| request.age_seconds)
            .max()
            .unwrap_or(0)
    }

    pub fn rpc_pending_routes_active(&self) -> usize {
        self.rpc_pending_snapshot()
            .into_iter()
            .map(|request| request.route)
            .collect::<HashSet<_>>()
            .len()
    }

    pub fn lease_active(&self) -> usize {
        self.domains
            .read()
            .as_ref()
            .map(|domains| domains.lease.lease_count())
            .unwrap_or_else(|| self.admin_read_model.leases(None).len())
    }

    pub fn stream_active(&self) -> usize {
        self.domains
            .read()
            .as_ref()
            .map(|domains| domains.stream.stream_count())
            .unwrap_or_else(|| self.admin_read_model.streams(None).len())
    }

    pub fn stream_append_sessions_active(&self) -> usize {
        self.domains
            .read()
            .as_ref()
            .map(|domains| domains.stream.append_session_count())
            .unwrap_or_else(|| {
                self.admin_read_model
                    .streams(None)
                    .into_iter()
                    .map(|stream| stream.sessions_active)
                    .sum()
            })
    }

    pub fn kv_operations_per_second(&self) -> f64 {
        0.0
    }

    pub fn stream_events_total(&self) -> usize {
        self.refresh_stream_admin_snapshot();
        self.admin_read_model.stream_events_total()
    }

    pub fn stream_requests_total(&self) -> u64 {
        metric_counter("fitz_stream_requests_total")
    }

    pub fn stream_success_total(&self) -> u64 {
        metric_counter("fitz_stream_success_total")
    }

    pub fn stream_failure_total(&self) -> u64 {
        metric_counter("fitz_stream_failure_total")
    }

    pub fn stream_append_conflicts_total(&self) -> u64 {
        metric_counter("fitz_stream_append_conflicts_total")
    }

    pub fn stream_notify_drops_total(&self) -> u64 {
        metric_counter("fitz_stream_notify_drops_total")
    }

    pub fn stream_append_sessions_started_total(&self) -> u64 {
        metric_counter("fitz_stream_append_sessions_started_total")
    }

    pub fn stream_append_sessions_ended_total(&self) -> u64 {
        metric_counter("fitz_stream_append_sessions_ended_total")
    }

    pub fn stream_operations_per_second(&self) -> f64 {
        let uptime_secs = self.uptime().as_secs_f64();
        if uptime_secs < 0.001 {
            return 0.0;
        }

        let total_operations =
            crate::boot::observability::metrics().counter_get("fitz_stream_operations_total");
        total_operations as f64 / uptime_secs
    }

    pub fn stream_subscriptions_active(&self) -> usize {
        self.domains
            .read()
            .as_ref()
            .map(|domains| domains.stream.subscription_count())
            .unwrap_or(0)
    }

    pub fn stream_watermark_lag_buckets(&self) -> crate::api::admin::StreamLagBuckets {
        self.admin_read_model
            .stream_area_watermarks()
            .into_iter()
            .fold(
                crate::api::admin::StreamLagBuckets::default(),
                |mut buckets, detail| {
                    let max_watermark = detail
                        .family_watermarks
                        .iter()
                        .map(|watermark| watermark.watermark)
                        .max()
                        .unwrap_or(0);
                    let mut area_buckets = crate::api::admin::StreamLagBuckets::default();
                    for watermark in detail.family_watermarks {
                        area_buckets
                            .record_lag_events(max_watermark.saturating_sub(watermark.watermark));
                    }
                    buckets.merge(area_buckets);
                    buckets
                },
            )
    }

    pub fn notice_publishes_per_second(&self) -> f64 {
        let uptime_secs = self.uptime().as_secs_f64();
        if uptime_secs < 0.001 {
            return 0.0;
        }

        self.notice_requests_total() as f64 / uptime_secs
    }

    pub fn notice_requests_total(&self) -> u64 {
        metric_counter("fitz_notice_requests_total")
    }

    pub fn notice_success_total(&self) -> u64 {
        metric_counter("fitz_notice_success_total")
    }

    pub fn notice_failure_total(&self) -> u64 {
        metric_counter("fitz_notice_failure_total")
    }

    pub fn notice_delivery_drops_total(&self) -> u64 {
        metric_counter("fitz_notice_delivery_drops_total")
    }

    pub fn notice_unsubscribes_total(&self) -> u64 {
        metric_counter("fitz_notice_unsubscribes_total")
    }

    pub fn notice_wildcard_limit_rejects_total(&self) -> u64 {
        metric_counter("fitz_notice_wildcard_limit_rejects_total")
    }

    pub fn queue_operations_per_second(&self) -> f64 {
        let uptime_secs = self.uptime().as_secs_f64();
        if uptime_secs < 0.001 {
            return 0.0;
        }

        self.queue_requests_total() as f64 / uptime_secs
    }

    pub fn rpc_operations_per_second(&self) -> f64 {
        let uptime_secs = self.uptime().as_secs_f64();
        if uptime_secs < 0.001 {
            return 0.0;
        }

        self.rpc_requests_total() as f64 / uptime_secs
    }

    pub fn rpc_requests_total(&self) -> u64 {
        metric_counter("fitz_rpc_requests_total")
    }

    pub fn rpc_success_total(&self) -> u64 {
        metric_counter("fitz_rpc_success_total")
    }

    pub fn rpc_failure_total(&self) -> u64 {
        metric_counter("fitz_rpc_failure_total")
    }

    pub fn rpc_request_timeouts_total(&self) -> u64 {
        metric_counter("rpc_request_timeouts_total")
    }

    pub fn rpc_backpressure_rejects_total(&self) -> u64 {
        metric_counter("rpc_backpressure_rejects_total")
    }

    pub fn rpc_duplicate_correlation_rejects_total(&self) -> u64 {
        metric_counter("rpc_requests_rejected_duplicate_correlation_total")
    }

    pub fn rpc_wrong_worker_rejects_total(&self) -> u64 {
        metric_counter("rpc_responses_rejected_wrong_worker_total")
    }

    pub fn rpc_responses_dropped_closed_caller_total(&self) -> u64 {
        metric_counter("rpc_responses_dropped_closed_caller_total")
    }

    pub fn rpc_responses_missing_pending_total(&self) -> u64 {
        metric_counter("rpc_responses_missing_pending_total")
    }

    pub fn rpc_acks_rejected_wrong_worker_total(&self) -> u64 {
        metric_counter("rpc_acks_rejected_wrong_worker_total")
    }

    pub fn lease_operations_per_second(&self) -> f64 {
        let uptime_secs = self.uptime().as_secs_f64();
        if uptime_secs < 0.001 {
            return 0.0;
        }

        self.lease_requests_total() as f64 / uptime_secs
    }

    pub fn lease_requests_total(&self) -> u64 {
        metric_counter("fitz_lease_requests_total")
    }

    pub fn lease_success_total(&self) -> u64 {
        metric_counter("fitz_lease_success_total")
    }

    pub fn lease_failure_total(&self) -> u64 {
        metric_counter("fitz_lease_failure_total")
    }

    pub fn lease_waiter_depth(&self) -> usize {
        let direct_depth = metric_gauge("fitz_lease_waiters_gauge");
        let legacy_depth = metric_gauge("fitz_lease_waiter_depth");
        direct_depth.max(legacy_depth) as usize
    }

    pub fn lease_acquire_timeouts_total(&self) -> u64 {
        metric_counter("fitz_lease_acquire_timeouts_total")
    }

    pub fn lease_forced_releases_total(&self) -> u64 {
        metric_counter("fitz_lease_forced_releases_total")
    }

    pub fn lease_invalid_token_rejects_total(&self) -> u64 {
        metric_counter("fitz_lease_invalid_token_rejects_total")
    }

    pub fn lease_ownership_churn_total(&self) -> u64 {
        metric_counter("fitz_lease_ownership_churn_total")
    }

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

    pub fn schedule_active(&self) -> usize {
        self.domains
            .read()
            .as_ref()
            .map(|domains| domains.schedule.schedule_count())
            .unwrap_or_else(|| self.admin_read_model.schedules(None).len())
    }

    pub fn schedule_executions_per_minute(&self) -> f64 {
        self.domains
            .read()
            .as_ref()
            .map(|domains| domains.schedule.executions_per_minute())
            .unwrap_or(0.0)
    }

    pub fn schedule_subscriptions_active(&self) -> usize {
        self.domains
            .read()
            .as_ref()
            .map(|domains| domains.schedule.subscription_count())
            .unwrap_or(0)
    }

    pub fn schedule_pending_fire_claims(&self) -> usize {
        self.domains
            .read()
            .as_ref()
            .map(|domains| domains.schedule.pending_fire_count())
            .unwrap_or(0)
    }

    pub fn schedule_pending_ack_retries(&self) -> usize {
        self.domains
            .read()
            .as_ref()
            .map(|domains| domains.schedule.pending_ack_retry_count())
            .unwrap_or(0)
    }

    pub fn schedule_oldest_pending_claim_age_seconds(&self) -> u64 {
        self.domains
            .read()
            .as_ref()
            .map(|domains| domains.schedule.oldest_pending_claim_age_seconds())
            .unwrap_or(0)
    }

    pub fn schedule_notify_failures(&self) -> u64 {
        self.domains
            .read()
            .as_ref()
            .map(|domains| domains.schedule.notify_failure_count())
            .unwrap_or(0)
    }

    pub fn schedule_ack_failures(&self) -> u64 {
        self.domains
            .read()
            .as_ref()
            .map(|domains| domains.schedule.ack_failure_count())
            .unwrap_or(0)
    }

    pub fn schedule_overdue_normalizations(&self) -> u64 {
        self.domains
            .read()
            .as_ref()
            .map(|domains| domains.schedule.overdue_normalization_count())
            .unwrap_or(0)
    }

    pub fn schedule_pending_claims_expired_total(&self) -> u64 {
        metric_counter("fitz_schedule_pending_claims_expired_total")
    }

    pub fn schedule_pending_claim_cleanup_failures_total(&self) -> u64 {
        metric_counter("fitz_schedule_pending_claim_cleanup_failure_total")
    }
}
