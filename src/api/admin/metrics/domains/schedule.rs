use crate::boot::Runtime;

pub(super) fn append_metrics(output: &mut String, runtime: &Runtime) {
    output.push_str("# HELP fitz_schedule_active Active schedules\n");
    output.push_str("# TYPE fitz_schedule_active gauge\n");
    output.push_str(&format!(
        "fitz_schedule_active {}\n",
        runtime.schedule_active()
    ));
    output.push('\n');

    output.push_str("# HELP fitz_schedule_executions_per_minute Acknowledged schedule handoffs over the last minute\n");
    output.push_str("# TYPE fitz_schedule_executions_per_minute gauge\n");
    output.push_str(&format!(
        "fitz_schedule_executions_per_minute {}\n",
        runtime.schedule_executions_per_minute()
    ));
    output.push('\n');

    output.push_str("# HELP fitz_schedule_subscriptions_active Active schedule subscriptions\n");
    output.push_str("# TYPE fitz_schedule_subscriptions_active gauge\n");
    output.push_str(&format!(
        "fitz_schedule_subscriptions_active {}\n",
        runtime.schedule_subscriptions_active()
    ));
    output.push('\n');

    output.push_str("# HELP fitz_schedule_pending_fire_claims Durably claimed schedule occurrences awaiting acknowledged live handoff\n");
    output.push_str("# TYPE fitz_schedule_pending_fire_claims gauge\n");
    output.push_str(&format!(
        "fitz_schedule_pending_fire_claims {}\n",
        runtime.schedule_pending_fire_claims()
    ));
    output.push('\n');

    output.push_str("# HELP fitz_schedule_pending_ack_retries Pending schedule live handoffs waiting on durable acknowledgement retry\n");
    output.push_str("# TYPE fitz_schedule_pending_ack_retries gauge\n");
    output.push_str(&format!(
        "fitz_schedule_pending_ack_retries {}\n",
        runtime.schedule_pending_ack_retries()
    ));
    output.push('\n');

    output.push_str("# HELP fitz_schedule_oldest_pending_claim_age_seconds Oldest pending schedule fire claim age in seconds\n");
    output.push_str("# TYPE fitz_schedule_oldest_pending_claim_age_seconds gauge\n");
    output.push_str(&format!(
        "fitz_schedule_oldest_pending_claim_age_seconds {}\n",
        runtime.schedule_oldest_pending_claim_age_seconds()
    ));
    output.push('\n');

    output.push_str("# HELP fitz_schedule_notify_failures_total Total schedule live publish handoffs that failed to route\n");
    output.push_str("# TYPE fitz_schedule_notify_failures_total counter\n");
    output.push_str(&format!(
        "fitz_schedule_notify_failures_total {}\n",
        runtime.schedule_notify_failures()
    ));
    output.push('\n');

    output.push_str("# HELP fitz_schedule_ack_failures_total Total pending-fire acknowledgement persistence failures\n");
    output.push_str("# TYPE fitz_schedule_ack_failures_total counter\n");
    output.push_str(&format!(
        "fitz_schedule_ack_failures_total {}\n",
        runtime.schedule_ack_failures()
    ));
    output.push('\n');

    output.push_str("# HELP fitz_schedule_overdue_normalizations_total Total schedule definitions normalized forward on last broker start\n");
    output.push_str("# TYPE fitz_schedule_overdue_normalizations_total counter\n");
    output.push_str(&format!(
        "fitz_schedule_overdue_normalizations_total {}\n",
        runtime.schedule_overdue_normalizations()
    ));
    output.push('\n');

    output.push_str("# HELP fitz_schedule_pending_claims_expired_total Total stale pending schedule claims cleaned up by this broker process\n");
    output.push_str("# TYPE fitz_schedule_pending_claims_expired_total counter\n");
    output.push_str(&format!(
        "fitz_schedule_pending_claims_expired_total {}\n",
        runtime.schedule_pending_claims_expired_total()
    ));
    output.push('\n');

    output.push_str("# HELP fitz_schedule_pending_claim_cleanup_failure_total Total failed pending schedule claim cleanup attempts\n");
    output.push_str("# TYPE fitz_schedule_pending_claim_cleanup_failure_total counter\n");
    output.push_str(&format!(
        "fitz_schedule_pending_claim_cleanup_failure_total {}\n",
        runtime.schedule_pending_claim_cleanup_failures_total()
    ));
    output.push('\n');

    output.push_str("# HELP fitz_schedule_create_persistence_failures_total Total schedule create mutations that failed to persist\n");
    output.push_str("# TYPE fitz_schedule_create_persistence_failures_total counter\n");
    output.push_str(&format!(
        "fitz_schedule_create_persistence_failures_total {}\n",
        runtime.schedule_create_persistence_failures_total()
    ));
    output.push('\n');

    output.push_str("# HELP fitz_schedule_upsert_persistence_failures_total Total schedule upsert mutations that failed to persist\n");
    output.push_str("# TYPE fitz_schedule_upsert_persistence_failures_total counter\n");
    output.push_str(&format!(
        "fitz_schedule_upsert_persistence_failures_total {}\n",
        runtime.schedule_upsert_persistence_failures_total()
    ));
    output.push('\n');

    output.push_str("# HELP fitz_schedule_cancel_persistence_failures_total Total schedule cancel mutations that failed to persist\n");
    output.push_str("# TYPE fitz_schedule_cancel_persistence_failures_total counter\n");
    output.push_str(&format!(
        "fitz_schedule_cancel_persistence_failures_total {}\n",
        runtime.schedule_cancel_persistence_failures_total()
    ));
    output.push('\n');
}
