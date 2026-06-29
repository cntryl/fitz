use crate::boot::Runtime;
use std::fmt::Write as _;

pub(super) fn append_metrics(output: &mut String, runtime: &Runtime) {
    output.push_str("# HELP fitz_queue_messages_pending Pending queue messages\n");
    output.push_str("# TYPE fitz_queue_messages_pending gauge\n");
    let _ = writeln!(
        output,
        "fitz_queue_messages_pending {}",
        runtime.queue_messages_pending()
    );
    output.push('\n');

    output.push_str("# HELP fitz_queue_inflight_active Active queue inflight entries\n");
    output.push_str("# TYPE fitz_queue_inflight_active gauge\n");
    let _ = writeln!(
        output,
        "fitz_queue_inflight_active {}",
        runtime.queue_inflight_active()
    );
    output.push('\n');

    output.push_str("# HELP fitz_queue_oldest_message_age_seconds Oldest visible queue message age in seconds\n");
    output.push_str("# TYPE fitz_queue_oldest_message_age_seconds gauge\n");
    let _ = writeln!(
        output,
        "fitz_queue_oldest_message_age_seconds {}",
        runtime.queue_oldest_message_age_seconds()
    );
    output.push('\n');

    let backlog_age_buckets = runtime.queue_backlog_age_buckets();
    let delay_age_buckets = runtime.queue_delay_age_buckets();

    output.push_str("# HELP fitz_queue_oldest_backlog_age_seconds Oldest ready-or-delayed queue backlog age in seconds\n");
    output.push_str("# TYPE fitz_queue_oldest_backlog_age_seconds gauge\n");
    let _ = writeln!(
        output,
        "fitz_queue_oldest_backlog_age_seconds {}",
        runtime.queue_oldest_backlog_age_seconds()
    );
    output.push('\n');

    output.push_str("# HELP fitz_queue_backlog_age_bucket_under_1m Ready-or-delayed queue messages younger than 1 minute\n");
    output.push_str("# TYPE fitz_queue_backlog_age_bucket_under_1m gauge\n");
    let _ = writeln!(
        output,
        "fitz_queue_backlog_age_bucket_under_1m {}",
        backlog_age_buckets.under_1m
    );
    output.push('\n');

    output.push_str("# HELP fitz_queue_backlog_age_bucket_under_5m Ready-or-delayed queue messages between 1 and 5 minutes old\n");
    output.push_str("# TYPE fitz_queue_backlog_age_bucket_under_5m gauge\n");
    let _ = writeln!(
        output,
        "fitz_queue_backlog_age_bucket_under_5m {}",
        backlog_age_buckets.under_5m
    );
    output.push('\n');

    output.push_str("# HELP fitz_queue_backlog_age_bucket_under_15m Ready-or-delayed queue messages between 5 and 15 minutes old\n");
    output.push_str("# TYPE fitz_queue_backlog_age_bucket_under_15m gauge\n");
    let _ = writeln!(
        output,
        "fitz_queue_backlog_age_bucket_under_15m {}",
        backlog_age_buckets.under_15m
    );
    output.push('\n');

    output.push_str("# HELP fitz_queue_backlog_age_bucket_over_15m Ready-or-delayed queue messages 15 minutes old or older\n");
    output.push_str("# TYPE fitz_queue_backlog_age_bucket_over_15m gauge\n");
    let _ = writeln!(
        output,
        "fitz_queue_backlog_age_bucket_over_15m {}",
        backlog_age_buckets.over_15m
    );
    output.push('\n');

    output.push_str("# HELP fitz_queue_delay_age_bucket_under_1m Delayed queue messages younger than 1 minute\n");
    output.push_str("# TYPE fitz_queue_delay_age_bucket_under_1m gauge\n");
    let _ = writeln!(
        output,
        "fitz_queue_delay_age_bucket_under_1m {}",
        delay_age_buckets.under_1m
    );
    output.push('\n');

    output.push_str("# HELP fitz_queue_delay_age_bucket_under_5m Delayed queue messages between 1 and 5 minutes old\n");
    output.push_str("# TYPE fitz_queue_delay_age_bucket_under_5m gauge\n");
    let _ = writeln!(
        output,
        "fitz_queue_delay_age_bucket_under_5m {}",
        delay_age_buckets.under_5m
    );
    output.push('\n');

    output.push_str("# HELP fitz_queue_delay_age_bucket_under_15m Delayed queue messages between 5 and 15 minutes old\n");
    output.push_str("# TYPE fitz_queue_delay_age_bucket_under_15m gauge\n");
    let _ = writeln!(
        output,
        "fitz_queue_delay_age_bucket_under_15m {}",
        delay_age_buckets.under_15m
    );
    output.push('\n');

    output.push_str("# HELP fitz_queue_delay_age_bucket_over_15m Delayed queue messages 15 minutes old or older\n");
    output.push_str("# TYPE fitz_queue_delay_age_bucket_over_15m gauge\n");
    let _ = writeln!(
        output,
        "fitz_queue_delay_age_bucket_over_15m {}",
        delay_age_buckets.over_15m
    );
    output.push('\n');

    output.push_str("# HELP fitz_queue_redeliveries_total Total queue message redeliveries recorded by this broker process\n");
    output.push_str("# TYPE fitz_queue_redeliveries_total counter\n");
    let _ = writeln!(
        output,
        "fitz_queue_redeliveries_total {}",
        runtime.queue_redeliveries_total()
    );
    output.push('\n');

    output.push_str("# HELP fitz_queue_notify_drops_total Total queue notifications dropped by this broker process\n");
    output.push_str("# TYPE fitz_queue_notify_drops_total counter\n");
    let _ = writeln!(
        output,
        "fitz_queue_notify_drops_total {}",
        runtime.queue_notify_drops_total()
    );
    output.push('\n');
}
