use crate::boot::Runtime;
use std::fmt::Write as _;

use super::super::rendering::encode_prometheus_label_value;

pub(super) fn append_metrics(output: &mut String, runtime: &Runtime) {
    append_core_metrics(output, runtime);
    append_lag_bucket_metrics(output, runtime);
    append_watermark_metrics(output, runtime);
}

fn append_core_metrics(output: &mut String, runtime: &Runtime) {
    output.push_str("# HELP fitz_stream_active Active streams\n");
    output.push_str("# TYPE fitz_stream_active gauge\n");
    let _ = writeln!(output, "fitz_stream_active {}", runtime.stream_active());
    output.push('\n');

    output.push_str("# HELP fitz_stream_response_drops_total Total Stream responses dropped by this broker process\n# TYPE fitz_stream_response_drops_total counter\n");
    let _ = writeln!(
        output,
        "fitz_stream_response_drops_total {}",
        runtime.stream_response_drops_total()
    );
    output.push('\n');

    output.push_str(
        "# HELP fitz_stream_append_sessions_active Live append sessions currently tracked by the broker process\n",
    );
    output.push_str("# TYPE fitz_stream_append_sessions_active gauge\n");
    let _ = writeln!(
        output,
        "fitz_stream_append_sessions_active {}",
        runtime.stream_append_sessions_active()
    );
    output.push('\n');

    output.push_str("# HELP fitz_stream_events_total Total committed stream events visible through the admin snapshot\n");
    output.push_str("# TYPE fitz_stream_events_total gauge\n");
    let _ = writeln!(
        output,
        "fitz_stream_events_total {}",
        runtime.stream_events_total()
    );
    output.push('\n');

    output.push_str(
        "# HELP fitz_stream_append_sessions_started_total Total stream append sessions started in this broker process\n",
    );
    output.push_str("# TYPE fitz_stream_append_sessions_started_total counter\n");
    let _ = writeln!(
        output,
        "fitz_stream_append_sessions_started_total {}",
        runtime.stream_append_sessions_started_total()
    );
    output.push('\n');

    output.push_str(
        "# HELP fitz_stream_append_sessions_ended_total Total stream append sessions ended in this broker process\n",
    );
    output.push_str("# TYPE fitz_stream_append_sessions_ended_total counter\n");
    let _ = writeln!(
        output,
        "fitz_stream_append_sessions_ended_total {}",
        runtime.stream_append_sessions_ended_total()
    );
    output.push('\n');

    output.push_str(
        "# HELP fitz_stream_operations_per_second Lifetime-average Stream operations per second\n",
    );
    output.push_str("# TYPE fitz_stream_operations_per_second gauge\n");
    let _ = writeln!(
        output,
        "fitz_stream_operations_per_second {}",
        runtime.stream_operations_per_second()
    );
    output.push('\n');

    output.push_str("# HELP fitz_stream_subscriptions_active Active Stream subscriptions\n");
    output.push_str("# TYPE fitz_stream_subscriptions_active gauge\n");
    let _ = writeln!(
        output,
        "fitz_stream_subscriptions_active {}",
        runtime.stream_subscriptions_active()
    );
    output.push('\n');

    output.push_str("# HELP fitz_stream_notify_drops_total Total stream notifications dropped by this broker process\n");
    output.push_str("# TYPE fitz_stream_notify_drops_total counter\n");
    let _ = writeln!(
        output,
        "fitz_stream_notify_drops_total {}",
        runtime.stream_notify_drops_total()
    );
    output.push('\n');
}

fn append_lag_bucket_metrics(output: &mut String, runtime: &Runtime) {
    let watermark_lag_buckets = runtime.stream_watermark_lag_buckets();
    output.push_str("# HELP fitz_stream_watermark_lag_bucket_caught_up Stream family watermarks aligned with the fastest family in their area\n");
    output.push_str("# TYPE fitz_stream_watermark_lag_bucket_caught_up gauge\n");
    let _ = writeln!(
        output,
        "fitz_stream_watermark_lag_bucket_caught_up {}",
        watermark_lag_buckets.caught_up
    );
    output.push('\n');

    output.push_str("# HELP fitz_stream_watermark_lag_bucket_under_10 Stream family watermarks trailing the fastest family by fewer than 10 events\n");
    output.push_str("# TYPE fitz_stream_watermark_lag_bucket_under_10 gauge\n");
    let _ = writeln!(
        output,
        "fitz_stream_watermark_lag_bucket_under_10 {}",
        watermark_lag_buckets.under_10
    );
    output.push('\n');

    output.push_str("# HELP fitz_stream_watermark_lag_bucket_under_100 Stream family watermarks trailing the fastest family by fewer than 100 events\n");
    output.push_str("# TYPE fitz_stream_watermark_lag_bucket_under_100 gauge\n");
    let _ = writeln!(
        output,
        "fitz_stream_watermark_lag_bucket_under_100 {}",
        watermark_lag_buckets.under_100
    );
    output.push('\n');

    output.push_str("# HELP fitz_stream_watermark_lag_bucket_over_100 Stream family watermarks trailing the fastest family by 100 events or more\n");
    output.push_str("# TYPE fitz_stream_watermark_lag_bucket_over_100 gauge\n");
    let _ = writeln!(
        output,
        "fitz_stream_watermark_lag_bucket_over_100 {}",
        watermark_lag_buckets.over_100
    );
    output.push('\n');
}

fn append_watermark_metrics(output: &mut String, runtime: &Runtime) {
    output.push_str(
        "# HELP fitz_stream_realm_watermark Highest committed realm watermark per Stream route family and realm\n",
    );
    output.push_str("# TYPE fitz_stream_realm_watermark gauge\n");
    for detail in runtime.stream_list_realm_watermark_details() {
        let realm = encode_prometheus_label_value(&detail.realm);
        for watermark in detail.family_watermarks {
            let _ = writeln!(
                output,
                "fitz_stream_realm_watermark{{realm=\"{}\",family=\"{}\"}} {}",
                realm, watermark.family, watermark.watermark
            );
        }
    }
    output.push('\n');

    output.push_str(
        "# HELP fitz_stream_area_watermark Highest committed area watermark per Stream route family, realm, and area\n",
    );
    output.push_str("# TYPE fitz_stream_area_watermark gauge\n");
    for detail in runtime.stream_list_area_watermark_details() {
        let realm = encode_prometheus_label_value(&detail.realm);
        let area = encode_prometheus_label_value(&detail.area);
        for watermark in detail.family_watermarks {
            let _ = writeln!(
                output,
                "fitz_stream_area_watermark{{realm=\"{}\",area=\"{}\",family=\"{}\"}} {}",
                realm, area, watermark.family, watermark.watermark
            );
        }
    }
    output.push('\n');
}
