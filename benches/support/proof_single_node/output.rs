use super::*;
use std::fmt::Write as _;
use std::fs;

pub(super) fn write_report(report: &ProofReport) {
    fs::create_dir_all("target/perf_proof").expect("create perf proof output dir");
    let json = serde_json::to_string_pretty(report).expect("serialize proof report");
    fs::write("target/perf_proof/single_node.json", json).expect("write proof json");
    fs::write(
        "target/perf_proof/single_node.md",
        render_markdown_report(report),
    )
    .expect("write proof markdown");
}

fn render_markdown_report(report: &ProofReport) -> String {
    let mut md = String::new();
    writeln!(md, "# Fitz Single-Node Performance Proof").expect("write markdown");
    writeln!(md).expect("write markdown");
    writeln!(md, "Generated: {}", report.generated_at).expect("write markdown");
    writeln!(
        md,
        "Samples: {}, warmup: {}",
        report.settings.samples, report.settings.warmup
    )
    .expect("write markdown");
    writeln!(md).expect("write markdown");
    writeln!(md, "## Conclusions").expect("write markdown");
    writeln!(
        md,
        "- Event-sourcing throughput capacity: {}",
        report.conclusions.event_sourcing_capacity
    )
    .expect("write markdown");
    writeln!(
        md,
        "- Queue enqueue p99 under 1ms: {}",
        report.conclusions.queue_enqueue_p99_under_1ms
    )
    .expect("write markdown");
    writeln!(
        md,
        "- Stream recovery isolated from queue depth: {}",
        report.conclusions.stream_recovery_is_queue_depth_isolated
    )
    .expect("write markdown");
    writeln!(
        md,
        "- Route-count sensitivity: {}",
        report.conclusions.route_count_effect
    )
    .expect("write markdown");

    render_latency_table(&mut md, "Queue Enqueue Latency", &report.queue_latency);
    render_latency_table(
        &mut md,
        "Stream Append Latency Context",
        &report.stream_append_latency,
    );
    render_recovery_table(&mut md, &report.recovery);
    render_route_table(&mut md, &report.route_sensitivity);
    render_throughput_table(&mut md, &report.throughput_evidence);
    md
}

fn render_latency_table(md: &mut String, title: &str, rows: &[LatencyRow]) {
    writeln!(md).expect("write markdown");
    writeln!(md, "## {title}").expect("write markdown");
    writeln!(
        md,
        "| Layer | Clients | Ops/sec | p50 us | p95 us | p99 us | Max us |"
    )
    .expect("write markdown");
    writeln!(md, "|---|---:|---:|---:|---:|---:|---:|").expect("write markdown");
    for row in rows {
        writeln!(
            md,
            "| {} | {} | {:.0} | {} | {} | {} | {} |",
            row.layer,
            row.client_count,
            row.stats.ops_sec,
            row.stats.p50_us,
            row.stats.p95_us,
            row.stats.p99_us,
            row.stats.max_us
        )
        .expect("write markdown");
    }
}

fn render_recovery_table(md: &mut String, rows: &[RecoveryRow]) {
    writeln!(md).expect("write markdown");
    writeln!(md, "## Recovery Scaling").expect("write markdown");
    writeln!(
        md,
        "| Stream events | Queue depth | Recovered events | Recovery us | Events/sec |"
    )
    .expect("write markdown");
    writeln!(md, "|---:|---:|---:|---:|---:|").expect("write markdown");
    for row in rows {
        writeln!(
            md,
            "| {} | {} | {} | {} | {:.0} |",
            row.stream_events,
            row.queue_depth,
            row.recovered_events,
            row.recovery_us,
            row.events_sec
        )
        .expect("write markdown");
    }
}

fn render_route_table(md: &mut String, rows: &[RouteSensitivityRow]) {
    writeln!(md).expect("write markdown");
    writeln!(md, "## Route-Count Sensitivity").expect("write markdown");
    writeln!(
        md,
        "| Domain | Routes | Ops/sec | p50 us | p95 us | p99 us | Max us |"
    )
    .expect("write markdown");
    writeln!(md, "|---|---:|---:|---:|---:|---:|---:|").expect("write markdown");
    for row in rows {
        writeln!(
            md,
            "| {} | {} | {:.0} | {} | {} | {} | {} |",
            row.domain,
            row.route_count,
            row.stats.ops_sec,
            row.stats.p50_us,
            row.stats.p95_us,
            row.stats.p99_us,
            row.stats.max_us
        )
        .expect("write markdown");
    }
}

fn render_throughput_table(md: &mut String, rows: &[ThroughputEvidence]) {
    writeln!(md).expect("write markdown");
    writeln!(md, "## Existing Throughput Evidence").expect("write markdown");
    writeln!(
        md,
        "| Suite | Scenario | Layer | Scope | Ops/sec | Source |"
    )
    .expect("write markdown");
    writeln!(md, "|---|---|---|---|---:|---|").expect("write markdown");
    for row in rows {
        writeln!(
            md,
            "| {} | {} | {} | {} | {:.0} | {} |",
            row.suite,
            row.scenario,
            row.layer.as_deref().unwrap_or(""),
            row.measurement_scope.as_deref().unwrap_or(""),
            row.ops_sec,
            row.source
        )
        .expect("write markdown");
    }
}
