use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use csv::Writer;

use super::compare::ComparisonOutcome;
use super::config::BenchSummaryConfig;
use super::model::{BenchmarkManifest, BenchmarkRecord, MetricDirection, SweepGroup};

pub fn write_adapter_csv(path: &Path, records: &[BenchmarkRecord]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }

    let mut writer = Writer::from_path(path)?;
    writer.write_record([
        "id",
        "adapter",
        "suite",
        "case",
        "scenario",
        "metric",
        "unit",
        "value",
        "lower_bound",
        "upper_bound",
        "samples",
        "metric_direction",
        "stability",
        "status",
        "rel_stddev",
        "tags",
        "metadata",
        "source_file",
    ])?;

    for record in records {
        writer.write_record([
            record.id.as_str(),
            record.adapter.as_str(),
            record.suite.as_str(),
            record.case.as_str(),
            record.scenario.as_deref().unwrap_or("NA"),
            record.metric.as_str(),
            record.unit.as_str(),
            &format_number(record.value),
            &format_optional_number(record.lower_bound),
            &format_optional_number(record.upper_bound),
            &record
                .samples
                .map(|value| value.to_string())
                .unwrap_or_else(|| "NA".to_string()),
            metric_direction_label(record.metric_direction),
            record.stability.as_deref().unwrap_or("NA"),
            record.status.as_deref().unwrap_or("NA"),
            &format_optional_number(record.rel_stddev),
            &serde_json::to_string(&record.tags)?,
            &serde_json::to_string(&record.metadata)?,
            record.source_file.as_str(),
        ])?;
    }

    writer.flush()?;
    Ok(())
}

pub fn write_markdown_report(
    config: &BenchSummaryConfig,
    manifest: &BenchmarkManifest,
    comparison: &ComparisonOutcome,
    sweeps: &[SweepGroup],
    notes: &[String],
) -> Result<()> {
    if let Some(parent) = config.markdown_report_file.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }

    let mut content = String::new();
    content.push_str(&format!("# {}\n\n", config.report_title));
    content.push_str(&format!("- product: {}\n", config.product_name));
    content.push_str(&format!("- generated_at: {}\n", manifest.generated_at));
    content.push_str(&format!(
        "- commit: {}\n",
        manifest.commit_hash.as_deref().unwrap_or("unknown")
    ));
    content.push_str(&format!(
        "- baseline_available: {}\n",
        manifest.comparison_summary.baseline_available
    ));
    content.push_str(&format!("- record_count: {}\n", manifest.records.len()));
    content.push_str(&format!(
        "- manifest: {}\n",
        display_relative(&config.root, &config.manifest_file)
    ));
    content.push_str(&format!(
        "- baseline: {}\n",
        display_relative(&config.root, &config.baseline_file)
    ));
    if let Some(path) = config.criterion_csv_output() {
        content.push_str(&format!(
            "- criterion_csv: {}\n",
            display_relative(&config.root, path)
        ));
    }
    if let Some(path) = config.stress_csv_output() {
        content.push_str(&format!(
            "- stress_csv: {}\n",
            display_relative(&config.root, path)
        ));
    }
    content.push('\n');

    write_summary_section(&mut content, manifest);
    write_current_results_section(&mut content, config, &manifest.records);
    write_delta_section(&mut content, manifest, comparison);
    write_sweep_section(&mut content, sweeps);
    write_notes_section(&mut content, notes);

    fs::write(&config.markdown_report_file, content)
        .with_context(|| format!("failed to write {}", config.markdown_report_file.display()))?;
    Ok(())
}

fn write_summary_section(content: &mut String, manifest: &BenchmarkManifest) {
    content.push_str("## Comparison Summary\n\n");
    write_table(
        content,
        &[
            "performance_changes",
            "improved",
            "regressions",
            "critical",
            "stability_regressions",
            "stability_improvements",
            "new",
            "missing",
            "risk_areas",
            "authoritative",
        ],
        vec![vec![
            manifest.comparison_summary.performance_changes.to_string(),
            manifest.comparison_summary.improved.to_string(),
            manifest.comparison_summary.regressions.to_string(),
            manifest.comparison_summary.critical.to_string(),
            manifest
                .comparison_summary
                .stability_regressions
                .to_string(),
            manifest
                .comparison_summary
                .stability_improvements
                .to_string(),
            manifest.comparison_summary.new.to_string(),
            manifest.comparison_summary.missing.to_string(),
            manifest.comparison_summary.risk_areas.to_string(),
            manifest.comparison_summary.authoritative.to_string(),
        ]],
    );
}

fn write_current_results_section(
    content: &mut String,
    config: &BenchSummaryConfig,
    records: &[BenchmarkRecord],
) {
    content.push_str("## Current Results\n\n");
    if records.is_empty() {
        content.push_str("- No benchmark records were collected.\n\n");
        return;
    }

    let mut grouped = BTreeMap::<String, BTreeMap<String, Vec<&BenchmarkRecord>>>::new();
    for record in records {
        grouped
            .entry(record.adapter.clone())
            .or_default()
            .entry(record.suite.clone())
            .or_default()
            .push(record);
    }

    for (adapter, suites) in grouped {
        content.push_str(&format!("### {}\n\n", adapter));
        for (suite, suite_records) in suites {
            content.push_str(&format!("#### {}\n\n", config.display_suite_label(&suite)));
            write_table(
                content,
                &[
                    "case",
                    "scenario",
                    "metric",
                    "value",
                    "unit",
                    "samples",
                    "rsd",
                    "stability",
                    "status",
                ],
                suite_records
                    .iter()
                    .map(|record| {
                        vec![
                            record.case.clone(),
                            record.scenario.clone().unwrap_or_else(|| "NA".to_string()),
                            record.metric.clone(),
                            format_measurement(record.value, &record.unit),
                            record.unit.clone(),
                            record
                                .samples
                                .map(|value| value.to_string())
                                .unwrap_or_else(|| "NA".to_string()),
                            format_rel_stddev(record.rel_stddev),
                            record.stability.clone().unwrap_or_else(|| "NA".to_string()),
                            record.status.clone().unwrap_or_else(|| "NA".to_string()),
                        ]
                    })
                    .collect(),
            );
        }
    }
}

fn write_delta_section(
    content: &mut String,
    manifest: &BenchmarkManifest,
    comparison: &ComparisonOutcome,
) {
    content.push_str("## Baseline Deltas\n\n");
    if !manifest.comparison_summary.baseline_available {
        content.push_str(
            "- No baseline available. Current normalized results are still written to the manifest, CSV, and report artifacts.\n\n",
        );
        return;
    }

    let lookup = manifest
        .records
        .iter()
        .map(|record| (record.id.as_str(), record))
        .collect::<BTreeMap<_, _>>();
    let mut grouped = BTreeMap::<String, Vec<_>>::new();
    for delta in &comparison.deltas {
        let adapter = lookup
            .get(delta.id.as_str())
            .map(|record| record.adapter.clone())
            .unwrap_or_else(|| "unknown".to_string());
        grouped.entry(adapter).or_default().push(delta);
    }

    for (adapter, deltas) in grouped {
        content.push_str(&format!("### {}\n\n", adapter));
        write_table(
            content,
            &[
                "suite",
                "case",
                "scenario",
                "metric",
                "baseline",
                "current",
                "delta_pct",
                "directional_delta_pct",
                "baseline_stability",
                "current_stability",
                "baseline_status",
                "current_status",
            ],
            deltas
                .iter()
                .map(|delta| {
                    let record = lookup.get(delta.id.as_str()).copied();
                    let unit = record.map(|value| value.unit.as_str()).unwrap_or("value");
                    vec![
                        delta.suite.clone(),
                        delta.case.clone(),
                        record
                            .and_then(|value| value.scenario.clone())
                            .unwrap_or_else(|| "NA".to_string()),
                        delta.metric.clone(),
                        delta
                            .baseline_value
                            .map(|value| format_measurement(value, unit))
                            .unwrap_or_else(|| "NA".to_string()),
                        format_measurement(delta.current_value, unit),
                        format_delta(delta.delta_pct),
                        format_delta(delta.directional_delta_pct),
                        delta
                            .baseline_stability
                            .clone()
                            .unwrap_or_else(|| "NA".to_string()),
                        delta
                            .current_stability
                            .clone()
                            .unwrap_or_else(|| "NA".to_string()),
                        delta
                            .baseline_status
                            .clone()
                            .unwrap_or_else(|| "NA".to_string()),
                        delta
                            .current_status
                            .clone()
                            .unwrap_or_else(|| "NA".to_string()),
                    ]
                })
                .collect(),
        );
    }

    if !comparison.new_records.is_empty() {
        content.push_str("### New Records\n\n");
        write_table(
            content,
            &[
                "adapter", "suite", "case", "scenario", "metric", "value", "status",
            ],
            comparison
                .new_records
                .iter()
                .map(|record| {
                    vec![
                        record.adapter.clone(),
                        record.suite.clone(),
                        record.case.clone(),
                        record.scenario.clone().unwrap_or_else(|| "NA".to_string()),
                        record.metric.clone(),
                        format_measurement(record.value, &record.unit),
                        record.status.clone().unwrap_or_else(|| "NA".to_string()),
                    ]
                })
                .collect(),
        );
    }

    if !comparison.missing_records.is_empty() {
        content.push_str("### Missing Records\n\n");
        write_table(
            content,
            &[
                "adapter", "suite", "case", "scenario", "metric", "value", "status",
            ],
            comparison
                .missing_records
                .iter()
                .map(|record| {
                    vec![
                        record.adapter.clone(),
                        record.suite.clone(),
                        record.case.clone(),
                        record.scenario.clone().unwrap_or_else(|| "NA".to_string()),
                        record.metric.clone(),
                        format_measurement(record.value, &record.unit),
                        record.status.clone().unwrap_or_else(|| "NA".to_string()),
                    ]
                })
                .collect(),
        );
    }
}

fn write_sweep_section(content: &mut String, sweeps: &[SweepGroup]) {
    content.push_str("## Parameter Sweeps\n\n");
    if sweeps.is_empty() {
        content.push_str("- No sweep-style parameter scenarios were detected.\n\n");
        return;
    }

    for group in sweeps {
        content.push_str(&format!("### {}\n\n", group.title));
        content.push_str(&format!("- metric: {} ({})\n\n", group.metric, group.unit));
        write_table(
            content,
            &[
                "parameter",
                "value",
                "delta_vs_previous_pct",
                "samples",
                "rsd",
                "status",
            ],
            group
                .points
                .iter()
                .map(|point| {
                    vec![
                        format!("{}={}", point.parameter, point.parameter_label),
                        format_measurement(point.metric_value, &group.unit),
                        format_delta(point.delta_vs_previous_pct),
                        point
                            .samples
                            .map(|value| value.to_string())
                            .unwrap_or_else(|| "NA".to_string()),
                        format_rel_stddev(point.rel_stddev),
                        point.status.clone().unwrap_or_else(|| "NA".to_string()),
                    ]
                })
                .collect(),
        );
    }
}

fn write_notes_section(content: &mut String, notes: &[String]) {
    content.push_str("## Measurement Notes\n\n");
    for note in notes {
        content.push_str(&format!("- {note}\n"));
    }
    content.push('\n');
}

fn write_table(content: &mut String, headers: &[&str], rows: Vec<Vec<String>>) {
    if rows.is_empty() {
        return;
    }

    content.push_str("| ");
    content.push_str(&headers.join(" | "));
    content.push_str(" |\n|");
    content.push_str(&vec!["---"; headers.len()].join("|"));
    content.push_str("|\n");
    for row in rows {
        content.push_str("| ");
        content.push_str(&row.join(" | "));
        content.push_str(" |\n");
    }
    content.push('\n');
}

fn metric_direction_label(direction: MetricDirection) -> &'static str {
    match direction {
        MetricDirection::LowerIsBetter => "lower_is_better",
        MetricDirection::HigherIsBetter => "higher_is_better",
    }
}

fn display_relative(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .display()
        .to_string()
}

fn format_measurement(value: f64, unit: &str) -> String {
    match unit {
        "ns" => format!("{value:.0}"),
        "us" | "ms" => format!("{value:.3}"),
        "ops/s" => format!("{value:.0}"),
        _ => format_number(value),
    }
}

fn format_number(value: f64) -> String {
    format!("{value:.6}")
}

fn format_optional_number(value: Option<f64>) -> String {
    value.map(format_number).unwrap_or_else(|| "NA".to_string())
}

fn format_rel_stddev(value: Option<f64>) -> String {
    value
        .map(|value| format!("{:.1}%", value * 100.0))
        .unwrap_or_else(|| "NA".to_string())
}

fn format_delta(value: Option<f64>) -> String {
    match value {
        None => "NA".to_string(),
        Some(value) => {
            let sign = if value >= 0.0 { "+" } else { "" };
            format!("{sign}{value:.1}%")
        }
    }
}
