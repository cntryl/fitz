mod adapters;
mod classify;
mod compare;
mod config;
mod model;
mod report;
mod sweep;

use std::fs;
use std::path::Path;
use std::process::Command;

use anyhow::{Context, Result};
use chrono::Utc;
use serde::Deserialize;

use self::adapters::enabled_adapters;
use self::classify::{
    build_comparison_summary, collect_measurement_notes, current_run_authoritative,
};
use self::compare::compare_records;
use self::config::BenchSummaryConfig;
use self::model::{BenchmarkManifest, BenchmarkRecord, MetricDirection};
use self::report::{write_adapter_csv, write_markdown_report};
use self::sweep::detect_sweep_groups;
use crate::BenchmarkSummaryArgs;

pub fn run(args: BenchmarkSummaryArgs) -> Result<i32> {
    let config = BenchSummaryConfig::for_fitz(args.root)?;
    run_with_config(config)
}

fn run_with_config(config: BenchSummaryConfig) -> Result<i32> {
    let mut records = collect_records(&config)?;
    records.retain(|record| !config.should_ignore_record(record));
    sort_records(&mut records);

    write_adapter_csv_artifacts(&config, &records)?;

    let baseline = load_baseline_manifest(&config.baseline_file)?;
    let comparison = compare_records(&records, baseline.as_ref());
    let notes = collect_measurement_notes(&records, &comparison, &config);
    let summary = build_comparison_summary(
        &records,
        &comparison,
        baseline.is_some(),
        notes.len(),
        &config,
    );
    let manifest = BenchmarkManifest {
        schema_version: config.schema_version,
        generated_at: Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        commit_hash: git_commit_hash(&config.root),
        comparison_summary: summary.clone(),
        records: records.clone(),
    };
    let sweeps = detect_sweep_groups(&records, &config)?;

    write_json(&config.manifest_file, &manifest)?;
    write_markdown_report(&config, &manifest, &comparison, &sweeps, &notes)?;

    println!("Wrote {} (manifest).", config.manifest_file.display());
    println!("Wrote {} (report).", config.markdown_report_file.display());

    if current_run_authoritative(&records, &config) && summary.critical == 0 {
        write_json(&config.baseline_file, &manifest)?;
        println!("Promoted {} (baseline).", config.baseline_file.display());
    }

    Ok(if summary.critical > 0 { 1 } else { 0 })
}

fn collect_records(config: &BenchSummaryConfig) -> Result<Vec<BenchmarkRecord>> {
    let mut records = Vec::new();
    for adapter in enabled_adapters(config) {
        records.extend(adapter.collect(config)?);
    }
    Ok(records)
}

fn sort_records(records: &mut [BenchmarkRecord]) {
    records.sort_by(|left, right| {
        left.adapter
            .cmp(&right.adapter)
            .then_with(|| left.suite.cmp(&right.suite))
            .then_with(|| left.case.cmp(&right.case))
            .then_with(|| left.scenario.cmp(&right.scenario))
            .then_with(|| left.metric.cmp(&right.metric))
            .then_with(|| left.id.cmp(&right.id))
    });
}

fn write_adapter_csv_artifacts(
    config: &BenchSummaryConfig,
    records: &[BenchmarkRecord],
) -> Result<()> {
    if let Some(path) = config.criterion_csv_output() {
        let adapter_records: Vec<_> = records
            .iter()
            .filter(|record| record.adapter == "criterion")
            .cloned()
            .collect();
        if !adapter_records.is_empty() {
            write_adapter_csv(path, &adapter_records)?;
            println!(
                "Wrote {} (criterion) with {} entries.",
                path.display(),
                adapter_records.len()
            );
        }
    }

    if let Some(path) = config.stress_csv_output() {
        let adapter_records: Vec<_> = records
            .iter()
            .filter(|record| record.adapter == "stress")
            .cloned()
            .collect();
        if !adapter_records.is_empty() {
            write_adapter_csv(path, &adapter_records)?;
            println!(
                "Wrote {} (stress) with {} entries.",
                path.display(),
                adapter_records.len()
            );
        }
    }

    Ok(())
}

fn load_baseline_manifest(path: &Path) -> Result<Option<BenchmarkManifest>> {
    if !path.exists() {
        return Ok(None);
    }

    let text = fs::read_to_string(path)
        .with_context(|| format!("failed to read baseline {}", path.display()))?;

    if let Ok(manifest) = serde_json::from_str::<BenchmarkManifest>(&text) {
        return Ok(Some(manifest));
    }

    let legacy: LegacyBaselineManifest = serde_json::from_str(&text)
        .with_context(|| format!("failed to parse baseline {}", path.display()))?;
    Ok(Some(legacy.into_manifest()))
}

fn write_json<T: serde::Serialize>(path: &Path, value: &T) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }

    let content = serde_json::to_string_pretty(value)?;
    fs::write(path, format!("{content}\n"))
        .with_context(|| format!("failed to write {}", path.display()))?;
    Ok(())
}

fn git_commit_hash(root: &Path) -> Option<String> {
    let output = Command::new("git")
        .arg("rev-parse")
        .arg("HEAD")
        .current_dir(root)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }

    let text = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if text.is_empty() {
        None
    } else {
        Some(text)
    }
}

fn infer_metric_unit(metric: &str) -> String {
    if metric.ends_with("_ns") {
        "ns".to_string()
    } else if metric.ends_with("_us") {
        "us".to_string()
    } else if metric.ends_with("_ms") {
        "ms".to_string()
    } else if metric.contains("ops_per_s") {
        "ops/s".to_string()
    } else {
        "value".to_string()
    }
}

fn infer_metric_direction(metric: &str) -> MetricDirection {
    if metric.contains("throughput") || metric.contains("ops_per_s") {
        MetricDirection::HigherIsBetter
    } else {
        MetricDirection::LowerIsBetter
    }
}

fn split_benchmark_path(path: &str) -> (String, String) {
    let mut parts = path.split('/');
    let suite = parts.next().unwrap_or("other").to_string();
    let case = parts.collect::<Vec<_>>().join("/");
    (
        suite,
        if case.is_empty() {
            path.to_string()
        } else {
            case
        },
    )
}

fn stress_case_name(name: &str) -> String {
    name.split("::").last().unwrap_or(name).to_string()
}

#[derive(Debug, Deserialize)]
struct LegacyBaselineManifest {
    schema_version: Option<u32>,
    generated_at: Option<String>,
    commit_hash: Option<String>,
    #[serde(default)]
    criterion: Vec<LegacyCriterionRecord>,
    #[serde(default)]
    stress: Vec<LegacyStressRecord>,
}

impl LegacyBaselineManifest {
    fn into_manifest(self) -> BenchmarkManifest {
        let mut records = Vec::with_capacity(self.criterion.len() + self.stress.len());

        for record in self.criterion {
            let (suite, case) = split_benchmark_path(&record.benchmark);
            records.push(BenchmarkRecord {
                id: record
                    .comparison_key
                    .clone()
                    .unwrap_or_else(|| record.benchmark.clone()),
                adapter: "criterion".to_string(),
                suite,
                case,
                scenario: Some(record.scenario.clone()).filter(|value| !value.is_empty()),
                metric: record.metric.clone(),
                unit: infer_metric_unit(&record.metric),
                value: record.median_value.or(record.median_ns).unwrap_or_default(),
                lower_bound: record.min_value,
                upper_bound: record.max_value,
                samples: record.runs,
                metric_direction: infer_metric_direction(&record.metric),
                stability: Some(record.stability.clone()),
                status: Some(record.status.clone()),
                rel_stddev: record.rel_stddev,
                tags: Default::default(),
                metadata: Default::default(),
                source_file: record.source_file,
            });
        }

        for record in self.stress {
            let mut tags = std::collections::BTreeMap::new();
            if let Some(transport) = &record.transport {
                tags.insert("transport".to_string(), transport.clone());
            }

            records.push(BenchmarkRecord {
                id: record.comparison_key.clone().unwrap_or_else(|| {
                    format!("{}|{}|{}", record.suite, record.name, record.scenario)
                }),
                adapter: "stress".to_string(),
                suite: record.suite.clone(),
                case: stress_case_name(&record.name),
                scenario: Some(record.scenario.clone()).filter(|value| !value.is_empty()),
                metric: record.metric.clone(),
                unit: infer_metric_unit(&record.metric),
                value: record
                    .median_value
                    .or(record.median_throughput_ops_per_s)
                    .unwrap_or_default(),
                lower_bound: record.min_value,
                upper_bound: record.max_value,
                samples: Some(record.runs),
                metric_direction: infer_metric_direction(&record.metric),
                stability: Some(record.stability.clone()),
                status: Some(record.status.clone()),
                rel_stddev: record.rel_stddev,
                tags,
                metadata: Default::default(),
                source_file: record.source_file,
            });
        }

        BenchmarkManifest {
            schema_version: self.schema_version.unwrap_or(1),
            generated_at: self.generated_at.unwrap_or_default(),
            commit_hash: self.commit_hash,
            comparison_summary: Default::default(),
            records,
        }
    }
}

#[derive(Debug, Deserialize)]
struct LegacyCriterionRecord {
    benchmark: String,
    #[serde(default = "default_latency_metric")]
    metric: String,
    #[serde(default)]
    scenario: String,
    #[serde(default)]
    median_value: Option<f64>,
    #[serde(default)]
    median_ns: Option<f64>,
    #[serde(default)]
    min_value: Option<f64>,
    #[serde(default)]
    max_value: Option<f64>,
    stability: String,
    status: String,
    #[serde(default)]
    runs: Option<usize>,
    #[serde(default)]
    rel_stddev: Option<f64>,
    source_file: String,
    #[serde(default)]
    comparison_key: Option<String>,
}

#[derive(Debug, Deserialize)]
struct LegacyStressRecord {
    suite: String,
    name: String,
    scenario: String,
    #[serde(default = "default_throughput_metric")]
    metric: String,
    #[serde(default)]
    median_value: Option<f64>,
    #[serde(default)]
    median_throughput_ops_per_s: Option<f64>,
    #[serde(default)]
    min_value: Option<f64>,
    #[serde(default)]
    max_value: Option<f64>,
    stability: String,
    status: String,
    runs: usize,
    #[serde(default)]
    rel_stddev: Option<f64>,
    source_file: String,
    #[serde(default)]
    comparison_key: Option<String>,
    #[serde(default)]
    transport: Option<String>,
}

fn default_latency_metric() -> String {
    "latency_ns".to_string()
}

fn default_throughput_metric() -> String {
    "throughput_ops_per_s".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn record(id: &str) -> BenchmarkRecord {
        BenchmarkRecord {
            id: id.to_string(),
            adapter: "criterion".to_string(),
            suite: "suite".to_string(),
            case: "case".to_string(),
            scenario: None,
            metric: "latency_ns".to_string(),
            unit: "ns".to_string(),
            value: 1.0,
            lower_bound: None,
            upper_bound: None,
            samples: None,
            metric_direction: MetricDirection::LowerIsBetter,
            stability: Some("stable".to_string()),
            status: Some("authoritative".to_string()),
            rel_stddev: Some(0.01),
            tags: BTreeMap::new(),
            metadata: BTreeMap::new(),
            source_file: "target/file".to_string(),
        }
    }

    #[test]
    fn should_filter_record_when_exact_id_is_ignored() {
        // Arrange
        let config = BenchSummaryConfig::for_tests_with_exact_ignore(["stale-id"]);

        // Act
        let ignored = config.should_ignore_record(&record("stale-id"));

        // Assert
        assert!(ignored);
    }

    #[test]
    fn should_filter_record_when_pattern_matches_metadata() {
        // Arrange
        let mut config = BenchSummaryConfig::for_tests();
        config.ignored_patterns =
            vec![regex::Regex::new("schedule_system_scan_and_fire").expect("valid ignore regex")];
        let mut record = record("record-id");
        record.metadata.insert(
            "raw_benchmark".to_string(),
            "schedule_system_scan_and_fire/case".to_string(),
        );

        // Act
        let ignored = config.should_ignore_record(&record);

        // Assert
        assert!(ignored);
    }
}
