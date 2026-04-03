use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use serde::Deserialize;
use walkdir::WalkDir;

use super::super::classify::classify_stability;
use super::super::config::BenchSummaryConfig;
use super::super::model::{BenchmarkRecord, MetricDirection};
use super::BenchmarkAdapter;

pub struct CriterionAdapter;

impl BenchmarkAdapter for CriterionAdapter {
    fn name(&self) -> &'static str {
        "criterion"
    }

    fn collect(&self, config: &BenchSummaryConfig) -> Result<Vec<BenchmarkRecord>> {
        let Some(adapter_config) = config.adapters.criterion.as_ref() else {
            return Ok(Vec::new());
        };
        if !adapter_config.input_root.exists() {
            return Ok(Vec::new());
        }

        let mut records = Vec::new();
        for path in WalkDir::new(&adapter_config.input_root)
            .into_iter()
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.file_type().is_file())
            .map(|entry| entry.into_path())
            .filter(|path| path.ends_with(Path::new("new").join("estimates.json")))
        {
            let text = fs::read_to_string(&path)
                .with_context(|| format!("failed to read {}", path.display()))?;
            let estimates: CriterionEstimates = match serde_json::from_str(&text) {
                Ok(data) => data,
                Err(error) => {
                    eprintln!("skipping {} (read error): {error}", path.display());
                    continue;
                }
            };

            let benchmark_path = path
                .strip_prefix(&adapter_config.input_root)
                .unwrap_or(&path)
                .parent()
                .and_then(Path::parent)
                .map(|value| value.to_string_lossy().replace('\\', "/"))
                .unwrap_or_default();
            if benchmark_path.is_empty() {
                continue;
            }

            let Some(mean_ns) = estimates
                .mean
                .as_ref()
                .and_then(|value| value.point_estimate)
                .filter(|value| {
                    value.is_finite()
                        && *value > adapter_config.min_reasonable_mean_ns
                        && *value < adapter_config.max_reasonable_mean_ns
                })
            else {
                continue;
            };

            let median_ns = estimates
                .median
                .as_ref()
                .and_then(|value| value.point_estimate)
                .filter(|value| value.is_finite() && *value > 0.0)
                .unwrap_or(mean_ns);
            let std_dev = estimates
                .std_dev
                .as_ref()
                .and_then(|value| value.point_estimate)
                .filter(|value| value.is_finite() && *value >= 0.0);
            let rel_stddev = std_dev.map(|value| value / mean_ns);
            let stability = classify_stability(rel_stddev, &config.stability_thresholds);
            let status = if matches!(stability.as_str(), "stable" | "acceptable") {
                "authoritative".to_string()
            } else {
                stability.clone()
            };
            let (suite, case) = split_suite_and_case(&benchmark_path);

            let mut metadata = BTreeMap::new();
            metadata.insert("raw_benchmark".to_string(), benchmark_path.clone());
            metadata.insert("mean_ns".to_string(), mean_ns.to_string());
            metadata.insert("mean_us".to_string(), (mean_ns / 1e3).to_string());
            metadata.insert("mean_ms".to_string(), (mean_ns / 1e6).to_string());
            metadata.insert("median_ns".to_string(), median_ns.to_string());
            metadata.insert("median_us".to_string(), (median_ns / 1e3).to_string());
            metadata.insert("median_ms".to_string(), (median_ns / 1e6).to_string());
            if let Some(value) = std_dev {
                metadata.insert("std_dev_ns".to_string(), value.to_string());
            }

            records.push(BenchmarkRecord {
                id: benchmark_path,
                adapter: self.name().to_string(),
                suite,
                case,
                scenario: None,
                metric: "latency_ns".to_string(),
                unit: "ns".to_string(),
                value: median_ns,
                lower_bound: estimates
                    .median
                    .as_ref()
                    .and_then(|value| value.confidence_interval.as_ref())
                    .and_then(|interval| interval.lower_bound),
                upper_bound: estimates
                    .median
                    .as_ref()
                    .and_then(|value| value.confidence_interval.as_ref())
                    .and_then(|interval| interval.upper_bound),
                samples: None,
                metric_direction: MetricDirection::LowerIsBetter,
                stability: Some(stability),
                status: Some(status),
                rel_stddev,
                tags: BTreeMap::new(),
                metadata,
                source_file: path.display().to_string(),
            });
        }

        Ok(records)
    }
}

fn split_suite_and_case(path: &str) -> (String, String) {
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

#[derive(Debug, Deserialize)]
struct CriterionEstimate {
    point_estimate: Option<f64>,
    confidence_interval: Option<CriterionConfidenceInterval>,
}

#[derive(Debug, Deserialize)]
struct CriterionConfidenceInterval {
    lower_bound: Option<f64>,
    upper_bound: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct CriterionEstimates {
    mean: Option<CriterionEstimate>,
    median: Option<CriterionEstimate>,
    std_dev: Option<CriterionEstimate>,
}
