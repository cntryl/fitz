use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fs;

use anyhow::{Context, Result};
use serde::Deserialize;
use walkdir::WalkDir;

use super::super::classify::classify_stability;
use super::super::config::BenchSummaryConfig;
use super::super::model::{BenchmarkRecord, MetricDirection};
use super::BenchmarkAdapter;

pub struct StressAdapter;

impl BenchmarkAdapter for StressAdapter {
    fn name(&self) -> &'static str {
        "stress"
    }

    fn collect(&self, config: &BenchSummaryConfig) -> Result<Vec<BenchmarkRecord>> {
        let Some(adapter_config) = config.adapters.stress.as_ref() else {
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
            .filter(|path| path.file_name().and_then(|name| name.to_str()) == Some("latest.json"))
        {
            let text = fs::read_to_string(&path)
                .with_context(|| format!("failed to read {}", path.display()))?;
            let suite_file: StressSuiteFile = match serde_json::from_str(&text) {
                Ok(data) => data,
                Err(error) => {
                    eprintln!("skipping {} (read error): {error}", path.display());
                    continue;
                }
            };

            let suite = suite_file.suite.unwrap_or_else(|| {
                path.parent()
                    .and_then(|value| value.file_name())
                    .map(|value| value.to_string_lossy().to_string())
                    .unwrap_or_else(|| "unknown".to_string())
            });

            for result in suite_file.results.unwrap_or_default() {
                let name = result.name.unwrap_or_default();
                let tags = result.tags.unwrap_or_default();
                let scenario = tags.get("scenario").cloned();
                let Some(elements) = result
                    .elements
                    .filter(|value| value.is_finite() && *value > 0.0)
                else {
                    continue;
                };

                let mut run_values: Vec<f64> = result
                    .all_runs
                    .unwrap_or_default()
                    .into_iter()
                    .filter(|value| value.is_finite() && *value > 0.0)
                    .collect();
                if run_values.is_empty() {
                    if let Some(duration) = result
                        .duration
                        .filter(|value| value.is_finite() && *value > 0.0)
                    {
                        run_values.push(duration);
                    }
                }
                if run_values.is_empty() {
                    continue;
                }

                let median_duration_ns = median(&run_values);
                if median_duration_ns <= 0.0 {
                    continue;
                }

                let throughput_ops_per_s = elements / median_duration_ns * 1e9;
                if !throughput_ops_per_s.is_finite()
                    || throughput_ops_per_s > adapter_config.max_reasonable_throughput_ops_per_s
                {
                    continue;
                }

                let mean_run_ns = mean(&run_values);
                let stddev_runs = stddev_population(&run_values, mean_run_ns);
                let rel_stddev = if mean_run_ns > 0.0 {
                    Some(stddev_runs / mean_run_ns)
                } else {
                    Some(0.0)
                };
                let stability = classify_stability(rel_stddev, &config.stability_thresholds);
                let meets_runtime_floor = median_duration_ns >= adapter_config.min_reasonable_duration_ns;
                let status = if run_values.len() < adapter_config.authoritative_min_runs {
                    "insufficient_data".to_string()
                } else if !meets_runtime_floor {
                    "invalid_for_throughput".to_string()
                } else {
                    "authoritative".to_string()
                };
                let case = stress_case_name(&name);
                let scenario_key = scenario.clone().unwrap_or_else(|| "unknown".to_string());

                let min_throughput = run_values
                    .iter()
                    .copied()
                    .map(|run| elements / run * 1e9)
                    .reduce(f64::min);
                let max_throughput = run_values
                    .iter()
                    .copied()
                    .map(|run| elements / run * 1e9)
                    .reduce(f64::max);
                let per_op_ns = median_duration_ns / elements;

                let mut metadata = BTreeMap::new();
                metadata.insert("name".to_string(), name.clone());
                metadata.insert("batch_size".to_string(), elements.to_string());
                metadata.insert("median_duration_ns".to_string(), median_duration_ns.to_string());
                metadata.insert("per_op_ns".to_string(), per_op_ns.to_string());
                metadata.insert("per_op_us".to_string(), (per_op_ns / 1e3).to_string());
                metadata.insert(
                    "meets_runtime_floor".to_string(),
                    meets_runtime_floor.to_string(),
                );
                metadata.insert(
                    "min_run_ns".to_string(),
                    run_values.iter().copied().fold(f64::INFINITY, f64::min).to_string(),
                );
                metadata.insert(
                    "max_run_ns".to_string(),
                    run_values
                        .iter()
                        .copied()
                        .fold(f64::NEG_INFINITY, f64::max)
                        .to_string(),
                );

                records.push(BenchmarkRecord {
                    id: format!("{}|{}|{}", suite, name, scenario_key),
                    adapter: self.name().to_string(),
                    suite: suite.clone(),
                    case,
                    scenario,
                    metric: "throughput_ops_per_s".to_string(),
                    unit: "ops/s".to_string(),
                    value: throughput_ops_per_s,
                    lower_bound: min_throughput,
                    upper_bound: max_throughput,
                    samples: Some(run_values.len()),
                    metric_direction: MetricDirection::HigherIsBetter,
                    stability: Some(stability),
                    status: Some(status),
                    rel_stddev,
                    tags,
                    metadata,
                    source_file: path.display().to_string(),
                });
            }
        }

        Ok(records)
    }
}

fn stress_case_name(name: &str) -> String {
    name.split("::").last().unwrap_or(name).to_string()
}

fn median(values: &[f64]) -> f64 {
    let mut ordered = values.to_vec();
    ordered.sort_by(compare_f64);
    if ordered.is_empty() {
        return 0.0;
    }
    let mid = ordered.len() / 2;
    if ordered.len().is_multiple_of(2) {
        (ordered[mid - 1] + ordered[mid]) / 2.0
    } else {
        ordered[mid]
    }
}

fn mean(values: &[f64]) -> f64 {
    if values.is_empty() {
        0.0
    } else {
        values.iter().sum::<f64>() / values.len() as f64
    }
}

fn stddev_population(values: &[f64], mean_value: f64) -> f64 {
    if values.len() <= 1 {
        return 0.0;
    }

    let variance = values
        .iter()
        .map(|value| {
            let delta = value - mean_value;
            delta * delta
        })
        .sum::<f64>()
        / values.len() as f64;
    variance.sqrt()
}

fn compare_f64(left: &f64, right: &f64) -> Ordering {
    left.partial_cmp(right).unwrap_or(Ordering::Equal)
}

#[derive(Debug, Deserialize)]
struct StressSuiteFile {
    suite: Option<String>,
    results: Option<Vec<StressResultFile>>,
}

#[derive(Debug, Deserialize)]
struct StressResultFile {
    name: Option<String>,
    duration: Option<f64>,
    elements: Option<f64>,
    all_runs: Option<Vec<f64>>,
    tags: Option<BTreeMap<String, String>>,
}