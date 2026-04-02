use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;
use std::process::Command;

use anyhow::{Context, Result};
use chrono::Utc;
use csv::Writer;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use walkdir::WalkDir;

use crate::BenchmarkSummaryArgs;

const WARNING_REGRESSION_PCT: f64 = 10.0;
const CRITICAL_REGRESSION_PCT: f64 = 25.0;
const MIN_REASONABLE_CRITERION_MEAN_NS: f64 = 1.0;
const MAX_REASONABLE_CRITERION_MEAN_NS: f64 = 1e12;
const MIN_REASONABLE_STRESS_DURATION_NS: f64 = 3e9;
const MAX_REASONABLE_STRESS_THROUGHPUT_OPS_PER_S: f64 = 1e9;

const STALE_CRITERION_BENCHMARKS: &[&str] = &[
    "hotpath_actor_messaging/actorref_clone_overhead",
    "hotpath_context/timer_id_new",
    "hotpath_envelope/messageid_new",
    "hotpath_envelope/metadata_extraction",
    "hotpath_envelope/is_expired_not_expired",
    "hotpath_envelope/is_expired_expired",
    "hotpath_envelope/is_expired_no_deadline",
    "hotpath_routing/full_address_from_string",
    "hotpath_routing/route_address_clone",
    "hotpath_routing/route_address_family_access",
    "hotpath_routing/route_address_route_access",
    "hotpath_routing/route_as_str",
    "hotpath_routing/route_clone_long",
    "hotpath_routing/route_clone_short",
    "hotpath_routing/route_equality_different",
    "hotpath_routing/route_equality_same",
    "hotpath_routing/route_family_from_u32",
    "hotpath_routing/route_family_new_u64",
];

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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CriterionEntry {
    benchmark: String,
    mean: f64,
    mean_ci_lower: Option<f64>,
    mean_ci_upper: Option<f64>,
    median_ns: f64,
    median_us: f64,
    median_ms: f64,
    median_ci_lower: Option<f64>,
    median_ci_upper: Option<f64>,
    std_dev: Option<f64>,
    rel_stddev: Option<f64>,
    high_variance: bool,
    stability: String,
    status: String,
    mean_us: f64,
    mean_ms: f64,
    file: String,
    suite: String,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StressEntry {
    suite: String,
    name: String,
    scenario: String,
    layer: Option<String>,
    tags: BTreeMap<String, String>,
    batch_size: f64,
    duration_ns: f64,
    median_duration_ns: f64,
    duration_us: f64,
    duration_ms: f64,
    median_duration_ms: f64,
    elements: f64,
    run_values: Vec<f64>,
    min_run_ns: f64,
    max_run_ns: f64,
    per_op_ns: f64,
    per_op_us: f64,
    throughput_ops_per_ns: f64,
    throughput_ops_per_us: f64,
    throughput_ops_per_ms: f64,
    throughput_ops_per_s: f64,
    median_throughput_ops_per_s: f64,
    num_runs: usize,
    meets_runtime_floor: bool,
    stddev_runs: f64,
    rel_stddev_runs: f64,
    stability: String,
    status: String,
    file: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CriterionRecord {
    kind: String,
    benchmark: String,
    domain: String,
    transport: Option<String>,
    scenario: String,
    metric: String,
    median_ns: f64,
    median_value: f64,
    min_value: Option<f64>,
    max_value: Option<f64>,
    stability: String,
    status: String,
    runs: Option<usize>,
    timestamp: String,
    commit_hash: Option<String>,
    rel_stddev: Option<f64>,
    mean_ns: f64,
    p50_latency_ns: f64,
    p95_latency_ns: Option<f64>,
    p99_latency_ns: Option<f64>,
    allocs_per_op: Option<f64>,
    bytes_per_op: Option<f64>,
    source_file: String,
    comparison_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StressRecord {
    kind: String,
    suite: String,
    name: String,
    domain: String,
    transport: Option<String>,
    scenario: String,
    metric: String,
    median_throughput_ops_per_s: f64,
    median_value: f64,
    min_value: Option<f64>,
    max_value: Option<f64>,
    stability: String,
    status: String,
    runs: usize,
    timestamp: String,
    commit_hash: Option<String>,
    rel_stddev: Option<f64>,
    mean_ns: Option<f64>,
    p50_latency_ns: Option<f64>,
    p95_latency_ns: Option<f64>,
    p99_latency_ns: Option<f64>,
    allocs_per_op: Option<f64>,
    bytes_per_op: Option<f64>,
    source_file: String,
    comparison_key: String,
    batch_size: f64,
    median_duration_ns: f64,
    min_run_ns: f64,
    max_run_ns: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ComparisonSummary {
    performance_changes: usize,
    improved: usize,
    regressions: usize,
    critical: usize,
    stability_regressions: usize,
    stability_improvements: usize,
    new: usize,
    missing: usize,
    risk_areas: usize,
    authoritative: bool,
    baseline_available: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ResultManifest {
    schema_version: u32,
    generated_at: String,
    commit_hash: Option<String>,
    comparison_summary: ComparisonSummary,
    criterion: Vec<CriterionRecord>,
    stress: Vec<StressRecord>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct BaselineManifest {
    schema_version: Option<u32>,
    generated_at: Option<String>,
    commit_hash: Option<String>,
    criterion: Vec<CriterionRecord>,
    stress: Vec<StressRecord>,
    source: Option<String>,
}

#[derive(Debug, Clone)]
struct CriterionDelta {
    benchmark: String,
    suite: String,
    baseline_value: Option<f64>,
    current_value: f64,
    delta_pct: Option<f64>,
    baseline_stability: Option<String>,
    current_stability: String,
    baseline_status: Option<String>,
    current_status: String,
    directional_delta_pct: Option<f64>,
}

#[derive(Debug, Clone)]
struct StressDelta {
    suite: String,
    name: String,
    scenario: String,
    layer: Option<String>,
    baseline_value: Option<f64>,
    current_value: f64,
    delta_pct: Option<f64>,
    baseline_stability: Option<String>,
    current_stability: String,
    baseline_status: Option<String>,
    current_status: String,
    directional_delta_pct: Option<f64>,
}

#[derive(Debug, Clone)]
struct SweepPoint {
    parameter: String,
    parameter_label: String,
    parameter_value: f64,
    throughput: f64,
    rel_stddev: f64,
    runs: usize,
    status: String,
    delta_vs_previous_pct: Option<f64>,
}

#[derive(Debug, Clone)]
struct SweepGroup {
    title: String,
    points: Vec<SweepPoint>,
}

pub fn run(args: BenchmarkSummaryArgs) -> Result<i32> {
    let root = args
        .root
        .canonicalize()
        .with_context(|| format!("failed to resolve repository root {}", args.root.display()))?;
    let target_root = root.join("target");
    let criterion_root = target_root.join("criterion");
    let stress_root = target_root.join("stress");
    let current_results_file = target_root.join("bench_results.json");
    let out_csv = criterion_root.join("benchmark_summary.csv");
    let stress_csv = stress_root.join("stress_summary.csv");
    let out_md = target_root.join("bench_summary.md");
    let baseline_file = root.join("config").join("bench_baseline.json");

    let criterion_entries = collect_criterion_entries(&criterion_root)?;
    let stress_entries = collect_stress_entries(&stress_root)?;

    if criterion_root.exists() {
        write_criterion_csv(&out_csv, &criterion_entries)?;
        println!(
            "Wrote {} (criterion) with {} entries.",
            out_csv.display(),
            criterion_entries.len()
        );
    }
    if stress_root.exists() {
        write_stress_csv(&stress_csv, &stress_entries)?;
        println!(
            "Wrote {} (stress) with {} entries.",
            stress_csv.display(),
            stress_entries.len()
        );
    }

    let generated_at = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let commit_hash = git_commit_hash(&root);

    let criterion_records: Vec<CriterionRecord> = criterion_entries
        .iter()
        .map(|entry| build_criterion_record(entry, &generated_at, commit_hash.as_deref()))
        .collect();
    let stress_records: Vec<StressRecord> = stress_entries
        .iter()
        .map(|entry| build_stress_record(entry, &generated_at, commit_hash.as_deref()))
        .collect();

    let baseline = load_baseline(&baseline_file)?;
    let criterion_deltas = build_criterion_deltas(&criterion_entries, baseline.as_ref());
    let stress_deltas = build_stress_deltas(&stress_entries, baseline.as_ref());
    let baseline_available = baseline.is_some();
    let authoritative = current_run_authoritative(&criterion_records, &stress_records);
    let critical = count_critical_regressions(&criterion_deltas, &stress_deltas);
    let risk_areas = collect_noise_warnings(&criterion_entries, &stress_entries).len();

    let manifest = ResultManifest {
        schema_version: 1,
        generated_at: generated_at.clone(),
        commit_hash: commit_hash.clone(),
        comparison_summary: ComparisonSummary {
            performance_changes: criterion_deltas
                .iter()
                .filter(|item| item.delta_pct.is_some())
                .count()
                + stress_deltas
                    .iter()
                    .filter(|item| item.delta_pct.is_some())
                    .count(),
            improved: criterion_deltas
                .iter()
                .filter(|item| item.directional_delta_pct.unwrap_or(0.0) >= WARNING_REGRESSION_PCT)
                .count()
                + stress_deltas
                    .iter()
                    .filter(|item| {
                        item.directional_delta_pct.unwrap_or(0.0) >= WARNING_REGRESSION_PCT
                    })
                    .count(),
            regressions: criterion_deltas
                .iter()
                .filter(|item| item.directional_delta_pct.unwrap_or(0.0) <= -WARNING_REGRESSION_PCT)
                .count()
                + stress_deltas
                    .iter()
                    .filter(|item| {
                        item.directional_delta_pct.unwrap_or(0.0) <= -WARNING_REGRESSION_PCT
                    })
                    .count(),
            critical,
            stability_regressions: 0,
            stability_improvements: 0,
            new: 0,
            missing: 0,
            risk_areas,
            authoritative,
            baseline_available,
        },
        criterion: criterion_records.clone(),
        stress: stress_records.clone(),
    };

    write_json(&current_results_file, &manifest)?;
    write_markdown_report(
        &out_md,
        &root,
        &generated_at,
        commit_hash.as_deref(),
        &criterion_entries,
        &stress_entries,
        &criterion_deltas,
        &stress_deltas,
        baseline_available,
        &current_results_file,
        &out_csv,
        &stress_csv,
    )?;
    println!("Wrote {} (summary).", out_md.display());

    if authoritative && critical == 0 {
        let promoted = BaselineManifest {
            schema_version: Some(1),
            generated_at: Some(generated_at),
            commit_hash,
            criterion: criterion_records,
            stress: stress_records,
            source: Some("authoritative promotion".to_string()),
        };
        write_json(&baseline_file, &promoted)?;
    }

    Ok(if critical > 0 { 1 } else { 0 })
}

fn collect_criterion_entries(root: &Path) -> Result<Vec<CriterionEntry>> {
    if !root.exists() {
        return Ok(Vec::new());
    }

    let mut entries = Vec::new();
    for path in WalkDir::new(root)
        .into_iter()
        .filter_map(Result::ok)
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
            .strip_prefix(root)
            .unwrap_or(&path)
            .parent()
            .and_then(Path::parent)
            .map(|value| value.to_string_lossy().replace('\\', "/"))
            .unwrap_or_default();

        if STALE_CRITERION_BENCHMARKS.contains(&benchmark_path.as_str())
            || benchmark_path.contains("schedule_system_scan_and_fire")
        {
            continue;
        }

        let mean = match estimates
            .mean
            .as_ref()
            .and_then(|value| value.point_estimate)
        {
            Some(value)
                if value.is_finite()
                    && value > MIN_REASONABLE_CRITERION_MEAN_NS
                    && value < MAX_REASONABLE_CRITERION_MEAN_NS =>
            {
                value
            }
            _ => continue,
        };
        let median = estimates
            .median
            .as_ref()
            .and_then(|value| value.point_estimate)
            .filter(|value| value.is_finite() && *value > 0.0)
            .unwrap_or(mean);
        let std_dev = estimates
            .std_dev
            .as_ref()
            .and_then(|value| value.point_estimate)
            .filter(|value| value.is_finite() && *value >= 0.0);
        let rel_stddev = std_dev.map(|value| value / mean);
        let stability = variance_band(rel_stddev);
        let suite = benchmark_path
            .split('/')
            .next()
            .unwrap_or("other")
            .to_string();

        entries.push(CriterionEntry {
            benchmark: benchmark_path,
            mean,
            mean_ci_lower: estimates
                .mean
                .as_ref()
                .and_then(|value| value.confidence_interval.as_ref())
                .and_then(|interval| interval.lower_bound),
            mean_ci_upper: estimates
                .mean
                .as_ref()
                .and_then(|value| value.confidence_interval.as_ref())
                .and_then(|interval| interval.upper_bound),
            median_ns: median,
            median_us: median / 1e3,
            median_ms: median / 1e6,
            median_ci_lower: estimates
                .median
                .as_ref()
                .and_then(|value| value.confidence_interval.as_ref())
                .and_then(|interval| interval.lower_bound),
            median_ci_upper: estimates
                .median
                .as_ref()
                .and_then(|value| value.confidence_interval.as_ref())
                .and_then(|interval| interval.upper_bound),
            std_dev,
            rel_stddev,
            high_variance: rel_stddev.unwrap_or(0.0) > 0.10,
            stability: stability.to_string(),
            status: if matches!(stability, "stable" | "acceptable") {
                "authoritative".to_string()
            } else {
                stability.to_string()
            },
            mean_us: mean / 1e3,
            mean_ms: mean / 1e6,
            file: path.display().to_string(),
            suite,
        });
    }

    entries.sort_by(|left, right| compare_f64(left.mean, right.mean));
    Ok(entries)
}

fn collect_stress_entries(root: &Path) -> Result<Vec<StressEntry>> {
    if !root.exists() {
        return Ok(Vec::new());
    }

    let mut entries = Vec::new();
    for path in WalkDir::new(root)
        .min_depth(2)
        .max_depth(2)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_type().is_file())
        .map(|entry| entry.into_path())
        .filter(|path| path.file_name().is_some_and(|name| name == "latest.json"))
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
                .and_then(Path::file_name)
                .map(|value| value.to_string_lossy().to_string())
                .unwrap_or_else(|| "unknown".to_string())
        });

        for result in suite_file.results.unwrap_or_default() {
            let name = result.name.unwrap_or_default();
            let tags = result.tags.unwrap_or_default();
            let scenario = tags
                .get("scenario")
                .cloned()
                .unwrap_or_else(|| "unknown".to_string());
            let elements = match result
                .elements
                .filter(|value| value.is_finite() && *value > 0.0)
            {
                Some(value) => value,
                None => continue,
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
                || throughput_ops_per_s > MAX_REASONABLE_STRESS_THROUGHPUT_OPS_PER_S
            {
                continue;
            }

            let mean_run_ns = mean(&run_values);
            let stddev_runs = stddev_population(&run_values, mean_run_ns);
            let rel_stddev_runs = if mean_run_ns > 0.0 {
                stddev_runs / mean_run_ns
            } else {
                0.0
            };
            let stability = variance_band(Some(rel_stddev_runs)).to_string();
            let meets_runtime_floor = median_duration_ns >= MIN_REASONABLE_STRESS_DURATION_NS;
            let status = stress_status(run_values.len(), meets_runtime_floor).to_string();
            let layer = tags.get("layer").cloned();

            entries.push(StressEntry {
                suite: suite.clone(),
                name,
                scenario,
                layer,
                tags,
                batch_size: elements,
                duration_ns: median_duration_ns,
                median_duration_ns,
                duration_us: median_duration_ns / 1e3,
                duration_ms: median_duration_ns / 1e6,
                median_duration_ms: median_duration_ns / 1e6,
                elements,
                min_run_ns: run_values.iter().copied().fold(f64::INFINITY, f64::min),
                max_run_ns: run_values.iter().copied().fold(f64::NEG_INFINITY, f64::max),
                per_op_ns: median_duration_ns / elements,
                per_op_us: median_duration_ns / elements / 1e3,
                throughput_ops_per_ns: elements / median_duration_ns,
                throughput_ops_per_us: elements / median_duration_ns * 1e3,
                throughput_ops_per_ms: elements / median_duration_ns * 1e6,
                throughput_ops_per_s,
                median_throughput_ops_per_s: throughput_ops_per_s,
                num_runs: run_values.len(),
                meets_runtime_floor,
                stddev_runs,
                rel_stddev_runs,
                stability,
                status,
                file: path.display().to_string(),
                run_values,
            });
        }
    }

    entries
        .sort_by(|left, right| compare_f64(right.throughput_ops_per_s, left.throughput_ops_per_s));
    Ok(entries)
}

fn load_baseline(path: &Path) -> Result<Option<BaselineManifest>> {
    if !path.exists() {
        return Ok(None);
    }

    let text = fs::read_to_string(path)
        .with_context(|| format!("failed to read baseline {}", path.display()))?;
    let json: Value = serde_json::from_str(&text)?;

    let criterion = json
        .get("criterion")
        .cloned()
        .unwrap_or_else(|| Value::Array(Vec::new()));
    let stress = json
        .get("stress")
        .cloned()
        .unwrap_or_else(|| Value::Array(Vec::new()));

    Ok(Some(BaselineManifest {
        schema_version: json
            .get("schema_version")
            .and_then(Value::as_u64)
            .map(|value| value as u32),
        generated_at: json
            .get("generated_at")
            .and_then(Value::as_str)
            .map(str::to_string),
        commit_hash: json
            .get("commit_hash")
            .and_then(Value::as_str)
            .map(str::to_string),
        criterion: serde_json::from_value(criterion).unwrap_or_default(),
        stress: serde_json::from_value(stress).unwrap_or_default(),
        source: json
            .get("source")
            .and_then(Value::as_str)
            .map(str::to_string),
    }))
}

fn build_criterion_record(
    entry: &CriterionEntry,
    generated_at: &str,
    commit_hash: Option<&str>,
) -> CriterionRecord {
    CriterionRecord {
        kind: "criterion".to_string(),
        benchmark: entry.benchmark.clone(),
        domain: benchmark_domain(&entry.benchmark),
        transport: None,
        scenario: benchmark_short_name(&entry.benchmark),
        metric: "latency_ns".to_string(),
        median_ns: entry.median_ns,
        median_value: entry.median_ns,
        min_value: entry.median_ci_lower,
        max_value: entry.median_ci_upper,
        stability: entry.stability.clone(),
        status: entry.status.clone(),
        runs: None,
        timestamp: generated_at.to_string(),
        commit_hash: commit_hash.map(str::to_string),
        rel_stddev: entry.rel_stddev,
        mean_ns: entry.mean,
        p50_latency_ns: entry.median_ns,
        p95_latency_ns: None,
        p99_latency_ns: None,
        allocs_per_op: None,
        bytes_per_op: None,
        source_file: entry.file.clone(),
        comparison_key: entry.benchmark.clone(),
    }
}

fn build_stress_record(
    entry: &StressEntry,
    generated_at: &str,
    commit_hash: Option<&str>,
) -> StressRecord {
    let min_value = entry
        .run_values
        .iter()
        .copied()
        .map(|run| entry.elements / run * 1e9)
        .reduce(f64::min);
    let max_value = entry
        .run_values
        .iter()
        .copied()
        .map(|run| entry.elements / run * 1e9)
        .reduce(f64::max);

    StressRecord {
        kind: "stress".to_string(),
        suite: entry.suite.clone(),
        name: entry.name.clone(),
        domain: domain_from_suite(&entry.suite),
        transport: entry.layer.clone(),
        scenario: entry.scenario.clone(),
        metric: "throughput_ops_per_s".to_string(),
        median_throughput_ops_per_s: entry.median_throughput_ops_per_s,
        median_value: entry.median_throughput_ops_per_s,
        min_value,
        max_value,
        stability: entry.stability.clone(),
        status: entry.status.clone(),
        runs: entry.num_runs,
        timestamp: generated_at.to_string(),
        commit_hash: commit_hash.map(str::to_string),
        rel_stddev: Some(entry.rel_stddev_runs),
        mean_ns: None,
        p50_latency_ns: None,
        p95_latency_ns: None,
        p99_latency_ns: None,
        allocs_per_op: None,
        bytes_per_op: None,
        source_file: entry.file.clone(),
        comparison_key: format!("{}|{}|{}", entry.suite, entry.name, entry.scenario),
        batch_size: entry.batch_size,
        median_duration_ns: entry.median_duration_ns,
        min_run_ns: entry.min_run_ns,
        max_run_ns: entry.max_run_ns,
    }
}

fn build_criterion_deltas(
    entries: &[CriterionEntry],
    baseline: Option<&BaselineManifest>,
) -> Vec<CriterionDelta> {
    let mut baseline_map = BTreeMap::new();
    if let Some(baseline) = baseline {
        for record in &baseline.criterion {
            baseline_map.insert(record.benchmark.clone(), record.clone());
        }
    }

    entries
        .iter()
        .map(|entry| {
            let baseline_record = baseline_map.get(&entry.benchmark);
            let baseline_value = baseline_record
                .map(|record| record.median_ns)
                .filter(|value| value.is_finite() && *value > 0.0);
            let delta_pct = baseline_value.map(|value| ((entry.median_ns - value) / value) * 100.0);
            CriterionDelta {
                benchmark: entry.benchmark.clone(),
                suite: entry.suite.clone(),
                baseline_value,
                current_value: entry.median_ns,
                delta_pct,
                baseline_stability: baseline_record.map(|record| record.stability.clone()),
                current_stability: entry.stability.clone(),
                baseline_status: baseline_record.map(|record| record.status.clone()),
                current_status: entry.status.clone(),
                directional_delta_pct: delta_pct.map(|value| -value),
            }
        })
        .collect()
}

fn build_stress_deltas(
    entries: &[StressEntry],
    baseline: Option<&BaselineManifest>,
) -> Vec<StressDelta> {
    let mut exact_map = BTreeMap::new();
    let mut by_name = BTreeMap::<String, Vec<StressRecord>>::new();

    if let Some(baseline) = baseline {
        for record in &baseline.stress {
            exact_map.insert(
                (
                    record.suite.clone(),
                    record.name.clone(),
                    record.scenario.clone(),
                ),
                record.clone(),
            );
            by_name
                .entry(record.name.clone())
                .or_default()
                .push(record.clone());
        }
    }

    entries
        .iter()
        .map(|entry| {
            let baseline_record = exact_map
                .get(&(
                    entry.suite.clone(),
                    entry.name.clone(),
                    entry.scenario.clone(),
                ))
                .cloned()
                .or_else(|| {
                    by_name
                        .get(&entry.name)
                        .and_then(|records| records.first().cloned())
                });
            let baseline_value = baseline_record
                .as_ref()
                .map(|record| record.median_throughput_ops_per_s)
                .filter(|value| value.is_finite() && *value > 0.0);
            let delta_pct = baseline_value
                .map(|value| ((entry.median_throughput_ops_per_s - value) / value) * 100.0);
            StressDelta {
                suite: entry.suite.clone(),
                name: entry.name.clone(),
                scenario: entry.scenario.clone(),
                layer: entry.layer.clone(),
                baseline_value,
                current_value: entry.median_throughput_ops_per_s,
                delta_pct,
                baseline_stability: baseline_record
                    .as_ref()
                    .map(|record| record.stability.clone()),
                current_stability: entry.stability.clone(),
                baseline_status: baseline_record.as_ref().map(|record| record.status.clone()),
                current_status: entry.status.clone(),
                directional_delta_pct: delta_pct,
            }
        })
        .collect()
}

fn write_criterion_csv(path: &Path, entries: &[CriterionEntry]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let mut writer = Writer::from_path(path)?;
    writer.write_record([
        "benchmark",
        "mean",
        "mean_ci_lower",
        "mean_ci_upper",
        "median_ns",
        "median_us",
        "median_ms",
        "std_dev",
        "rel_stddev",
        "high_variance",
        "mean_us(assume_ns)",
        "mean_ms(assume_ns)",
        "file",
    ])?;
    for entry in entries {
        writer.write_record([
            entry.benchmark.as_str(),
            &format_float(entry.mean),
            &format_optional_float(entry.mean_ci_lower),
            &format_optional_float(entry.mean_ci_upper),
            &format_float(entry.median_ns),
            &format_float(entry.median_us),
            &format_float(entry.median_ms),
            &format_optional_float(entry.std_dev),
            &format_optional_float(entry.rel_stddev),
            if entry.high_variance { "true" } else { "false" },
            &format_float(entry.mean_us),
            &format_float(entry.mean_ms),
            entry.file.as_str(),
        ])?;
    }
    writer.flush()?;
    Ok(())
}

fn write_stress_csv(path: &Path, entries: &[StressEntry]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let mut writer = Writer::from_path(path)?;
    writer.write_record([
        "suite",
        "name",
        "scenario",
        "batch_size",
        "median_duration_ms",
        "elements",
        "per_op_us",
        "throughput_ops_per_s",
        "median_throughput_ops_per_s",
        "runs",
        "min_run_ns",
        "max_run_ns",
        "stddev_runs_ns",
        "rel_stddev_runs",
        "stability",
        "status",
        "file",
    ])?;
    for entry in entries {
        writer.write_record([
            entry.suite.as_str(),
            entry.name.as_str(),
            entry.scenario.as_str(),
            &format_short_float(entry.batch_size),
            &format_two_decimal(entry.median_duration_ms),
            &format_short_float(entry.elements),
            &format_float(entry.per_op_us),
            &format_two_decimal(entry.throughput_ops_per_s),
            &format_two_decimal(entry.median_throughput_ops_per_s),
            &entry.num_runs.to_string(),
            &format_two_decimal(entry.min_run_ns),
            &format_two_decimal(entry.max_run_ns),
            &format_two_decimal(entry.stddev_runs),
            &format_optional_float(Some(entry.rel_stddev_runs)),
            entry.stability.as_str(),
            entry.status.as_str(),
            entry.file.as_str(),
        ])?;
    }
    writer.flush()?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn write_markdown_report(
    path: &Path,
    root: &Path,
    generated_at: &str,
    commit_hash: Option<&str>,
    criterion_entries: &[CriterionEntry],
    stress_entries: &[StressEntry],
    criterion_deltas: &[CriterionDelta],
    stress_deltas: &[StressDelta],
    baseline_available: bool,
    manifest_path: &Path,
    criterion_csv_path: &Path,
    stress_csv_path: &Path,
) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }

    let mut content = String::new();
    content.push_str("# Fitz Benchmark Report\n\n");
    content.push_str(&format!("- generated_at: {generated_at}\n"));
    content.push_str(&format!("- commit: {}\n", commit_hash.unwrap_or("unknown")));
    content.push_str(&format!("- baseline_available: {}\n", baseline_available));
    content.push_str(&format!("- criterion_rows: {}\n", criterion_entries.len()));
    content.push_str(&format!("- stress_rows: {}\n", stress_entries.len()));
    content.push_str(&format!(
        "- criterion_csv: {}\n",
        display_relative(root, criterion_csv_path)
    ));
    content.push_str(&format!(
        "- stress_csv: {}\n",
        display_relative(root, stress_csv_path)
    ));
    content.push_str(&format!(
        "- manifest: {}\n\n",
        display_relative(root, manifest_path)
    ));

    content.push_str("## Current Criterion Results\n\n");
    if criterion_entries.is_empty() {
        content.push_str("- No Criterion results found under target/criterion.\n\n");
    } else {
        let mut suites = BTreeSet::new();
        for entry in criterion_entries {
            suites.insert(entry.suite.clone());
        }
        for suite in suites {
            content.push_str(&format!("### {suite}\n\n"));
            write_table(
                &mut content,
                &[
                    "benchmark",
                    "median_ns",
                    "median_us",
                    "mean_ns",
                    "rsd",
                    "stability",
                    "status",
                ],
                criterion_entries
                    .iter()
                    .filter(|entry| entry.suite == suite)
                    .map(|entry| {
                        vec![
                            benchmark_short_name(&entry.benchmark),
                            format_ns(entry.median_ns),
                            format_us(entry.median_ns),
                            format_ns(entry.mean),
                            format_rel_stddev(entry.rel_stddev),
                            entry.stability.clone(),
                            entry.status.clone(),
                        ]
                    })
                    .collect(),
            );
        }
    }

    content.push_str("## Current Stress Results\n\n");
    if stress_entries.is_empty() {
        content.push_str("- No stress results found under target/stress.\n\n");
    } else {
        let mut suites = BTreeSet::new();
        for entry in stress_entries {
            suites.insert(entry.suite.clone());
        }
        for suite in suites {
            content.push_str(&format!("### {suite}\n\n"));
            write_table(
                &mut content,
                &[
                    "case",
                    "scenario",
                    "layer",
                    "runs",
                    "batch_size",
                    "duration_ms",
                    "per_op_us",
                    "ops_per_s",
                    "rsd",
                    "status",
                ],
                stress_entries
                    .iter()
                    .filter(|entry| entry.suite == suite)
                    .map(|entry| {
                        vec![
                            raw_stress_case_name(&entry.name),
                            entry.scenario.clone(),
                            entry.layer.clone().unwrap_or_else(|| "NA".to_string()),
                            entry.num_runs.to_string(),
                            format_short_float(entry.batch_size),
                            format_two_decimal(entry.median_duration_ms),
                            format_float(entry.per_op_us),
                            format_ops_short(entry.median_throughput_ops_per_s),
                            format_rel_stddev(Some(entry.rel_stddev_runs)),
                            entry.status.clone(),
                        ]
                    })
                    .collect(),
            );
        }
    }

    content.push_str("## Baseline Deltas\n\n");
    if !baseline_available {
        content.push_str("- No baseline available. Current raw results are still written above and in the CSV/JSON artifacts.\n\n");
    } else {
        content.push_str("### Criterion\n\n");
        write_table(
            &mut content,
            &[
                "suite",
                "benchmark",
                "baseline_median_ns",
                "current_median_ns",
                "delta_ns_pct",
                "baseline_stability",
                "current_stability",
                "baseline_status",
                "current_status",
            ],
            criterion_deltas
                .iter()
                .map(|delta| {
                    vec![
                        delta.suite.clone(),
                        benchmark_short_name(&delta.benchmark),
                        delta
                            .baseline_value
                            .map(format_ns)
                            .unwrap_or_else(|| "NA".to_string()),
                        format_ns(delta.current_value),
                        format_delta(delta.delta_pct),
                        delta
                            .baseline_stability
                            .clone()
                            .unwrap_or_else(|| "NA".to_string()),
                        delta.current_stability.clone(),
                        delta
                            .baseline_status
                            .clone()
                            .unwrap_or_else(|| "NA".to_string()),
                        delta.current_status.clone(),
                    ]
                })
                .collect(),
        );
        content.push_str("### Stress\n\n");
        write_table(
            &mut content,
            &[
                "suite",
                "case",
                "scenario",
                "layer",
                "baseline_ops_s",
                "current_ops_s",
                "delta_ops_pct",
                "baseline_stability",
                "current_stability",
                "baseline_status",
                "current_status",
            ],
            stress_deltas
                .iter()
                .map(|delta| {
                    vec![
                        delta.suite.clone(),
                        raw_stress_case_name(&delta.name),
                        delta.scenario.clone(),
                        delta.layer.clone().unwrap_or_else(|| "NA".to_string()),
                        delta
                            .baseline_value
                            .map(format_ops_short)
                            .unwrap_or_else(|| "NA".to_string()),
                        format_ops_short(delta.current_value),
                        format_delta(delta.delta_pct),
                        delta
                            .baseline_stability
                            .clone()
                            .unwrap_or_else(|| "NA".to_string()),
                        delta.current_stability.clone(),
                        delta
                            .baseline_status
                            .clone()
                            .unwrap_or_else(|| "NA".to_string()),
                        delta.current_status.clone(),
                    ]
                })
                .collect(),
        );
    }

    content.push_str("## Parameter Sweeps\n\n");
    let sweep_groups = detect_sweep_groups(stress_entries)?;
    if sweep_groups.is_empty() {
        content.push_str("- No sweep-style parameter scenarios detected (concurrency/payload/depth/fanout/batch/subscribers).\n\n");
    } else {
        for group in sweep_groups {
            content.push_str(&format!("### {}\n\n", group.title));
            write_table(
                &mut content,
                &[
                    "parameter",
                    "ops_per_s",
                    "delta_vs_previous_pct",
                    "rsd",
                    "run_count",
                    "status",
                ],
                group
                    .points
                    .iter()
                    .map(|point| {
                        vec![
                            format!("{}={}", point.parameter, point.parameter_label),
                            format_ops_short(point.throughput),
                            format_delta(point.delta_vs_previous_pct),
                            format_rel_stddev(Some(point.rel_stddev)),
                            point.runs.to_string(),
                            point.status.clone(),
                        ]
                    })
                    .collect(),
            );
        }
    }

    content.push_str("## Measurement Notes\n\n");
    for warning in collect_noise_warnings(criterion_entries, stress_entries) {
        content.push_str(&format!("- {warning}\n"));
    }
    content.push('\n');

    fs::write(path, content).with_context(|| format!("failed to write {}", path.display()))?;
    Ok(())
}

fn detect_sweep_groups(entries: &[StressEntry]) -> Result<Vec<SweepGroup>> {
    let scenario_patterns = [
        (
            "concurrency",
            Regex::new(r"(?P<prefix>scaling|concurrency|clients?)_(?P<value>\d+[a-zA-Z]*)")?,
        ),
        (
            "subscriber_count",
            Regex::new(r"(?P<prefix>subscribers?|subscriber_count)_(?P<value>\d+[a-zA-Z]*)")?,
        ),
        (
            "payload_size",
            Regex::new(r"(?P<prefix>payload|message|msg)_(?P<value>\d+[a-zA-Z]*)")?,
        ),
        (
            "route_depth",
            Regex::new(r"(?P<prefix>depth)_(?P<value>\d+[a-zA-Z]*)")?,
        ),
        (
            "fanout_size",
            Regex::new(r"(?P<prefix>fanout)_(?P<value>\d+[a-zA-Z]*)")?,
        ),
        (
            "batch_size",
            Regex::new(r"(?P<prefix>batch|batch_size)_(?P<value>\d+[a-zA-Z]*)")?,
        ),
        (
            "list_size",
            Regex::new(r"(?P<prefix>list)_(?P<value>\d+[a-zA-Z]*)")?,
        ),
    ];

    let mut groups = BTreeMap::<String, (String, Vec<SweepPoint>)>::new();
    for entry in entries {
        let mut grouped_by_tag = false;
        for (parameter, value_label, value) in tag_sweep_parameters(entry) {
            grouped_by_tag = true;
            let context = tag_sweep_context(entry);
            let key = format!("{}|{}|{}", entry.suite, parameter, context);
            let title = format!("{} / {} ({})", entry.suite, context, parameter);
            groups
                .entry(key)
                .or_insert_with(|| (title, Vec::new()))
                .1
                .push(SweepPoint {
                    parameter: parameter.to_string(),
                    parameter_label: value_label,
                    parameter_value: value,
                    throughput: entry.median_throughput_ops_per_s,
                    rel_stddev: entry.rel_stddev_runs,
                    runs: entry.num_runs,
                    status: entry.status.clone(),
                    delta_vs_previous_pct: None,
                });
        }
        if grouped_by_tag {
            continue;
        }

        let source = if entry.scenario != "unknown" {
            entry.scenario.as_str()
        } else {
            entry.name.as_str()
        };
        let lowered = source.to_lowercase();
        let Some((parameter, prefix, value_label, matched)) = scenario_patterns.iter().find_map(
            |(parameter, regex)| {
                let captures = regex.captures(&lowered)?;
                Some((
                    *parameter,
                    captures
                        .name("prefix")
                        .map(|value| value.as_str().to_string())
                        .unwrap_or_else(|| parameter.to_string()),
                    captures
                        .name("value")
                        .map(|value| value.as_str().to_string())
                        .unwrap_or_default(),
                    captures
                        .get(0)
                        .map(|value| value.as_str().to_string())
                        .unwrap_or_default(),
                ))
            },
        ) else {
            continue;
        };

        let Some(value) = parse_numeric_token(&value_label) else {
            continue;
        };
        let family = scenario_family_without_sweep_token(&lowered, &matched, &prefix);
        let context = if let Some(layer) = &entry.layer {
            format!("{} [layer={layer}]", family)
        } else {
            family
        };
        let key = format!("{}|{}|{}", entry.suite, parameter, context);
        let title = format!("{} / {} ({})", entry.suite, context, parameter);
        groups
            .entry(key)
            .or_insert_with(|| (title, Vec::new()))
            .1
            .push(SweepPoint {
                parameter: parameter.to_string(),
                parameter_label: value_label,
                parameter_value: value,
                throughput: entry.median_throughput_ops_per_s,
                rel_stddev: entry.rel_stddev_runs,
                runs: entry.num_runs,
                status: entry.status.clone(),
                delta_vs_previous_pct: None,
            });
    }

    let mut output = Vec::new();
    for (_key, (title, mut points)) in groups {
        points.sort_by(|left, right| compare_f64(left.parameter_value, right.parameter_value));
        points.dedup_by(|left, right| left.parameter_value == right.parameter_value);
        if points.len() < 2 {
            continue;
        }

        for index in 1..points.len() {
            let previous = points[index - 1].throughput;
            let current = points[index].throughput;
            if previous > 0.0 {
                points[index].delta_vs_previous_pct =
                    Some(((current - previous) / previous) * 100.0);
            }
        }

        output.push(SweepGroup { title, points });
    }
    output.sort_by(|left, right| left.title.cmp(&right.title));
    Ok(output)
}

fn tag_sweep_parameters(entry: &StressEntry) -> Vec<(&str, String, f64)> {
    let mut parameters = Vec::new();
    for (key, value_label) in &entry.tags {
        if !is_sweep_tag_key(key) {
            continue;
        }
        let Some(value) = parse_numeric_token(value_label) else {
            continue;
        };
        parameters.push((key.as_str(), value_label.clone(), value));
    }
    parameters
}

fn is_sweep_tag_key(key: &str) -> bool {
    matches!(
        key,
        "client_count"
            | "subscriber_count"
            | "publisher_count"
            | "worker_count"
            | "route_count"
            | "queue_count"
            | "area_count"
            | "family_count"
            | "payload_size"
            | "message_size"
            | "fanout_size"
            | "list_size"
            | "depth"
    ) || key.ends_with("_count")
        || key.ends_with("_size")
        || key.ends_with("_depth")
}

fn tag_sweep_context(entry: &StressEntry) -> String {
    let mut qualifiers = Vec::new();
    for key in ["measurement_scope", "match_kind", "ready_state", "operation"] {
        if let Some(value) = entry.tags.get(key) {
            qualifiers.push(format!("{key}={value}"));
        }
    }
    if let Some(layer) = &entry.layer {
        qualifiers.push(format!("layer={layer}"));
    }

    let base = if entry.scenario != "unknown" {
        entry.scenario.clone()
    } else {
        raw_stress_case_name(&entry.name)
    };
    if qualifiers.is_empty() {
        base
    } else {
        format!("{} [{}]", base, qualifiers.join(", "))
    }
}

fn scenario_family_without_sweep_token(source: &str, matched: &str, fallback: &str) -> String {
    let family = source
        .replacen(matched, "", 1)
        .trim_matches('_')
        .to_string();
    family.if_empty_then(|| fallback.to_string())
}

fn collect_noise_warnings(
    criterion_entries: &[CriterionEntry],
    stress_entries: &[StressEntry],
) -> Vec<String> {
    let mut warnings = Vec::new();

    for entry in stress_entries
        .iter()
        .filter(|entry| entry.status == "insufficient_data")
        .take(12)
    {
        warnings.push(format!(
            "{} / {}: insufficient data ({} run(s), need >=5).",
            entry.suite, entry.scenario, entry.num_runs
        ));
    }

    let mut noisy_criterion: Vec<&CriterionEntry> = criterion_entries
        .iter()
        .filter(|entry| matches!(entry.stability.as_str(), "noisy" | "untrustworthy"))
        .collect();
    noisy_criterion.sort_by(|left, right| {
        compare_f64(
            right.rel_stddev.unwrap_or(0.0),
            left.rel_stddev.unwrap_or(0.0),
        )
    });
    for entry in noisy_criterion.into_iter().take(8) {
        warnings.push(format!(
            "{}: unstable variance ({:.1}% RSD).",
            entry.benchmark,
            entry.rel_stddev.unwrap_or(0.0) * 100.0
        ));
    }

    let mut noisy_stress: Vec<&StressEntry> = stress_entries
        .iter()
        .filter(|entry| matches!(entry.stability.as_str(), "noisy" | "untrustworthy"))
        .collect();
    noisy_stress.sort_by(|left, right| compare_f64(right.rel_stddev_runs, left.rel_stddev_runs));
    for entry in noisy_stress.into_iter().take(8) {
        warnings.push(format!(
            "{} / {}: unstable variance ({:.1}% RSD).",
            entry.suite,
            entry.scenario,
            entry.rel_stddev_runs * 100.0
        ));
    }

    for entry in stress_entries
        .iter()
        .filter(|entry| entry.batch_size >= 50_000_000.0)
        .take(8)
    {
        warnings.push(format!(
            "{} / {}: suspicious batch size {}.",
            entry.suite,
            entry.scenario,
            format_short_float(entry.batch_size)
        ));
    }

    for entry in stress_entries
        .iter()
        .filter(|entry| entry.median_throughput_ops_per_s >= 50_000_000.0)
        .take(8)
    {
        warnings.push(format!(
            "{} / {}: throughput {} ops/s may be unrealistic.",
            entry.suite,
            entry.scenario,
            format_ops_short(entry.median_throughput_ops_per_s)
        ));
    }

    if warnings.is_empty() {
        warnings.push("No major measurement warnings detected in current data.".to_string());
    }

    warnings
}

fn current_run_authoritative(criterion: &[CriterionRecord], stress: &[StressRecord]) -> bool {
    !criterion.is_empty()
        && !stress.is_empty()
        && criterion.iter().all(|record| {
            record.status == "authoritative"
                && matches!(record.stability.as_str(), "stable" | "acceptable")
        })
        && stress.iter().all(|record| {
            record.status == "authoritative"
                && matches!(record.stability.as_str(), "stable" | "acceptable")
        })
}

fn count_critical_regressions(criterion: &[CriterionDelta], stress: &[StressDelta]) -> usize {
    criterion
        .iter()
        .filter(|item| item.directional_delta_pct.unwrap_or(0.0) <= -CRITICAL_REGRESSION_PCT)
        .count()
        + stress
            .iter()
            .filter(|item| item.directional_delta_pct.unwrap_or(0.0) <= -CRITICAL_REGRESSION_PCT)
            .count()
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

fn variance_band(value: Option<f64>) -> &'static str {
    match value {
        None => "unknown",
        Some(value) if value <= 0.05 => "stable",
        Some(value) if value <= 0.10 => "acceptable",
        Some(value) if value <= 0.20 => "noisy",
        Some(_) => "untrustworthy",
    }
}

fn stress_status(num_runs: usize, meets_runtime_floor: bool) -> &'static str {
    if num_runs < 5 {
        return "insufficient_data";
    }
    if !meets_runtime_floor {
        return "invalid_for_throughput";
    }
    "authoritative"
}

fn benchmark_domain(benchmark: &str) -> String {
    benchmark.split('/').next().unwrap_or(benchmark).to_string()
}

fn benchmark_short_name(benchmark: &str) -> String {
    benchmark
        .split('/')
        .skip(1)
        .collect::<Vec<_>>()
        .join("/")
        .if_empty_then(|| benchmark.to_string())
}

fn raw_stress_case_name(name: &str) -> String {
    name.split("::").last().unwrap_or(name).to_string()
}

fn domain_from_suite(suite: &str) -> String {
    let normalized = suite.replace('_', "-").to_lowercase();
    let tail = normalized
        .split("system-")
        .nth(1)
        .or_else(|| normalized.split("integration-").nth(1))
        .unwrap_or(&normalized);
    match tail.split('-').next_back().unwrap_or(tail) {
        "kv" => "KV",
        "lease" => "Lease",
        "notice" => "Notice",
        "queue" => "Queue",
        "rpc" => "RPC",
        "schedule" => "Schedule",
        "stream" => "Stream",
        other => other,
    }
    .to_string()
}

fn median(values: &[f64]) -> f64 {
    let mut ordered = values.to_vec();
    ordered.sort_by(|left, right| compare_f64(*left, *right));
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

fn parse_numeric_token(token: &str) -> Option<f64> {
    let value = token.trim().to_lowercase();
    let (number, factor) = if let Some(stripped) = value.strip_suffix("kb") {
        (stripped, 1024.0)
    } else if let Some(stripped) = value.strip_suffix("mb") {
        (stripped, 1024.0 * 1024.0)
    } else if let Some(stripped) = value.strip_suffix('b') {
        (stripped, 1.0)
    } else if let Some(stripped) = value.strip_suffix('k') {
        (stripped, 1000.0)
    } else if let Some(stripped) = value.strip_suffix('m') {
        (stripped, 1_000_000.0)
    } else {
        (value.as_str(), 1.0)
    };
    number.parse::<f64>().ok().map(|parsed| parsed * factor)
}

fn display_relative(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .display()
        .to_string()
}

fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let content = serde_json::to_string_pretty(value)?;
    fs::write(path, format!("{content}\n"))
        .with_context(|| format!("failed to write {}", path.display()))?;
    Ok(())
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

fn compare_f64(left: f64, right: f64) -> Ordering {
    left.partial_cmp(&right).unwrap_or(Ordering::Equal)
}

fn format_float(value: f64) -> String {
    format!("{value:.6}")
}

fn format_two_decimal(value: f64) -> String {
    format!("{value:.2}")
}

fn format_short_float(value: f64) -> String {
    if value.fract() == 0.0 {
        format!("{value:.0}")
    } else {
        format!("{value:.3}")
    }
}

fn format_optional_float(value: Option<f64>) -> String {
    value.map(format_float).unwrap_or_else(|| "NA".to_string())
}

fn format_ns(value: f64) -> String {
    format!("{value:.0}")
}

fn format_us(value: f64) -> String {
    format!("{:.3}", value / 1e3)
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

fn format_ops_short(value: f64) -> String {
    if value >= 1_000_000_000.0 {
        return "REJECTED".to_string();
    }
    format!("{value:.0}")
}

trait EmptyFallback {
    fn if_empty_then<F>(self, fallback: F) -> String
    where
        F: FnOnce() -> String;
}

impl EmptyFallback for String {
    fn if_empty_then<F>(self, fallback: F) -> String
    where
        F: FnOnce() -> String,
    {
        if self.is_empty() {
            fallback()
        } else {
            self
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stress_entry_for_test(
        suite: &str,
        name: &str,
        scenario: &str,
        throughput: f64,
        tags: &[(&str, &str)],
    ) -> StressEntry {
        StressEntry {
            suite: suite.to_string(),
            name: name.to_string(),
            scenario: scenario.to_string(),
            layer: None,
            tags: tags
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect(),
            batch_size: 1.0,
            duration_ns: 1.0,
            median_duration_ns: 1.0,
            duration_us: 0.001,
            duration_ms: 0.000001,
            median_duration_ms: 0.000001,
            elements: 1.0,
            run_values: vec![1.0; 5],
            min_run_ns: 1.0,
            max_run_ns: 1.0,
            per_op_ns: 1.0,
            per_op_us: 0.001,
            throughput_ops_per_ns: throughput / 1e9,
            throughput_ops_per_us: throughput / 1e6,
            throughput_ops_per_ms: throughput / 1e3,
            throughput_ops_per_s: throughput,
            median_throughput_ops_per_s: throughput,
            num_runs: 5,
            meets_runtime_floor: true,
            stddev_runs: 0.0,
            rel_stddev_runs: 0.0,
            stability: "stable".to_string(),
            status: "authoritative".to_string(),
            file: "target/stress/test/latest.json".to_string(),
        }
    }

    #[test]
    fn should_detect_sweep_groups_from_subscriber_count_tags() {
        // Arrange
        let entries = vec![
            stress_entry_for_test(
                "tier3-system-notice",
                "notice::fanout_16",
                "fanout_subscriber_scaling",
                1000.0,
                &[
                    ("subscriber_count", "16"),
                    ("measurement_scope", "routed_fanout"),
                    ("match_kind", "single_star"),
                ],
            ),
            stress_entry_for_test(
                "tier3-system-notice",
                "notice::fanout_64",
                "fanout_subscriber_scaling",
                750.0,
                &[
                    ("subscriber_count", "64"),
                    ("measurement_scope", "routed_fanout"),
                    ("match_kind", "single_star"),
                ],
            ),
        ];

        // Act
        let groups = detect_sweep_groups(&entries).expect("detect sweep groups");

        // Assert
        assert_eq!(groups.len(), 1);
        assert!(groups[0].title.contains("subscriber_count"));
        assert_eq!(groups[0].points[0].parameter_label, "16");
        assert_eq!(groups[0].points[1].parameter_label, "64");
    }

    #[test]
    fn should_keep_dispatch_and_roundtrip_scaling_sweeps_separate() {
        // Arrange
        let entries = vec![
            stress_entry_for_test(
                "tier3-system-rpc",
                "rpc::dispatch_64",
                "scaling_64_dispatch_only",
                1200.0,
                &[],
            ),
            stress_entry_for_test(
                "tier3-system-rpc",
                "rpc::dispatch_256",
                "scaling_256_dispatch_only",
                900.0,
                &[],
            ),
            stress_entry_for_test(
                "tier3-system-rpc",
                "rpc::roundtrip_64",
                "scaling_64_full_roundtrip",
                800.0,
                &[],
            ),
            stress_entry_for_test(
                "tier3-system-rpc",
                "rpc::roundtrip_256",
                "scaling_256_full_roundtrip",
                600.0,
                &[],
            ),
        ];

        // Act
        let groups = detect_sweep_groups(&entries).expect("detect sweep groups");

        // Assert
        assert_eq!(groups.len(), 2);
        assert!(groups.iter().any(|group| group.title.contains("dispatch_only")));
        assert!(groups.iter().any(|group| group.title.contains("full_roundtrip")));
        assert!(groups.iter().all(|group| group.points.len() == 2));
    }
}
