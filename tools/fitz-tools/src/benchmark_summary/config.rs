use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use anyhow::{Context, Result};
use regex::Regex;

use super::model::BenchmarkRecord;

#[derive(Debug, Clone)]
pub struct BenchSummaryConfig {
    pub schema_version: u32,
    pub product_name: String,
    pub report_title: String,
    pub root: PathBuf,
    pub baseline_file: PathBuf,
    pub manifest_file: PathBuf,
    pub markdown_report_file: PathBuf,
    pub adapters: AdapterConfigs,
    pub thresholds: SummaryThresholds,
    pub stability_thresholds: StabilityThresholds,
    pub authoritative_statuses: BTreeSet<String>,
    pub authoritative_stabilities: BTreeSet<String>,
    pub ignored_exact_ids: BTreeSet<String>,
    pub ignored_patterns: Vec<Regex>,
    pub sweep: SweepConfig,
    pub suite_display_overrides: BTreeMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct AdapterConfigs {
    pub criterion: Option<CriterionAdapterConfig>,
    pub stress: Option<StressAdapterConfig>,
}

#[derive(Debug, Clone)]
pub struct CriterionAdapterConfig {
    pub input_root: PathBuf,
    pub csv_output: Option<PathBuf>,
    pub min_reasonable_mean_ns: f64,
    pub max_reasonable_mean_ns: f64,
}

#[derive(Debug, Clone)]
pub struct StressAdapterConfig {
    pub input_root: PathBuf,
    pub csv_output: Option<PathBuf>,
    pub min_reasonable_duration_ns: f64,
    pub max_reasonable_throughput_ops_per_s: f64,
    pub authoritative_min_runs: usize,
}

#[derive(Debug, Clone)]
pub struct SummaryThresholds {
    pub warning_regression_pct: f64,
    pub critical_regression_pct: f64,
}

#[derive(Debug, Clone)]
pub struct StabilityThresholds {
    pub stable_rsd_max: f64,
    pub acceptable_rsd_max: f64,
    pub noisy_rsd_max: f64,
}

#[derive(Debug, Clone)]
pub struct SweepConfig {
    pub tag_keys: BTreeSet<String>,
    pub tag_key_suffixes: Vec<String>,
    pub context_tag_keys: Vec<String>,
    pub name_patterns: Vec<SweepNamePatternConfig>,
}

#[derive(Debug, Clone)]
pub struct SweepNamePatternConfig {
    pub parameter: String,
    pub regex: Regex,
    pub value_capture: String,
    pub prefix_capture: Option<String>,
}

impl BenchSummaryConfig {
    pub fn for_fitz(root: PathBuf) -> Result<Self> {
        let root = root
            .canonicalize()
            .with_context(|| format!("failed to resolve repository root {}", root.display()))?;
        Self::build_fitz(root)
    }

    #[cfg(test)]
    pub fn for_tests() -> Self {
        Self::build_fitz(PathBuf::from(".")).expect("build test benchmark summary config")
    }

    #[cfg(test)]
    pub fn for_tests_with_exact_ignore<const N: usize>(ids: [&str; N]) -> Self {
        let mut config = Self::for_tests();
        config.ignored_exact_ids = ids.into_iter().map(str::to_string).collect();
        config
    }

    pub fn criterion_csv_output(&self) -> Option<&PathBuf> {
        self.adapters
            .criterion
            .as_ref()
            .and_then(|config| config.csv_output.as_ref())
    }

    pub fn stress_csv_output(&self) -> Option<&PathBuf> {
        self.adapters
            .stress
            .as_ref()
            .and_then(|config| config.csv_output.as_ref())
    }

    pub fn display_suite_label(&self, suite: &str) -> String {
        self.suite_display_overrides
            .get(suite)
            .cloned()
            .unwrap_or_else(|| suite.to_string())
    }

    pub fn should_ignore_record(&self, record: &BenchmarkRecord) -> bool {
        if self.ignored_exact_ids.contains(&record.id) {
            return true;
        }

        self.ignored_patterns
            .iter()
            .any(|pattern| self.matches_ignore_pattern(record, pattern))
    }

    fn matches_ignore_pattern(&self, record: &BenchmarkRecord, pattern: &Regex) -> bool {
        if pattern.is_match(&record.id)
            || pattern.is_match(&record.adapter)
            || pattern.is_match(&record.suite)
            || pattern.is_match(&record.case)
            || pattern.is_match(&record.metric)
            || pattern.is_match(&record.source_file)
        {
            return true;
        }

        if let Some(scenario) = &record.scenario {
            if pattern.is_match(scenario) {
                return true;
            }
        }

        record.tags.iter().any(|(key, value)| {
            pattern.is_match(key)
                || pattern.is_match(value)
                || pattern.is_match(&format!("{key}={value}"))
        }) || record.metadata.iter().any(|(key, value)| {
            pattern.is_match(key)
                || pattern.is_match(value)
                || pattern.is_match(&format!("{key}={value}"))
        })
    }

    fn build_fitz(root: PathBuf) -> Result<Self> {
        Ok(Self {
            schema_version: 2,
            product_name: "Fitz".to_string(),
            report_title: "Fitz Benchmark Report".to_string(),
            baseline_file: root.join("config").join("bench_baseline.json"),
            manifest_file: root.join("target").join("bench_results.json"),
            markdown_report_file: root.join("target").join("bench_summary.md"),
            adapters: AdapterConfigs {
                criterion: Some(CriterionAdapterConfig {
                    input_root: root.join("target").join("criterion"),
                    csv_output: Some(
                        root.join("target")
                            .join("criterion")
                            .join("benchmark_summary.csv"),
                    ),
                    min_reasonable_mean_ns: 1.0,
                    max_reasonable_mean_ns: 1e12,
                }),
                stress: Some(StressAdapterConfig {
                    input_root: root.join("target").join("stress"),
                    csv_output: Some(root.join("target").join("stress").join("stress_summary.csv")),
                    min_reasonable_duration_ns: 3e9,
                    max_reasonable_throughput_ops_per_s: 1e9,
                    authoritative_min_runs: 5,
                }),
            },
            thresholds: SummaryThresholds {
                warning_regression_pct: 10.0,
                critical_regression_pct: 25.0,
            },
            stability_thresholds: StabilityThresholds {
                stable_rsd_max: 0.05,
                acceptable_rsd_max: 0.10,
                noisy_rsd_max: 0.20,
            },
            authoritative_statuses: ["authoritative"].into_iter().map(str::to_string).collect(),
            authoritative_stabilities: ["stable", "acceptable"]
                .into_iter()
                .map(str::to_string)
                .collect(),
            ignored_exact_ids: [
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
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
            ignored_patterns: vec![Regex::new(r"schedule_system_scan_and_fire")?],
            sweep: SweepConfig {
                tag_keys: [
                    "client_count",
                    "subscriber_count",
                    "publisher_count",
                    "worker_count",
                    "pending_count",
                    "route_count",
                    "queue_count",
                    "area_count",
                    "family_count",
                    "payload_size",
                    "message_size",
                    "fanout_size",
                    "batch_size",
                    "list_size",
                    "depth",
                ]
                .into_iter()
                .map(str::to_string)
                .collect(),
                tag_key_suffixes: vec!["_count".to_string(), "_size".to_string(), "_depth".to_string()],
                context_tag_keys: vec![
                    "measurement_scope".to_string(),
                    "match_kind".to_string(),
                    "ready_state".to_string(),
                    "operation".to_string(),
                    "layer".to_string(),
                ],
                name_patterns: vec![
                    SweepNamePatternConfig {
                        parameter: "concurrency".to_string(),
                        regex: Regex::new(r"(?P<prefix>scaling|concurrency|clients?)_(?P<value>\d+[a-zA-Z]*)")?,
                        value_capture: "value".to_string(),
                        prefix_capture: Some("prefix".to_string()),
                    },
                    SweepNamePatternConfig {
                        parameter: "subscriber_count".to_string(),
                        regex: Regex::new(r"(?P<prefix>subscribers?|subscriber_count)_(?P<value>\d+[a-zA-Z]*)")?,
                        value_capture: "value".to_string(),
                        prefix_capture: Some("prefix".to_string()),
                    },
                    SweepNamePatternConfig {
                        parameter: "payload_size".to_string(),
                        regex: Regex::new(r"(?P<prefix>payload|message|msg)_(?P<value>\d+[a-zA-Z]*)")?,
                        value_capture: "value".to_string(),
                        prefix_capture: Some("prefix".to_string()),
                    },
                    SweepNamePatternConfig {
                        parameter: "route_depth".to_string(),
                        regex: Regex::new(r"(?P<prefix>depth)_(?P<value>\d+[a-zA-Z]*)")?,
                        value_capture: "value".to_string(),
                        prefix_capture: Some("prefix".to_string()),
                    },
                    SweepNamePatternConfig {
                        parameter: "fanout_size".to_string(),
                        regex: Regex::new(r"(?P<prefix>fanout)_(?P<value>\d+[a-zA-Z]*)")?,
                        value_capture: "value".to_string(),
                        prefix_capture: Some("prefix".to_string()),
                    },
                    SweepNamePatternConfig {
                        parameter: "batch_size".to_string(),
                        regex: Regex::new(r"(?P<prefix>batch|batch_size)_(?P<value>\d+[a-zA-Z]*)")?,
                        value_capture: "value".to_string(),
                        prefix_capture: Some("prefix".to_string()),
                    },
                    SweepNamePatternConfig {
                        parameter: "list_size".to_string(),
                        regex: Regex::new(r"(?P<prefix>list)_(?P<value>\d+[a-zA-Z]*)")?,
                        value_capture: "value".to_string(),
                        prefix_capture: Some("prefix".to_string()),
                    },
                ],
            },
            suite_display_overrides: BTreeMap::new(),
            root,
        })
    }
}