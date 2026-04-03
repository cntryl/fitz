use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkManifest {
    pub schema_version: u32,
    pub generated_at: String,
    pub commit_hash: Option<String>,
    #[serde(default)]
    pub comparison_summary: ComparisonSummary,
    pub records: Vec<BenchmarkRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkRecord {
    pub id: String,
    pub adapter: String,
    pub suite: String,
    pub case: String,
    pub scenario: Option<String>,
    pub metric: String,
    pub unit: String,
    pub value: f64,
    pub lower_bound: Option<f64>,
    pub upper_bound: Option<f64>,
    pub samples: Option<usize>,
    pub metric_direction: MetricDirection,
    pub stability: Option<String>,
    pub status: Option<String>,
    pub rel_stddev: Option<f64>,
    pub tags: BTreeMap<String, String>,
    pub metadata: BTreeMap<String, String>,
    pub source_file: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum MetricDirection {
    LowerIsBetter,
    HigherIsBetter,
}

impl MetricDirection {
    pub fn directional_delta(self, raw_delta_pct: f64) -> f64 {
        match self {
            Self::LowerIsBetter => -raw_delta_pct,
            Self::HigherIsBetter => raw_delta_pct,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ComparisonSummary {
    pub performance_changes: usize,
    pub improved: usize,
    pub regressions: usize,
    pub critical: usize,
    pub stability_regressions: usize,
    pub stability_improvements: usize,
    pub new: usize,
    pub missing: usize,
    pub risk_areas: usize,
    pub authoritative: bool,
    pub baseline_available: bool,
}

#[derive(Debug, Clone)]
pub struct BenchmarkDelta {
    pub id: String,
    pub suite: String,
    pub case: String,
    pub metric: String,
    pub baseline_value: Option<f64>,
    pub current_value: f64,
    pub delta_pct: Option<f64>,
    pub directional_delta_pct: Option<f64>,
    pub baseline_stability: Option<String>,
    pub current_stability: Option<String>,
    pub baseline_status: Option<String>,
    pub current_status: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SweepPoint {
    pub parameter: String,
    pub parameter_label: String,
    pub parameter_value: f64,
    pub metric_value: f64,
    pub delta_vs_previous_pct: Option<f64>,
    pub rel_stddev: Option<f64>,
    pub samples: Option<usize>,
    pub status: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SweepGroup {
    pub title: String,
    pub metric: String,
    pub unit: String,
    pub points: Vec<SweepPoint>,
}
