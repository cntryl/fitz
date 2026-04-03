use std::collections::{BTreeMap, BTreeSet};

use super::model::{BenchmarkDelta, BenchmarkManifest, BenchmarkRecord};

#[derive(Debug, Clone, Default)]
pub struct ComparisonOutcome {
    pub deltas: Vec<BenchmarkDelta>,
    pub new_records: Vec<BenchmarkRecord>,
    pub missing_records: Vec<BenchmarkRecord>,
}

pub fn compare_records(
    current: &[BenchmarkRecord],
    baseline: Option<&BenchmarkManifest>,
) -> ComparisonOutcome {
    let mut baseline_map = BTreeMap::new();
    if let Some(baseline) = baseline {
        for record in &baseline.records {
            baseline_map.insert(record.id.clone(), record.clone());
        }
    }

    let mut deltas = Vec::with_capacity(current.len());
    let mut new_records = Vec::new();
    let mut current_ids = BTreeSet::new();

    for record in current {
        current_ids.insert(record.id.clone());

        let baseline_record = baseline_map.get(&record.id);
        let baseline_value = baseline_record
            .map(|value| value.value)
            .filter(|value| value.is_finite() && *value > 0.0);
        let delta_pct = baseline_value.map(|value| ((record.value - value) / value) * 100.0);

        if baseline_record.is_none() {
            new_records.push(record.clone());
        }

        deltas.push(BenchmarkDelta {
            id: record.id.clone(),
            suite: record.suite.clone(),
            case: record.case.clone(),
            metric: record.metric.clone(),
            baseline_value,
            current_value: record.value,
            delta_pct,
            directional_delta_pct: delta_pct
                .map(|value| record.metric_direction.directional_delta(value)),
            baseline_stability: baseline_record.and_then(|value| value.stability.clone()),
            current_stability: record.stability.clone(),
            baseline_status: baseline_record.and_then(|value| value.status.clone()),
            current_status: record.status.clone(),
        });
    }

    deltas.sort_by(|left, right| {
        left.suite
            .cmp(&right.suite)
            .then_with(|| left.case.cmp(&right.case))
            .then_with(|| left.metric.cmp(&right.metric))
            .then_with(|| left.id.cmp(&right.id))
    });

    let mut missing_records = baseline_map
        .into_iter()
        .filter(|(id, _)| !current_ids.contains(id))
        .map(|(_, record)| record)
        .collect::<Vec<_>>();
    missing_records.sort_by(|left, right| {
        left.adapter
            .cmp(&right.adapter)
            .then_with(|| left.suite.cmp(&right.suite))
            .then_with(|| left.case.cmp(&right.case))
            .then_with(|| left.metric.cmp(&right.metric))
            .then_with(|| left.id.cmp(&right.id))
    });

    ComparisonOutcome {
        deltas,
        new_records,
        missing_records,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::benchmark_summary::model::{BenchmarkManifest, ComparisonSummary, MetricDirection};

    fn record(id: &str, case: &str, direction: MetricDirection, value: f64) -> BenchmarkRecord {
        BenchmarkRecord {
            id: id.to_string(),
            adapter: "criterion".to_string(),
            suite: "suite".to_string(),
            case: case.to_string(),
            scenario: None,
            metric: match direction {
                MetricDirection::LowerIsBetter => "latency_ns".to_string(),
                MetricDirection::HigherIsBetter => "throughput_ops_per_s".to_string(),
            },
            unit: match direction {
                MetricDirection::LowerIsBetter => "ns".to_string(),
                MetricDirection::HigherIsBetter => "ops/s".to_string(),
            },
            value,
            lower_bound: None,
            upper_bound: None,
            samples: None,
            metric_direction: direction,
            stability: Some("stable".to_string()),
            status: Some("authoritative".to_string()),
            rel_stddev: Some(0.01),
            tags: BTreeMap::new(),
            metadata: BTreeMap::new(),
            source_file: "target/file".to_string(),
        }
    }

    fn manifest(records: Vec<BenchmarkRecord>) -> BenchmarkManifest {
        BenchmarkManifest {
            schema_version: 2,
            generated_at: "now".to_string(),
            commit_hash: None,
            comparison_summary: ComparisonSummary::default(),
            records,
        }
    }

    #[test]
    fn should_compute_positive_directional_delta_when_lower_is_better_metric_improves() {
        // Arrange
        let baseline = manifest(vec![record(
            "stable-id",
            "baseline-case",
            MetricDirection::LowerIsBetter,
            100.0,
        )]);
        let current = vec![record(
            "stable-id",
            "current-case",
            MetricDirection::LowerIsBetter,
            80.0,
        )];

        // Act
        let comparison = compare_records(&current, Some(&baseline));

        // Assert
        assert_eq!(comparison.deltas.len(), 1);
        assert_eq!(comparison.deltas[0].delta_pct, Some(-20.0));
        assert_eq!(comparison.deltas[0].directional_delta_pct, Some(20.0));
    }

    #[test]
    fn should_compute_positive_directional_delta_when_higher_is_better_metric_improves() {
        // Arrange
        let baseline = manifest(vec![record(
            "stable-id",
            "baseline-case",
            MetricDirection::HigherIsBetter,
            100.0,
        )]);
        let current = vec![record(
            "stable-id",
            "current-case",
            MetricDirection::HigherIsBetter,
            120.0,
        )];

        // Act
        let comparison = compare_records(&current, Some(&baseline));

        // Assert
        assert_eq!(comparison.deltas.len(), 1);
        assert_eq!(comparison.deltas[0].delta_pct, Some(20.0));
        assert_eq!(comparison.deltas[0].directional_delta_pct, Some(20.0));
    }

    #[test]
    fn should_match_baseline_by_stable_id_given_current_case_label_changes() {
        // Arrange
        let baseline = manifest(vec![record(
            "stable-id",
            "old-case",
            MetricDirection::LowerIsBetter,
            100.0,
        )]);
        let current = vec![record(
            "stable-id",
            "renamed-case",
            MetricDirection::LowerIsBetter,
            95.0,
        )];

        // Act
        let comparison = compare_records(&current, Some(&baseline));

        // Assert
        assert_eq!(comparison.deltas[0].baseline_value, Some(100.0));
        assert_eq!(comparison.deltas[0].case, "renamed-case");
    }
}
