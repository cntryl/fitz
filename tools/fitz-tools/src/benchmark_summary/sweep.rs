use std::cmp::Ordering;
use std::collections::BTreeMap;

use anyhow::Result;

use super::config::BenchSummaryConfig;
use super::model::{BenchmarkRecord, SweepGroup, SweepPoint};

pub fn detect_sweep_groups(
    records: &[BenchmarkRecord],
    config: &BenchSummaryConfig,
) -> Result<Vec<SweepGroup>> {
    let mut groups = BTreeMap::<String, SweepGroup>::new();

    for record in records {
        let mut grouped_by_tag = false;
        for (parameter, label, value) in tag_sweep_parameters(record, config) {
            grouped_by_tag = true;
            let context = tag_sweep_context(record, config);
            let key = group_key(record, &parameter, &context);
            groups
                .entry(key)
                .or_insert_with(|| new_group(record, &parameter, &context))
                .points
                .push(SweepPoint {
                    parameter: parameter.clone(),
                    parameter_label: label,
                    parameter_value: value,
                    metric_value: record.value,
                    delta_vs_previous_pct: None,
                    rel_stddev: record.rel_stddev,
                    samples: record.samples,
                    status: record.status.clone(),
                });
        }
        if grouped_by_tag {
            continue;
        }

        let source = record.scenario.as_deref().unwrap_or(&record.case);
        let lowered = source.to_lowercase();
        let Some((parameter, prefix, value_label, matched)) =
            config.sweep.name_patterns.iter().find_map(|pattern| {
                let captures = pattern.regex.captures(&lowered)?;
                Some((
                    pattern.parameter.clone(),
                    pattern
                        .prefix_capture
                        .as_ref()
                        .and_then(|name| captures.name(name))
                        .map(|value| value.as_str().to_string())
                        .unwrap_or_else(|| pattern.parameter.clone()),
                    captures
                        .name(&pattern.value_capture)
                        .map(|value| value.as_str().to_string())
                        .unwrap_or_default(),
                    captures
                        .get(0)
                        .map(|value| value.as_str().to_string())
                        .unwrap_or_default(),
                ))
            })
        else {
            continue;
        };

        let Some(value) = parse_numeric_token(&value_label) else {
            continue;
        };
        let family = scenario_family_without_sweep_token(&lowered, &matched, &prefix);
        let context = regex_sweep_context(record, &family, config);
        let key = group_key(record, &parameter, &context);
        groups
            .entry(key)
            .or_insert_with(|| new_group(record, &parameter, &context))
            .points
            .push(SweepPoint {
                parameter: parameter.clone(),
                parameter_label: value_label,
                parameter_value: value,
                metric_value: record.value,
                delta_vs_previous_pct: None,
                rel_stddev: record.rel_stddev,
                samples: record.samples,
                status: record.status.clone(),
            });
    }

    let mut output = Vec::new();
    for (_key, mut group) in groups {
        group
            .points
            .sort_by(|left, right| compare_f64(left.parameter_value, right.parameter_value));
        if group.points.len() < 2 {
            continue;
        }

        for index in 1..group.points.len() {
            let previous = group.points[index - 1].metric_value;
            let current = group.points[index].metric_value;
            if previous > 0.0 {
                group.points[index].delta_vs_previous_pct =
                    Some(((current - previous) / previous) * 100.0);
            }
        }

        output.push(group);
    }

    output.sort_by(|left, right| left.title.cmp(&right.title));
    Ok(output)
}

fn tag_sweep_parameters(
    record: &BenchmarkRecord,
    config: &BenchSummaryConfig,
) -> Vec<(String, String, f64)> {
    let mut parameters = Vec::new();
    for (key, value_label) in &record.tags {
        if !is_sweep_tag_key(key, config) {
            continue;
        }
        let Some(value) = parse_numeric_token(value_label) else {
            continue;
        };
        parameters.push((key.clone(), value_label.clone(), value));
    }
    parameters
}

fn is_sweep_tag_key(key: &str, config: &BenchSummaryConfig) -> bool {
    config.sweep.tag_keys.contains(key)
        || config
            .sweep
            .tag_key_suffixes
            .iter()
            .any(|suffix| key.ends_with(suffix))
}

fn tag_sweep_context(record: &BenchmarkRecord, config: &BenchSummaryConfig) -> String {
    let mut qualifiers = Vec::new();
    for key in &config.sweep.context_tag_keys {
        if let Some(value) = record.tags.get(key) {
            qualifiers.push(format!("{key}={value}"));
        }
    }

    let base = record
        .scenario
        .clone()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| record.case.clone());
    if qualifiers.is_empty() {
        base
    } else {
        format!("{} [{}]", base, qualifiers.join(", "))
    }
}

fn regex_sweep_context(
    record: &BenchmarkRecord,
    family: &str,
    config: &BenchSummaryConfig,
) -> String {
    let mut qualifiers = Vec::new();
    for key in &config.sweep.context_tag_keys {
        if let Some(value) = record.tags.get(key) {
            qualifiers.push(format!("{key}={value}"));
        }
    }
    if qualifiers.is_empty() {
        family.to_string()
    } else {
        format!("{} [{}]", family, qualifiers.join(", "))
    }
}

fn scenario_family_without_sweep_token(source: &str, matched: &str, fallback: &str) -> String {
    let family = source.replacen(matched, "", 1).trim_matches('_').to_string();
    if family.is_empty() {
        fallback.to_string()
    } else {
        family
    }
}

fn new_group(record: &BenchmarkRecord, parameter: &str, context: &str) -> SweepGroup {
    SweepGroup {
        title: format!(
            "{} / {} / {} ({}, {})",
            record.adapter, record.suite, context, parameter, record.metric
        ),
        metric: record.metric.clone(),
        unit: record.unit.clone(),
        points: Vec::new(),
    }
}

fn group_key(record: &BenchmarkRecord, parameter: &str, context: &str) -> String {
    format!(
        "{}|{}|{}|{}|{}|{}",
        record.adapter, record.suite, record.metric, record.unit, parameter, context
    )
}

fn parse_numeric_token(token: &str) -> Option<f64> {
    let compact: String = token
        .trim()
        .to_lowercase()
        .chars()
        .take_while(|value| value.is_ascii_alphanumeric() || *value == '.')
        .collect();
    if compact.is_empty() {
        return None;
    }

    let (number, factor) = if let Some(stripped) = compact.strip_suffix("kb") {
        (stripped, 1024.0)
    } else if let Some(stripped) = compact.strip_suffix("mb") {
        (stripped, 1024.0 * 1024.0)
    } else if let Some(stripped) = compact.strip_suffix('b') {
        (stripped, 1.0)
    } else if let Some(stripped) = compact.strip_suffix('k') {
        (stripped, 1000.0)
    } else if let Some(stripped) = compact.strip_suffix('m') {
        (stripped, 1_000_000.0)
    } else {
        (compact.as_str(), 1.0)
    };

    number.parse::<f64>().ok().map(|value| value * factor)
}

fn compare_f64(left: f64, right: f64) -> Ordering {
    left.partial_cmp(&right).unwrap_or(Ordering::Equal)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::benchmark_summary::config::BenchSummaryConfig;
    use crate::benchmark_summary::model::{BenchmarkRecord, MetricDirection};

    fn record(
        case: &str,
        scenario: &str,
        value: f64,
        tags: &[(&str, &str)],
    ) -> BenchmarkRecord {
        BenchmarkRecord {
            id: format!("stress|{case}|{scenario}"),
            adapter: "stress".to_string(),
            suite: "tier3-system-rpc".to_string(),
            case: case.to_string(),
            scenario: Some(scenario.to_string()),
            metric: "throughput_ops_per_s".to_string(),
            unit: "ops/s".to_string(),
            value,
            lower_bound: None,
            upper_bound: None,
            samples: Some(5),
            metric_direction: MetricDirection::HigherIsBetter,
            stability: Some("stable".to_string()),
            status: Some("authoritative".to_string()),
            rel_stddev: Some(0.0),
            tags: tags
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect::<BTreeMap<_, _>>(),
            metadata: BTreeMap::new(),
            source_file: "target/stress/test/latest.json".to_string(),
        }
    }

    #[test]
    fn should_detect_sweep_groups_from_subscriber_count_tags() {
        // Arrange
        let config = BenchSummaryConfig::for_tests();
        let records = vec![
            record(
                "notice::fanout_16",
                "fanout_subscriber_scaling",
                1000.0,
                &[
                    ("subscriber_count", "16"),
                    ("measurement_scope", "routed_fanout"),
                    ("match_kind", "single_star"),
                ],
            ),
            record(
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
        let groups = detect_sweep_groups(&records, &config).expect("detect sweep groups");

        // Assert
        assert_eq!(groups.len(), 1);
        assert!(groups[0].title.contains("subscriber_count"));
        assert_eq!(groups[0].points[0].parameter_label, "16");
        assert_eq!(groups[0].points[1].parameter_label, "64");
    }

    #[test]
    fn should_keep_dispatch_and_roundtrip_scaling_sweeps_separate() {
        // Arrange
        let config = BenchSummaryConfig::for_tests();
        let records = vec![
            record("rpc::dispatch_64", "scaling_64_dispatch_only", 1200.0, &[]),
            record("rpc::dispatch_256", "scaling_256_dispatch_only", 900.0, &[]),
            record("rpc::roundtrip_64", "scaling_64_full_roundtrip", 800.0, &[]),
            record("rpc::roundtrip_256", "scaling_256_full_roundtrip", 600.0, &[]),
        ];

        // Act
        let groups = detect_sweep_groups(&records, &config).expect("detect sweep groups");

        // Assert
        assert_eq!(groups.len(), 2);
        assert!(groups
            .iter()
            .any(|group| group.title.contains("dispatch_only")));
        assert!(groups
            .iter()
            .any(|group| group.title.contains("full_roundtrip")));
        assert!(groups.iter().all(|group| group.points.len() == 2));
    }

    #[test]
    fn should_detect_sweep_groups_from_pending_count_tags() {
        // Arrange
        let config = BenchSummaryConfig::for_tests();
        let records = vec![
            record(
                "rpc::pending_64",
                "pending_cardinality_steady_state",
                1200.0,
                &[("pending_count", "64"), ("measurement_scope", "routed_pending")],
            ),
            record(
                "rpc::pending_256",
                "pending_cardinality_steady_state",
                900.0,
                &[("pending_count", "256"), ("measurement_scope", "routed_pending")],
            ),
        ];

        // Act
        let groups = detect_sweep_groups(&records, &config).expect("detect sweep groups");

        // Assert
        assert_eq!(groups.len(), 1);
        assert!(groups[0].title.contains("pending_count"));
        assert_eq!(groups[0].points[0].parameter_label, "64");
        assert_eq!(groups[0].points[1].parameter_label, "256");
    }
}