use super::*;
use std::collections::BTreeSet;

fn summaries() -> Summaries {
    let mut names: Vec<_> = groups()
        .into_iter()
        .filter(|(target, _)| target == "tier4_stream_gate")
        .map(|(_, pattern)| {
            pattern
                .unwrap()
                .trim_start_matches("should_measure_")
                .to_owned()
        })
        .collect();
    names.extend(
        [
            "disk_sync_write_max_event",
            "hot_resource_append_depth_100000",
            "disk_sync_write_64b",
        ]
        .map(str::to_owned),
    );
    names.extend((names.len()..32).map(|index| format!("characterization_{index}")));
    ["before", "after"]
        .into_iter()
        .map(|phase| {
            (
                phase.into(),
                names
                    .iter()
                    .flat_map(|name| {
                        ["", "_latency"].map(|suffix| {
                            (
                                format!("{name}{suffix}"),
                                vec![json!({"stats":{"mean":100},"quality":"noisy"}); 3],
                            )
                        })
                    })
                    .collect(),
            )
        })
        .collect()
}

fn changed_budget(row: &str, suffix: &str, value: u64, budget: &str) -> Value {
    let mut summaries = summaries();
    summaries.get_mut("after").unwrap().insert(
        format!("{row}{suffix}"),
        vec![json!({"stats":{"mean":value},"quality":"noisy"}); 3],
    );
    compare(&summaries).unwrap().1[budget].clone()
}

#[test]
fn should_pair_every_transport_workload_separately() {
    // Arrange
    let source =
        std::fs::read_to_string(super::super::support::root().join("benches/tier4_stream_gate.rs"))
            .unwrap();
    // Act
    let selectors: BTreeSet<_> = groups()
        .into_iter()
        .filter(|(target, _)| target == "tier4_stream_gate")
        .map(|(_, pattern)| pattern.unwrap())
        .collect();
    // Assert
    assert_eq!(selectors.len(), 8);
    assert!(selectors
        .iter()
        .all(|selector| source.contains(&format!("fn {selector}("))));
}

#[test]
fn should_preserve_all_original_stream_budgets() {
    // Arrange
    let summaries = summaries();
    // Act
    let (rows, checks) = compare(&summaries).unwrap();
    // Assert
    assert_eq!(rows.len(), 32);
    assert_eq!(checks.as_object().unwrap().len(), 8);
    assert!(checks
        .as_object()
        .unwrap()
        .values()
        .all(|passed| passed == true));
    assert!(rows
        .iter()
        .all(|row| row["before_quality"] == "noisy,noisy,noisy"));
}

#[test]
fn should_reject_missing_benchmark_quality() {
    // Arrange
    let mut summaries = summaries();
    summaries
        .get_mut("after")
        .unwrap()
        .get_mut("memory_ws_exact_replay")
        .unwrap()[0]
        .as_object_mut()
        .unwrap()
        .remove("quality");
    // Act
    let result = compare(&summaries);
    // Assert
    assert!(result.is_err());
}

#[test]
fn should_reject_transport_throughput_regression() {
    // Arrange
    let row = "memory_ws_exact_replay";
    // Act
    let passed = changed_budget(row, "", 89, row);
    // Assert
    assert_eq!(passed, false);
}
#[test]
fn should_reject_transport_latency_regression() {
    // Arrange
    let row = "memory_ws_exact_replay";
    // Act
    let passed = changed_budget(row, "_latency", 111, row);
    // Assert
    assert_eq!(passed, false);
}
#[test]
fn should_reject_maximum_event_throughput_regression() {
    // Arrange
    let row = "disk_sync_write_max_event";
    // Act
    let passed = changed_budget(row, "", 94, "maximum_valid_event");
    // Assert
    assert_eq!(passed, false);
}
#[test]
fn should_reject_maximum_event_latency_regression() {
    // Arrange
    let row = "disk_sync_write_max_event";
    // Act
    let passed = changed_budget(row, "_latency", 106, "maximum_valid_event");
    // Assert
    assert_eq!(passed, false);
}
#[test]
fn should_reject_history_throughput_regression() {
    // Arrange
    let row = "hot_resource_append_depth_100000";
    // Act
    let passed = changed_budget(row, "", 89, "100k_vs_empty");
    // Assert
    assert_eq!(passed, false);
}
#[test]
fn should_reject_history_latency_regression() {
    // Arrange
    let row = "hot_resource_append_depth_100000";
    // Act
    let passed = changed_budget(row, "_latency", 111, "100k_vs_empty");
    // Assert
    assert_eq!(passed, false);
}

fn controls(stable: bool) -> Value {
    CONTROLS
        .into_iter()
        .map(|(name, _, _, _)| {
            (
                name.to_owned(),
                json!({"stable_within_five_percent":stable}),
            )
        })
        .collect()
}

#[test]
fn should_distinguish_budget_failure_from_unstable_timing() {
    // Arrange
    let controls = controls(true);
    // Act
    let result = verdict(&json!({"gate":false}), &controls).unwrap();
    // Assert
    assert_eq!(result, "budget_failure");
}
#[test]
fn should_reject_unstable_controls_despite_passing_budgets() {
    // Arrange
    let controls = controls(false);
    // Act
    let result = verdict(&json!({"gate":true}), &controls).unwrap();
    // Assert
    assert_eq!(result, "measurement_unstable");
}
#[test]
fn should_reject_a_missing_timing_control() {
    // Arrange
    let controls = json!({"max_event":{"stable_within_five_percent":true}});
    // Act
    let result = verdict(&json!({"gate":true}), &controls);
    // Assert
    assert!(result.is_err());
}
#[test]
fn should_reject_missing_control_captures() {
    assert!(control(&[], &[], "row", "hash").is_err());
}

fn control_rows(throughput: f64, p95: f64) -> Vec<Value> {
    (0..3)
        .flat_map(|_| {
            [
                json!({"name":"row","stats":{"mean":throughput}}),
                json!({"name":"row_latency","stats":{"mean":p95}}),
            ]
        })
        .collect()
}

#[test]
fn should_reject_control_throughput_outside_five_percent() {
    // Arrange
    let left = control_rows(1.0, 1.0);
    let right = control_rows(0.94, 1.0);
    // Act
    let result = control(&left, &right, "row", "hash").unwrap();
    // Assert
    assert_eq!(result["stable_within_five_percent"], false);
}
#[test]
fn should_reject_control_latency_outside_five_percent() {
    // Arrange
    let left = control_rows(1.0, 1.0);
    let right = control_rows(1.0, 1.06);
    // Act
    let result = control(&left, &right, "row", "hash").unwrap();
    // Assert
    assert_eq!(result["stable_within_five_percent"], false);
}
#[test]
fn should_accept_stable_identical_binary_controls() {
    // Arrange
    let rows = control_rows(1.0, 1.0);
    // Act
    let result = control(&rows, &rows, "row", "hash").unwrap();
    // Assert
    assert_eq!(result["stable_within_five_percent"], true);
}

#[test]
fn should_keep_history_scaling_trials_adjacent() {
    // Arrange
    let plan = capture_plan();
    let history = [
        "should_characterize_disk_sync_write_64b",
        "should_characterize_hot_resource_append_after_100000_events",
    ];
    // Act
    let positions: Vec<_> = (1..=3)
        .map(|iteration| {
            plan.iter()
                .enumerate()
                .filter(|(_, capture)| {
                    capture.control.is_none()
                        && capture.label.split('-').nth(2) == Some(iteration.to_string().as_str())
                        && capture
                            .pattern
                            .as_deref()
                            .is_some_and(|pattern| history.contains(&pattern))
                })
                .map(|(index, _)| index)
                .collect::<Vec<_>>()
        })
        .collect();
    // Assert
    for (index, positions) in positions.iter().enumerate() {
        assert_eq!(positions.len(), 4);
        assert_eq!(positions[3] - positions[0], 3);
        assert_eq!(
            plan[positions[0]].pattern.as_deref(),
            Some(history[index % 2])
        );
    }
}

#[test]
fn should_capture_every_shape_three_times_per_source() {
    // Arrange
    let mut counts = BTreeMap::new();
    // Act
    for capture in capture_plan()
        .into_iter()
        .filter(|capture| capture.control.is_none())
    {
        *counts
            .entry((capture.phase, capture.target, capture.pattern))
            .or_insert(0) += 1;
    }
    // Assert
    assert_eq!(counts.len(), groups().len() * 2);
    assert!(counts.values().all(|count| *count == 3));
}

#[test]
fn should_reject_zero_baseline_measurements() {
    // Arrange
    let mut data = summaries();
    data.get_mut("before").unwrap().insert(
        "memory_ws_exact_replay".into(),
        vec![json!({"stats":{"mean":0},"quality":"noisy"}); 3],
    );
    // Act
    let result = compare(&data);
    // Assert
    assert!(result.is_err());
}
