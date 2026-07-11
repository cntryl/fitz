use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use serde_json::Value;

#[test]
fn should_keep_perf_targets_resolvable_to_benchmark_baseline_rows() {
    // Arrange
    let repo_root = repo_root();
    let baseline_ids = baseline_record_ids(&repo_root);
    let target_entries = perf_target_entries(&repo_root);

    // Act
    let missing_targets = target_entries
        .iter()
        .filter(|(_, benchmark_id)| !baseline_ids.contains(*benchmark_id))
        .map(|(key, benchmark_id)| format!("{key}: {benchmark_id}"))
        .collect::<Vec<_>>();

    // Assert
    assert!(
        missing_targets.is_empty(),
        "perf target benchmark_id entries missing from config/bench_baseline.json:\n{}",
        missing_targets.join("\n")
    );
}

#[test]
fn should_keep_release_benchmark_ids_resolvable_to_baseline_rows() {
    // Arrange
    let repo_root = repo_root();
    let baseline_ids = baseline_record_ids(&repo_root);
    let release_ids = release_benchmark_ids(&repo_root);

    // Act
    let missing_release_ids = release_ids
        .iter()
        .filter(|benchmark_id| !baseline_ids.contains(*benchmark_id))
        .cloned()
        .collect::<Vec<_>>();

    // Assert
    assert!(
        missing_release_ids.is_empty(),
        "config/bench_release_ids.txt entries missing from config/bench_baseline.json:\n{}",
        missing_release_ids.join("\n")
    );
}

#[test]
fn should_keep_exactly_fourteen_primary_tier4_rows_in_the_release_manifest() {
    // Arrange
    let repo_root = repo_root();
    let release_ids = release_benchmark_ids(&repo_root);

    // Act
    let invalid_rows = release_ids
        .iter()
        .filter(|id| !id.contains("|throughput_ops_per_s|") || id.contains("_latency"))
        .collect::<Vec<_>>();

    // Assert
    assert_eq!(release_ids.len(), 14, "release manifest row count");
    assert!(
        invalid_rows.is_empty(),
        "release manifest must contain primary throughput rows only:\n{}",
        invalid_rows
            .iter()
            .map(|id| id.as_str())
            .collect::<Vec<_>>()
            .join("\n")
    );
}

#[test]
fn should_keep_latency_records_out_of_perf_targets() {
    // Arrange
    let repo_root = repo_root();
    let targets = perf_target_entries(&repo_root);

    // Act
    let latency_targets = targets
        .into_iter()
        .filter(|(_, benchmark_id)| benchmark_id.contains("_latency"))
        .map(|(key, benchmark_id)| format!("{key}: {benchmark_id}"))
        .collect::<Vec<_>>();

    // Assert
    assert!(
        latency_targets.is_empty(),
        "latency records are report-only and cannot become perf targets:\n{}",
        latency_targets.join("\n")
    );
}

#[test]
fn should_omit_unsupported_direct_lease_rows_from_the_transport_gate() {
    // Arrange
    let repo_root = repo_root();
    let targets = perf_targets(&repo_root);

    // Act
    let direct_gate_rows = targets
        .iter()
        .filter(|(key, _)| key.starts_with("stress:tier4-lease-gate|"))
        .filter(|(_, target)| target_str(target, "layer") == "direct")
        .collect::<Vec<_>>();

    // Assert
    assert!(
        direct_gate_rows.is_empty(),
        "Lease transport gates must not claim an unsupported direct transport row"
    );
}

#[test]
fn should_label_tier4_rpc_targets_with_completion_semantics() {
    // Arrange
    let repo_root = repo_root();
    let targets = perf_targets(&repo_root);

    // Act
    let missing_labels = targets
        .iter()
        .filter(|(key, _)| key.starts_with("stress:tier4-rpc-"))
        .filter_map(|(key, target)| {
            let missing = missing_fields(
                target,
                &[
                    "completion_mode",
                    "completed_unit",
                    "inflight_per_client",
                    "worker_count",
                ],
            );
            (!missing.is_empty()).then(|| format!("{key}: {}", missing.join(", ")))
        })
        .collect::<Vec<_>>();

    // Assert
    assert!(
        missing_labels.is_empty(),
        "tier RPC perf targets must label completion semantics:\n{}",
        missing_labels.join("\n")
    );
}

#[test]
fn should_label_tier4_notice_targets_with_completion_semantics() {
    // Arrange
    let repo_root = repo_root();
    let targets = perf_targets(&repo_root);

    // Act
    let missing_labels = targets
        .iter()
        .filter(|(key, _)| key.starts_with("stress:tier4-notice-"))
        .filter_map(|(key, target)| {
            let missing = missing_fields(
                target,
                &[
                    "completion_mode",
                    "completed_unit",
                    "publisher_count",
                    "subscriber_count",
                ],
            );
            (!missing.is_empty()).then(|| format!("{key}: {}", missing.join(", ")))
        })
        .collect::<Vec<_>>();

    // Assert
    assert!(
        missing_labels.is_empty(),
        "tier Notice perf targets must label completion semantics:\n{}",
        missing_labels.join("\n")
    );
}

#[test]
fn should_keep_unstable_rpc_pipelined_tcp_row_out_of_release_ids() {
    // Arrange
    let repo_root = repo_root();
    let release_ids = release_benchmark_ids(&repo_root);

    // Act
    let promoted = release_ids.iter().any(|id| {
        id.contains("tier4-rpc-pipeline")
            || id.contains("should_characterize_tcp_32_inflight_pipeline")
    });

    // Assert
    assert!(
        !promoted,
        "tcp multiclient pipelined RPC row is variance-gated and must not be release-gated"
    );
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn baseline_record_ids(repo_root: &Path) -> BTreeSet<String> {
    let baseline = read_json(repo_root, "config/bench_baseline.json");
    baseline
        .get("records")
        .and_then(Value::as_array)
        .expect("bench baseline records array")
        .iter()
        .map(|record| target_str(record, "id").to_owned())
        .collect()
}

fn perf_target_entries(repo_root: &Path) -> BTreeMap<String, String> {
    perf_targets(repo_root)
        .iter()
        .map(|(key, target)| (key.clone(), target_str(target, "benchmark_id").to_owned()))
        .collect()
}

fn perf_targets(repo_root: &Path) -> BTreeMap<String, Value> {
    let targets = read_json(repo_root, "config/perf_targets.json");
    targets
        .get("targets")
        .and_then(Value::as_object)
        .expect("perf targets object")
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

fn release_benchmark_ids(repo_root: &Path) -> Vec<String> {
    fs::read_to_string(repo_root.join("config/bench_release_ids.txt"))
        .expect("read config/bench_release_ids.txt")
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(ToOwned::to_owned)
        .collect()
}

fn read_json(repo_root: &Path, relative_path: &str) -> Value {
    let path = repo_root.join(relative_path);
    let content = fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));
    serde_json::from_str(&content)
        .unwrap_or_else(|error| panic!("failed to parse {}: {error}", path.display()))
}

fn target_str<'a>(value: &'a Value, field: &str) -> &'a str {
    value
        .get(field)
        .and_then(Value::as_str)
        .unwrap_or_else(|| panic!("missing string field {field} in {value}"))
}

fn missing_fields(value: &Value, fields: &[&str]) -> Vec<String> {
    fields
        .iter()
        .filter(|field| value.get(**field).is_none())
        .map(|field| (*field).to_string())
        .collect()
}
