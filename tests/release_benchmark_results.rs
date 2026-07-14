use std::fs;
use std::path::{Path, PathBuf};

use serde_json::Value;

const RELEASE_ROW_COUNT: usize = 14;

#[test]
#[ignore = "requires a fresh full benchmark summary"]
fn should_accept_current_release_benchmark_results() {
    // Arrange
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let results = read_json(&repo_root.join("target/bench_results.json"));
    let release_ids = read_release_ids(&repo_root.join("config/bench_release_ids.txt"));
    let stress_dir = repo_root.join("target/stress");

    // Act
    let result = validate_release_results(&results, &release_ids, &stress_dir);

    // Assert
    assert!(result.is_ok(), "{}", result.unwrap_err());
}

#[test]
fn should_accept_complete_trustworthy_release_rows() {
    // Arrange
    let release_ids = release_ids();
    let results = release_results(&release_ids);
    let stress_dir = tempfile::tempdir().expect("create stress directory");

    // Act
    let result = validate_release_results(&results, &release_ids, stress_dir.path());

    // Assert
    assert_eq!(result, Ok(()));
}

#[test]
fn should_reject_missing_release_rows() {
    // Arrange
    let release_ids = release_ids();
    let mut results = release_results(&release_ids);
    results["records"]
        .as_array_mut()
        .expect("records array")
        .pop();
    let stress_dir = tempfile::tempdir().expect("create stress directory");

    // Act
    let result = validate_release_results(&results, &release_ids, stress_dir.path());

    // Assert
    assert_eq!(
        result,
        Err("release manifest IDs are missing from the summary".to_owned())
    );
}

#[test]
fn should_reject_noisy_release_rows() {
    // Arrange
    let release_ids = release_ids();
    let mut results = release_results(&release_ids);
    results["records"][0]["metadata"]["quality"] = Value::String("noisy".to_owned());
    let stress_dir = tempfile::tempdir().expect("create stress directory");

    // Act
    let result = validate_release_results(&results, &release_ids, stress_dir.path());

    // Assert
    assert!(result
        .expect_err("noisy row must fail")
        .contains("release-0: quality noisy"));
}

#[test]
fn should_reject_legacy_tier_directories() {
    // Arrange
    let release_ids = release_ids();
    let results = release_results(&release_ids);
    let stress_dir = tempfile::tempdir().expect("create stress directory");
    fs::create_dir(stress_dir.path().join("nested"))
        .and_then(|()| fs::create_dir(stress_dir.path().join("nested/tier-rpc")))
        .expect("create legacy suite directory");

    // Act
    let result = validate_release_results(&results, &release_ids, stress_dir.path());

    // Assert
    assert_eq!(
        result,
        Err("legacy Tier 4 suite IDs found under target/stress".to_owned())
    );
}

fn validate_release_results(
    results: &Value,
    release_ids: &[String],
    stress_dir: &Path,
) -> Result<(), String> {
    if release_ids.len() != RELEASE_ROW_COUNT {
        return Err(format!(
            "the release manifest must contain exactly {RELEASE_ROW_COUNT} primary rows"
        ));
    }
    if contains_legacy_tier_directory(stress_dir)? {
        return Err("legacy Tier 4 suite IDs found under target/stress".to_owned());
    }

    let records = results
        .get("records")
        .and_then(Value::as_array)
        .ok_or_else(|| "benchmark summary is missing its records array".to_owned())?;
    let release_records = records
        .iter()
        .filter(|record| {
            record
                .get("id")
                .and_then(Value::as_str)
                .is_some_and(|id| release_ids.iter().any(|release_id| release_id == id))
        })
        .collect::<Vec<_>>();
    if release_records.len() != release_ids.len() {
        return Err("release manifest IDs are missing from the summary".to_owned());
    }

    let failures = release_records
        .iter()
        .filter_map(|record| release_failure(record))
        .collect::<Vec<_>>();
    if failures.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "release primary rows must pass correctness with acceptable quality:\n{}",
            failures.join("\n")
        ))
    }
}

fn release_failure(record: &Value) -> Option<String> {
    let id = record
        .get("id")
        .and_then(Value::as_str)
        .unwrap_or("<missing id>");
    let metric = record
        .get("metric")
        .and_then(Value::as_str)
        .unwrap_or("<missing>");
    let correctness = record
        .pointer("/metadata/correctness_passed")
        .and_then(Value::as_str)
        .unwrap_or("<missing>");
    let quality = record
        .pointer("/metadata/quality")
        .and_then(Value::as_str)
        .unwrap_or("<missing>");

    if metric != "throughput_ops_per_s" {
        Some(format!("{id}: metric {metric}"))
    } else if correctness != "true" {
        Some(format!("{id}: correctness {correctness}"))
    } else if !matches!(quality, "acceptable" | "excellent") {
        Some(format!("{id}: quality {quality}"))
    } else {
        None
    }
}

fn contains_legacy_tier_directory(directory: &Path) -> Result<bool, String> {
    let entries = fs::read_dir(directory)
        .map_err(|error| format!("failed to read {}: {error}", directory.display()))?;
    for entry in entries {
        let entry = entry.map_err(|error| format!("failed to read directory entry: {error}"))?;
        if !entry
            .file_type()
            .map_err(|error| format!("failed to inspect {}: {error}", entry.path().display()))?
            .is_dir()
        {
            continue;
        }
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with("tierr-") || name.starts_with("tier-") {
            return Ok(true);
        }
        if contains_legacy_tier_directory(&entry.path())? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn read_release_ids(path: &Path) -> Vec<String> {
    fs::read_to_string(path)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()))
        .lines()
        .map(|line| line.split_once('#').map_or(line, |(value, _)| value).trim())
        .filter(|line| !line.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn read_json(path: &Path) -> Value {
    let content = fs::read_to_string(path)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));
    serde_json::from_str(&content)
        .unwrap_or_else(|error| panic!("failed to parse {}: {error}", path.display()))
}

fn release_ids() -> Vec<String> {
    (0..RELEASE_ROW_COUNT)
        .map(|index| format!("release-{index}"))
        .collect()
}

fn release_results(release_ids: &[String]) -> Value {
    let records = release_ids
        .iter()
        .map(|id| {
            serde_json::json!({
                "id": id,
                "metric": "throughput_ops_per_s",
                "metadata": {
                    "correctness_passed": "true",
                    "quality": "acceptable"
                }
            })
        })
        .collect::<Vec<_>>();
    serde_json::json!({ "records": records })
}
