use super::support::{self, TestResult};
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

const MIB: u64 = 1024 * 1024;
const PROVIDER: &str = "ghcr.io/sqrzl/sqrzl-emulator@sha256:bb5bf54895fcb3b9730665b48e0f7b602791c61c3d18e5e7a060a4dce5c90964";

fn unsigned(value: &Value) -> TestResult<u64> {
    value
        .as_u64()
        .ok_or_else(|| "Missing unsigned evidence field".into())
}
fn maximum(value: &Value, maximum: f64) -> TestResult {
    let number = support::number(value)?;
    support::require(
        (0.0..=maximum).contains(&number),
        "Acceptance deadline or limit exceeded",
    )
}

fn floor(data: &Value, source: &str) -> TestResult {
    support::require(data["source_sha"] == source, "Wrong native floor source")?;
    support::require(
        data["accepted_and_verified_acked"] == 16384 && data["remaining"] == 0,
        "Incorrect floor ACK accounting",
    )?;
    support::require(
        data["all_seven_domains_before_and_after"] == true,
        "Missing seven-domain verification",
    )?;
    maximum(&data["seconds"], 600.0)?;
    maximum(&data["maximum_health_millis"], 10000.0)
}

fn large(data: &Value) -> TestResult {
    support::require(
        data["synthetic_offline_wal"] == true
            && unsigned(&data["backlog"]["remote_wal_bytes"])? >= 640 * MIB,
        "Missing 640 MiB offline WAL fixture",
    )?;
    maximum(&data["ready_seconds"], 180.0)?;
    support::require(
        data["exact_kv_values_verified"] == 40960 && unsigned(&data["wal_bytes"])? >= 640 * MIB,
        "Incomplete large WAL recovery",
    )
}

fn restart(data: &Value, cold: bool) -> TestResult {
    support::require(
        data["queue_persistence"] == "best_effort"
            && data["fixture_checkpoint"] == "orderly_shutdown_before_crash",
        "Wrong Queue recovery contract",
    )?;
    support::require(
        data["accepted_payload_bytes"] == 4096 * 16 * 1024 && data["cold_cache"] == cold,
        "Wrong restart fixture",
    )?;
    maximum(&data["crash_to_ready_seconds"], 180.0)?;
    maximum(&data["restart_and_verify_seconds"], 1200.0)?;
    support::require(
        data["accepted_and_verified_acked"] == 4096 && data["remaining"] == 0,
        "Incomplete restarted Queue drain",
    )
}

fn retention(data: &Value) -> TestResult {
    support::require(
        unsigned(&data["historical_wal_bytes"])? >= 64 * MIB
            && unsigned(&data["historical_backlog"]["remote_wal_bytes"])? >= 64 * MIB,
        "Missing 64 MiB historical WAL fixture",
    )?;
    support::require(
        data["payload_bytes_written"] == 16384 * 16 * 1024 && data["wal_limit_bytes"] == 128 * MIB,
        "Wrong retention fixture or WAL cap",
    )?;
    support::require(
        data["accepted_and_verified_acked"] == 16384
            && data["historical_kv_values_verified"] == 4096
            && data["remaining"] == 0,
        "Incomplete retention accounting or historical KV verification",
    )?;
    maximum(&data["maximum_health_millis"], 10000.0)?;
    let final_state = &data["final"];
    for state in std::iter::once(final_state).chain(
        data["samples"]
            .as_array()
            .ok_or("Missing retention samples")?,
    ) {
        support::require(
            unsigned(&state["catalog_bytes"])? <= 128 * MIB
                && unsigned(&state["remote_wal_bytes"])? <= 128 * MIB,
            "WAL or catalog exceeded retention cap",
        )?;
    }
    support::require(
        unsigned(&final_state["sst_objects"])? > 0,
        "Missing persisted SST objects",
    )?;
    let original = data["original_segments"]
        .as_array()
        .ok_or("Missing original WAL segment IDs")?;
    let final_ids = final_state["segment_ids"]
        .as_array()
        .ok_or("Missing final WAL segment IDs")?;
    support::require(
        !original.is_empty() && original.iter().all(|segment| !final_ids.contains(segment)),
        "Historical WAL segments remain",
    )
}

fn caps(snapshot: &Value) -> TestResult<String> {
    let limits = &snapshot["HostConfig"];
    support::require(
        limits["NanoCpus"] == 250_000_000
            && limits["Memory"] == 512 * MIB
            && limits["MemorySwap"] == 512 * MIB,
        "Changed CPU, memory or swap caps",
    )?;
    support::require(
        snapshot["State"]["OOMKilled"] == false,
        "Container OOM or missing state evidence",
    )?;
    Ok(snapshot["Image"]
        .as_str()
        .ok_or("Missing container image ID")?
        .into())
}

fn platform(root: &Path, arch: &str, source: &str) -> TestResult<Value> {
    let floor_root = root.join(format!("floor-{arch}"));
    let s3_root = root.join(format!("s3-{arch}"));
    let floor_data = support::load(&floor_root.join("report.json"))?;
    floor(&floor_data, source)?;
    let mut snapshots = vec![floor_data["before"].clone(), floor_data["after"].clone()];
    let reports = support::files(&s3_root, "json")?
        .into_iter()
        .filter(|file| file.file_name().is_some_and(|name| name == "report.json"))
        .collect::<Vec<_>>();
    support::require(reports.len() == 4, "Require all four S3 test reports")?;
    let mut seen = BTreeSet::new();
    let mut cases = Vec::new();
    for path in reports {
        let data = support::load(&path)?;
        support::require(
            data["source_sha"] == source && data["provider_image"] == PROVIDER,
            "Wrong S3 test source or provider",
        )?;
        let name = path
            .parent()
            .and_then(Path::file_name)
            .and_then(|name| name.to_str())
            .ok_or("Invalid S3 report directory")?;
        let kind = name.split('-').next().ok_or("Missing case kind")?;
        support::require(seen.insert(kind.to_owned()), "Duplicate S3 test case")?;
        let measurements = &data["measurements"];
        match kind {
            "large" => large(measurements)?,
            "warm" => restart(measurements, false)?,
            "cold" => restart(measurements, true)?,
            "retention" => retention(measurements)?,
            _ => return Err("Unknown S3 test case".into()),
        }
        for key in if kind == "large" {
            vec!["after"]
        } else {
            vec!["before", "after"]
        } {
            snapshots.push(
                measurements
                    .get(key)
                    .ok_or("Missing resource cap snapshot")?
                    .clone(),
            );
        }
        let mut case = json!({"kind":kind});
        for key in [
            "ready_seconds",
            "crash_to_ready_seconds",
            "restart_and_verify_seconds",
            "accepted_and_verified_acked",
            "remaining",
            "exact_kv_values_verified",
            "wal_bytes",
            "historical_kv_values_verified",
            "health_samples",
            "maximum_health_millis",
        ] {
            if let Some(value) = measurements.get(key) {
                case[key] = value.clone();
            }
        }
        if kind == "retention" {
            case["final_wal_bytes"] = measurements["final"]["remote_wal_bytes"].clone();
            case["final_catalog_bytes"] = measurements["final"]["catalog_bytes"].clone();
            case["all_historical_segments_retired"] = json!(true);
        }
        cases.push(case);
    }
    support::require(
        seen == ["large", "warm", "cold", "retention"]
            .into_iter()
            .map(str::to_owned)
            .collect(),
        "Missing declared S3 case",
    )?;
    let provider = support::load(&s3_root.join("provider.json"))?;
    support::require(
        provider[0]["Config"]["Image"] == PROVIDER && provider[0]["State"]["OOMKilled"] == false,
        "Provider identity or OOM evidence failed",
    )?;
    let images = snapshots
        .iter()
        .map(caps)
        .collect::<TestResult<BTreeSet<_>>>()?;
    support::require(
        images.len() == 1 && snapshots.len() == 9,
        "Native test image or cap evidence incomplete",
    )?;
    for directory in [&floor_root, &s3_root] {
        verify_logs(directory)?;
    }
    Ok(
        json!({"arch":arch,"floor_seconds":floor_data["seconds"],"floor_health_max_ms":floor_data["maximum_health_millis"],"cases":cases,"provider_image":PROVIDER,"image_ids":images,"container_snapshots_checked":snapshots.len(),"panic_markers":0}),
    )
}

fn verify_logs(directory: &Path) -> TestResult {
    for path in support::files(directory, "log")? {
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or("Invalid log filename")?;
        if ["broker.log", "provider.log", "campaign.log"].contains(&name)
            || name.contains("-broker-")
        {
            let body = String::from_utf8_lossy(&fs::read(&path)?).into_owned();
            support::require(
                !body.lines().any(|line| {
                    line.contains("panicked at")
                        || (line.contains("thread ") && line.contains(" panicked"))
                        || line.contains("fatal runtime error")
                        || (line.contains("memory allocation of ") && line.contains(" failed"))
                }),
                "Native test logs contain a panic or allocation failure",
            )?;
        }
    }
    Ok(())
}

#[test]
#[ignore = "Requires downloaded native CI artifacts and exact source/run identity"]
fn should_verify_native_runtime_evidence() -> TestResult {
    // Arrange
    let root = std::path::PathBuf::from(support::variable("FITZ_NATIVE_EVIDENCE_ROOT")?);
    let source = support::variable("FITZ_NATIVE_SOURCE_SHA")?;
    let run_id = support::variable("FITZ_NATIVE_RUN_ID")?;
    let run: Value = serde_json::from_slice(&support::output(
        &support::root(),
        &support::Environment::new(),
        &[
            "gh",
            "api",
            &format!("repos/cntryl/fitz/actions/runs/{run_id}"),
        ],
    )?)?;
    support::require(
        run["head_sha"] == source && run["conclusion"] == "success",
        "Native CI run source or conclusion mismatch",
    )?;
    // Act
    let platforms = ["amd64", "arm64"]
        .into_iter()
        .map(|arch| platform(&root, arch, &source))
        .collect::<TestResult<Vec<_>>>()?;
    // Assert
    assert_eq!(platforms.len(), 2);
    support::save(
        &root.join("rust-native-proof.json"),
        &json!({"source_sha":source,"ci_run":run_id.parse::<u64>()?,"platforms":platforms}),
    )
}

#[test]
fn should_reject_changed_resource_caps() {
    // Arrange
    let snapshot = json!({"HostConfig":{"NanoCpus":500_000_000,"Memory":512*MIB,"MemorySwap":512*MIB},"State":{"OOMKilled":false},"Image":"image"});
    // Act
    let result = caps(&snapshot);
    // Assert
    assert!(result.is_err());
}

#[test]
fn should_reject_uncheckpointed_queue_recovery_claims() {
    // Arrange
    let report = json!({"queue_persistence":"best_effort","fixture_checkpoint":"unflushed","accepted_payload_bytes":4096*16*1024,"cold_cache":false,"crash_to_ready_seconds":1,"restart_and_verify_seconds":2,"accepted_and_verified_acked":4096,"remaining":0});
    // Act
    let result = restart(&report, false);
    // Assert
    assert!(result.is_err());
}
