use super::support::{number, positive, require, TestResult};
use serde_json::{json, Value};
use std::collections::BTreeMap;

pub const WINDOW_SECONDS: u64 = 120;
pub const BACKLOGS: [u64; 3] = [10_000, 25_000, 100_000];

pub fn offered_rate(backlog: u64) -> u64 {
    backlog * 4 / (5 * WINDOW_SECONDS)
}

fn count(value: &Value) -> TestResult<u64> {
    value
        .as_u64()
        .ok_or_else(|| "Missing unsigned accounting value".into())
}

pub fn p99_upper_ns(distribution: &Value) -> TestResult<u64> {
    let target = (u128::from(count(&distribution["count"])?) * 99).div_ceil(100);
    if target == 0 {
        return Ok(0);
    }
    let mut seen = 0_u128;
    for (index, value) in distribution["bins"]
        .as_array()
        .ok_or("Missing latency bins")?
        .iter()
        .enumerate()
    {
        seen += u128::from(count(value)?);
        if seen >= target {
            return Ok(1_u64.checked_shl(u32::try_from(index)?).unwrap_or(u64::MAX));
        }
    }
    count(&distribution["max_ns"])
}

pub fn metrics(stage: &Value) -> TestResult<Value> {
    Ok(
        json!({"accepted_per_second":number(&stage["accounting"]["accepted"])? * 1e9 / positive(&stage["active_elapsed_ns"])?,
        "drain_seconds":number(&stage["drain_elapsed_ns"])? / 1e9,
        "ack_p99_ns":p99_upper_ns(&stage["consumer_timing"]["ack"]["distribution"])?,
        "cycle_p99_ns":p99_upper_ns(&stage["consumer_timing"]["cycle"]["distribution"])?}),
    )
}

pub fn compare(before: &Value, after: &Value) -> TestResult<Vec<Value>> {
    [("accepted_per_second", true, 0.90, 0.0), ("drain_seconds", false, 1.10, 1.0),
        ("ack_p99_ns", false, 1.10, 0.0), ("cycle_p99_ns", false, 1.10, 0.0)]
        .into_iter().map(|(metric, higher, ratio, slack)| {
            let before = number(&before[metric])?;
            let after = number(&after[metric])?;
            require(before >= 0.0 && after >= 0.0, "Negative performance metric")?;
            let limit = if higher { before * ratio } else { (before * ratio).max(before + slack) };
            Ok(json!({"metric":metric,"before":before,"after":after,"limit":limit,"passed":if higher { after >= limit } else { after <= limit }}))
        }).collect()
}

pub fn validate(report: &Value, head: &str, backlog: u64, finite: bool) -> TestResult {
    let rate = if finite { 16000 } else { offered_rate(backlog) };
    require(
        report["source_sha"] == head && report["source_dirty"] == false,
        "Require exact clean source",
    )?;
    require(
        report["comparison_label"] == "corrected_pacing"
            && report["pacing_implementation"]
                == "dedicated_sleep_worker_capacity_one_monotonic_deadline",
        "Require corrected pacing",
    )?;
    require(
        report["config"]
            == json!({"rates":[rate],"stage_seconds":WINDOW_SECONDS,"producer_connections":256,
        "consumer_delay_ms":5,"max_backlog":backlog,"max_attempts_per_stage":500_000,"drain_seconds":600}),
        "Changed Queue workload or deadline",
    )?;
    require(
        report["status"] == "passed"
            && report["cleanup_status"] == "completed"
            && report["recovery_probe_passed"] == true,
        "Require drain, cleanup and recovery success",
    )?;
    let stages = report["stages"].as_array().ok_or("Missing stages")?;
    require(stages.len() == 1, "Exactly one pressure stage is required")?;
    let stage = &stages[0];
    require(
        stage["drained"] == true
            && stage["empty_verified"] == true
            && stage.get("cleanup_failure") == Some(&Value::Null),
        "Require verified empty drain and successful cleanup",
    )?;
    let counts = &stage["accounting"];
    let accepted = count(&counts["accepted"])?;
    let window = stage["termination"] == "configured_window";
    let arrivals_met = || -> TestResult<bool> {
        Ok(u128::from(count(&stage["harness_missed"])?) * 100
            <= u128::from(count(&stage["offered"])?))
    };
    if finite {
        require(
            [
                "configured_window",
                "backlog_safety_guard",
                "accounting_safety_guard",
                "broker_enqueue_rejection",
            ]
            .iter()
            .any(|reason| stage["termination"] == *reason),
            "Invalid finite termination",
        )?;
        require(
            accepted >= backlog,
            "Early stop below intended message envelope",
        )?;
        if window {
            require(arrivals_met()?, "Offered arrivals were not met")?;
        }
    } else {
        require(
            window && stage["configured_window_completed"] == true,
            "Only a completed configured window qualifies",
        )?;
        require(arrivals_met()?, "Offered arrivals were not met")?;
        require(
            u128::from(accepted) * 100 >= u128::from(rate) * u128::from(WINDOW_SECONDS) * 99,
            "Accepted work below configured window envelope",
        )?;
    }
    require(
        accepted == count(&counts["acknowledged"])?
            && count(&counts["accepted_unacknowledged"])? == 0,
        "Unreconciled accepted work",
    )?;
    require(
        u128::from(count(&counts["sent"])?)
            == u128::from(accepted) + u128::from(count(&counts["rejected"])?),
        "Unknown ENQUEUE outcomes",
    )?;
    for name in ["ack", "pause", "worker_pause", "handoff", "cycle"] {
        require(
            count(&stage["consumer_timing"][name]["distribution"]["count"])? == accepted,
            "Missing per-ACK timing",
        )?;
    }
    for name in ["pause", "worker_pause"] {
        require(
            count(&stage["consumer_timing"][name]["min_ns"])? >= 5_000_000,
            "Pause below minimum",
        )?;
    }
    Ok(())
}

pub fn capture_plan() -> Vec<(&'static str, u64, bool, String)> {
    let mut result = Vec::new();
    for finite in [false, true] {
        for (index, backlog) in BACKLOGS.into_iter().enumerate() {
            for phase in if index % 2 == 0 {
                ["before", "after"]
            } else {
                ["after", "before"]
            } {
                result.push((
                    phase,
                    backlog,
                    finite,
                    format!(
                        "{}pressure-{backlog}-{phase}",
                        if finite { "finite-" } else { "" }
                    ),
                ));
            }
        }
    }
    result
}

pub fn comparisons(captures: &[Value]) -> TestResult<Value> {
    let by_label: BTreeMap<_, _> = captures
        .iter()
        .filter_map(|row| row["label"].as_str().map(|label| (label, row)))
        .collect();
    require(
        by_label.len() == 12 && captures.len() == 12,
        "All twelve distinct captures are required",
    )?;
    let mut result = serde_json::Map::new();
    for scope in ["", "finite-"] {
        for backlog in BACKLOGS {
            let pair = format!("{scope}pressure-{backlog}");
            let before = by_label
                .get(format!("{pair}-before").as_str())
                .ok_or("Missing baseline capture")?;
            let after = by_label
                .get(format!("{pair}-after").as_str())
                .ok_or("Missing candidate capture")?;
            let rows = if before["metrics"].is_object() && after["metrics"].is_object() {
                compare(&before["metrics"], &after["metrics"])?
            } else {
                Vec::new()
            };
            result.insert(pair, json!(rows));
        }
    }
    Ok(Value::Object(result))
}

pub fn accepted(captures: &[Value], comparisons: &Value) -> bool {
    captures.iter().all(|row| row["status"] == "drain_passed")
        && comparisons.as_object().is_some_and(|pairs| {
            pairs.len() == 6
                && pairs.values().all(|rows| {
                    rows.as_array().is_some_and(|rows| {
                        rows.len() == 4 && rows.iter().all(|row| row["passed"] == true)
                    })
                })
        })
}

#[path = "queue_tests.rs"]
mod tests;
