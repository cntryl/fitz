use super::support::{number, positive, require, TestResult};
use serde_json::{json, Value};
use std::collections::BTreeMap;

pub type Rows = BTreeMap<String, Vec<Value>>;
pub type Summaries = BTreeMap<String, Rows>;
pub const CONTROLS: [(&str, &str, &str, &str); 2] = [
    (
        "ws_exact_replay",
        "tier4_stream_gate",
        "should_measure_memory_ws_exact_replay",
        "memory_ws_exact_replay",
    ),
    (
        "max_event",
        "tier4_stream_shapes",
        "should_characterize_disk_sync_write_max_event",
        "disk_sync_write_max_event",
    ),
];

pub fn groups() -> Vec<(String, Option<String>)> {
    let mut groups: Vec<_> = [
        "memory_tcp_append_open_session",
        "memory_ws_append_open_session",
        "memory_tcp_sync_write_lifecycle",
        "memory_ws_sync_write_lifecycle",
        "memory_tcp_exact_replay",
        "memory_ws_exact_replay",
        "local_disk_tcp_sync_write_lifecycle",
        "local_disk_ws_sync_write_lifecycle",
    ]
    .into_iter()
    .map(|name| {
        (
            "tier4_stream_gate".to_owned(),
            Some(format!("should_measure_{name}")),
        )
    })
    .collect();
    groups.push(("tier4_stream_shapes".into(), Some("should_replay_".into())));
    groups.push(("tier4_stream_compacted".into(), None));
    groups.extend(
        ["64b", "1k", "15k", "16k", "17k", "max_event"]
            .into_iter()
            .map(|size| {
                (
                    "tier4_stream_shapes".into(),
                    Some(format!("should_characterize_disk_sync_write_{size}")),
                )
            }),
    );
    groups.push((
        "tier4_stream_shapes".into(),
        Some("should_characterize_hot_resource_append_after_100000_events".into()),
    ));
    groups
}

#[derive(Debug)]
pub struct Capture {
    pub phase: &'static str,
    pub target: String,
    pub pattern: Option<String>,
    pub label: String,
    pub control: Option<(&'static str, &'static str)>,
}

pub fn capture_plan() -> Vec<Capture> {
    let groups = groups();
    let hot = groups.len() - 1;
    let mut plan = Vec::new();
    for index in 0..hot {
        for iteration in 1..=3 {
            let mut adjacent =
                if groups[index].1.as_deref() == Some("should_characterize_disk_sync_write_64b") {
                    vec![index, hot]
                } else {
                    vec![index]
                };
            if iteration % 2 == 0 {
                adjacent.reverse();
            }
            for group_index in adjacent {
                let (target, pattern) = &groups[group_index];
                let phases = if iteration % 2 == 1 {
                    ["before", "after"]
                } else {
                    ["after", "before"]
                };
                for phase in phases {
                    plan.push(Capture {
                        phase,
                        target: target.clone(),
                        pattern: pattern.clone(),
                        label: format!("pair-{group_index}-{iteration}-{phase}"),
                        control: None,
                    });
                }
                if let Some((name, _, _, _)) = CONTROLS
                    .iter()
                    .find(|(_, ct, cp, _)| ct == target && Some(*cp) == pattern.as_deref())
                {
                    let sides = if iteration % 2 == 1 {
                        ["left", "right"]
                    } else {
                        ["right", "left"]
                    };
                    for side in sides {
                        plan.push(Capture {
                            phase: "before",
                            target: target.clone(),
                            pattern: pattern.clone(),
                            label: format!("control-{name}-{iteration}-{side}"),
                            control: Some((name, side)),
                        });
                    }
                }
            }
        }
    }
    plan
}

fn median(rows: &[Value]) -> TestResult<f64> {
    require(rows.len() == 3, "Every row requires three captures")?;
    let mut values = rows
        .iter()
        .map(|row| positive(&row["stats"]["mean"]))
        .collect::<TestResult<Vec<_>>>()?;
    values.sort_by(f64::total_cmp);
    Ok(values[1])
}

fn metric(rows: &Rows, name: &str) -> TestResult<f64> {
    median(rows.get(name).ok_or("Missing benchmark row")?)
}

pub fn control(left: &[Value], right: &[Value], name: &str, hash: &str) -> TestResult<Value> {
    let value = |rows: &[Value], name: &str| {
        median(
            &rows
                .iter()
                .filter(|row| row["name"] == name)
                .cloned()
                .collect::<Vec<_>>(),
        )
    };
    let throughput = value(right, name)? / value(left, name)?;
    let p95 = value(right, &format!("{name}_latency"))? / value(left, &format!("{name}_latency"))?;
    Ok(
        json!({"binary_sha256":hash, "throughput_ratio":throughput, "p95_ratio":p95,
        "stable_within_five_percent":(0.95..=1.05).contains(&throughput) && (0.95..=1.05).contains(&p95)}),
    )
}

pub fn compare(summaries: &Summaries) -> TestResult<(Vec<Value>, Value)> {
    let before = summaries
        .get("before")
        .ok_or("Missing baseline summaries")?;
    let after = summaries
        .get("after")
        .ok_or("Missing candidate summaries")?;
    require(before.keys().eq(after.keys()), "Mismatched workload rows")?;
    let mut records = Vec::new();
    let mut checks = serde_json::Map::new();
    for (name, before_rows) in before {
        if name.ends_with("_latency") {
            continue;
        }
        let bt = median(before_rows)?;
        let at = metric(after, name)?;
        let bp = metric(before, &format!("{name}_latency"))?;
        let ap = metric(after, &format!("{name}_latency"))?;
        let quality = |rows: &[Value]| {
            rows.iter()
                .map(|row| row["quality"].as_str().ok_or("Missing benchmark quality"))
                .collect::<Result<Vec<_>, _>>()
                .map(|values| values.join(","))
        };
        let before_quality = quality(before_rows)?;
        let after_quality = quality(&after[name])?;
        let record = json!({"name":name,"before_ops_s":bt,"after_ops_s":at,"throughput_ratio":at/bt,
            "before_p95_us":bp/1000.0,"after_p95_us":ap/1000.0,"p95_ratio":ap/bp,
            "before_quality":before_quality,"after_quality":after_quality});
        if name.starts_with("memory_") {
            checks.insert(name.clone(), json!(at / bt >= 0.90 && ap / bp <= 1.10));
        }
        records.push(record);
    }
    let row = |name: &str| {
        records
            .iter()
            .find(|row| row["name"] == name)
            .ok_or("Missing required workload")
    };
    let maximum = row("disk_sync_write_max_event")?;
    checks.insert(
        "maximum_valid_event".into(),
        json!(
            number(&maximum["throughput_ratio"])? >= 0.95 && number(&maximum["p95_ratio"])? <= 1.05
        ),
    );
    let hot = row("hot_resource_append_depth_100000")?;
    let empty = row("disk_sync_write_64b")?;
    checks.insert(
        "100k_vs_empty".into(),
        json!(
            number(&hot["after_ops_s"])? >= 0.90 * number(&empty["after_ops_s"])?
                && number(&hot["after_p95_us"])? <= 1.10 * number(&empty["after_p95_us"])?
        ),
    );
    require(
        records.len() == 32 && checks.len() == 8,
        "Require all 32 workloads and eight budgets",
    )?;
    Ok((records, Value::Object(checks)))
}

pub fn verdict(checks: &Value, controls: &Value) -> TestResult<&'static str> {
    let controls = controls.as_object().ok_or("Missing timing controls")?;
    require(
        controls.len() == 2
            && CONTROLS
                .iter()
                .all(|(name, _, _, _)| controls.contains_key(*name)),
        "Both timing controls are mandatory",
    )?;
    if !controls
        .values()
        .all(|control| control["stable_within_five_percent"] == true)
    {
        return Ok("measurement_unstable");
    }
    Ok(
        if checks
            .as_object()
            .ok_or("Missing budgets")?
            .values()
            .all(|check| check == true)
        {
            "passed"
        } else {
            "budget_failure"
        },
    )
}

#[path = "stream_tests.rs"]
mod tests;
