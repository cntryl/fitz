use super::*;
use serde_json::Value;
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

pub(super) fn load_throughput_evidence() -> Vec<ThroughputEvidence> {
    let mut rows = BTreeMap::<String, ThroughputEvidence>::new();
    load_bench_results_json(&mut rows);
    for suite in ["tier3-system-stream", "tier4-integration-stream"] {
        load_stress_latest_json(&mut rows, suite);
    }
    rows.into_values()
        .filter(|row| {
            row.scenario == "sustained_append"
                || row.scenario == "batch_write"
                || row.scenario == "multiarea_writes"
                || row.scenario == "append"
        })
        .collect()
}

pub(super) fn event_sourcing_capacity_answer(rows: &[ThroughputEvidence]) -> String {
    let routed = max_ops(rows, |row| {
        row.suite.contains("tier3")
            && row.suite.contains("stream")
            && row.scenario == "sustained_append"
    });
    let batched = max_ops(rows, |row| {
        row.suite.contains("tier3") && row.suite.contains("stream") && row.scenario == "batch_write"
    });

    match (routed, batched) {
        (Some(routed), Some(batched)) if routed >= 100_000.0 && batched >= 1_000_000.0 => {
            "yes".to_string()
        }
        (Some(_), Some(_)) => "no".to_string(),
        _ => "unknown: run tier3_system_stream first".to_string(),
    }
}

fn load_bench_results_json(rows: &mut BTreeMap<String, ThroughputEvidence>) {
    let path = Path::new("target/bench_results.json");
    let Ok(contents) = fs::read_to_string(path) else {
        return;
    };
    let Ok(json) = serde_json::from_str::<Value>(&contents) else {
        return;
    };
    let Some(records) = json.get("records").and_then(Value::as_array) else {
        return;
    };
    for record in records {
        let Some(value) = record.get("value").and_then(Value::as_f64) else {
            continue;
        };
        let suite = record
            .get("suite")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let tags = record.get("tags").unwrap_or(&Value::Null);
        let scenario = tags
            .get("scenario")
            .or_else(|| record.get("scenario"))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        if !suite.contains("stream") || scenario.is_empty() {
            continue;
        }
        let row = ThroughputEvidence {
            suite,
            scenario,
            layer: tags
                .get("layer")
                .and_then(Value::as_str)
                .map(str::to_string),
            measurement_scope: tags
                .get("measurement_scope")
                .and_then(Value::as_str)
                .map(str::to_string),
            ops_sec: value,
            source: path.display().to_string(),
        };
        rows.entry(throughput_key(&row)).or_insert(row);
    }
}

fn load_stress_latest_json(rows: &mut BTreeMap<String, ThroughputEvidence>, suite: &str) {
    let path = format!("target/stress/{suite}/latest.json");
    let Ok(contents) = fs::read_to_string(&path) else {
        return;
    };
    let Ok(json) = serde_json::from_str::<Value>(&contents) else {
        return;
    };
    let Some(results) = json.get("results").and_then(Value::as_array) else {
        return;
    };
    for result in results {
        let tags = result.get("tags").unwrap_or(&Value::Null);
        let scenario = tags
            .get("scenario")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        if scenario.is_empty() {
            continue;
        }
        let elements = result
            .get("elements")
            .and_then(Value::as_u64)
            .unwrap_or_default();
        let duration_ns = result
            .get("duration")
            .and_then(Value::as_u64)
            .unwrap_or_default();
        if elements == 0 || duration_ns == 0 {
            continue;
        }
        let row = ThroughputEvidence {
            suite: suite.to_string(),
            scenario,
            layer: tags
                .get("layer")
                .and_then(Value::as_str)
                .map(str::to_string),
            measurement_scope: tags
                .get("measurement_scope")
                .and_then(Value::as_str)
                .map(str::to_string),
            ops_sec: elements as f64 * 1_000_000_000.0 / duration_ns as f64,
            source: path.clone(),
        };
        rows.entry(throughput_key(&row)).or_insert(row);
    }
}

fn throughput_key(row: &ThroughputEvidence) -> String {
    format!(
        "{}|{}|{}|{}",
        row.suite,
        row.scenario,
        row.layer.as_deref().unwrap_or(""),
        row.measurement_scope.as_deref().unwrap_or("")
    )
}

fn max_ops<F>(rows: &[ThroughputEvidence], predicate: F) -> Option<f64>
where
    F: Fn(&ThroughputEvidence) -> bool,
{
    rows.iter()
        .filter(|row| predicate(row))
        .map(|row| row.ops_sec)
        .max_by(f64::total_cmp)
}
