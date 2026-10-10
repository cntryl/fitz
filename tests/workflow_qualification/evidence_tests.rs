use super::{stream, stream_campaign, support};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

fn recompute(
    root: &Path,
    phase: &str,
    provenance: &Value,
    hashes: &Value,
) -> support::TestResult<Value> {
    let mut summaries = stream::Summaries::from([
        ("before".into(), BTreeMap::new()),
        ("after".into(), BTreeMap::new()),
    ]);
    let mut controls: BTreeMap<(String, String), Vec<Value>> = BTreeMap::new();
    for capture in stream::capture_plan() {
        let destination = root.join(phase).join(&capture.label);
        let metadata = support::load(&destination.join("capture.json"))?;
        support::require(
            metadata["exit_code"] == 0 && metadata["head"] == provenance[capture.phase]["head"],
            "Failed or incorrectly sourced capture",
        )?;
        support::require(
            metadata["binary_sha256"] == hashes[capture.phase][&capture.target],
            "Wrong capture binary",
        )?;
        let files = support::files(&destination.join("stress"), "json")?
            .into_iter()
            .filter(|file| file.file_name().is_some_and(|name| name == "latest.json"))
            .collect::<Vec<_>>();
        support::require(files.len() == 1, "Require one stress report per capture")?;
        let run = support::load(&files[0])?;
        stream_campaign::validate_capture(
            &run,
            provenance[capture.phase]["head"]
                .as_str()
                .ok_or("Missing capture head")?,
        )?;
        let rows = run["summaries"].as_array().ok_or("Missing summaries")?;
        if let Some((name, side)) = capture.control {
            controls
                .entry((name.into(), side.into()))
                .or_default()
                .extend(rows.clone());
        } else {
            for row in rows {
                summaries
                    .get_mut(capture.phase)
                    .ok_or("Unknown phase")?
                    .entry(row["name"].as_str().ok_or("Missing row name")?.into())
                    .or_default()
                    .push(row.clone());
            }
        }
    }
    let (records, budgets) = stream::compare(&summaries)?;
    let mut timing = json!({});
    for (name, target, _, row) in stream::CONTROLS {
        timing[name] = stream::control(
            &controls[&(name.into(), "left".into())],
            &controls[&(name.into(), "right".into())],
            row,
            hashes["before"][target]
                .as_str()
                .ok_or("Missing binary hash")?,
        )?;
    }
    support::require(
        budgets == support::load(&root.join(phase).join("budgets.json"))?,
        "Rust budgets differ from preserved decisions",
    )?;
    let saved = support::load(&root.join(phase).join("control.json"))?;
    for (name, _, _, _) in stream::CONTROLS {
        support::require(
            timing[name]["binary_sha256"] == saved[name]["binary_sha256"]
                && timing[name]["stable_within_five_percent"]
                    == saved[name]["stable_within_five_percent"],
            "Rust timing verdict differs from preserved decision",
        )?;
        for metric in ["throughput_ratio", "p95_ratio"] {
            support::require(
                (support::number(&timing[name][metric])? - support::number(&saved[name][metric])?)
                    .abs()
                    <= 1e-12,
                "Rust timing ratio differs from preserved calculation",
            )?;
        }
    }
    let verdict = stream::verdict(&budgets, &timing)?;
    support::require(
        support::load(&root.join(phase).join("result.json"))?["status"] == verdict,
        "Rust phase verdict differs from preserved result",
    )?;
    Ok(
        json!({"phase":phase,"status":verdict,"workloads":records.len(),"budgets":budgets,"controls":timing}),
    )
}

#[test]
#[ignore = "Requires preserved complete Stream artifacts in FITZ_STREAM_CAPTURE_ROOT"]
fn should_recompute_preserved_stream_results() -> support::TestResult {
    // Arrange
    let root = std::path::PathBuf::from(support::variable("FITZ_STREAM_CAPTURE_ROOT")?);
    let provenance = support::load(&root.join("provenance.json"))?;
    let hashes = support::load(&root.join("binary-hashes.json"))?;
    let saved = support::load(&root.join("acceptance.json"))?;
    // Act
    let results = ["qualification", "confirmation"]
        .into_iter()
        .map(|phase| recompute(&root, phase, &provenance, &hashes))
        .collect::<support::TestResult<Vec<_>>>()?;
    // Assert
    for result in &results {
        assert_eq!(
            result["status"],
            saved[result["phase"].as_str().ok_or("Missing result phase")?]
        );
    }
    let proof = json!({"source_sha":provenance["after"]["head"],"preserved_acceptance":saved,"recomputed":results});
    fs::write(
        root.join("rust-recomputed-proof.json"),
        serde_json::to_vec_pretty(&proof)?,
    )?;
    Ok(())
}
