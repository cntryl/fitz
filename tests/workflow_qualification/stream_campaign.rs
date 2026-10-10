use super::campaign::Campaign;
use super::support::{self, TestResult};
use super::{compiler, stream};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};

pub struct StreamCampaign {
    campaign: Campaign,
    binaries: BTreeMap<(String, String), PathBuf>,
}

impl StreamCampaign {
    pub fn new() -> TestResult<Self> {
        Ok(Self {
            campaign: Campaign::new("STREAM")?,
            binaries: BTreeMap::new(),
        })
    }

    fn build(&mut self) -> TestResult<bool> {
        let mut provenance = self.campaign.provenance("benchkit", false)?;
        let kind = std::env::var("STREAM_COMPARISON_KIND").unwrap_or_else(|_| "source".into());
        compiler::validate_inputs(&kind, &provenance)?;
        if kind == "dependencies" {
            for (filename, field) in [
                ("Cargo.toml", "manifest_sha256"),
                ("Cargo.lock", "lock_sha256"),
            ] {
                use sha2::{Digest, Sha256};
                let original = support::output(
                    &self.campaign.root,
                    &self.campaign.env,
                    &[
                        "git",
                        "show",
                        &format!("{}:{filename}", self.campaign.production),
                    ],
                )?;
                support::require(
                    provenance["before"][field] == hex::encode(Sha256::digest(original)),
                    "Baseline production dependencies were replaced",
                )?;
            }
        }
        provenance["comparison_kind"] = json!(kind);
        provenance["planned_runs"] = json!(["qualification", "confirmation"]);
        support::save(&self.campaign.output.join("provenance.json"), &provenance)?;
        self.build_sources(&mut provenance)?;
        let mut hashes = json!({"before":{},"after":{}});
        for ((phase, target), binary) in &self.binaries {
            hashes[phase][target] = json!(support::sha256(binary)?);
        }
        support::save(&self.campaign.output.join("binary-hashes.json"), &hashes)?;
        let equivalent = compiler::binary_equivalence(&kind, &provenance, &hashes)?;
        provenance["evidence_mode"] = json!(if equivalent {
            "binary_equivalence"
        } else {
            "timing_comparison"
        });
        if equivalent {
            provenance["planned_runs"] = json!([]);
        }
        support::save(&self.campaign.output.join("provenance.json"), &provenance)?;
        Ok(equivalent)
    }

    fn build_sources(&mut self, provenance: &mut Value) -> TestResult {
        for phase in ["before", "after"] {
            let source = self.campaign.sources[phase].clone();
            support::execute(
                &source,
                &self.campaign.env,
                &["cargo", "clean", "--release", "--package", "fitz"],
            )?;
            for (index, target) in [
                "tier4_stream_gate",
                "tier4_stream_shapes",
                "tier4_stream_compacted",
            ]
            .into_iter()
            .enumerate()
            {
                let mut command: Vec<String> = [
                    "cargo",
                    "bench",
                    "--locked",
                    "--features",
                    "benchkit",
                    "--bench",
                    target,
                    "--message-format=json",
                    "--",
                ]
                .into_iter()
                .map(str::to_owned)
                .collect();
                if target == "tier4_stream_shapes" {
                    command.extend(["--workload".into(), "should_replay_".into()]);
                }
                let (metadata, destination) = self.campaign.record(
                    phase,
                    &format!("build-{phase}-{target}"),
                    &command,
                    &self.campaign.env,
                    None,
                    false,
                )?;
                support::require(
                    metadata["exit_code"] == 0,
                    "Diagnostic benchmark failed; raw logs retained",
                )?;
                fs::copy(
                    source
                        .join("target/stress")
                        .join(target.replace('_', "-"))
                        .join("latest.json"),
                    destination.join("initial.json"),
                )?;
                let log = destination.join("run.log");
                let package = provenance[phase]["package_id"]
                    .as_str()
                    .ok_or("Missing root Cargo package")?;
                if index == 0 {
                    let witness = compiler::compiled_inputs(&source, &log, package)?;
                    provenance[phase]["compiled_source_inputs"] = witness["hashes"].clone();
                    support::save(
                        &self
                            .campaign
                            .output
                            .join(format!("{phase}-compiled-source-inputs.json")),
                        &witness,
                    )?;
                }
                let package = provenance[phase]["package_id"]
                    .as_str()
                    .ok_or("Missing root Cargo package")?;
                let artifact = compiler::fresh_artifact(
                    &log,
                    package,
                    "bench",
                    &source.join(format!("benches/{target}.rs")),
                    Some(target),
                )?;
                support::save(&destination.join("compiler-artifact.json"), &artifact)?;
                let executable = Path::new(
                    artifact["executable"]
                        .as_str()
                        .ok_or("Missing benchmark executable")?,
                );
                self.binaries.insert(
                    (phase.into(), target.into()),
                    self.campaign.archive(phase, executable)?,
                );
            }
        }
        Ok(())
    }

    fn capture(&self, run_name: &str, capture: &stream::Capture) -> TestResult<Vec<Value>> {
        let relative = format!("{run_name}/{}", capture.label);
        let binary = &self.binaries[&(capture.phase.into(), capture.target.clone())];
        let mut command = vec![
            binary.to_string_lossy().into_owned(),
            "--bench".into(),
            "--output-dir".into(),
            self.campaign
                .output
                .join(&relative)
                .join("stress")
                .to_string_lossy()
                .into_owned(),
        ];
        if let Some(pattern) = &capture.pattern {
            command.extend(["--workload".into(), pattern.clone()]);
        }
        println!("{relative} {} {:?}", capture.target, capture.pattern);
        let (metadata, destination) = self.campaign.record(
            capture.phase,
            &relative,
            &command,
            &self.campaign.env,
            Some(binary),
            true,
        )?;
        support::require(
            metadata["exit_code"] == 0,
            "Failed benchmark; raw capture retained",
        )?;
        let artifacts = support::files(&destination.join("stress"), "json")?
            .into_iter()
            .filter(|path| path.file_name().is_some_and(|name| name == "latest.json"))
            .collect::<Vec<_>>();
        support::require(artifacts.len() == 1, "Require exactly one stress artifact")?;
        let run = support::load(&artifacts[0])?;
        validate_capture(&run, &self.campaign.heads[capture.phase])?;
        Ok(run["summaries"]
            .as_array()
            .ok_or("Missing stress summaries")?
            .clone())
    }

    fn measure(&self, run_name: &str) -> TestResult<String> {
        let mut summaries = stream::Summaries::from([
            ("before".into(), BTreeMap::new()),
            ("after".into(), BTreeMap::new()),
        ]);
        let mut controls: BTreeMap<(&str, &str), Vec<Value>> = BTreeMap::new();
        for capture in stream::capture_plan() {
            let rows = self.capture(run_name, &capture)?;
            if let Some(key) = capture.control {
                controls.entry(key).or_default().extend(rows);
            } else {
                for row in rows {
                    summaries
                        .get_mut(capture.phase)
                        .ok_or("Unknown phase")?
                        .entry(row["name"].as_str().ok_or("Missing workload name")?.into())
                        .or_default()
                        .push(row);
                }
            }
        }
        let mut timing = json!({});
        for (name, target, _, row) in stream::CONTROLS {
            timing[name] = stream::control(
                controls
                    .get(&(name, "left"))
                    .ok_or("Missing left timing control")?,
                controls
                    .get(&(name, "right"))
                    .ok_or("Missing right timing control")?,
                row,
                &support::sha256(&self.binaries[&("before".into(), target.into())])?,
            )?;
        }
        let (records, checks) = stream::compare(&summaries)?;
        let destination = self.campaign.output.join(run_name);
        fs::write(
            destination.join("comparison.csv"),
            comparison_csv(&records)?,
        )?;
        support::save(&destination.join("budgets.json"), &checks)?;
        support::save(&destination.join("control.json"), &timing)?;
        let verdict = stream::verdict(&checks, &timing)?;
        support::save(
            &destination.join("result.json"),
            &json!({"status":verdict,"head":self.campaign.heads["after"]}),
        )?;
        let mut summary = format!("\n{run_name}: **{verdict}**. Three-run medians, default profile, one host; p95 uses the median run mean of per-sample p95.\n\n| Row | Throughput ratio | p95 ratio |\n|---|---:|---:|\n");
        for row in records {
            writeln!(
                &mut summary,
                "| {} | {:.3} | {:.3} |",
                row["name"].as_str().ok_or("Missing row name")?,
                support::number(&row["throughput_ratio"])?,
                support::number(&row["p95_ratio"])?
            )?;
        }
        writeln!(
            &mut summary,
            "\nBudget observations: {checks}\n\nUnchanged-binary controls: {timing}"
        )?;
        support::summary(&summary)?;
        Ok(verdict.into())
    }

    pub fn run(&mut self) -> TestResult {
        if self.build()? {
            support::save(
                &self.campaign.output.join("acceptance.json"),
                &json!({"status":"binary_equivalent","head":self.campaign.heads["after"]}),
            )?;
            return support::summary("\nIndependently rebuilt dependency variants are byte-identical with matching compiler-recorded source inputs. No timing samples were collected.\n");
        }
        let mut results = json!({});
        for name in ["qualification", "confirmation"] {
            results[name] = json!(self.measure(name)?);
        }
        support::save(&self.campaign.output.join("acceptance.json"), &results)?;
        support::require(
            results["qualification"] == "passed" && results["confirmation"] == "passed",
            "Both complete Stream campaigns must pass; failure evidence retained",
        )
    }
}

pub fn validate_capture(run: &Value, head: &str) -> TestResult {
    support::require(
        run["environment"]["git_commit"] == head,
        "Wrong benchmark source",
    )?;
    let profile = &run["environment"]["profile_config"];
    support::require(
        profile["profile"] == "default"
            && profile["warmup_samples"] == 1
            && profile["measured_samples"] == 5
            && profile["sample_duration"] == 500_000_000,
        "Default benchmark profile changed",
    )?;
    let rows = run["summaries"]
        .as_array()
        .ok_or("Missing stress summaries")?;
    support::require(
        !rows.is_empty() && rows.iter().all(|row| row["correctness"]["passed"] == true),
        "Benchmark correctness failed",
    )
}

pub fn comparison_csv(records: &[Value]) -> TestResult<String> {
    let fields = [
        "name",
        "before_ops_s",
        "after_ops_s",
        "throughput_ratio",
        "before_p95_us",
        "after_p95_us",
        "p95_ratio",
        "before_quality",
        "after_quality",
    ];
    let mut csv = fields.join(",") + "\n";
    for row in records {
        let values = fields
            .iter()
            .map(|field| {
                let value = row.get(*field).ok_or("Missing CSV field")?;
                let text = value
                    .as_str()
                    .map_or_else(|| value.to_string(), str::to_owned);
                Ok(format!("\"{}\"", text.replace('"', "\"\"")))
            })
            .collect::<TestResult<Vec<_>>>()?;
        csv.push_str(&(values.join(",") + "\n"));
    }
    Ok(csv)
}
