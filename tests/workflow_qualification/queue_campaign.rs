use super::campaign::Campaign;
use super::queue;
use super::support::{self, TestResult};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::fs;
use std::path::PathBuf;

pub struct QueueCampaign {
    campaign: Campaign,
    binaries: BTreeMap<String, PathBuf>,
}

impl QueueCampaign {
    pub fn new() -> TestResult<Self> {
        let mut campaign = Campaign::new("QUEUE")?;
        campaign.env.extend([
            ("STRESS_SUITE".into(), "queue-pressure".into()),
            (
                "FITZ_QUEUE_PRESSURE_STAGE_SECS".into(),
                queue::WINDOW_SECONDS.to_string(),
            ),
            ("FITZ_QUEUE_PRESSURE_PRODUCERS".into(), "256".into()),
            ("FITZ_QUEUE_PRESSURE_CONSUMER_DELAY_MS".into(), "5".into()),
            ("FITZ_QUEUE_PRESSURE_MAX_ATTEMPTS".into(), "500000".into()),
            ("FITZ_QUEUE_PRESSURE_DRAIN_SECS".into(), "600".into()),
        ]);
        Ok(Self {
            campaign,
            binaries: BTreeMap::new(),
        })
    }

    fn build(&mut self) -> TestResult {
        let mut provenance = self.campaign.provenance("benchkit,stress-soak", true)?;
        for field in ["lock_sha256", "dependencies", "fixtures"] {
            support::require(
                provenance["before"][field] == provenance["after"][field],
                "Queue build inputs differ",
            )?;
        }
        provenance["identical_instrumented_runtime_sources"] = json!(
            provenance["before"]["runtime_sources"] == provenance["after"]["runtime_sources"]
        );
        provenance["diagnostic_only_commit"] = json!(support::variable("QUEUE_TIMING_COMMIT")?);
        support::save(&self.campaign.output.join("provenance.json"), &provenance)?;
        for phase in ["before", "after"] {
            let source = &self.campaign.sources[phase];
            support::execute(
                source,
                &self.campaign.env,
                &["cargo", "clean", "--release", "--package", "fitz"],
            )?;
            let command = [
                "cargo",
                "bench",
                "--locked",
                "--bench",
                "queue_pressure",
                "--features",
                "benchkit,stress-soak",
                "--message-format=json",
                "--",
            ]
            .into_iter()
            .map(str::to_owned)
            .collect();
            self.capture(phase, 1000, &format!("build-{phase}"), command, true, false)?;
            let artifact = super::compiler::fresh_artifact(
                &self.campaign.output.join(format!("build-{phase}/run.log")),
                provenance[phase]["package_id"]
                    .as_str()
                    .ok_or("Missing root Cargo package")?,
                "bench",
                &source.join("benches/queue_pressure.rs"),
                Some("queue_pressure"),
            )?;
            let executable = PathBuf::from(
                artifact["executable"]
                    .as_str()
                    .ok_or("Missing Queue benchmark executable")?,
            );
            self.binaries
                .insert(phase.into(), self.campaign.archive(phase, &executable)?);
        }
        let mut hashes = json!({});
        for (phase, path) in &self.binaries {
            hashes[phase] = json!(support::sha256(path)?);
        }
        support::save(&self.campaign.output.join("binary-hashes.json"), &hashes)
    }

    fn capture(
        &self,
        phase: &str,
        backlog: u64,
        label: &str,
        mut command: Vec<String>,
        build: bool,
        finite: bool,
    ) -> TestResult<Value> {
        let source = &self.campaign.sources[phase];
        let physical = source.join("target/fitz-stress/queue-pressure");
        let prior = support::files(&physical, "json")?
            .into_iter()
            .collect::<BTreeSet<_>>();
        let mut env = self.campaign.env.clone();
        env.insert(
            "FITZ_QUEUE_PRESSURE_MAX_BACKLOG".into(),
            backlog.to_string(),
        );
        env.insert(
            "FITZ_QUEUE_PRESSURE_RATES".into(),
            if finite {
                16000
            } else {
                queue::offered_rate(backlog)
            }
            .to_string(),
        );
        command.extend([
            "--output-dir".into(),
            self.campaign
                .output
                .join(label)
                .join("stress")
                .to_string_lossy()
                .into_owned(),
        ]);
        let (mut metadata, destination) = self.campaign.record(
            phase,
            label,
            &command,
            &env,
            self.binaries.get(phase).map(PathBuf::as_path),
            false,
        )?;
        let reports = support::files(&physical, "json")?
            .into_iter()
            .filter(|path| !prior.contains(path))
            .collect::<Vec<_>>();
        let mut names = Vec::new();
        for path in &reports {
            let name = path.file_name().ok_or("Invalid report filename")?;
            fs::copy(path, destination.join(name))?;
            names.push(name.to_string_lossy().into_owned());
        }
        metadata["reports"] = json!(names);
        let validation = (|| -> TestResult {
            support::require(
                metadata["exit_code"] == 0,
                "Workload failed; raw accounting retained",
            )?;
            support::require(reports.len() == 1, "Exactly one owned campaign is required")?;
            let report = support::load(&reports[0])?;
            validate_report(
                &mut metadata,
                &report,
                &self.campaign.heads[phase],
                backlog,
                build,
                finite,
            )
        })();
        if let Err(error) = validation {
            metadata["status"] = json!("failed");
            metadata["error"] = json!(error.to_string());
        }
        support::save(&destination.join("capture.json"), &metadata)?;
        if build {
            support::require(
                metadata["status"] == "diagnostic_build",
                "Diagnostic Queue build failed; evidence retained",
            )?;
        }
        Ok(metadata)
    }

    pub fn run(&mut self) -> TestResult {
        self.build()?;
        let mut results = Vec::new();
        for (phase, backlog, finite, label) in queue::capture_plan() {
            println!("{label}");
            let command = vec![
                self.binaries[phase].to_string_lossy().into_owned(),
                "--bench".into(),
            ];
            let metadata = self.capture(phase, backlog, &label, command, false, finite)?;
            results.push(json!({"label":label,"head":self.campaign.heads[phase],"status":metadata["status"],
                "termination":metadata["termination"],"window_completed":metadata["configured_window_completed"],
                "harness_miss_fraction":metadata["harness_miss_fraction"],"metrics":metadata["metrics"]}));
            support::save(
                &self.campaign.output.join("acceptance.json"),
                &json!(results),
            )?;
        }
        let comparisons = queue::comparisons(&results)?;
        support::save(&self.campaign.output.join("comparison.json"), &comparisons)?;
        let mut summary = String::from("Matched corrected-pacing drains. All six full-window captures and all six unchanged finite-envelope captures must pass. Each full-window capture must complete 120 seconds with at most 1% missed arrivals.\n\n| Capture | Qualification | Termination | Full active window | Missed arrivals |\n|---|---|---|---|---|\n");
        for row in &results {
            writeln!(
                &mut summary,
                "| {} | {} | {} | {} | {} |",
                row["label"],
                row["status"],
                row["termination"],
                row["window_completed"],
                row["harness_miss_fraction"]
            )?;
        }
        summary.push_str("\nGuard and broker-rejection stops fail full-window qualification. A finite-envelope guard drain qualifies only its verified accepted-message count under the original 16,000/s load and 600-second drain deadline. Resource-floor survival and published-dependency qualification remain separate.\n\nMatched before/after comparison: throughput must keep at least 90% and drain time and p99s stay at most 110% of before (drain time also allows 1 second). p99s are power-of-two bucket upper bounds.\n\n| Scope and envelope | Metric | Before | After | Limit | Result |\n|---|---|---|---|---|---|\n");
        for (pair, rows) in comparisons.as_object().ok_or("Missing comparisons")? {
            let rows = rows.as_array().ok_or("Invalid comparison rows")?;
            if rows.is_empty() {
                writeln!(
                    &mut summary,
                    "| {pair} | all | | | | not compared: a capture failed |"
                )?;
            }
            for row in rows {
                writeln!(
                    &mut summary,
                    "| {pair} | {} | {:.2} | {:.2} | {:.2} | {} |",
                    row["metric"],
                    support::number(&row["before"])?,
                    support::number(&row["after"])?,
                    support::number(&row["limit"])?,
                    if row["passed"] == true {
                        "pass"
                    } else {
                        "regressed"
                    }
                )?;
            }
        }
        support::summary(&summary)?;
        support::require(
            queue::accepted(&results, &comparisons),
            "All twelve Queue captures and six comparisons must pass; evidence retained",
        )
    }
}

fn validate_report(
    metadata: &mut Value,
    report: &Value,
    head: &str,
    backlog: u64,
    build: bool,
    finite: bool,
) -> TestResult {
    let stages = report["stages"]
        .as_array()
        .ok_or("Missing pressure stages")?;
    support::require(stages.len() == 1, "Exactly one pressure stage is required")?;
    let stage = &stages[0];
    metadata["qualification_scope"] = json!(if finite {
        "finite_accepted_message_drain"
    } else {
        "completed_window_drain"
    });
    for key in [
        "termination",
        "configured_window_completed",
        "offered",
        "harness_missed",
    ] {
        metadata[key] = stage[key].clone();
    }
    let offered = support::number(&stage["offered"])?;
    metadata["harness_miss_fraction"] = if offered > 0.0 {
        json!(support::number(&stage["harness_missed"])? / offered)
    } else {
        Value::Null
    };
    if !build {
        queue::validate(report, head, backlog, finite)?;
        metadata["metrics"] = queue::metrics(stage)?;
    }
    metadata["status"] = json!(if build {
        "diagnostic_build"
    } else {
        "drain_passed"
    });
    Ok(())
}
