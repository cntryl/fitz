use super::support::{self, Environment, TestResult};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

pub struct Campaign {
    pub root: PathBuf,
    pub output: PathBuf,
    pub sources: BTreeMap<String, PathBuf>,
    pub env: Environment,
    pub heads: BTreeMap<String, String>,
    pub production: String,
}

impl Campaign {
    pub fn new(prefix: &str) -> TestResult<Self> {
        let root = support::root();
        let output = PathBuf::from(support::variable(&format!("{prefix}_OUTPUT"))?);
        support::require(
            output.is_absolute() && !output.exists(),
            "A new absolute evidence directory is required",
        )?;
        fs::create_dir_all(&output)?;
        Ok(Self {
            sources: [
                (
                    "before".into(),
                    PathBuf::from(support::variable(&format!("{prefix}_BASELINE"))?),
                ),
                ("after".into(), root.clone()),
            ]
            .into(),
            env: [(
                "CARGO_TARGET_DIR".into(),
                root.join("target").to_string_lossy().into_owned(),
            )]
            .into(),
            root,
            output,
            heads: BTreeMap::new(),
            production: support::variable(&format!("{prefix}_BASE_PRODUCTION"))?,
        })
    }

    pub fn provenance(&mut self, features: &str, runtime: bool) -> TestResult<Value> {
        let mut provenance = json!({"production_baseline":self.production,
            "rustc":support::text(&self.root, &self.env, &["rustc", "-Vv"])?});
        for phase in ["before", "after"] {
            let source = &self.sources[phase];
            support::require(
                support::text(source, &self.env, &["git", "status", "--porcelain"])?.is_empty(),
                "Source must be clean",
            )?;
            let head = support::text(source, &self.env, &["git", "rev-parse", "HEAD"])?;
            self.heads.insert(phase.into(), head.clone());
            let metadata: Value = serde_json::from_slice(&support::output(
                source,
                &self.env,
                &[
                    "cargo",
                    "metadata",
                    "--locked",
                    "--format-version",
                    "1",
                    "--features",
                    features,
                ],
            )?)?;
            support::save(
                &self.output.join(format!("{phase}-cargo-metadata.json")),
                &metadata,
            )?;
            provenance[phase] = json!({"head":head,"lock_sha256":support::sha256(&source.join("Cargo.lock"))?,
                "manifest_sha256":support::sha256(&source.join("Cargo.toml"))?,"package_id":metadata["resolve"]["root"],
                "dependencies":support::dependencies(&metadata)?,"fixtures":support::source_hashes(source,"benches")?});
            if runtime {
                provenance[phase]["runtime_sources"] = support::source_hashes(source, "src")?;
            }
        }
        Ok(provenance)
    }

    pub fn record(
        &self,
        phase: &str,
        label: &str,
        command: &[String],
        env: &Environment,
        binary: Option<&Path>,
        sync: bool,
    ) -> TestResult<(Value, PathBuf)> {
        let source = &self.sources[phase];
        let destination = self.output.join(label);
        support::require(!destination.exists(), "Evidence capture already exists")?;
        fs::create_dir_all(&destination)?;
        let started = std::time::Instant::now();
        if sync {
            support::execute(source, env, &["sync"])?;
        }
        let paths = [
            "/proc/stat",
            "/proc/loadavg",
            "/proc/meminfo",
            "/proc/vmstat",
            "/proc/diskstats",
            "/proc/pressure/cpu",
            "/proc/pressure/io",
            "/proc/self/stat",
            "/proc/self/status",
            "/proc/self/sched",
            "/proc/self/mountinfo",
        ];
        let mut metadata = json!({"host_sync_seconds":started.elapsed().as_secs_f64(),"host_before":support::host(&paths),
            "head":self.heads[phase],"production_baseline":self.production,"command":command,"cwd":source,
            "started_utc":chrono::Utc::now().to_rfc3339()});
        if let Some(binary) = binary {
            metadata["binary_sha256"] = json!(support::sha256(binary)?);
        }
        support::save(&destination.join("capture.json"), &metadata)?;
        let code = support::recorded(source, env, command, &destination.join("run.log"))?;
        metadata["exit_code"] = json!(code);
        metadata["host_after"] = support::host(&paths);
        metadata["finished_utc"] = json!(chrono::Utc::now().to_rfc3339());
        support::save(&destination.join("capture.json"), &metadata)?;
        Ok((metadata, destination))
    }

    pub fn archive(&self, phase: &str, executable: &Path) -> TestResult<PathBuf> {
        support::require(executable.is_file(), "Missing compiled executable")?;
        let directory = self.output.join("binaries").join(phase);
        fs::create_dir_all(&directory)?;
        let archive = directory.join(executable.file_name().ok_or("Invalid executable name")?);
        fs::copy(executable, &archive)?;
        Ok(archive)
    }
}
