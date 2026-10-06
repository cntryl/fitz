#[path = "kv_pressure_oracle.rs"]
mod oracle;
// Growing unique authoritative keys and bounded transaction sizes over real TCP.
use super::durable::{complete, empty_success, payload, request, require_ok};
use super::fixture::{BrokerFixture, StorageDirectory, StorageProfile};
use super::types::BenchFailure;
use cntryl_stress::{LogicalUnit, OperationOutcome, StressContext, StressError, StressResult};
use fitz::benchkit::{
    build_kv_begin, build_kv_commit, build_kv_put, build_kv_rollback, shared_bench_runtime,
};
use fitz::protocol::payload_codec::{PayloadDecoder, PayloadEncoder};
use fitz::testkit::{TestClient, TlvFrameBuilder};
use serde::Serialize;
use std::future::Future;
use std::path::PathBuf;
use std::time::{Duration, Instant};
const ROUTE: &str = "kv://stress-bench/state/cardinality-pressure";

#[derive(Clone, Serialize)]
struct Config {
    keys: Vec<u64>,
    batches: Vec<u64>,
    stage_seconds: u64,
}
impl Config {
    fn load() -> Result<Self, BenchFailure> {
        let stage = list("FITZ_KV_PRESSURE_STAGE_SECS", "120", 600)?;
        if stage.len() != 1 {
            return Err(BenchFailure::validation(
                "stage duration must be one integer",
            ));
        }
        Ok(Self {
            keys: list("FITZ_KV_PRESSURE_KEYS", "100,1000,10000", 200_000)?,
            batches: list("FITZ_KV_PRESSURE_BATCHES", "1,16,256,4096", 4096)?,
            stage_seconds: stage[0],
        })
    }
}
fn list(name: &str, fallback: &str, max: u64) -> Result<Vec<u64>, BenchFailure> {
    parse_list(
        &std::env::var(name).unwrap_or_else(|_| fallback.into()),
        max,
    )
}
fn parse_list(value: &str, max: u64) -> Result<Vec<u64>, BenchFailure> {
    let values: Vec<u64> = value
        .split(',')
        .map(|v| {
            v.parse::<u64>()
                .map_err(|e| BenchFailure::validation(e.to_string()))
        })
        .collect::<Result<_, _>>()?;
    if values.is_empty()
        || values.len() > 16
        || values.iter().any(|v| *v == 0 || *v > max)
        || values.windows(2).any(|v| v[0] >= v[1])
    {
        return Err(BenchFailure::validation(
            "pressure values must strictly increase, with 1..16 positive bounded entries",
        ));
    }
    Ok(values)
}
#[derive(Default, Serialize)]
struct Stage {
    batch_keys: u64,
    target_keys: u64,
    commits_accepted: u64,
    keys_committed: u64,
    keys_verified: u64,
    elapsed_ns: u128,
    verification_ns: u128,
    termination: String,
}
#[derive(Serialize)]
struct Report {
    schema: &'static str,
    source_sha: Option<String>,
    source_dirty: Option<bool>,
    config: Config,
    storage_scope: &'static str,
    storage_path: Option<PathBuf>,
    status: &'static str,
    failure: Option<String>,
    cleanup_failure: Option<String>,
    stages: Vec<Stage>,
    total_keys_committed: u64,
    keys_verified_after_restart: u64,
    recovery_probe_passed: bool,
    clean_restart_passed: bool,
    process_rss_bytes: Option<u64>,
    process_peak_rss_bytes: Option<u64>,
    wall_elapsed_ns: u128,
    cleanup_status: &'static str,
    #[serde(skip)]
    path: PathBuf,
}
impl Report {
    fn new(config: Config) -> Result<Self, BenchFailure> {
        let directory = PathBuf::from("target/fitz-stress/kv-pressure");
        std::fs::create_dir_all(&directory).map_err(BenchFailure::transport)?;
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(BenchFailure::transport)?
            .as_nanos();
        Ok(Self {
            schema: "fitz.kv-pressure.v1",
            source_sha: git(&["rev-parse", "HEAD"]).map(|v| v.trim().into()),
            source_dirty: git(&["status", "--porcelain"]).map(|v| !v.is_empty()),
            config,
            storage_scope: "local_disk_sync_clean_shutdown_reopen_not_crash_recovery",
            storage_path: None,
            status: "running",
            failure: None,
            cleanup_failure: None,
            stages: Vec::new(),
            total_keys_committed: 0,
            keys_verified_after_restart: 0,
            recovery_probe_passed: false,
            clean_restart_passed: false,
            process_rss_bytes: None,
            process_peak_rss_bytes: None,
            wall_elapsed_ns: 0,
            cleanup_status: "not_started",
            path: directory.join(format!("{stamp}.json")),
        })
    }
    fn save(&self) -> Result<(), BenchFailure> {
        let tmp = self.path.with_extension("tmp");
        std::fs::write(
            &tmp,
            serde_json::to_vec_pretty(self).map_err(BenchFailure::transport)?,
        )
        .map_err(BenchFailure::transport)?;
        std::fs::rename(tmp, &self.path).map_err(BenchFailure::transport)
    }
}
fn git(args: &[&str]) -> Option<String> {
    let out = std::process::Command::new("git").args(args).output().ok()?;
    out.status
        .success()
        .then(|| String::from_utf8(out.stdout).ok())
        .flatten()
}
async fn bounded<T>(
    future: impl Future<Output = Result<T, BenchFailure>>,
) -> Result<T, BenchFailure> {
    tokio::time::timeout(Duration::from_secs(60), future)
        .await
        .map_err(|_| {
            BenchFailure::transport(
                "KV operation exceeded 60 seconds; commit outcome may be indeterminate",
            )
        })?
}
async fn begin(client: &mut TestClient, mode: u8) -> Result<u64, BenchFailure> {
    let body = bounded(request(client, &build_kv_begin(ROUTE, mode, 1), 100)).await?;
    require_ok(&body, "KV BEGIN")?;
    let mut decoder = PayloadDecoder::new(&body);
    decoder.get_u8().map_err(BenchFailure::validation)?;
    let id = decoder.get_u64().map_err(BenchFailure::validation)?;
    complete(&decoder)?;
    Ok(id)
}
async fn write(client: &mut TestClient, start: u64, count: u64) -> Result<(), BenchFailure> {
    let tx = begin(client, 1).await?;
    for key in start..start + count {
        let body = bounded(request(
            client,
            &build_kv_put(
                tx,
                ROUTE,
                format!("key-{key:020}").as_bytes(),
                &payload(0, key),
            ),
            104,
        ))
        .await?;
        empty_success(&body, "KV PUT")?;
    }
    let body = bounded(request(client, &build_kv_commit(tx, ROUTE), 101)).await?;
    empty_success(&body, "KV COMMIT")
}
async fn verify(
    client: &mut TestClient,
    start: u64,
    count: u64,
    ctx: &StressContext,
    checked: &mut u64,
) -> Result<(), BenchFailure> {
    for chunk in (start..start + count).step_by(64) {
        let tx = begin(client, 0).await?;
        for key in chunk..(chunk + 64).min(start + count) {
            let mut encoder = PayloadEncoder::new();
            encoder.put_u64(tx);
            encoder.put_string(ROUTE);
            encoder.put_bytes(format!("key-{key:020}").as_bytes());
            let mut frame = TlvFrameBuilder::new();
            frame.encode_field(103, &encoder.finish());
            let body = bounded(request(client, &frame.build(), 103)).await?;
            require_ok(&body, "KV GET")?;
            oracle::verify_value(&body, &payload(0, key)).map_err(BenchFailure::verification)?;
            *checked += 1;
            ctx.progress_handle().advance();
        }
        let body = bounded(request(client, &build_kv_rollback(tx, ROUTE), 102)).await?;
        empty_success(&body, "KV read ROLLBACK")?;
    }
    Ok(())
}
async fn campaign(
    ctx: &StressContext,
    report: &mut Report,
    address: std::net::SocketAddr,
) -> Result<(), BenchFailure> {
    let mut client = bounded(async {
        TestClient::new(address)
            .await
            .map_err(BenchFailure::transport)
    })
    .await?;
    for batch in report.config.batches.clone() {
        for target in report.config.keys.clone() {
            let goal = report.total_keys_committed + target;
            report.stages.push(Stage {
                batch_keys: batch,
                target_keys: target,
                ..Stage::default()
            });
            let idx = report.stages.len() - 1;
            let start = Instant::now();
            while report.total_keys_committed < goal
                && start.elapsed() < Duration::from_secs(report.config.stage_seconds)
            {
                let count = batch.min(goal - report.total_keys_committed);
                let first = report.total_keys_committed;
                write(&mut client, first, count).await?;
                report.total_keys_committed += count;
                report.stages[idx].commits_accepted += 1;
                report.stages[idx].keys_committed += count;
                ctx.progress_handle().advance();
                report.stages[idx].elapsed_ns = start.elapsed().as_nanos();
                if report.stages[idx].commits_accepted.is_multiple_of(64) {
                    report.save()?;
                }
            }
            report.stages[idx].termination = if report.total_keys_committed == goal {
                "configured_key_guard"
            } else {
                "configured_time_guard"
            }
            .into();
            let verification = Instant::now();
            let count = report.stages[idx].keys_committed;
            verify(
                &mut client,
                report.total_keys_committed - count,
                count,
                ctx,
                &mut report.stages[idx].keys_verified,
            )
            .await?;
            report.stages[idx].verification_ns = verification.elapsed().as_nanos();
            report.save()?;
        }
    }
    write(&mut client, report.total_keys_committed, 1).await?;
    report.total_keys_committed += 1;
    verify(&mut client, report.total_keys_committed - 1, 1, ctx, &mut 0).await?;
    report.recovery_probe_passed = true;
    bounded(async { client.close().await.map_err(BenchFailure::transport) }).await
}

pub(crate) fn run(ctx: &mut StressContext) -> StressResult {
    execute(ctx).map_err(|e| StressError::new(e.to_string()))
}
fn reopen(
    ctx: &StressContext,
    report: &mut Report,
    storage: StorageDirectory,
) -> Result<(), BenchFailure> {
    let runtime = shared_bench_runtime();
    let mut result: Result<(), BenchFailure>;
    match runtime.block_on(bounded(BrokerFixture::start(
        StorageProfile::LocalDisk,
        storage,
    ))) {
        Ok(reopened) => {
            result = runtime.block_on(async {
                let mut client = bounded(async {
                    TestClient::new(reopened.tcp_addr())
                        .await
                        .map_err(BenchFailure::transport)
                })
                .await?;
                verify(
                    &mut client,
                    0,
                    report.total_keys_committed,
                    ctx,
                    &mut report.keys_verified_after_restart,
                )
                .await?;
                bounded(async { client.close().await.map_err(BenchFailure::transport) }).await?;
                report.clean_restart_passed = true;
                Ok(())
            });
            if let Err(error) = &result {
                report.status = "failed";
                report.failure = Some(error.to_string());
            }
            if let Err(error) = report.save() {
                if result.is_ok() {
                    result = Err(error);
                }
            }
            let (server, storage) = reopened.into_parts();
            let shutdown = runtime.block_on(bounded(async {
                server.shutdown().await.map_err(BenchFailure::transport)
            }));
            if let Err(e) = shutdown {
                report.cleanup_failure = Some(e.to_string());
                if result.is_ok() {
                    result = Err(e);
                }
            }
            if result.is_ok() {
                if let Err(e) = storage.release() {
                    report.cleanup_failure = Some(e.to_string());
                    result = Err(e);
                }
            }
        }
        Err(e) => result = Err(e),
    }
    result
}
fn execute(ctx: &mut StressContext) -> Result<(), BenchFailure> {
    let started = Instant::now();
    let mut report = Report::new(Config::load()?)?;
    // Product bounds limit stored bytes even when every configured stage fills.
    if report.config.keys.iter().sum::<u64>()
        * u64::try_from(report.config.batches.len()).unwrap_or(u64::MAX)
        > 500_000
    {
        return Err(BenchFailure::validation(
            "combined key guard exceeds 500000",
        ));
    }
    ctx.parameter("workload", "kv_unique_keys_and_transaction_size");
    ctx.metadata("target_class", "stress_characterization");
    let runtime = shared_bench_runtime();
    let storage = StorageDirectory::new(StorageProfile::LocalDisk)?;
    report.storage_path = storage.path().map(std::path::Path::to_path_buf);
    report.save()?;
    let fixture = match runtime.block_on(bounded(BrokerFixture::start(
        StorageProfile::LocalDisk,
        storage,
    ))) {
        Ok(value) => value,
        Err(error) => {
            report.status = "failed";
            report.failure = Some(error.to_string());
            report.wall_elapsed_ns = started.elapsed().as_nanos();
            let _ = report.save();
            return Err(error);
        }
    };
    let mut result = runtime.block_on(campaign(ctx, &mut report, fixture.tcp_addr()));
    if let Err(error) = &result {
        report.status = "failed";
        report.failure = Some(error.to_string());
    }
    report.cleanup_status = "running";
    // Preserve original failure before shutdown; artifact errors must not skip cleanup.
    if let Err(error) = report.save() {
        if result.is_ok() {
            result = Err(error);
        }
    }
    let (server, storage) = fixture.into_parts();
    if let Err(e) = runtime.block_on(bounded(async {
        server.shutdown().await.map_err(BenchFailure::transport)
    })) {
        report.cleanup_failure = Some(e.to_string());
        if result.is_ok() {
            result = Err(e);
        }
    }
    if result.is_ok() {
        result = reopen(ctx, &mut report, storage);
    }
    report.wall_elapsed_ns = started.elapsed().as_nanos();
    (report.process_rss_bytes, report.process_peak_rss_bytes) = super::artifacts::memory_sample();
    report.cleanup_status = if report.cleanup_failure.is_some() {
        "failed"
    } else {
        "completed"
    };
    report.status = if result.is_ok() { "passed" } else { "failed" };
    if let Err(e) = &result {
        report.failure = Some(e.to_string());
    }
    if let Err(error) = report.save() {
        if result.is_ok() {
            result = Err(error);
        }
    }
    for (idx, stage) in report.stages.iter().enumerate() {
        let failures = u64::from(
            stage.keys_committed != stage.keys_verified
                || (result.is_err() && idx + 1 == report.stages.len()),
        );
        ctx.record_external_outcome(
            format!("stage_{idx}_batch_{}", stage.batch_keys),
            Duration::from_nanos(u64::try_from(stage.elapsed_ns).unwrap_or(u64::MAX)),
            LogicalUnit::new("committed_authoritative_key"),
            OperationOutcome::new(stage.keys_verified + failures, stage.keys_verified)
                .failures(failures),
        );
    }
    result
}
