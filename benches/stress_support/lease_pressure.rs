//! A shared-route bounded contender/waiter campaign; all state is ephemeral.
#[path = "lease_pressure_oracle.rs"]
mod oracle;
use super::durable::{complete, empty_success, error_code, request, require_ok};
use super::fixture::{BrokerFixture, StorageDirectory, StorageProfile};
use super::types::BenchFailure;
use cntryl_stress::{LogicalUnit, OperationOutcome, StressContext, StressError, StressResult};
use fitz::benchkit::{
    build_lease_acquire_immediate, build_lease_query, build_lease_release, shared_bench_runtime,
};
use fitz::protocol::error_codes::lease;
use fitz::protocol::payload_codec::{PayloadDecoder, PayloadEncoder};
use fitz::testkit::{TestClient, TlvFrameBuilder};
use futures_util::{stream::FuturesUnordered, StreamExt};
use serde::Serialize;
use std::future::Future;
use std::path::PathBuf;
use std::time::{Duration, Instant};
const ROUTE: &str = "lease://stress-bench/ownership/shared-pressure";
const MAX_WAITERS: usize = 100;
#[derive(Default, Serialize)]
struct Stage {
    contenders: usize,
    immediate_held_rejections: u64,
    queued: u64,
    queue_full_rejections: u64,
    disconnected_waiters: u64,
    grants_verified: u64,
    stale_token_rejections: u64,
    elapsed_ns: u128,
    recovery_verified: bool,
}
#[derive(Serialize)]
struct Report {
    schema: &'static str,
    source_sha: Option<String>,
    source_dirty: Option<bool>,
    counts: Vec<usize>,
    status: &'static str,
    failure: Option<String>,
    cleanup_failure: Option<String>,
    stages: Vec<Stage>,
    ttl_verified: bool,
    holder_disconnect_verified: bool,
    wall_elapsed_ns: u128,
    process_rss_bytes: Option<u64>,
    process_peak_rss_bytes: Option<u64>,
    cleanup_status: &'static str,
    #[serde(skip)]
    path: PathBuf,
}
impl Report {
    fn new() -> Result<Self, BenchFailure> {
        let counts = std::env::var("FITZ_LEASE_PRESSURE_CONTENDERS")
            .unwrap_or_else(|_| "1,32,100,128,256".into())
            .split(',')
            .map(|v| {
                v.parse::<usize>()
                    .map_err(|e| BenchFailure::validation(e.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        if counts.is_empty()
            || counts.len() > 16
            || counts.iter().any(|v| *v == 0 || *v > 256)
            || counts.windows(2).any(|v| v[0] >= v[1])
        {
            return Err(BenchFailure::validation(
                "contender list must strictly increase, 1..256, at most16 entries",
            ));
        }
        let directory = PathBuf::from("target/fitz-stress/lease-pressure");
        std::fs::create_dir_all(&directory).map_err(BenchFailure::transport)?;
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(BenchFailure::transport)?
            .as_nanos();
        Ok(Self {
            schema: "fitz.lease-pressure.v1",
            source_sha: git(&["rev-parse", "HEAD"]).map(|v| v.trim().into()),
            source_dirty: git(&["status", "--porcelain"]).map(|v| !v.is_empty()),
            counts,
            status: "running",
            failure: None,
            cleanup_failure: None,
            stages: Vec::new(),
            ttl_verified: false,
            holder_disconnect_verified: false,
            wall_elapsed_ns: 0,
            process_rss_bytes: None,
            process_peak_rss_bytes: None,
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
    tokio::time::timeout(Duration::from_secs(10), future)
        .await
        .map_err(|_| {
            BenchFailure::transport("Lease request/deferred response exceeded10 seconds")
        })?
}
async fn connect(addr: std::net::SocketAddr) -> Result<TestClient, BenchFailure> {
    bounded(async { TestClient::new(addr).await.map_err(BenchFailure::transport) }).await
}
fn acquire_token(body: &[u8], expected: u8) -> Result<u64, BenchFailure> {
    require_ok(body, "Lease ACQUIRE")?;
    oracle::token(body, expected).map_err(BenchFailure::verification)
}
async fn acquire(client: &mut TestClient, owner: &str, ttl: i32) -> Result<u64, BenchFailure> {
    let body = bounded(request(
        client,
        &build_lease_acquire_immediate(ROUTE, owner, ttl),
        400,
    ))
    .await?;
    acquire_token(&body, 0)
}
async fn release(client: &mut TestClient, owner: &str, token: u64) -> Result<(), BenchFailure> {
    let body = bounded(request(
        client,
        &build_lease_release(ROUTE, owner, token),
        402,
    ))
    .await?;
    empty_success(&body, "Lease RELEASE")
}
fn waiting(owner: &str) -> Vec<u8> {
    let mut e = PayloadEncoder::new();
    e.put_string(ROUTE);
    e.put_string(owner);
    e.put_u64(30);
    e.put_u32(30);
    let mut f = TlvFrameBuilder::new();
    f.encode_field(400, &e.finish());
    f.build()
}
async fn query(client: &mut TestClient) -> Result<(Option<String>, usize), BenchFailure> {
    let body = bounded(request(client, &build_lease_query(ROUTE), 403)).await?;
    query_body(&body)
}
fn query_body(body: &[u8]) -> Result<(Option<String>, usize), BenchFailure> {
    require_ok(body, "Lease QUERY")?;
    let mut d = PayloadDecoder::new(body);
    d.get_u8().map_err(BenchFailure::validation)?;
    let held = d.get_u8().map_err(BenchFailure::validation)?;
    let owner = match held {
        0 => None,
        1 => {
            let owner = d
                .get_string_ref()
                .map_err(BenchFailure::validation)?
                .to_owned();
            d.get_u64().map_err(BenchFailure::validation)?;
            Some(owner)
        }
        _ => return Err(BenchFailure::validation("invalid Lease holder flag")),
    };
    let count = usize::try_from(d.get_u32().map_err(BenchFailure::validation)?)
        .map_err(BenchFailure::transport)?;
    complete(&d)?;
    Ok((owner, count))
}
fn expected_owner(actual: Option<&str>, owner: &str) -> bool {
    actual
        .and_then(|v| v.strip_prefix("session:"))
        .and_then(|v| v.split_once(':'))
        .is_some_and(|(session, logical)| session.parse::<u64>().is_ok() && logical == owner)
}
async fn assert_query(
    observer: &mut TestClient,
    owner: Option<&str>,
    waiters: usize,
) -> Result<(), BenchFailure> {
    let (actual, count) = query(observer).await?;
    if count != waiters
        || match owner {
            Some(v) => !expected_owner(actual.as_deref(), v),
            None => actual.is_some(),
        }
    {
        return Err(BenchFailure::verification(format!(
            "Lease holder/waiters mismatch: {actual:?}/{count}, expected{owner:?}/{waiters}"
        )));
    }
    Ok(())
}
async fn wait_empty(observer: &mut TestClient) -> Result<(), BenchFailure> {
    let start = Instant::now();
    loop {
        let body = bounded(request(observer, &build_lease_query(ROUTE), 403)).await?;
        let expired = oracle::pending_expiry(&body).map_err(BenchFailure::validation)?;
        if !expired {
            let (owner, count) = query_body(&body)?;
            if owner.is_none() && count == 0 {
                return Ok(());
            }
        }
        if start.elapsed() > Duration::from_secs(5) {
            return Err(BenchFailure::verification(
                "Lease ephemeral state did not clear within5 seconds",
            ));
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
type Contender = (TestClient, String);
type Waiter = (u64, TestClient, String);
async fn contention(
    stage: &mut Stage,
    clients: Vec<Contender>,
) -> Result<Vec<Contender>, BenchFailure> {
    let mut work = FuturesUnordered::new();
    for (mut client, owner) in clients {
        work.push(async move {
            let result = bounded(request(
                &mut client,
                &build_lease_acquire_immediate(ROUTE, &owner, 30),
                400,
            ))
            .await;
            (client, owner, result)
        });
    }
    let mut clients = Vec::new();
    while let Some((client, owner, body)) = work.next().await {
        let body = body?;
        if error_code(&body, "contender ACQUIRE")? != u32::from(lease::ERR_LEASE_HELD) {
            return Err(BenchFailure::verification(
                "two live holders or unexpected contention response",
            ));
        }
        stage.immediate_held_rejections += 1;
        clients.push((client, owner));
    }
    Ok(clients)
}
async fn admission(
    stage: &mut Stage,
    clients: Vec<Contender>,
) -> Result<Vec<Waiter>, BenchFailure> {
    let mut work = FuturesUnordered::new();
    for (mut client, owner) in clients {
        work.push(async move {
            let result = bounded(request(&mut client, &waiting(&owner), 400)).await;
            (client, owner, result)
        });
    }
    let mut queued = Vec::new();
    while let Some((client, owner, body)) = work.next().await {
        let body = body?;
        if body.first() == Some(&0) {
            let token = acquire_token(&body, 2)?;
            stage.queued += 1;
            queued.push((token, client, owner));
        } else if error_code(&body, "waiter ACQUIRE")? == u32::from(lease::ERR_QUEUE_FULL) {
            stage.queue_full_rejections += 1;
            bounded(async { client.close().await.map_err(BenchFailure::transport) }).await?;
        } else {
            return Err(BenchFailure::verification(
                "unexpected waiter admission response",
            ));
        }
    }
    Ok(queued)
}
async fn disconnect_waiters(
    stage: &mut Stage,
    queued: Vec<Waiter>,
    observer: &mut TestClient,
) -> Result<Vec<Waiter>, BenchFailure> {
    // Remove every fourth queued session; this tests cleanup without extending a session across reconnect.
    let mut remaining = Vec::new();
    for (i, (token, client, owner)) in queued.into_iter().enumerate() {
        if i.is_multiple_of(4) && stage.queued > 1 {
            bounded(async { client.close().await.map_err(BenchFailure::transport) }).await?;
            stage.disconnected_waiters += 1;
        } else {
            remaining.push((token, client, owner));
        }
    }
    let wait = Instant::now();
    loop {
        let (actual, pending) = query(observer).await?;
        if expected_owner(actual.as_deref(), "holder") && pending == remaining.len() {
            break;
        }
        if wait.elapsed() > Duration::from_secs(5) {
            return Err(BenchFailure::verification(
                "disconnected waiters were not cleaned",
            ));
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Ok(remaining)
}
async fn stage(
    ctx: &StressContext,
    report: &mut Report,
    addr: std::net::SocketAddr,
    count: usize,
    last: &mut u64,
) -> Result<(), BenchFailure> {
    let mut holder = connect(addr).await?;
    let mut observer = connect(addr).await?;
    let mut clients = Vec::with_capacity(count);
    for i in 0..count {
        clients.push((connect(addr).await?, format!("contender-{i}")));
    }
    report.stages.push(Stage {
        contenders: count,
        ..Stage::default()
    });
    let idx = report.stages.len() - 1;
    let start = Instant::now();
    let token = acquire(&mut holder, "holder", 30).await?;
    if token <= *last {
        return Err(BenchFailure::verification("fencing token did not advance"));
    }
    *last = token;
    let clients = contention(&mut report.stages[idx], clients).await?;
    assert_query(&mut observer, Some("holder"), 0).await?;
    let mut queued = admission(&mut report.stages[idx], clients).await?;
    report.stages[idx].elapsed_ns = start.elapsed().as_nanos();
    report.save()?;
    if queued.len() != count.min(MAX_WAITERS)
        || report.stages[idx].queue_full_rejections
            != u64::try_from(count.saturating_sub(MAX_WAITERS)).unwrap_or(u64::MAX)
    {
        return Err(BenchFailure::verification(
            "Lease did not enforce declared100-waiter boundary",
        ));
    }
    assert_query(&mut observer, Some("holder"), queued.len()).await?;
    queued.sort_by_key(|v| v.0);
    let remaining = disconnect_waiters(&mut report.stages[idx], queued, &mut observer).await?;
    release(&mut holder, "holder", token).await?;
    let total = remaining.len();
    for (i, (expected, mut client, owner)) in remaining.into_iter().enumerate() {
        let bytes = bounded(async {
            client
                .recv_frame_bytes_without_timeout()
                .await
                .map_err(BenchFailure::transport)
        })
        .await?;
        let body = super::ephemeral::body_for(&bytes, 400)?;
        let granted = acquire_token(body, 0)?;
        if granted != expected || granted <= *last {
            return Err(BenchFailure::verification(
                "deferred grant changed token or FIFO fencing order",
            ));
        }
        *last = granted;
        assert_query(&mut observer, Some(&owner), total - i - 1).await?;
        let stale = bounded(request(
            &mut client,
            &build_lease_release(ROUTE, &owner, granted - 1),
            402,
        ))
        .await?;
        if error_code(&stale, "stale release")? != u32::from(lease::ERR_INVALID_FENCE) {
            return Err(BenchFailure::verification(
                "stale token RELEASE was not fenced",
            ));
        }
        report.stages[idx].stale_token_rejections += 1;
        assert_query(&mut observer, Some(&owner), total - i - 1).await?;
        release(&mut client, &owner, granted).await?;
        report.stages[idx].grants_verified += 1;
        report.stages[idx].elapsed_ns = start.elapsed().as_nanos();
        ctx.progress_handle().advance();
        bounded(async { client.close().await.map_err(BenchFailure::transport) }).await?;
    }
    wait_empty(&mut observer).await?;
    let fresh = acquire(&mut holder, "holder", 30).await?;
    if fresh <= *last {
        return Err(BenchFailure::verification(
            "post-pressure fencing did not advance",
        ));
    }
    *last = fresh;
    release(&mut holder, "holder", fresh).await?;
    assert_query(&mut observer, None, 0).await?;
    report.stages[idx].recovery_verified = true;
    report.stages[idx].elapsed_ns = start.elapsed().as_nanos();
    report.save()?;
    bounded(async { holder.close().await.map_err(BenchFailure::transport) }).await?;
    bounded(async { observer.close().await.map_err(BenchFailure::transport) }).await
}
async fn boundaries(
    ctx: &StressContext,
    report: &mut Report,
    addr: std::net::SocketAddr,
    last: &mut u64,
) -> Result<(), BenchFailure> {
    let mut holder = connect(addr).await?;
    let mut observer = connect(addr).await?;
    let token = acquire(&mut holder, "ttl", 1).await?;
    if token <= *last {
        return Err(BenchFailure::verification("TTL token did not advance"));
    }
    *last = token;
    wait_empty(&mut observer).await?;
    let token = acquire(&mut holder, "disconnect", 30).await?;
    if token <= *last {
        return Err(BenchFailure::verification("post-TTL token did not advance"));
    }
    *last = token;
    report.ttl_verified = true;
    bounded(async { holder.close().await.map_err(BenchFailure::transport) }).await?;
    wait_empty(&mut observer).await?;
    let mut replacement = connect(addr).await?;
    let token = acquire(&mut replacement, "replacement", 30).await?;
    if token <= *last {
        return Err(BenchFailure::verification(
            "new session token did not advance",
        ));
    }
    release(&mut replacement, "replacement", token).await?;
    assert_query(&mut observer, None, 0).await?;
    report.holder_disconnect_verified = true;
    ctx.progress_handle().advance();
    bounded(async { replacement.close().await.map_err(BenchFailure::transport) }).await?;
    bounded(async { observer.close().await.map_err(BenchFailure::transport) }).await
}
pub(crate) fn run(ctx: &mut StressContext) -> StressResult {
    execute(ctx).map_err(|e| StressError::new(e.to_string()))
}
fn execute(ctx: &mut StressContext) -> Result<(), BenchFailure> {
    let started = Instant::now();
    let mut report = Report::new()?;
    ctx.parameter("workload", "lease_shared_route_waiter_pressure");
    ctx.metadata(
        "durability_scope",
        "ephemeral_process_local_tokens_and_sessions",
    );
    ctx.metadata("target_class", "stress_characterization");
    report.save()?;
    let runtime = shared_bench_runtime();
    let fixture = match runtime.block_on(bounded(BrokerFixture::start(
        StorageProfile::Memory,
        StorageDirectory::new(StorageProfile::Memory)?,
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
    let mut last = 0;
    let mut result = runtime.block_on(async {
        for count in report.counts.clone() {
            stage(ctx, &mut report, fixture.tcp_addr(), count, &mut last).await?;
        }
        boundaries(ctx, &mut report, fixture.tcp_addr(), &mut last).await
    });
    if let Err(error) = &result {
        report.status = "failed";
        report.failure = Some(error.to_string());
    }
    report.cleanup_status = "running";
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
    drop(storage);
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
    for s in &report.stages {
        let failed = u64::from(!s.recovery_verified);
        ctx.record_external_outcome(
            format!("contenders_{}", s.contenders),
            Duration::from_nanos(u64::try_from(s.elapsed_ns).unwrap_or(u64::MAX)),
            LogicalUnit::new("validated_waiter_grant_release"),
            OperationOutcome::new(s.grants_verified + failed, s.grants_verified).failures(failed),
        );
    }
    result
}
