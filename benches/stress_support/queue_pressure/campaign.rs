use super::super::types::BenchFailure;
use super::{
    config::Config,
    io::{self, Delivery},
    ledger::Ledger,
    report::{Report, Stage},
};
use cntryl_stress::{ProgressHandle, StressContext};
use fitz::testkit::TestClient;
use futures_util::{stream::FuturesUnordered, StreamExt};
use std::fmt::Write;
use std::net::SocketAddr;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::time::{Duration, Instant};

#[derive(Default)]
struct State {
    ledger: Ledger,
    last_ack: Option<Instant>,
    reserve_rejections: u64,
}

async fn connect(address: SocketAddr) -> Result<TestClient, BenchFailure> {
    io::bounded(async {
        TestClient::new(address)
            .await
            .map_err(BenchFailure::transport)
    })
    .await
}

async fn consumer(
    mut client: TestClient,
    shared: Arc<Mutex<State>>,
    stop: Arc<AtomicBool>,
    delay: Duration,
    progress: ProgressHandle,
) -> Result<TestClient, BenchFailure> {
    while !stop.load(Ordering::Relaxed) {
        let pause = match io::bounded(io::consume(&mut client)).await? {
            Delivery::Acknowledged { sequence, id } => {
                let mut shared = shared.lock().map_err(BenchFailure::transport)?;
                shared
                    .ledger
                    .acknowledged(sequence, id)
                    .map_err(BenchFailure::verification)?;
                shared.last_ack = Some(Instant::now());
                progress.advance();
                delay
            }
            Delivery::Empty => Duration::from_millis(1),
            Delivery::Rejected => {
                shared
                    .lock()
                    .map_err(BenchFailure::transport)?
                    .reserve_rejections += 1;
                Duration::from_millis(5)
            }
        };
        tokio::time::sleep(pause).await;
    }
    Ok(client)
}

async fn produce(
    mut client: TestClient,
    sequence: usize,
    scheduled: tokio::time::Instant,
) -> (
    TestClient,
    usize,
    Result<Option<u64>, BenchFailure>,
    Duration,
) {
    let result = io::bounded(io::enqueue(&mut client, sequence)).await;
    (client, sequence, result, scheduled.elapsed())
}

fn record_reply(
    stage: &mut Stage,
    shared: &Mutex<State>,
    sequence: usize,
    result: Result<Option<u64>, BenchFailure>,
    elapsed: Duration,
) -> Result<(), BenchFailure> {
    stage.enqueue_latencies.record(elapsed);
    let mut shared = shared.lock().map_err(BenchFailure::transport)?;
    match result? {
        Some(id) => shared.ledger.accepted(sequence, id),
        None => shared.ledger.rejected(sequence),
    }
    .map_err(BenchFailure::verification)
}

fn snapshot(stage: &mut Stage, shared: &Mutex<State>) -> Result<(), BenchFailure> {
    {
        let oracle = shared.lock().map_err(BenchFailure::transport)?;
        stage.accounting = oracle.ledger.counts;
        stage.reserve_rejections = oracle.reserve_rejections;
    }
    stage.p95_enqueue_upper_ns = stage.enqueue_latencies.percentile_upper_ns(95);
    (stage.process_rss_bytes, stage.process_peak_rss_bytes) =
        super::super::artifacts::memory_sample();
    Ok(())
}

fn pressure(
    shared: &Mutex<State>,
    config: &Config,
) -> Result<(u64, Option<Instant>, Option<&'static str>), BenchFailure> {
    let oracle = shared.lock().map_err(BenchFailure::transport)?;
    let ledger = &oracle.ledger;
    let pending = ledger
        .counts
        .sent
        .saturating_sub(ledger.counts.rejected + ledger.counts.acknowledged);
    let limit = if ledger.counts.rejected > 0 {
        Some("broker_enqueue_rejection")
    } else if pending >= config.max_backlog {
        Some("backlog_safety_guard")
    } else if ledger.counts.sent
        >= u64::try_from(config.max_attempts_per_stage).map_err(BenchFailure::transport)?
    {
        Some("accounting_safety_guard")
    } else {
        None
    };
    Ok((ledger.counts.acknowledged, oracle.last_ack, limit))
}

/// The absolute arrival schedule never waits for a producer reply. Exhausting
/// available connections records a harness miss rather than reducing offered rate.
async fn active(
    report: &mut Report,
    stage: &mut Stage,
    shared: &Mutex<State>,
    consumer: &tokio::task::JoinHandle<Result<TestClient, BenchFailure>>,
    clients: &mut Vec<TestClient>,
    config: &Config,
) -> Result<(), BenchFailure> {
    let begin = tokio::time::Instant::now();
    let end = begin + Duration::from_secs(config.stage_seconds);
    let mut arrivals = 0_u64;
    let mut producers = FuturesUnordered::new();
    let mut last_save = Instant::now();
    let mut result = Ok(());
    stage.termination = "configured_window".into();
    loop {
        let scheduled = begin
            + Duration::from_secs(arrivals / stage.rate_per_second)
            + Duration::from_nanos(
                (arrivals % stage.rate_per_second) * 1_000_000_000 / stage.rate_per_second,
            );
        if tokio::time::Instant::now() >= end {
            break;
        }
        if consumer.is_finished() {
            result = Err(BenchFailure::verification(
                "consumer stopped during offered load",
            ));
            break;
        }
        let (_, last_ack, limit) = pressure(shared, config)?;
        if last_ack.map_or_else(|| begin.elapsed(), |last| last.elapsed()) > Duration::from_secs(60)
        {
            result = Err(BenchFailure::verification(
                "no acknowledged Queue work for 60 seconds",
            ));
            break;
        }
        if let Some(limit) = limit {
            stage.termination = limit.into();
            break;
        }
        if last_save.elapsed() >= Duration::from_secs(1) {
            stage.active_elapsed_ns = begin.elapsed().as_nanos();
            snapshot(stage, shared)?;
            report.current = Some(stage.clone());
            report.save()?;
            last_save = Instant::now();
        }
        tokio::select! {
            () = tokio::time::sleep_until(scheduled.min(end)) => {
                if scheduled >= end { break; }
                arrivals += 1;
                stage.offered += 1;
                if let Some(client) = clients.pop() {
                    let sequence = shared.lock().map_err(BenchFailure::transport)?.ledger.dispatch();
                    producers.push(produce(client, sequence, scheduled));
                } else { stage.harness_missed += 1; }
            }
            Some((client, sequence, reply, elapsed)) = producers.next(), if !producers.is_empty() => {
                clients.push(client);
                if let Err(error) = record_reply(stage, shared, sequence, reply, elapsed) { result = Err(error); break; }
            }
        }
    }
    stage.active_elapsed_ns = begin.elapsed().as_nanos();
    finish_active(report, stage, shared, &result)?;
    let settling = Instant::now();
    // Complete every issued request, even after a boundary or first error. An
    // unknown enqueue outcome cannot be discarded to make the drain pass.
    while let Some((client, sequence, reply, elapsed)) = producers.next().await {
        clients.push(client);
        if let Err(error) = record_reply(stage, shared, sequence, reply, elapsed) {
            if result.is_ok() {
                result = Err(error);
            }
        }
    }
    stage.producer_settle_elapsed_ns = settling.elapsed().as_nanos();
    result
}

fn finish_active(
    report: &mut Report,
    stage: &mut Stage,
    shared: &Mutex<State>,
    result: &Result<(), BenchFailure>,
) -> Result<(), BenchFailure> {
    {
        let oracle = shared.lock().map_err(BenchFailure::transport)?;
        stage.accepted_at_stop = oracle.ledger.counts.accepted;
        stage.acknowledged_at_stop = oracle.ledger.counts.acknowledged;
        stage.backlog_at_stop = oracle.ledger.counts.accepted_unacknowledged;
        stage.pending_enqueue_outcomes_at_stop = oracle.ledger.counts.sent
            - oracle.ledger.counts.accepted
            - oracle.ledger.counts.rejected;
    }
    let due = u64::try_from(
        (stage
            .active_elapsed_ns
            .min(u128::from(report.config.stage_seconds) * 1_000_000_000)
            * u128::from(stage.rate_per_second))
        .div_ceil(1_000_000_000),
    )
    .map_err(BenchFailure::transport)?;
    let missed = due.saturating_sub(stage.offered);
    stage.offered += missed;
    stage.harness_missed += missed;
    if stage.termination == "configured_window" && stage.harness_missed * 100 > stage.offered {
        stage.termination = "offered_rate_not_met".into();
    }
    if let Err(error) = result {
        report.failure = Some(error.to_string());
        report.status = "failed";
        stage.termination = "failed".into();
    }
    snapshot(stage, shared)?;
    report.current = Some(stage.clone());
    report.save()
}

async fn drain(
    shared: &Mutex<State>,
    consumer: &tokio::task::JoinHandle<Result<TestClient, BenchFailure>>,
    report: &mut Report,
    stage: &mut Stage,
    seconds: u64,
) -> Result<(), BenchFailure> {
    let begin = Instant::now();
    let mut saved = Instant::now();
    loop {
        let (drained, last_ack) = {
            let oracle = shared.lock().map_err(BenchFailure::transport)?;
            (oracle.ledger.verify_drained().is_ok(), oracle.last_ack)
        };
        stage.drain_elapsed_ns = begin.elapsed().as_nanos();
        if drained {
            stage.drained = true;
            return Ok(());
        }
        if consumer.is_finished() {
            return Err(BenchFailure::verification("consumer failed during drain"));
        }
        if last_ack.is_some_and(|last| last.elapsed() > Duration::from_secs(60)) {
            return Err(BenchFailure::verification(
                "no useful Queue progress during drain",
            ));
        }
        if begin.elapsed() >= Duration::from_secs(seconds) {
            return Err(BenchFailure::verification(
                "accepted Queue backlog did not drain before deadline",
            ));
        }
        if saved.elapsed() >= Duration::from_secs(1) {
            snapshot(stage, shared)?;
            report.current = Some(stage.clone());
            report.save()?;
            saved = Instant::now();
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

pub(super) async fn run_stage(
    ctx: &mut StressContext,
    report: &mut Report,
    address: SocketAddr,
    rate: u64,
) -> Result<(), BenchFailure> {
    let config = report.config.clone();
    let mut stage = Stage {
        rate_per_second: rate,
        ..Stage::default()
    };
    let shared = Arc::new(Mutex::new(State::default()));
    let stop = Arc::new(AtomicBool::new(false));
    let mut clients = Vec::new();
    for _ in 0..config.producer_connections {
        clients.push(connect(address).await?);
    }
    let consumer_client = connect(address).await?;
    let consumer = tokio::spawn(consumer(
        consumer_client,
        Arc::clone(&shared),
        Arc::clone(&stop),
        Duration::from_millis(config.consumer_delay_ms),
        ctx.progress_handle(),
    ));
    let mut result = active(
        report,
        &mut stage,
        &shared,
        &consumer,
        &mut clients,
        &config,
    )
    .await;
    if result.is_ok() {
        result = drain(&shared, &consumer, report, &mut stage, config.drain_seconds).await;
    }
    stop.store(true, Ordering::Relaxed);
    if let Err(error) = &result {
        report.status = "failed";
        report.failure = Some(error.to_string());
        stage.termination = "failed".into();
        snapshot(&mut stage, &shared)?;
        report.current = Some(stage.clone());
        if let Err(saved) = report.save() {
            append_failure(&mut result, saved, "failure artifact");
        }
    }
    match consumer.await.map_err(BenchFailure::transport) {
        Ok(Ok(mut client)) => {
            if result.is_ok() {
                result = match io::bounded(io::consume(&mut client)).await {
                    Ok(Delivery::Empty) => {
                        stage.empty_verified = true;
                        Ok(())
                    }
                    Ok(_) => Err(BenchFailure::verification("Queue not empty after drain")),
                    Err(error) => Err(error),
                };
            }
            clients.push(client);
        }
        Ok(Err(error)) | Err(error) => {
            append_failure(&mut result, error, "consumer");
        }
    }
    snapshot(&mut stage, &shared)?;
    if result.is_err() {
        stage.termination = "failed".into();
    }
    report.current = Some(stage.clone());
    report.save()?;
    for client in clients {
        if let Err(error) =
            io::bounded(async { client.close().await.map_err(BenchFailure::transport) }).await
        {
            stage.cleanup_failure = Some(error.to_string());
            if result.is_ok() {
                result = Err(error);
            }
        }
    }
    report.stages.push(stage);
    report.current = None;
    report.save()?;
    result
}

fn append_failure(result: &mut Result<(), BenchFailure>, error: BenchFailure, context: &str) {
    match result {
        Ok(()) => *result = Err(error),
        Err(original) => {
            let _ = write!(original.detail, "; {context}: {error}");
        }
    }
}

pub(super) async fn recovery(address: SocketAddr) -> Result<(), BenchFailure> {
    let mut client = connect(address).await?;
    let result = async {
        let id = io::bounded(io::enqueue(&mut client, 0))
            .await?
            .ok_or_else(|| BenchFailure::verification("recovery enqueue rejected"))?;
        match io::bounded(io::consume(&mut client)).await? {
            Delivery::Acknowledged {
                sequence: 0,
                id: actual,
            } if actual == id => {}
            _ => {
                return Err(BenchFailure::verification(
                    "recovery payload or message ID mismatch",
                ))
            }
        }
        if !matches!(
            io::bounded(io::consume(&mut client)).await?,
            Delivery::Empty
        ) {
            return Err(BenchFailure::verification("recovery left Queue work"));
        }
        Ok(())
    }
    .await;
    let closed = io::bounded(async { client.close().await.map_err(BenchFailure::transport) }).await;
    result.and(closed)
}
