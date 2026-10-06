use super::super::types::BenchFailure;
use super::{
    io,
    ledger::Ledger,
    report::{Report, Stage},
};
use cntryl_stress::StressContext;
use fitz::testkit::{TestClient, TestServer};
use std::time::{Duration, Instant};

async fn connect(server: &TestServer) -> Result<TestClient, BenchFailure> {
    io::bounded(async {
        TestClient::new(server.tcp_addr)
            .await
            .map_err(BenchFailure::transport)
    })
    .await
}

pub async fn stage(
    ctx: &StressContext,
    server: &mut Option<TestServer>,
    report: &mut Report,
    count: usize,
    mode: u8,
    generation: u64,
    recovery: bool,
) -> Result<(), BenchFailure> {
    report.stages.push(Stage {
        definitions: count,
        mode,
        recovery,
        ..Stage::default()
    });
    report.save()?;
    let result = execute(ctx, server, report, count, mode, generation).await;
    if let Err(error) = &result {
        report.stages.last_mut().expect("stage exists").failure = Some(error.to_string());
    }
    let saved = report.save();
    result.and(saved)
}

async fn execute(
    ctx: &StressContext,
    server: &mut Option<TestServer>,
    report: &mut Report,
    count: usize,
    mode: u8,
    generation: u64,
) -> Result<(), BenchFailure> {
    let mut writer = connect(server.as_ref().expect("running server")).await?;
    let started = Instant::now();
    for index in 0..count {
        let request_started = Instant::now();
        io::create(&mut writer, index, generation, mode).await?;
        // Validated setup work advances liveness, never measured receipt counts.
        ctx.progress_handle().advance();
        let stage = report.stages.last_mut().expect("stage exists");
        stage.accepted_definitions += 1;
        stage.create_latencies.record(request_started.elapsed());
        if index % 32 == 31 {
            report.save()?;
        }
    }
    report
        .stages
        .last_mut()
        .expect("stage exists")
        .create_elapsed_ns = started.elapsed().as_nanos();
    io::bounded(async { writer.close().await.map_err(BenchFailure::transport) }).await?;
    let old = server.take().expect("running server");
    let restart_started = Instant::now();
    io::bounded(async { old.shutdown().await.map_err(BenchFailure::transport) }).await?;
    *server = Some(
        io::bounded(async {
            TestServer::start_with_local_storage(
                report.local_storage_path.to_string_lossy().into_owned(),
            )
            .await
            .map_err(BenchFailure::transport)
        })
        .await?,
    );
    let active = server.as_ref().expect("restarted server");
    let mut writer = connect(active).await?;
    io::verify_definitions(&mut writer, count, generation, mode, &ctx.progress_handle()).await?;
    report
        .stages
        .last_mut()
        .expect("stage exists")
        .restart_elapsed_ns = restart_started.elapsed().as_nanos();
    report
        .stages
        .last_mut()
        .expect("stage exists")
        .restart_verified = true;
    report.save()?;
    fire(ctx, active, report, count, mode, generation).await?;
    for index in 0..count {
        io::cancel(&mut writer, index).await?;
        ctx.progress_handle().advance();
    }
    io::verify_definitions(&mut writer, 0, generation, mode, &ctx.progress_handle()).await?;
    report
        .stages
        .last_mut()
        .expect("stage exists")
        .cancelled_verified = true;
    io::bounded(async { writer.close().await.map_err(BenchFailure::transport) }).await
}

async fn fire(
    ctx: &StressContext,
    server: &TestServer,
    report: &mut Report,
    count: usize,
    mode: u8,
    generation: u64,
) -> Result<(), BenchFailure> {
    let (sender, mut receiver) = tokio::sync::mpsc::channel(128);
    let mut tasks = tokio::task::JoinSet::new();
    for index in 0..2 {
        let mut client = connect(server).await?;
        let subscription = io::subscribe(&mut client).await?;
        let sender = sender.clone();
        tasks.spawn(async move {
            loop {
                let result = client
                    .recv_frame_bytes_without_timeout()
                    .await
                    .map_err(BenchFailure::transport)
                    .and_then(|frame| io::delivery(&frame, subscription, generation, count));
                let failed = result.is_err();
                if sender.send((index, result)).await.is_err() || failed {
                    break;
                }
            }
        });
    }
    drop(sender);
    let mut ledger = Ledger::new(count, mode == 0);
    let started = Instant::now();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(report.fire_deadline_seconds);
    let result = async {
        // Exactly one reset: further ordinary scans consume the remaining due heap.
        server
            .force_schedule_scan_for_tests(count)
            .map_err(BenchFailure::transport)?;
        while ledger.completed < u64::try_from(count).unwrap_or(u64::MAX) {
            let (recipient, outcome) = tokio::time::timeout_at(deadline, receiver.recv())
                .await
                .map_err(|_| {
                    BenchFailure::verification(
                        "healthy Schedule receipt window exhausted; delivery is best effort",
                    )
                })?
                .ok_or_else(|| BenchFailure::transport("Schedule receiver tasks stopped"))?;
            let occurrence = outcome?;
            if ledger
                .receive(occurrence, recipient)
                .map_err(BenchFailure::verification)?
            {
                ctx.progress_handle().advance();
            }
            let stage = report.stages.last_mut().expect("stage exists");
            stage.completed_occurrences = ledger.completed;
            stage.received = ledger.received;
            stage.fire_elapsed_ns = started.elapsed().as_nanos();
            if ledger.completed.is_multiple_of(32) {
                report.save()?;
            }
        }
        ledger.verify().map_err(BenchFailure::verification)?;
        // Catch queued duplicates, without claiming absence beyond this bounded window.
        if let Ok(event) = tokio::time::timeout(Duration::from_millis(250), receiver.recv()).await {
            let (_, result) =
                event.ok_or_else(|| BenchFailure::transport("receiver tasks stopped"))?;
            result?;
            return Err(BenchFailure::verification(
                "extra Schedule delivery after complete occurrence set",
            ));
        }
        Ok(())
    }
    .await;
    report
        .stages
        .last_mut()
        .expect("stage exists")
        .fire_elapsed_ns = started.elapsed().as_nanos();
    let stage = report.stages.last_mut().expect("stage exists");
    stage.pending_claims = server.runtime.schedule_pending_fire_claims();
    stage.pending_ack_retries = server.runtime.schedule_pending_ack_retries();
    stage.acknowledgement_failures = server.runtime.schedule_ack_failures();
    (stage.process_rss_bytes, stage.process_peak_rss_bytes) =
        super::super::artifacts::memory_sample();
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
    result
}
