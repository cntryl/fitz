use super::durable::{
    empty_success, kv::KvDriver, queue::QueueDriver, request, response_payload,
    stream::StreamDriver,
};
use super::fixture::{StorageDirectory, StorageProfile};
use super::runner::{bounded, configured_duration};
use super::types::BenchFailure;
use cntryl_stress::{LogicalUnit, OperationOutcome, StressContext, StressError, StressResult};
use fitz::benchkit::{build_schedule_create, shared_bench_runtime};
use fitz::protocol::payload_codec::{PayloadDecoder, PayloadEncoder};
use fitz_e2e::testkit::{FrameBuilder, TestClient, TestServer};
use std::collections::HashMap;
use std::time::{Duration, Instant};

const SCHEDULE_ROUTE: &str = "schedule://stress-bench/jobs/mixed/run";
const SCHEDULE_BODY: &[u8] = b"mixed endurance delivery";
const BACKLOG: usize = 4;

struct ScheduleSubscriber {
    client: TestClient,
    id: u64,
}

impl ScheduleSubscriber {
    async fn connect(server: &TestServer) -> Result<Self, BenchFailure> {
        let mut client = TestClient::new(server.tcp_addr)
            .await
            .map_err(BenchFailure::transport)?;
        let mut encoder = PayloadEncoder::new();
        encoder.put_string(SCHEDULE_ROUTE);
        let mut frame = FrameBuilder::new();
        frame.encode_field(703, &encoder.finish());
        let response = request(&mut client, &frame.build(), 703).await?;
        let mut decoder = PayloadDecoder::new(&response);
        let status = decoder.get_u8().map_err(BenchFailure::validation)?;
        let id = decoder
            .get_optional_u64()
            .map_err(BenchFailure::validation)?
            .unwrap_or_default();
        if status != 0 || id == 0 || !decoder.is_complete() {
            return Err(BenchFailure::verification(
                "invalid Schedule subscription receipt",
            ));
        }
        Ok(Self { client, id })
    }

    async fn delivery(&mut self) -> Result<(), BenchFailure> {
        let frame = self
            .client
            .recv_frame_bytes_without_timeout()
            .await
            .map_err(BenchFailure::transport)?;
        let body = response_payload(&frame, 705)?;
        let mut decoder = PayloadDecoder::new(&body);
        let id = decoder.get_u64().map_err(BenchFailure::validation)?;
        let route = decoder.get_string_ref().map_err(BenchFailure::validation)?;
        let payload = decoder.get_bytes().map_err(BenchFailure::validation)?;
        if id != self.id
            || route != SCHEDULE_ROUTE
            || payload.as_ref() != SCHEDULE_BODY
            || !decoder.is_complete()
        {
            return Err(BenchFailure::verification(
                "Schedule delivered the wrong subscription, route or payload",
            ));
        }
        Ok(())
    }
}

async fn queue_cycle(
    queue: &mut QueueDriver,
    cycle: u64,
) -> Result<[(u64, u64); BACKLOG], BenchFailure> {
    let width = u64::try_from(BACKLOG).expect("bounded Queue backlog");
    let first = cycle * width;
    let mut entries = [(0, 0); BACKLOG];
    for (offset, entry) in (0..width).zip(&mut entries) {
        let sequence = first + offset;
        let id = queue.enqueue(sequence).await?.ok_or_else(|| {
            BenchFailure::verification("Queue rejected a bounded backlog enqueue")
        })?;
        *entry = (id, sequence);
    }
    for (id, sequence) in entries {
        queue.acknowledge(id, sequence).await?;
    }
    queue.verify().await?;
    Ok(entries)
}

async fn schedule_fires(
    subscriber: &mut ScheduleSubscriber,
    duration: Duration,
) -> Result<u64, BenchFailure> {
    let deadline = tokio::time::Instant::now() + duration;
    let mut count = 0;
    while tokio::time::Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        match tokio::time::timeout(
            remaining.min(Duration::from_secs(75)),
            subscriber.delivery(),
        )
        .await
        {
            Ok(delivery) => {
                delivery?;
                count += 1;
            }
            Err(_) if remaining > Duration::from_secs(75) => {
                return Err(BenchFailure::verification(
                    "Schedule missed its one-minute fire window",
                ));
            }
            Err(_) => break,
        }
    }
    Ok(count)
}

async fn create_schedule(address: std::net::SocketAddr) -> Result<(), BenchFailure> {
    let mut client = TestClient::new(address)
        .await
        .map_err(BenchFailure::transport)?;
    let create = build_schedule_create(SCHEDULE_ROUTE, "* * * * *", SCHEDULE_BODY);
    empty_success(
        &request(&mut client, &create, 700).await?,
        "Schedule CREATE",
    )?;
    client.close().await.map_err(BenchFailure::transport)
}

async fn open(path: &str) -> Result<TestServer, BenchFailure> {
    TestServer::start_with_local_storage(path.to_owned())
        .await
        .map_err(BenchFailure::transport)
}

async fn exercise(ctx: &mut StressContext, duration: Duration) -> Result<(), BenchFailure> {
    let directory = StorageDirectory::new(StorageProfile::LocalDisk)?;
    let path = directory
        .path()
        .and_then(std::path::Path::to_str)
        .ok_or_else(|| BenchFailure::validation("storage path must be UTF-8"))?;
    let server = bounded(open(path)).await?;
    let mut queue = bounded(QueueDriver::open(server.tcp_addr, 0)).await?;
    let mut kv = bounded(KvDriver::connect(server.tcp_addr, 0)).await?;
    let mut stream = bounded(StreamDriver::connect(server.tcp_addr, 0)).await?;
    let mut subscriber = bounded(ScheduleSubscriber::connect(&server)).await?;
    bounded(create_schedule(server.tcp_addr)).await?;

    // A reduced smoke can finish before the first wall-clock minute.
    if duration < Duration::from_secs(75) {
        server
            .force_schedule_scan_for_tests(1)
            .map_err(BenchFailure::transport)?;
    }
    let progress = ctx.progress_handle();
    let started = Instant::now();
    let mut acknowledged = HashMap::new();
    let work = async {
        let mut cycles = 0;
        while started.elapsed() < duration || cycles == 0 {
            let next = cycles + 1;
            let (entries, written, _) = bounded(async {
                tokio::try_join!(
                    queue_cycle(&mut queue, next),
                    kv.step(next),
                    stream.verify()
                )
            })
            .await?;
            if !written.is_completed() {
                return Err(BenchFailure::verification(
                    "KV rejected a bounded mixed load commit",
                ));
            }
            for (id, sequence) in entries {
                if acknowledged.insert(id, sequence).is_some() {
                    return Err(BenchFailure::verification(
                        "Queue reused a live-run message ID",
                    ));
                }
            }
            cycles = next;
            progress.advance();
        }
        Ok(cycles)
    };
    let (cycles, fires) = tokio::try_join!(work, schedule_fires(&mut subscriber, duration))?;
    let elapsed = started.elapsed();
    if fires == 0 {
        return Err(BenchFailure::verification(
            "mixed load observed no Schedule deliveries",
        ));
    }
    bounded(kv.verify()).await?;
    bounded(queue.close()).await?;
    bounded(stream.close()).await?;
    drop(subscriber);
    bounded(async { server.shutdown().await.map_err(BenchFailure::transport) }).await?;
    progress.advance();

    let reopened = bounded(open(path)).await?;
    let mut queue = bounded(QueueDriver::open(reopened.tcp_addr, 0)).await?;
    let mut stream = bounded(StreamDriver::open(reopened.tcp_addr, 0)).await?;
    let mut subscriber = bounded(ScheduleSubscriber::connect(&reopened)).await?;
    bounded(kv.reconnect(reopened.tcp_addr)).await?;
    let redelivered = bounded(async {
        let (redelivered, _, _) = tokio::try_join!(
            queue.drain_redeliveries(&mut acknowledged),
            kv.verify(),
            stream.verify()
        )?;
        // A new subscription only observes a fire if the durable definition was recovered.
        reopened
            .force_schedule_scan_for_tests(1)
            .map_err(BenchFailure::transport)?;
        subscriber.delivery().await?;
        Ok(redelivered)
    })
    .await?;
    bounded(queue.close()).await?;
    bounded(stream.close()).await?;
    bounded(kv.close()).await?;
    drop(subscriber);
    bounded(async { reopened.shutdown().await.map_err(BenchFailure::transport) }).await?;

    directory.release()?;
    ctx.record_external_outcome(
        "mixed_local_restart",
        elapsed,
        LogicalUnit::new("verified_mixed_cycle"),
        OperationOutcome::new(cycles, cycles),
    );
    ctx.parameter("schedule_deliveries", fires);
    ctx.parameter("queue_backlog", BACKLOG);
    ctx.parameter("queue_redeliveries_after_reopen", redelivered);
    ctx.parameter("restart", "graceful_local_reopen");
    Ok(())
}

pub(crate) fn run(ctx: &mut StressContext) -> StressResult {
    let duration = configured_duration(6).map_err(|error| StressError::new(error.to_string()))?;
    shared_bench_runtime()
        .block_on(exercise(ctx, duration))
        .map_err(|error| StressError::new(error.to_string()))
}
