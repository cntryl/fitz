//! Paced Stream subscription capacity qualification. Run each case in a fresh process.

#![allow(dead_code)] // This target selects a subset of the shared Stream transport helpers.

mod tier4_stream_support;
mod tier4_stream_transport;
mod tier4_support;

#[path = "stream_subscription_capacity/heap.rs"]
mod heap;

use fitz::benchkit::{
    build_stream_append, build_stream_begin, build_stream_commit, build_stream_unsubscribe,
    extract_single_tlv_field, shared_bench_runtime, stream_dispatch,
};
use fitz::protocol::payload_codec::PayloadDecoder;
use fitz::testkit::TestServer;
use serde_json::json;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tier4_stream_support::{TransportKind, STREAM_SYNC_COMMIT_MODE};
use tier4_stream_transport::{begin_session, request_success, subscribe, StreamBenchClient};

const HOT_ROUTE: &str = "stream://capacity/events/orders";
const HOT_WILDCARD: &str = "stream://capacity/events/*";
const DROP_COUNTER: &str = "fitz_stream_notify_drops_total";

#[derive(Clone, Copy)]
struct Shape {
    registrations: usize,
    wildcards: usize,
    readers: usize,
    slow: bool,
}

impl Shape {
    fn for_case(case: &str) -> Self {
        let (registrations, wildcards, readers, slow) = match case {
            "common-exact" => (1_024, 0, 1, false),
            "common-mixed" => (1_024, 128, 1, false),
            "large-exact" => (10_000, 0, 1, false),
            "large-wildcard" => (10_000, 10_000, 1, false),
            "hot" => (1, 0, 1_024, false),
            "slow" => (1, 0, 16, true),
            _ => panic!("unknown case {case}"),
        };
        Self {
            registrations,
            wildcards,
            readers,
            slow,
        }
    }

    fn selector(self, index: usize) -> String {
        match (index, self.wildcards) {
            (0, 0) => HOT_ROUTE.to_owned(),
            (0, _) => HOT_WILDCARD.to_owned(),
            (index, wildcards) if index < wildcards => format!("stream://capacity/other-{index}/*"),
            (index, _) => format!("stream://capacity/events/other-{index}"),
        }
    }
}

fn main() {
    let args: Vec<_> = std::env::args()
        .skip(1)
        .filter(|arg| arg != "--bench")
        .collect();
    let case = args.first().map_or("common-mixed", String::as_str);
    let transport = match args.get(1).map_or("tcp", String::as_str) {
        "tcp" => TransportKind::Tcp,
        "ws" => TransportKind::WebSocket,
        _ => panic!("expected tcp or ws"),
    };
    let count = args.get(2).map_or(160, |argument| {
        argument.parse::<usize>().expect("sample count")
    });
    let period_ms = args.get(3).map_or(75, |argument| {
        argument.parse::<u64>().expect("period in ms")
    });
    assert!(count > 0 && period_ms > 0);
    assert!(count < 10_000);

    shared_bench_runtime().block_on(async {
        let temp = tempfile::tempdir().expect("isolated Stream storage");
        let server =
            TestServer::start_with_local_storage(temp.path().to_string_lossy().into_owned())
                .await
                .expect("start benchmark broker");
        if case == "churn" {
            churn(&server, transport).await;
        } else {
            let shape = Shape::for_case(case);
            campaign(&server, case, shape, transport, count, period_ms).await;
        }
        server.shutdown().await.expect("stop benchmark broker");
    });
}

async fn campaign(
    server: &TestServer,
    case: &str,
    shape: Shape,
    transport: TransportKind,
    count: usize,
    period_ms: u64,
) {
    let mut writer = StreamBenchClient::connect(server, transport)
        .await
        .expect("connect writer");
    for offset in 0..6 {
        publish(&mut writer, "stream://capacity/warmup/orders", offset, None).await;
    }

    let mut readers = Vec::new();
    let starts: Arc<[OnceLock<Instant>]> = (0..count).map(|_| OnceLock::new()).collect();
    for _ in 0..(shape.readers + usize::from(shape.slow)) {
        let mut reader = StreamBenchClient::connect(server, transport)
            .await
            .expect("connect reader");
        for index in 0..shape.registrations {
            subscribe(&mut reader, &shape.selector(index)).await;
        }
        readers.push(reader);
    }
    let charged_bytes = server.stream_subscription_charged_bytes();
    let metrics = fitz::observability::metrics();
    let drops_at_start = metrics.counter_get(DROP_COUNTER);
    stream_dispatch::start_sampling(count * readers.len());

    let mut workers = Vec::new();
    for (index, reader) in readers.into_iter().enumerate() {
        let starts = starts.clone();
        let slow = index == shape.readers;
        workers.push(tokio::spawn(read_notifications(reader, starts, slow)));
    }

    // The pacer keeps scheduling work independently of the writer and each reader.
    let (send, mut arrivals) = tokio::sync::mpsc::unbounded_channel();
    let period = Duration::from_millis(period_ms);
    let pacer = tokio::spawn(async move {
        let mut tick = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
        for offset in 0..count {
            send.send((offset, tick.tick().await.into_std()))
                .expect("writer active");
        }
    });
    let mut commits = Vec::with_capacity(count);
    let mut start_lag = Vec::with_capacity(count);
    while let Some((offset, arrival)) = arrivals.recv().await {
        start_lag.push(arrival.elapsed());
        commits.push(
            publish(
                &mut writer,
                HOT_ROUTE,
                u64::try_from(offset).unwrap(),
                Some(&starts[offset]),
            )
            .await,
        );
    }
    pacer.await.expect("finish the configured arrival schedule");
    let mut notifications = Vec::new();
    let mut slow_notifications = Vec::new();
    for worker in workers {
        let (delivery, slow) = worker.await.expect("reader task");
        if slow {
            slow_notifications = delivery;
        } else {
            notifications.extend(delivery);
        }
    }
    let mut dispatch = stream_dispatch::finish_sampling();
    let drops = metrics.counter_get(DROP_COUNTER) - drops_at_start;
    let confirmed = notifications.len();
    let expected = count * shape.readers;
    println!(
        "{}",
        json!({
            "case": case,
            "transport": transport.label(),
            "pace_ms": period_ms,
            "commits": commits.len(),
            "dispatch_attempts": dispatch.len(),
            "confirmed_healthy": confirmed,
            "expected_healthy": expected,
            "slow_confirmed": slow_notifications.len(),
            "notify_drops": drops,
            "charged_bytes": charged_bytes,
            "p99_schedule_lag_ns": p99_ns(&mut start_lag),
            "p99_commit_ns": p99_ns(&mut commits),
            "p99_notification_ns": p99_ns(&mut notifications),
            "p99_dispatch_ns": p99_ns(&mut dispatch),
            "p99_slow_notification_ns": p99_ns(&mut slow_notifications),
        })
    );
    assert_eq!(
        confirmed, expected,
        "{case}: healthy readers must receive every notification"
    );
}

async fn publish(
    writer: &mut StreamBenchClient,
    route: &str,
    offset: u64,
    started: Option<&OnceLock<Instant>>,
) -> Duration {
    let session = begin_session(writer, &build_stream_begin(route)).await;
    request_success(writer, &build_stream_append(session, offset, &[0x5A; 128])).await;
    let frame = build_stream_commit(session, STREAM_SYNC_COMMIT_MODE);
    let now = Instant::now();
    if let Some(started) = started {
        started.set(now).unwrap();
    }
    request_success(writer, &frame).await;
    now.elapsed()
}

async fn read_notifications(
    mut reader: StreamBenchClient,
    starts: Arc<[OnceLock<Instant>]>,
    slow: bool,
) -> (Vec<Duration>, bool) {
    let mut times = Vec::with_capacity(starts.len());
    let mut seen = vec![false; starts.len()];
    if slow {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    while times.len() < starts.len() {
        let Ok(frame) = reader.recv_frame(5_000).await else {
            break;
        };
        let received = Instant::now();
        let (kind, payload) = extract_single_tlv_field(&frame);
        assert_eq!(kind, 609);
        let mut decoder = PayloadDecoder::new(&payload);
        let _id = decoder.get_u64().expect("subscription ID");
        assert_eq!(decoder.get_string().unwrap(), HOT_ROUTE);
        let body: serde_json::Value =
            serde_json::from_slice(&decoder.get_bytes().unwrap()).unwrap();
        let offset = usize::try_from(body["first_resource_offset"].as_u64().unwrap()).unwrap();
        assert!(
            !std::mem::replace(&mut seen[offset], true),
            "duplicate commit notification"
        );
        times.push(
            received.duration_since(*starts[offset].get().expect("scheduled commit timestamp")),
        );
        if slow {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    reader.close().await.expect("close reader");
    (times, slow)
}

async fn churn(server: &TestServer, transport: TransportKind) {
    let mut reader = StreamBenchClient::connect(server, transport)
        .await
        .expect("connect churn client");
    subscribe(&mut reader, HOT_ROUTE).await;
    let persistent_charge = server.stream_subscription_charged_bytes();
    let mut first_drained = 0;
    let mut maximum_drift = 0;
    let selectors: Vec<_> = (1..10_000)
        .map(|index| format!("stream://capacity/churn/item-{index}"))
        .collect();
    for cycle in 0_u8..=5 {
        for selector in &selectors {
            subscribe(&mut reader, selector).await;
        }
        let peak_charge = server.stream_subscription_charged_bytes();
        drop(server.runtime.stream_list_streams(None));
        for selector in &selectors {
            request_success(&mut reader, &build_stream_unsubscribe(selector)).await;
        }
        let rows = server.runtime.stream_list_streams(None);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].subscriptions_active, 1);
        drop(rows);
        tokio::time::sleep(Duration::from_millis(250)).await;
        let drained = heap::live_bytes();
        if cycle == 1 {
            first_drained = drained;
        }
        if cycle > 1 {
            maximum_drift = maximum_drift.max(drained.abs_diff(first_drained));
        }
        let drained_charge = server.stream_subscription_charged_bytes();
        println!(
            "{}",
            json!({
                "case": "churn", "cycle": cycle, "registered_charge": peak_charge,
                "drained_charge": drained_charge, "drained_heap_bytes": drained,
            })
        );
        assert_eq!(drained_charge, persistent_charge);
        assert!(peak_charge > drained_charge);
    }
    assert!(
        maximum_drift <= 1024 * 1024,
        "drained heap drift: {maximum_drift} bytes"
    );
    reader.close().await.expect("close churn client");
}

fn p99_ns(samples: &mut [Duration]) -> Option<u128> {
    samples.sort_unstable();
    let index = (samples.len() * 99).div_ceil(100).checked_sub(1)?;
    Some(samples[index].as_nanos())
}
