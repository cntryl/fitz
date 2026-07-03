use super::*;
use fitz::benchkit::create_bench_store;
use fitz::domains::queue::QueueResponse;
use fitz::domains::stream::protocol::{StreamReadItem, StreamWriteMode};
use fitz::domains::stream::store::{CommitRecordsParams, EventPayload};
use fitz::domains::stream::{StreamActor, StreamStore};
use fitz::runtime::routing::RouteFamily;

pub(super) fn measure_recovery_matrix() -> Vec<RecoveryRow> {
    let mut rows = Vec::new();
    for stream_events in [10_000usize, 100_000, 1_000_000] {
        for queue_depth in [0usize, 100_000] {
            rows.push(measure_recovery_case(stream_events, queue_depth));
        }
    }
    rows
}

pub(super) fn recovery_is_queue_depth_isolated(rows: &[RecoveryRow]) -> bool {
    let grows_with_log = recovery_us(rows, 100_000, 0) > recovery_us(rows, 10_000, 0)
        && recovery_us(rows, 1_000_000, 0) > recovery_us(rows, 100_000, 0);
    let queue_isolated = [10_000usize, 100_000, 1_000_000]
        .into_iter()
        .all(|stream_events| {
            let baseline = recovery_us(rows, stream_events, 0);
            let with_queue = recovery_us(rows, stream_events, 100_000);
            let allowed = (baseline / 10).max(50_000);
            with_queue.abs_diff(baseline) <= allowed
        });
    grows_with_log && queue_isolated
}

fn measure_recovery_case(stream_events: usize, queue_depth: usize) -> RecoveryRow {
    let store = create_bench_store();
    seed_stream_store(store.clone(), stream_events);
    if queue_depth > 0 {
        seed_queue_depth(store.clone(), stream_events, queue_depth);
    }
    let actor = StreamActor::new(
        RouteFamily::new(FAMILY_ID),
        "proof".to_string(),
        "recovery".to_string(),
        "events".to_string(),
        Arc::new(StreamStore::new(store)),
    )
    .expect("recover proof stream actor");

    let start = Instant::now();
    let recovered_events = replay_stream_events(&actor, stream_events);
    let elapsed = start.elapsed();
    RecoveryRow {
        stream_events,
        queue_depth,
        recovered_events,
        recovery_us: duration_to_us(elapsed),
        events_sec: if elapsed.is_zero() {
            0.0
        } else {
            recovered_events as f64 / elapsed.as_secs_f64()
        },
    }
}

fn seed_stream_store(store: Arc<cntryl_midge::Engine>, event_count: usize) {
    let stream_store = StreamStore::new(store);
    let mut expected_offset = 0u64;
    while usize::try_from(expected_offset).expect("offset") < event_count {
        let remaining = event_count - usize::try_from(expected_offset).expect("offset");
        let batch_size = remaining.min(STREAM_SEED_BATCH);
        let events = (0..batch_size)
            .map(|_| EventPayload {
                body: Bytes::from_static(STREAM_EVENT_BYTES),
                metadata: None,
                discriminator: None,
            })
            .collect::<Vec<_>>();
        stream_store
            .commit_records(CommitRecordsParams {
                family: FAMILY_ID,
                realm: "proof",
                area: "recovery",
                resource: "events",
                expected_resource_next_offset: expected_offset,
                events: &events,
                ingest_metadata: None,
                mode: StreamWriteMode::Buffered,
            })
            .expect("seed stream proof events");
        expected_offset += u64::try_from(batch_size).expect("batch size");
    }
}

fn seed_queue_depth(store: Arc<cntryl_midge::Engine>, stream_events: usize, queue_depth: usize) {
    let mut actor = queue_actor_on_store(
        store,
        "proof",
        "recovery",
        &format!("queue-depth-{stream_events}"),
    );
    let batch = (0..QUEUE_SEED_BATCH)
        .map(|_| (Bytes::from_static(QUEUE_MESSAGE_BYTES), None))
        .collect::<Vec<_>>();
    let mut remaining = queue_depth;
    while remaining > 0 {
        let batch_size = remaining.min(QUEUE_SEED_BATCH);
        let response = actor.handle_send_batch(&batch[..batch_size]);
        assert!(matches!(response, QueueResponse::SentBatch { .. }));
        remaining -= batch_size;
    }
}

fn replay_stream_events(actor: &StreamActor, expected_count: usize) -> usize {
    let mut offset = 0u64;
    let mut recovered = 0usize;
    while recovered < expected_count {
        let response = actor
            .read(offset, STREAM_READ_LIMIT, None)
            .expect("read proof stream recovery batch");
        let event_count = response
            .items
            .iter()
            .filter(|item| matches!(item, StreamReadItem::Event(_)))
            .count();
        assert!(
            event_count > 0,
            "stream replay stopped before expected count"
        );
        recovered += event_count;
        offset = response.cursor.last_resource_offset.saturating_add(1);
    }
    recovered
}

fn recovery_us(rows: &[RecoveryRow], stream_events: usize, queue_depth: usize) -> u64 {
    rows.iter()
        .find(|row| row.stream_events == stream_events && row.queue_depth == queue_depth)
        .map_or(0, |row| row.recovery_us)
}
