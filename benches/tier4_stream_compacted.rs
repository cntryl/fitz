//! Projection replay after many individual commands have been compacted.

#[path = "tier4_support.rs"]
mod tier4_support;

use bytes::Bytes;
use cntryl_stress::{stress, StressContext};
use fitz::benchkit::create_write_heavy_bench_store;
use fitz::domains::stream::protocol::StreamWriteMode;
use fitz::domains::stream::StreamReadItem;
use fitz::testkit::domain_internals::stream::{
    encode_compact_global_page_key, CommitRecordsParams, CompactGlobalPageValue, EventPayload,
    StreamStore,
};
use std::time::Instant;
use tier4_support::measure_operations;

#[derive(Clone, Copy)]
enum Scope {
    Area,
    Realm,
}

fn command_history(size: usize) -> (StreamStore, Vec<EventPayload>) {
    let engine = create_write_heavy_bench_store();
    let store = StreamStore::new(engine.clone());
    let events = (0..64_u64)
        .map(|offset| {
            let mut body = vec![0x45; size];
            body[..8].copy_from_slice(&offset.to_le_bytes());
            EventPayload {
                body: Bytes::from(body),
                metadata: Some(Bytes::from_static(
                    b"event-kind=OrderChanged;command=cmd-001",
                )),
                discriminator: Some("OrderChanged".into()),
            }
        })
        .collect::<Vec<_>>();
    for (offset, event) in events.iter().enumerate() {
        store
            .commit_records(CommitRecordsParams {
                family: 1,
                realm: "replay",
                area: "orders",
                resource: "order-123",
                expected_resource_next_offset: u64::try_from(offset).unwrap(),
                events: std::slice::from_ref(event),
                ingest_metadata: None,
                mode: StreamWriteMode::Sync,
            })
            .expect("commit one event per command");
    }
    for _ in 0..128 {
        if store
            .run_maintenance(1)
            .expect("compact committed command history")
            .buckets_compacted
            == 0
        {
            let txn = engine
                .begin_tx(1, cntryl_midge::TransactionMode::ReadOnly)
                .expect("inspect compacted fixture");
            assert!(
                txn.get(&encode_compact_global_page_key(31))
                    .unwrap()
                    .is_none(),
                "stale parent hint must be gone"
            );
            let mut prefix = encode_compact_global_page_key(0);
            prefix.truncate(prefix.len() - 24);
            let rows: Vec<_> = txn
                .scan(
                    &cntryl_midge::Query::new()
                        .prefix(Bytes::from(prefix))
                        .start_key(Bytes::from(encode_compact_global_page_key(0)))
                        .limit(2),
                )
                .expect("inspect generation-stamped global replacement")
                .try_collect()
                .unwrap();
            assert_eq!(
                rows.len(),
                1,
                "global history must occupy one compacted fragment"
            );
            let page = CompactGlobalPageValue::try_decode(&rows[0].1).unwrap();
            assert_eq!(
                page.records.len(),
                64,
                "fixture must merge the complete global bucket"
            );
            return (store, events);
        }
    }
    panic!("fixture must finish compaction");
}

fn measure_projection(
    ctx: &mut StressContext,
    scope: Scope,
    size: usize,
    from: u64,
    limit: u64,
    name: &'static str,
) {
    let (store, events) = command_history(size);
    ctx.parameter("scenario", "compacted_command_projection");
    ctx.parameter("measurement_scope", "direct_store");
    ctx.parameter("storage_profile", "memory");
    ctx.parameter("history_depth", 64);
    ctx.parameter("commit_batch_size", 1);
    ctx.parameter(
        "read_scope",
        match scope {
            Scope::Area => "area",
            Scope::Realm => "realm",
        },
    );
    ctx.parameter("payload_size", size);
    ctx.parameter("from_offset", from);
    ctx.parameter("read_limit", limit);
    ctx.parameter("filter_selectivity", "unfiltered");
    ctx.parameter("logical_unit", "read_request");
    ctx.parameter("read_requests_per_logical_operation", 1);
    measure_operations(ctx, name, 1, |latencies| {
        let started = Instant::now();
        let (items, cursor) = match scope {
            Scope::Area => store.read_area(1, "replay", "orders", from, limit, None),
            Scope::Realm => store.read_realm(1, "replay", from, limit, None),
        }
        .expect("replay compacted command history");
        assert_eq!(u64::try_from(items.len()).unwrap(), limit);
        for (index, item) in items.into_iter().enumerate() {
            let StreamReadItem::Event(record) = item else {
                panic!("unfiltered replay must return events")
            };
            let offset = from + u64::try_from(index).unwrap();
            let expected = &events[usize::try_from(offset).unwrap()];
            assert_eq!(record.resource_offset, offset);
            assert_eq!(record.area_offset, Some(offset));
            assert_eq!(
                record.realm_offset,
                matches!(scope, Scope::Realm).then_some(offset)
            );
            assert_eq!(record.global_offset, None);
            assert_eq!(record.route.as_str(), "stream://replay/orders/order-123");
            assert_eq!(record.body, expected.body);
            assert_eq!(record.metadata, expected.metadata);
        }
        let last = from + limit - 1;
        match scope {
            Scope::Area => assert_eq!(cursor.last_area_offset, Some(last)),
            Scope::Realm => assert_eq!(cursor.last_realm_offset, Some(last)),
        }
        assert_eq!(cursor.has_more, from + limit < 64);
        latencies.push(started.elapsed());
    });
}

macro_rules! projection_row {
    ($name:ident, $scope:ident, $size:expr, $from:expr, $limit:expr, $measurement:literal) => {
        #[stress(tier = 4)]
        fn $name(ctx: &mut StressContext) {
            measure_projection(ctx, Scope::$scope, $size, $from, $limit, $measurement);
        }
    };
}

projection_row!(
    should_replay_compacted_area_small_events,
    Area,
    64,
    0,
    48,
    "compacted_area_small_events"
);
projection_row!(
    should_replay_compacted_realm_small_events,
    Realm,
    64,
    0,
    48,
    "compacted_realm_small_events"
);
projection_row!(
    should_replay_compacted_area_inline_checkpoint,
    Area,
    15 * 1024,
    31,
    3,
    "compacted_area_inline_checkpoint"
);
projection_row!(
    should_replay_compacted_realm_inline_checkpoint,
    Realm,
    15 * 1024,
    31,
    3,
    "compacted_realm_inline_checkpoint"
);
projection_row!(
    should_replay_compacted_area_blob_checkpoint,
    Area,
    17 * 1024,
    31,
    3,
    "compacted_area_blob_checkpoint"
);
projection_row!(
    should_replay_compacted_realm_blob_checkpoint,
    Realm,
    17 * 1024,
    31,
    3,
    "compacted_realm_blob_checkpoint"
);

cntryl_stress::stress_main!();
