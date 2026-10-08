//! Validated bounded replay from full fragments, including blob-backed events.

use crate::tier4_stream_support::measure_operations;
use bytes::Bytes;
use cntryl_stress::{stress, StressContext};
use fitz::benchkit::{create_local_bench_store, create_write_heavy_bench_store};
use fitz::domains::stream::protocol::StreamWriteMode;
use fitz::domains::stream::StreamReadItem;
use fitz::testkit::domain_internals::stream::{
    CommitRecordsParams, EventPayload, ReadResourceParams, StreamStore,
};
use std::sync::Arc;
use std::time::Instant;

const HISTORY: usize = 64;
const REALM: &str = "replay";
const AREA: &str = "orders";
const RESOURCE: &str = "order-123";

#[derive(Clone, Copy)]
enum Scope {
    Resource,
    Area,
    Realm,
    Global,
}

impl Scope {
    fn label(self) -> &'static str {
        match self {
            Self::Resource => "resource",
            Self::Area => "area",
            Self::Realm => "realm",
            Self::Global => "global",
        }
    }
}

fn measure_replay(
    ctx: &mut StressContext,
    scope: Scope,
    from: u64,
    limit: u64,
    payload_size: usize,
    measurement: &'static str,
    recovered: bool,
) {
    let events = (0..HISTORY)
        .map(|offset| {
            let mut body = vec![0x45; payload_size];
            body[..8].copy_from_slice(&u64::try_from(offset).unwrap().to_le_bytes());
            EventPayload {
                body: Bytes::from(body),
                metadata: Some(Bytes::from_static(
                    b"event-kind=OrderChanged;command=cmd-001",
                )),
                discriminator: Some("OrderChanged".into()),
            }
        })
        .collect::<Vec<_>>();
    let (store, _history_dir) = replay_store(&events, recovered);
    let route = format!("stream://{REALM}/{AREA}/{RESOURCE}");
    ctx.parameter("scenario", "bounded_replay");
    ctx.parameter("measurement_scope", "direct_store");
    ctx.parameter(
        "storage_profile",
        if recovered {
            "recovered_local_disk"
        } else {
            "memory"
        },
    );
    ctx.parameter("read_scope", scope.label());
    ctx.parameter("history_depth", HISTORY);
    ctx.parameter("from_offset", from);
    ctx.parameter("read_limit", limit);
    ctx.parameter("payload_size", payload_size);
    ctx.parameter("discriminator_present", true);
    ctx.parameter("filter_selectivity", "unfiltered");
    ctx.parameter("fragment_shape", "full_64_record_fragment");
    ctx.parameter("logical_unit", "read_request");
    ctx.parameter("read_requests_per_logical_operation", 1);
    measure_operations(ctx, measurement, 1, |latencies| {
        let started = Instant::now();
        let (items, cursor) = match scope {
            Scope::Resource => store.read_resource(&ReadResourceParams {
                family: 1,
                realm: REALM,
                area: AREA,
                resource: RESOURCE,
                from_offset: from,
                limit,
                max_bytes: None,
            }),
            Scope::Area => store.read_area(1, REALM, AREA, from, limit, None),
            Scope::Realm => store.read_realm(1, REALM, from, limit, None),
            Scope::Global => store.read_global(1, from, limit, None, None),
        }
        .expect("read bounded committed history");
        assert_eq!(u64::try_from(items.len()).unwrap(), limit);
        for (index, item) in items.into_iter().enumerate() {
            let StreamReadItem::Event(record) = item else {
                panic!("unfiltered replay must return events");
            };
            let offset = from + u64::try_from(index).unwrap();
            let expected = &events[usize::try_from(offset).unwrap()];
            assert_eq!(record.resource_offset, offset);
            assert_eq!(record.route.as_str(), route);
            assert_eq!(record.body, expected.body);
            assert_eq!(record.metadata, expected.metadata);
        }
        let last = from + limit - 1;
        match scope {
            Scope::Resource => assert_eq!(cursor.last_resource_offset, last),
            Scope::Area => assert_eq!(cursor.last_area_offset, Some(last)),
            Scope::Realm => assert_eq!(cursor.last_realm_offset, Some(last)),
            Scope::Global => assert_eq!(cursor.last_global_offset, Some(last)),
        }
        assert_eq!(cursor.has_more, from + limit < 64);
        latencies.push(started.elapsed());
    });
}

fn replay_store(
    events: &[EventPayload],
    recovered: bool,
) -> (StreamStore, Option<tempfile::TempDir>) {
    let (engine, history_dir) = if recovered {
        let (engine, directory) = create_local_bench_store();
        (engine, Some(directory))
    } else {
        (create_write_heavy_bench_store(), None)
    };
    let mut store = StreamStore::new(engine);
    store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm: REALM,
            area: AREA,
            resource: RESOURCE,
            expected_resource_next_offset: 0,
            events,
            ingest_metadata: None,
            mode: StreamWriteMode::Sync,
        })
        .expect("commit full replay fragment");
    store.set_watermark(1, REALM, AREA, 63).unwrap();
    store.set_realm_watermark(1, REALM, 63).unwrap();
    if let Some(directory) = &history_dir {
        drop(store);
        let engine = cntryl_midge::Engine::open(
            cntryl_midge::OpenOptions::local(directory.path())
                .build()
                .expect("recovery options"),
        )
        .expect("recover committed replay history");
        store = StreamStore::new(Arc::new(engine));
    }
    (store, history_dir)
}

macro_rules! replay_row {
    ($name:ident, $scope:ident, $from:expr, $limit:expr, $size:expr, $measurement:literal) => {
        replay_row!($name, $scope, $from, $limit, $size, $measurement, false);
    };
    ($name:ident, $scope:ident, $from:expr, $limit:expr, $size:expr, $measurement:literal, $recovered:expr) => {
        #[stress(tier = 4)]
        fn $name(ctx: &mut StressContext) {
            measure_replay(
                ctx,
                Scope::$scope,
                $from,
                $limit,
                $size,
                $measurement,
                $recovered,
            );
        }
    };
}

replay_row!(
    should_replay_one_blob_resource_start,
    Resource,
    0,
    1,
    17 * 1024,
    "blob_resource_start_one"
);
replay_row!(
    should_replay_one_blob_resource_checkpoint,
    Resource,
    31,
    1,
    17 * 1024,
    "blob_resource_checkpoint_one"
);
replay_row!(
    should_replay_three_blob_resource_checkpoint,
    Resource,
    31,
    3,
    17 * 1024,
    "blob_resource_checkpoint_three"
);
replay_row!(
    should_replay_one_blob_area_checkpoint,
    Area,
    31,
    1,
    17 * 1024,
    "blob_area_checkpoint_one"
);
replay_row!(
    should_replay_one_blob_realm_checkpoint,
    Realm,
    31,
    1,
    17 * 1024,
    "blob_realm_checkpoint_one"
);
replay_row!(
    should_replay_one_blob_global_checkpoint,
    Global,
    31,
    1,
    17 * 1024,
    "blob_global_checkpoint_one"
);
replay_row!(
    should_replay_global_projection_with_discriminators,
    Global,
    0,
    48,
    64,
    "global_projection_unfiltered"
);

replay_row!(
    should_replay_recovered_blob_resource_checkpoint,
    Resource,
    31,
    1,
    17 * 1024,
    "recovered_blob_resource_checkpoint_one",
    true
);
replay_row!(
    should_replay_recovered_blob_area_checkpoint,
    Area,
    31,
    1,
    17 * 1024,
    "recovered_blob_area_checkpoint_one",
    true
);
replay_row!(
    should_replay_recovered_blob_realm_checkpoint,
    Realm,
    31,
    1,
    17 * 1024,
    "recovered_blob_realm_checkpoint_one",
    true
);
replay_row!(
    should_replay_recovered_blob_global_checkpoint,
    Global,
    31,
    1,
    17 * 1024,
    "recovered_blob_global_checkpoint_one",
    true
);
