use super::*;
use proptest::prelude::*;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

const MODEL_TTL_SECONDS: u64 = 10;

struct ModelClock {
    epoch_ms: AtomicU64,
}

impl ModelClock {
    fn new(epoch_ms: u64) -> Self {
        Self {
            epoch_ms: AtomicU64::new(epoch_ms),
        }
    }

    fn advance(&self, milliseconds: u64) {
        self.epoch_ms.fetch_add(milliseconds, Ordering::AcqRel);
    }

    fn now(&self) -> u64 {
        self.epoch_ms.load(Ordering::Acquire)
    }
}

impl crate::runtime::clock::Clock for ModelClock {
    fn now_instant(&self) -> std::time::Instant {
        std::time::Instant::now()
    }

    fn now_epoch_ms(&self) -> u64 {
        self.epoch_ms.load(Ordering::Acquire)
    }
}

#[derive(Clone)]
struct ModelRecord {
    resource_index: usize,
    body: u8,
    expires_at: u64,
}

fn model_store(db: Arc<cntryl_midge::Engine>, clock: Arc<ModelClock>) -> StreamStore {
    StreamStore::with_config(
        db,
        BatchLimits::default(),
        StreamTTL::with_seconds(MODEL_TTL_SECONDS),
    )
    .with_clock_for_tests(clock)
}

fn alive_bodies<'a>(records: impl Iterator<Item = &'a ModelRecord>, now_epoch_ms: u64) -> Vec<u8> {
    records
        .filter(|record| record.expires_at > now_epoch_ms)
        .map(|record| record.body)
        .collect()
}

fn stored_bodies(items: Vec<StreamReadItem>) -> Vec<u8> {
    event_records(items)
        .into_iter()
        .map(|record| record.body[0])
        .collect()
}

fn assert_store_matches_model(store: &StreamStore, model: &[ModelRecord], now_epoch_ms: u64) {
    let expected_global = alive_bodies(model.iter(), now_epoch_ms);
    let area = store
        .read_area(1, "model", "events", 0, 128, None)
        .expect("read model area")
        .0;
    let realm = store
        .read_realm(1, "model", 0, 128, None)
        .expect("read model realm")
        .0;
    let global = store
        .read_global(1, 0, 128, None, None)
        .expect("read model global")
        .0;
    assert_eq!(stored_bodies(area), expected_global);
    assert_eq!(stored_bodies(realm), expected_global);
    assert_eq!(stored_bodies(global), expected_global);

    for (resource_index, resource) in ["alpha", "beta"].into_iter().enumerate() {
        let expected = alive_bodies(
            model
                .iter()
                .filter(|record| record.resource_index == resource_index),
            now_epoch_ms,
        );
        let actual = store
            .read_resource(&ReadResourceParams {
                family: 1,
                realm: "model",
                area: "events",
                resource,
                from_offset: 0,
                limit: 128,
                max_bytes: None,
            })
            .expect("read model resource")
            .0;
        assert_eq!(stored_bodies(actual), expected);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]

    #[test]
    fn should_match_reference_model_through_reopen_expiration_plus_compaction(
        operations in prop::collection::vec((0_u8..7, any::<u8>()), 1..40),
    ) {
        // Arrange
        let db = create_test_engine_with_cfs(vec![1]);
        let clock = Arc::new(ModelClock::new(1_000));
        let mut store = model_store(db.clone(), clock.clone());
        let mut model = Vec::new();
        let mut next_offsets = [0_u64; 2];

        // Act
        for (action, value) in operations {
            match action {
                0 | 1 => {
                    let resource_index = usize::from(action);
                    let resource = ["alpha", "beta"][resource_index];
                    store.commit_records(CommitRecordsParams {
                        family: 1,
                        realm: "model",
                        area: "events",
                        resource,
                        expected_resource_next_offset: next_offsets[resource_index],
                        events: &[EventPayload {
                            body: Bytes::from(vec![value]),
                            metadata: None,
                            discriminator: None,
                        }],
                        ingest_metadata: None,
                        mode: StreamWriteMode::Buffered,
                    }).expect("commit model event");
                    next_offsets[resource_index] += 1;
                    model.push(ModelRecord {
                        resource_index,
                        body: value,
                        expires_at: clock.now() + MODEL_TTL_SECONDS * 1_000,
                    });
                }
                2 => clock.advance(u64::from(value) * 100),
                3 => { store.run_maintenance(1).expect("run model maintenance"); }
                4 => store = model_store(db.clone(), clock.clone()),
                5 => {
                    let session = store.begin_session(1, "model", "events", "aborted", None)
                        .expect("begin model rollback session");
                    store.append_to_session(1, session, EventPayload {
                        body: Bytes::from(vec![value]),
                        metadata: None,
                        discriminator: None,
                    }).expect("stage model rollback event");
                    store.abort_session(session).expect("abort model session");
                }
                _ => {}
            }

            // Assert
            assert_store_matches_model(&store, &model, clock.now());
        }
    }
}
