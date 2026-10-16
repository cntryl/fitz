use super::*;
use proptest::prelude::*;
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug)]
enum Operation {
    Create(u8, u8),
    Cancel(u8),
    Advance(u64),
    Scan,
    Ack(u8),
    Recreate,
}

#[derive(Clone, Copy, Debug)]
struct Definition {
    payload: u8,
    due: u64,
}

fn operation() -> impl Strategy<Value = Operation> {
    prop_oneof![
        (0_u8..3, any::<u8>()).prop_map(|(route, payload)| Operation::Create(route, payload)),
        (0_u8..3).prop_map(Operation::Cancel),
        (0_u64..=4).prop_map(Operation::Advance),
        Just(Operation::Scan),
        (0_u8..3).prop_map(Operation::Ack),
        Just(Operation::Recreate),
    ]
}

fn route(slot: u8) -> String {
    format!("schedule://model/jobs/slot-{slot}/run")
}

fn new_actor(engine: Arc<cntryl_midge::Engine>, clock: Arc<MockClock>) -> ScheduleActor {
    ScheduleActor::new_with_clock(
        RouteFamily::new(1),
        ScheduleStore::new(engine),
        crate::domains::WritePolicy::Buffered,
        clock,
    )
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 128, max_shrink_iters: 10_000, ..ProptestConfig::default() })]

    #[test]
    fn should_match_schedule_claim_reference_state_model(operations in prop::collection::vec(operation(), 1..=48)) {
        // Arrange
        let engine = create_test_engine_with_cfs(vec![1]);
        let clock = Arc::new(MockClock::new(epoch_ms(2026, 1, 1, 0, 0, 0)));
        let mut actor = new_actor(engine.clone(), clock.clone());
        let mut definitions = BTreeMap::<String, Definition>::new();
        let mut pending = BTreeMap::<(u64, String), u8>::new();
        let mut now = clock.now_epoch_ms();

        // Act
        for operation in operations {
            match operation {
                Operation::Create(slot, payload) => {
                    let route = route(slot);
                    actor.create_schedule_at(route.clone(), "* * * * *".into(), Bytes::from(vec![payload]), clock.now_instant()).expect("create");
                    if definitions.get(&route).is_none_or(|old| old.payload != payload) {
                        definitions.insert(route, Definition { payload, due: now / 60_000 * 60_000 + 60_000 });
                    }
                }
                Operation::Cancel(slot) => {
                    let route = route(slot);
                    actor.delete_schedule(&route).expect("cancel");
                    definitions.remove(&route);
                    pending.retain(|(_, claim_route), _| claim_route != &route);
                }
                Operation::Advance(minutes) => {
                    now += minutes * 60_000;
                    clock.advance(Duration::from_secs(minutes * 60));
                }
                Operation::Scan => {
                    let mut expected = Vec::new();
                    for (route, definition) in &mut definitions {
                        if definition.due <= now {
                            pending.insert((definition.due, route.clone()), definition.payload);
                            expected.push((route.clone(), Bytes::from(vec![definition.payload])));
                            definition.due = now / 60_000 * 60_000 + 60_000;
                        }
                    }
                    let mut actual = actor.collect_due_occurrences_for_publish_at(clock.now_instant());
                    actual.sort();
                    expected.sort();
                    prop_assert_eq!(actual, expected, "due scan must skip missed intervals without losing retained claims");
                }
                Operation::Ack(slot) => {
                    let key = pending.keys().find(|(_, claim_route)| claim_route == &route(slot)).cloned();
                    if let Some(key) = key {
                        prop_assert_eq!(actor.ack_pending_fire_claims(std::slice::from_ref(&key)).expect("ack").0, 1);
                        pending.remove(&key);
                    }
                }
                Operation::Recreate => {
                    actor = new_actor(engine.clone(), clock.clone());
                    actor.preload_from_store_at(clock.now_instant()).expect("reconstruct from retained engine");
                    for definition in definitions.values_mut() {
                        if definition.due <= now { definition.due = now / 60_000 * 60_000 + 60_000; }
                    }
                }
            }

            // Assert
            prop_assert_eq!(actor.schedule_count(), definitions.len(), "after {:?}", operation);
            for (route, expected) in &definitions {
                let actual = actor.schedules.get(route).expect("model definition");
                prop_assert_eq!(actual.payload.as_ref(), &[expected.payload]);
                prop_assert_eq!(actual.next_fire_ms, expected.due);
            }
            let actual: BTreeMap<_, _> = actor.pending_claimed_occurrences_for_publish().into_iter().map(|claim| ((claim.fire_ms, claim.route), claim.payload[0])).collect();
            prop_assert_eq!(&actual, &pending, "after {:?}", operation);
            let persisted: BTreeMap<_, _> = actor.store.load_pending_fire_claims(1).expect("persisted claims").into_iter().map(|claim| ((claim.fire_ms, claim.route), claim.payload[0])).collect();
            prop_assert_eq!(&persisted, &pending, "persisted claims after {:?}", operation);
        }
    }
}
