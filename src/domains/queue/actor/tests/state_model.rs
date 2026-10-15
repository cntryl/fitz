use super::*;
use proptest::prelude::*;
use std::collections::HashSet;

#[derive(Clone, Copy, Debug)]
enum Operation {
    Send(u8),
    Reserve(u64),
    Ack(u64, bool),
    Extend(u64, bool),
    Disconnect(u64),
    Advance(u64),
}

#[derive(Clone, Copy, Debug)]
enum State {
    Absent,
    Ready,
    Reserved {
        owner: u64,
        token: u64,
        expires: u64,
    },
}

fn operation() -> impl Strategy<Value = Operation> {
    prop_oneof![
        any::<u8>().prop_map(Operation::Send),
        (1_u64..=2).prop_map(Operation::Reserve),
        (1_u64..=2, any::<bool>()).prop_map(|(owner, valid)| Operation::Ack(owner, valid)),
        (1_u64..=2, any::<bool>()).prop_map(|(owner, valid)| Operation::Extend(owner, valid)),
        (1_u64..=2).prop_map(Operation::Disconnect),
        (0_u64..=6).prop_map(Operation::Advance),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 256, max_shrink_iters: 10_000, ..ProptestConfig::default() })]

    #[test]
    fn should_match_queue_reservation_reference_state_model(operations in prop::collection::vec(operation(), 1..=64)) {
        // Arrange
        let clock = MockClock::new();
        let store = create_test_engine_with_cfs(vec![1]);
        let mut key = unique_queue_key("reservation-model");
        key.family = RouteFamily::new(1);
        let mut actor = QueueActor::with_clock(RouteFamily::new(1), key, store, Box::new(clock.clone()), None, crate::utils::idempotency::default_dedup_store());
        let mut state = State::Absent;
        let mut id = None;
        let mut body = 0_u8;
        let mut now = 0_u64;
        let mut seen_tokens = HashSet::new();

        // Act
        for operation in operations {
            match operation {
                Operation::Send(value) if matches!(state, State::Absent) => {
                    let response = actor.handle_send(Bytes::from(vec![value]), None);
                    let QueueResponse::Sent { id: accepted } = response else { return Err(TestCaseError::fail(format!("send failed: {response:?}"))) };
                    id = Some(accepted);
                    body = value;
                    seen_tokens.clear();
                    state = State::Ready;
                }
                Operation::Reserve(owner) => {
                    let response = actor.handle_receive_for_session(owner, 5, Some(1));
                    let QueueResponse::Received { messages } = response else { return Err(TestCaseError::fail(format!("reserve failed: {response:?}"))) };
                    if matches!(state, State::Ready) {
                        prop_assert_eq!(messages.len(), 1);
                        let received = &messages[0];
                        prop_assert_eq!(Some(received.id), id);
                        prop_assert_eq!(received.body.as_ref(), &[body]);
                        prop_assert_ne!(received.token, 0);
                        prop_assert!(seen_tokens.insert(received.token), "reservation token reused for one message");
                        state = State::Reserved { owner, token: received.token, expires: now + 5 };
                    } else {
                        prop_assert!(messages.is_empty(), "reserved or absent work became deliverable");
                    }
                }
                Operation::Ack(caller, valid) => {
                    if let State::Reserved { owner, token, .. } = state {
                        let actual = actor.handle_ack_for_session(caller, id.expect("model id"), if valid { token } else { 0 });
                        let expected = if caller != owner { QueueResponse::NotFound } else if !valid { QueueResponse::InvalidToken } else { state = State::Absent; QueueResponse::Acked };
                        prop_assert_eq!(actual, expected);
                    }
                }
                Operation::Extend(caller, valid) => {
                    if let State::Reserved { owner, token, .. } = state {
                        let actual = actor.handle_extend_for_session(caller, id.expect("model id"), if valid { token } else { 0 }, 5);
                        let expected = if caller != owner { QueueResponse::NotFound } else if !valid { QueueResponse::InvalidToken } else { state = State::Reserved { owner, token, expires: now + 5 }; QueueResponse::Extended };
                        prop_assert_eq!(actual, expected);
                    }
                }
                Operation::Disconnect(caller) => {
                    let released = actor.cleanup_session_inflight(caller);
                    let owned = matches!(state, State::Reserved { owner, .. } if owner == caller);
                    prop_assert_eq!(released, usize::from(owned));
                    if owned { state = State::Ready; }
                }
                Operation::Advance(seconds) => {
                    now += seconds;
                    clock.advance(Duration::from_secs(seconds));
                    actor.process_expired_timers();
                    if matches!(state, State::Reserved { expires, .. } if expires <= now) { state = State::Ready; }
                }
                Operation::Send(_) => {}
            }

            // Assert
            prop_assert_eq!(actor.ready_len(), usize::from(matches!(state, State::Ready)), "after {:?}", operation);
            prop_assert_eq!(actor.inflight.len(), usize::from(matches!(state, State::Reserved { .. })), "after {:?}", operation);
            prop_assert_eq!(actor.records.len(), usize::from(!matches!(state, State::Absent)), "after {:?}", operation);
        }
    }
}
