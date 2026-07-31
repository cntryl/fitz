use super::*;
use crate::domains::stream::StreamReadItem;
use proptest::prelude::*;

#[derive(Clone, Copy, Debug)]
enum Operation {
    Begin,
    Append(u8),
    ConflictingAppend(u8),
    Commit,
    Abort,
    Disconnect,
    Recreate,
    Read(u8, u8),
}

fn operation() -> impl Strategy<Value = Operation> {
    prop_oneof![
        Just(Operation::Begin),
        any::<u8>().prop_map(Operation::Append),
        any::<u8>().prop_map(Operation::ConflictingAppend),
        Just(Operation::Commit),
        Just(Operation::Abort),
        Just(Operation::Disconnect),
        Just(Operation::Recreate),
        (0_u8..16, 1_u8..8).prop_map(|(offset, limit)| Operation::Read(offset, limit)),
    ]
}

fn new_actor(store: Arc<StreamStore>) -> StreamActor {
    StreamActor::new(
        RouteFamily::new(1),
        "model".to_string(),
        "events".to_string(),
        "orders".to_string(),
        store,
    )
    .expect("create stream state model actor")
}

fn assert_committed(actor: &StreamActor, expected: &[u8]) -> Result<(), TestCaseError> {
    let read = actor.read(0, u64::MAX, None).map_err(TestCaseError::fail)?;
    let actual = read
        .items
        .into_iter()
        .map(|item| match item {
            StreamReadItem::Event(record) => Ok(record.body[0]),
            other => Err(TestCaseError::fail(format!(
                "unfiltered resource read returned {other:?}"
            ))),
        })
        .collect::<Result<Vec<_>, _>>()?;
    prop_assert_eq!(actual, expected);
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 256,
        max_shrink_iters: 10_000,
        ..ProptestConfig::default()
    })]

    #[test]
    fn should_match_stream_reference_state_model(operations in prop::collection::vec(operation(), 1..=64)) {
        // Arrange
        let store = Arc::new(StreamStore::new(create_test_engine_with_cfs(vec![1])));
        let mut actor = new_actor(Arc::clone(&store));
        let mut committed = Vec::<u8>::new();
        let mut staged: Option<Vec<u8>> = None;
        let owner = 7_u64;
        let session = 11_u64;

        // Act
        for operation in operations {
            match operation {
                Operation::Begin => {
                    let result = actor.begin_append_session(owner, session, None);
                    if staged.is_some() {
                        prop_assert_eq!(result.expect_err("duplicate begin must fail"), "session already active");
                    } else {
                        prop_assert_eq!(result.expect("begin session"), session);
                        staged = Some(Vec::new());
                    }
                }
                Operation::Append(value) => {
                    let expected_offset = u64::try_from(
                        committed.len() + staged.as_ref().map_or(0, Vec::len),
                    )
                    .expect("model offset");
                    let result = actor.append_to_session_with_discriminator_for_owner(
                        owner,
                        session,
                        expected_offset,
                        Bytes::from(vec![value]),
                        None,
                        None,
                    );
                    if let Some(events) = staged.as_mut() {
                        prop_assert_eq!(result.expect("append event"), expected_offset);
                        events.push(value);
                    } else {
                        prop_assert_eq!(result.expect_err("append without session"), "session not found");
                    }
                }
                Operation::ConflictingAppend(value) => {
                    let wrong_offset = u64::try_from(
                        committed.len() + staged.as_ref().map_or(0, Vec::len) + 1,
                    )
                    .expect("conflicting model offset");
                    let result = actor.append_to_session_with_discriminator_for_owner(
                        owner,
                        session,
                        wrong_offset,
                        Bytes::from(vec![value]),
                        None,
                        None,
                    );
                    let expected = if staged.is_some() {
                        "concurrency conflict"
                    } else {
                        "session not found"
                    };
                    prop_assert_eq!(result.expect_err("conflicting append must fail"), expected);
                }
                Operation::Commit if staged.as_ref().is_some_and(|events| !events.is_empty()) => {
                    let response = actor
                        .commit_session_for_owner(owner, session, StreamWriteMode::Buffered)
                        .expect("commit staged events");
                    let events = staged.take().expect("checked staged events");
                    prop_assert_eq!(response.batch_size, events.len());
                    committed.extend(events);
                }
                Operation::Commit if staged.is_some() => {
                    prop_assert_eq!(
                        actor
                            .commit_session_for_owner(owner, session, StreamWriteMode::Buffered)
                            .expect_err("empty commit must fail"),
                        "empty batch"
                    );
                }
                Operation::Abort if staged.is_some() => {
                    actor
                        .rollback_session_for_owner(owner, session)
                        .expect("abort active session");
                    staged = None;
                }
                Operation::Commit | Operation::Abort => {}
                Operation::Disconnect => {
                    let cleaned = actor.cleanup_session(owner);
                    prop_assert_eq!(cleaned.is_some(), staged.is_some());
                    staged = None;
                }
                Operation::Recreate => {
                    staged = None;
                    actor = new_actor(Arc::clone(&store));
                }
                Operation::Read(offset, limit) => {
                    let response = actor
                        .read(u64::from(offset), u64::from(limit), None)
                        .expect("paginated read");
                    let start = usize::from(offset).min(committed.len());
                    let end = start.saturating_add(usize::from(limit)).min(committed.len());
                    let actual = response
                        .items
                        .into_iter()
                        .filter_map(|item| match item {
                            StreamReadItem::Event(record) => Some(record.body[0]),
                            _ => None,
                        })
                        .collect::<Vec<_>>();
                    prop_assert_eq!(actual.as_slice(), &committed[start..end]);
                    prop_assert_eq!(response.cursor.has_more, end < committed.len());
                }
            }

            // Assert
            prop_assert_eq!(actor.has_active_session(), staged.is_some());
            assert_committed(&actor, &committed)?;
        }
    }
}
