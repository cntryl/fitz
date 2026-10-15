use super::*;

#[test]
fn should_preserve_stream_session_after_foreign_finalization_domain_invariant() {
    // Arrange
    for mode in [StreamWriteMode::Buffered, StreamWriteMode::Sync] {
        for (owner, session) in [(8, 100), (7, 101), (8, 101)] {
            let store = Arc::new(StreamStore::new(create_test_engine_with_cfs(vec![1])));
            let mut actor = StreamActor::new(
                RouteFamily::new(1),
                "realm".into(),
                "events".into(),
                "orders".into(),
                store,
            )
            .expect("actor");
            actor.begin_append_session(7, 100, None).expect("begin");
            actor
                .append_to_session_with_discriminator_for_owner(
                    7,
                    100,
                    0,
                    Bytes::from_static(b"original"),
                    None,
                    None,
                )
                .expect("append");

            // Act
            let commit = actor.commit_session_for_owner(owner, session, mode);
            let rollback = actor.rollback_session_for_owner(owner, session);
            let retained = actor.has_active_session();
            let read_before = actor.read(0, 10, None).expect("read before owner commit");
            let accepted = actor.commit_session_for_owner(7, 100, mode);
            let read_after = actor.read(0, 10, None).expect("read after owner commit");

            // Assert
            assert!(commit.is_err());
            assert!(rollback.is_err());
            assert!(retained);
            assert!(read_before.items.is_empty());
            assert!(accepted.is_ok());
            assert!(!actor.has_active_session());
            let [crate::domains::stream::StreamReadItem::Event(record)] =
                read_after.items.as_slice()
            else {
                panic!("expected one committed event")
            };
            assert_eq!(record.body.as_ref(), b"original");
            assert_eq!(record.resource_offset, 0);
        }
    }
}
