//! Stream domain basic tests for the store-authoritative runtime path.

use bytes::Bytes;
use fitz::domains::stream::protocol::{StreamWriteMode, MAX_EVENT_SIZE};
use fitz::domains::stream::store::{BatchLimits, StreamStore, StreamTTL};
use fitz::domains::stream::StreamActor;
use fitz::runtime::routing::RouteFamily;
use fitz::testkit::create_test_db;
use std::sync::Arc;

fn make_actor_with_store(
    store: Arc<StreamStore>,
    realm: &str,
    area: &str,
    resource: &str,
) -> StreamActor {
    StreamActor::new(
        RouteFamily::new(1),
        realm.to_string(),
        area.to_string(),
        resource.to_string(),
        store,
    )
    .expect("create stream actor with store")
}

fn make_actor_with_limits(
    limits: BatchLimits,
    realm: &str,
    area: &str,
    resource: &str,
) -> StreamActor {
    let store = Arc::new(StreamStore::with_config(
        create_test_db(),
        limits,
        StreamTTL::default(),
    ));

    make_actor_with_store(store, realm, area, resource)
}

fn append_for_owner(
    actor: &mut StreamActor,
    owner_session_id: u64,
    stream_session_id: u64,
    expected_offset: u64,
    body: Bytes,
    metadata: Option<Bytes>,
) -> Result<u64, String> {
    actor.append_to_session_with_discriminator_for_owner(
        owner_session_id,
        stream_session_id,
        expected_offset,
        body,
        metadata,
        None,
    )
}

fn commit_for_owner(
    actor: &mut StreamActor,
    owner_session_id: u64,
    stream_session_id: u64,
) -> Result<fitz::domains::stream::protocol::CommitSessionResponse, String> {
    actor.commit_session_for_owner(owner_session_id, stream_session_id, StreamWriteMode::Sync)
}

fn rollback_for_owner(
    actor: &mut StreamActor,
    owner_session_id: u64,
    stream_session_id: u64,
) -> Result<(), String> {
    actor.rollback_session_for_owner(owner_session_id, stream_session_id)
}

#[test]
fn should_reject_second_active_session_on_same_resource() {
    // Arrange
    let store = Arc::new(StreamStore::new(create_test_db()));
    let mut actor = make_actor_with_store(store, "realm1", "area1", "orders");

    // Act
    let session_id = actor.begin_append_session(10, 100, None).unwrap();
    assert_eq!(session_id, 100);

    let error = actor
        .begin_append_session(11, 101, None)
        .expect_err("second session should be rejected");

    // Assert
    assert_eq!(error, "session already active");
    assert!(actor.has_active_session());
}

#[test]
fn should_allow_new_session_after_commit() {
    // Arrange
    let store = Arc::new(StreamStore::new(create_test_db()));
    let mut actor = make_actor_with_store(store, "realm1", "area1", "orders");

    // Act
    actor.begin_append_session(10, 100, None).unwrap();
    append_for_owner(&mut actor, 10, 100, 0, Bytes::from_static(b"event-0"), None).unwrap();
    let commit = commit_for_owner(&mut actor, 10, 100).unwrap();
    assert_eq!(commit.last_resource_offset, 0);

    actor.begin_append_session(10, 101, None).unwrap();

    // Assert
    assert!(actor.has_active_session());
}

#[test]
fn should_allow_new_session_after_rollback() {
    // Arrange
    let store = Arc::new(StreamStore::new(create_test_db()));
    let mut actor = make_actor_with_store(store, "realm1", "area1", "orders");

    // Act
    actor.begin_append_session(10, 100, None).unwrap();
    rollback_for_owner(&mut actor, 10, 100).unwrap();

    actor.begin_append_session(10, 101, None).unwrap();

    // Assert
    assert!(actor.has_active_session());
}

#[test]
fn should_reject_stale_expected_offset_after_commit() {
    // Arrange
    let store = Arc::new(StreamStore::new(create_test_db()));
    let mut actor = make_actor_with_store(store, "realm1", "area1", "orders");

    // Act
    actor.begin_append_session(10, 100, None).unwrap();
    append_for_owner(&mut actor, 10, 100, 0, Bytes::from_static(b"event-0"), None).unwrap();
    commit_for_owner(&mut actor, 10, 100).unwrap();

    actor.begin_append_session(10, 101, None).unwrap();

    let error = actor
        .append_to_session_with_discriminator_for_owner(
            10,
            101,
            0,
            Bytes::from_static(b"event-1"),
            None,
            None,
        )
        .expect_err("stale expected offset should be rejected at append");

    // Assert
    assert_eq!(error, "concurrency conflict");
}

#[test]
fn should_reject_future_expected_offset_given_empty_resource() {
    // Arrange
    let store = Arc::new(StreamStore::new(create_test_db()));
    let mut actor = make_actor_with_store(store, "realm1", "area1", "orders");

    // Act
    actor.begin_append_session(10, 101, None).unwrap();

    let error = actor
        .append_to_session_with_discriminator_for_owner(
            10,
            101,
            101,
            Bytes::from_static(b"event"),
            None,
            None,
        )
        .expect_err("future expected offset should be rejected at append");

    // Assert
    assert_eq!(error, "concurrency conflict");
    assert!(actor.has_active_session());
}

#[test]
fn should_return_error_given_append_to_nonexistent_session() {
    // Arrange
    let store = Arc::new(StreamStore::new(create_test_db()));
    let mut actor = make_actor_with_store(store, "realm1", "area1", "orders");

    // Act
    let error = actor
        .append_to_session_with_discriminator_for_owner(
            10,
            999,
            0,
            Bytes::from_static(b"event"),
            None,
            None,
        )
        .expect_err("missing session append should fail");

    // Assert
    assert_eq!(error, "session not found");
}

#[test]
fn should_return_error_given_commit_with_mismatched_session_id() {
    // Arrange
    let store = Arc::new(StreamStore::new(create_test_db()));
    let mut actor = make_actor_with_store(store, "realm1", "area1", "orders");
    actor.begin_append_session(10, 100, None).unwrap();
    append_for_owner(&mut actor, 10, 100, 0, Bytes::from_static(b"event"), None).unwrap();

    // Act
    let error = actor
        .commit_session_for_owner(10, 101, StreamWriteMode::Sync)
        .expect_err("mismatched session commit should fail");

    // Assert
    assert_eq!(error, "session not found");
    assert!(actor.has_active_session());
}

#[test]
fn should_return_error_given_rollback_with_mismatched_session_id() {
    // Arrange
    let store = Arc::new(StreamStore::new(create_test_db()));
    let mut actor = make_actor_with_store(store, "realm1", "area1", "orders");
    actor.begin_append_session(10, 100, None).unwrap();

    // Act
    let error = actor
        .rollback_session_for_owner(10, 101)
        .expect_err("mismatched session rollback should fail");

    // Assert
    assert_eq!(error, "session not found");
    assert!(actor.has_active_session());
}

#[test]
fn should_return_error_given_append_after_rollback() {
    // Arrange
    let store = Arc::new(StreamStore::new(create_test_db()));
    let mut actor = make_actor_with_store(store, "realm1", "area1", "orders");
    actor.begin_append_session(10, 100, None).unwrap();
    rollback_for_owner(&mut actor, 10, 100).unwrap();

    // Act
    let error = actor
        .append_to_session_with_discriminator_for_owner(
            10,
            100,
            0,
            Bytes::from_static(b"event"),
            None,
            None,
        )
        .expect_err("rolled back session should be gone");

    // Assert
    assert_eq!(error, "session not found");
}

#[test]
fn should_return_none_given_cleanup_for_non_matching_owner() {
    // Arrange
    let store = Arc::new(StreamStore::new(create_test_db()));
    let mut actor = make_actor_with_store(store, "realm1", "area1", "orders");
    actor.begin_append_session(77, 555, None).unwrap();

    // Act
    let removed = actor.cleanup_session(78);

    // Assert
    assert_eq!(removed, None);
    assert!(actor.has_active_session());
}

#[test]
fn should_return_none_given_cleanup_after_session_already_removed() {
    // Arrange
    let store = Arc::new(StreamStore::new(create_test_db()));
    let mut actor = make_actor_with_store(store, "realm1", "area1", "orders");
    actor.begin_append_session(77, 555, None).unwrap();
    actor.cleanup_session(77);

    // Act
    let removed = actor.cleanup_session(77);

    // Assert
    assert_eq!(removed, None);
    assert!(!actor.has_active_session());
}

#[test]
fn should_return_error_given_empty_batch_commit() {
    // Arrange
    let store = Arc::new(StreamStore::new(create_test_db()));
    let mut actor = make_actor_with_store(store, "realm1", "area1", "orders");
    actor.begin_append_session(10, 100, None).unwrap();

    // Act
    let error = actor
        .commit_session_for_owner(10, 100, StreamWriteMode::Sync)
        .expect_err("empty session commit should fail");

    // Assert
    assert_eq!(error, "empty batch");
    assert!(actor.has_active_session());
}

#[test]
fn should_return_empty_read_given_zero_limit_with_committed_data() {
    // Arrange
    let store = Arc::new(StreamStore::new(create_test_db()));
    let mut actor = make_actor_with_store(store, "realm1", "area1", "orders");
    actor.begin_append_session(10, 100, None).unwrap();
    append_for_owner(&mut actor, 10, 100, 0, Bytes::from_static(b"event"), None).unwrap();
    commit_for_owner(&mut actor, 10, 100).unwrap();

    // Act
    let response = actor.read(0, 0, None).unwrap();

    // Assert
    assert!(response.items.is_empty());
    assert_eq!(response.cursor.last_resource_offset, 0);
    assert_eq!(response.cursor.last_area_offset, None);
    assert_eq!(response.cursor.last_realm_offset, None);
    assert!(!response.cursor.has_more);
}

#[test]
fn should_return_none_given_last_on_empty_resource() {
    // Arrange
    let store = Arc::new(StreamStore::new(create_test_db()));
    let actor = make_actor_with_store(store, "realm1", "area1", "orders");

    // Act
    let response = actor.last().unwrap();

    // Assert
    assert!(response.record.is_none());
}

#[test]
fn should_return_empty_metadata_given_resource_never_written() {
    // Arrange
    let store = Arc::new(StreamStore::new(create_test_db()));
    let actor = make_actor_with_store(store, "realm1", "area1", "orders");

    // Act
    let response = actor.metadata().unwrap();

    // Assert
    assert_eq!(response.metadata.first_resource_offset, None);
    assert_eq!(response.metadata.last_resource_offset, None);
    assert_eq!(response.metadata.resource_count, 0);
    assert_eq!(response.metadata.area_watermark, 0);
    assert_eq!(response.metadata.realm_watermark, 0);
}

#[test]
fn should_allow_append_given_event_size_exactly_at_max_boundary() {
    // Arrange
    let store = Arc::new(StreamStore::new(create_test_db()));
    let mut actor = make_actor_with_store(store, "realm1", "area1", "orders");
    actor.begin_append_session(10, 100, None).unwrap();
    let body = Bytes::from(vec![b'x'; MAX_EVENT_SIZE]);

    // Act
    let assigned_offset = append_for_owner(&mut actor, 10, 100, 0, body, None).unwrap();

    // Assert
    assert_eq!(assigned_offset, 0);
}

#[test]
fn should_return_error_given_metadata_and_body_exceed_max_event_size() {
    // Arrange
    let store = Arc::new(StreamStore::new(create_test_db()));
    let mut actor = make_actor_with_store(store, "realm1", "area1", "orders");
    actor.begin_append_session(10, 100, None).unwrap();
    let body = Bytes::from(vec![b'x'; MAX_EVENT_SIZE - 1]);

    // Act
    let error = actor
        .append_to_session_with_discriminator_for_owner(
            10,
            100,
            0,
            body,
            Some(Bytes::from_static(b"yz")),
            None,
        )
        .expect_err("oversized event should fail");

    // Assert
    assert_eq!(error, "event too large");
    assert!(actor.has_active_session());
}

#[test]
fn should_allow_append_given_batch_event_count_at_limit() {
    // Arrange
    let mut actor = make_actor_with_limits(
        BatchLimits {
            max_batch_events: 2,
            max_batch_bytes: 1024,
        },
        "realm1",
        "area1",
        "orders",
    );
    actor.begin_append_session(10, 100, None).unwrap();
    append_for_owner(&mut actor, 10, 100, 0, Bytes::from_static(b"one"), None).unwrap();

    // Act
    let assigned_offset =
        append_for_owner(&mut actor, 10, 100, 1, Bytes::from_static(b"two"), None).unwrap();

    // Assert
    assert_eq!(assigned_offset, 1);
}

#[test]
fn should_return_error_given_batch_event_count_over_limit() {
    // Arrange
    let mut actor = make_actor_with_limits(
        BatchLimits {
            max_batch_events: 2,
            max_batch_bytes: 1024,
        },
        "realm1",
        "area1",
        "orders",
    );
    actor.begin_append_session(10, 100, None).unwrap();
    append_for_owner(&mut actor, 10, 100, 0, Bytes::from_static(b"one"), None).unwrap();
    append_for_owner(&mut actor, 10, 100, 1, Bytes::from_static(b"two"), None).unwrap();

    // Act
    let error = actor
        .append_to_session_with_discriminator_for_owner(
            10,
            100,
            2,
            Bytes::from_static(b"three"),
            None,
            None,
        )
        .expect_err("batch event overflow should fail");

    // Assert
    assert_eq!(error, "batch too large");
}

#[test]
fn should_allow_append_given_batch_bytes_at_limit() {
    // Arrange
    let mut actor = make_actor_with_limits(
        BatchLimits {
            max_batch_events: 10,
            max_batch_bytes: 5,
        },
        "realm1",
        "area1",
        "orders",
    );
    actor.begin_append_session(10, 100, None).unwrap();
    append_for_owner(&mut actor, 10, 100, 0, Bytes::from_static(b"ab"), None).unwrap();

    // Act
    let assigned_offset =
        append_for_owner(&mut actor, 10, 100, 1, Bytes::from_static(b"cde"), None).unwrap();

    // Assert
    assert_eq!(assigned_offset, 1);
}

#[test]
fn should_return_error_given_batch_bytes_over_limit() {
    // Arrange
    let mut actor = make_actor_with_limits(
        BatchLimits {
            max_batch_events: 10,
            max_batch_bytes: 5,
        },
        "realm1",
        "area1",
        "orders",
    );
    actor.begin_append_session(10, 100, None).unwrap();
    append_for_owner(&mut actor, 10, 100, 0, Bytes::from_static(b"ab"), None).unwrap();

    // Act
    let error = actor
        .append_to_session_with_discriminator_for_owner(
            10,
            100,
            1,
            Bytes::from_static(b"cdef"),
            None,
            None,
        )
        .expect_err("batch byte overflow should fail");

    // Assert
    assert_eq!(error, "batch too large");
}

#[test]
fn should_abort_append_session_on_owner_cleanup() {
    // Arrange
    let store = Arc::new(StreamStore::new(create_test_db()));
    let mut actor = make_actor_with_store(store, "realm1", "area1", "orders");

    // Act
    actor.begin_append_session(77, 555, None).unwrap();
    append_for_owner(&mut actor, 77, 555, 0, Bytes::from_static(b"staged"), None).unwrap();

    assert_eq!(actor.cleanup_session(77), Some(555));

    // Assert
    assert!(!actor.has_active_session());
    assert_eq!(
        actor
            .rollback_session_for_owner(77, 555)
            .expect_err("stale session should be gone"),
        "session not found"
    );
}

#[test]
fn should_replay_stream_records_given_resource_offset_after_restart_tcp() {
    // Arrange
    let store = Arc::new(StreamStore::new(create_test_db()));
    let mut writer = make_actor_with_store(store.clone(), "realm1", "area1", "orders");

    // Act
    writer.begin_append_session(10, 100, None).unwrap();
    append_for_owner(
        &mut writer,
        10,
        100,
        0,
        Bytes::from_static(b"event-0"),
        None,
    )
    .unwrap();
    commit_for_owner(&mut writer, 10, 100).unwrap();

    let mut restarted = make_actor_with_store(store, "realm1", "area1", "orders");
    restarted.begin_append_session(10, 101, None).unwrap();
    let metadata = restarted.metadata().unwrap().metadata;

    // Assert
    assert_eq!(metadata.last_resource_offset, Some(0));
}

#[test]
fn should_preserve_staged_session_after_commit_conflict() {
    // Arrange
    let store = Arc::new(StreamStore::new(create_test_db()));
    let mut committed_writer = make_actor_with_store(store.clone(), "realm1", "area1", "orders");
    let mut stale_writer = make_actor_with_store(store, "realm1", "area1", "orders");

    // Act
    committed_writer
        .begin_append_session(10, 100, None)
        .unwrap();
    append_for_owner(
        &mut committed_writer,
        10,
        100,
        0,
        Bytes::from_static(b"committed"),
        None,
    )
    .unwrap();
    commit_for_owner(&mut committed_writer, 10, 100).unwrap();

    stale_writer.begin_append_session(20, 200, None).unwrap();
    append_for_owner(
        &mut stale_writer,
        20,
        200,
        0,
        Bytes::from_static(b"stale"),
        None,
    )
    .unwrap();

    let error = stale_writer
        .commit_session_for_owner(20, 200, StreamWriteMode::Sync)
        .expect_err("stale writer should fail optimistic concurrency");
    assert_eq!(error, "concurrency conflict");
    assert!(stale_writer.has_active_session());

    rollback_for_owner(&mut stale_writer, 20, 200).unwrap();

    // Assert
    assert!(!stale_writer.has_active_session());
}

#[test]
fn should_allow_retry_given_recreated_actor_after_commit_conflict() {
    // Arrange
    let store = Arc::new(StreamStore::new(create_test_db()));
    let mut committed_writer = make_actor_with_store(store.clone(), "realm1", "area1", "orders");
    let mut stale_writer = make_actor_with_store(store.clone(), "realm1", "area1", "orders");

    committed_writer
        .begin_append_session(10, 100, None)
        .unwrap();
    append_for_owner(
        &mut committed_writer,
        10,
        100,
        0,
        Bytes::from_static(b"committed"),
        None,
    )
    .unwrap();
    commit_for_owner(&mut committed_writer, 10, 100).unwrap();

    stale_writer.begin_append_session(20, 200, None).unwrap();
    append_for_owner(
        &mut stale_writer,
        20,
        200,
        0,
        Bytes::from_static(b"stale"),
        None,
    )
    .unwrap();
    stale_writer
        .commit_session_for_owner(20, 200, StreamWriteMode::Sync)
        .expect_err("stale writer should fail optimistic concurrency");
    rollback_for_owner(&mut stale_writer, 20, 200).unwrap();

    // Act
    let mut retry_writer = make_actor_with_store(store, "realm1", "area1", "orders");
    retry_writer.begin_append_session(30, 300, None).unwrap();
    append_for_owner(
        &mut retry_writer,
        30,
        300,
        1,
        Bytes::from_static(b"retried"),
        None,
    )
    .unwrap();
    let commit = commit_for_owner(&mut retry_writer, 30, 300).unwrap();
    let metadata = retry_writer.metadata().unwrap().metadata;

    // Assert
    assert_eq!(commit.first_resource_offset, 1);
    assert_eq!(commit.last_resource_offset, 1);
    assert_eq!(metadata.last_resource_offset, Some(1));
}
