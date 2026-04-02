//! Stream domain basic tests for the store-authoritative runtime path.

use bytes::Bytes;
use fitz::domains::stream::session::SessionActor;
use fitz::domains::stream::protocol::StreamWriteMode;
use fitz::domains::stream::store::StreamStore;
use fitz::domains::stream::{StreamActor, StreamMessage};
use fitz::runtime::actor::Context;
use fitz::runtime::router::Router;
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use fitz::session::permissions::SessionPermissions;
use fitz::testkit::create_test_db;
use std::sync::Arc;

fn make_actor_context(
    realm: &str,
    area: &str,
    resource: &str,
) -> (StreamActor, Context<StreamActor>) {
    let router = Arc::new(Router::new());
    let family = RouteFamily::new(1);
    let addr = RouteAddress::new(
        family,
        Route::new(format!("stream://{}/{}/{}/append", realm, area, resource)),
    );
    let store = Arc::new(StreamStore::new(create_test_db()));
    let actor = StreamActor::new(
        family,
        realm.to_string(),
        area.to_string(),
        resource.to_string(),
        store,
    );
    let ctx = Context::new(addr, router);
    (actor, ctx)
}

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
}

fn make_session_with_write_access() -> SessionActor {
    let perms =
        vec![fitz::auth::Permission::parse("stream://realm1/area1/orders/*#write").unwrap()];
    SessionActor::new(
        fitz::session::session::SessionId(1),
        SessionPermissions::from_permissions(perms),
    )
}

fn make_session_with_read_only() -> SessionActor {
    let perms = vec![fitz::auth::Permission::parse("stream://realm1/area1/orders/*#read").unwrap()];
    SessionActor::new(
        fitz::session::session::SessionId(2),
        SessionPermissions::from_permissions(perms),
    )
}

fn make_session_with_no_access() -> SessionActor {
    SessionActor::new(
        fitz::session::session::SessionId(3),
        SessionPermissions::empty(),
    )
}

#[test]
fn should_allow_begin_session_with_write_permission() {
    let (mut actor, mut ctx) = make_actor_context("realm1", "area1", "orders");
    let session = make_session_with_write_access();

    let result = session.begin_session(
        StreamMessage::Begin {
            family_id: *ctx.address().family(),
            route: Route::new("stream://realm1/area1/orders/append"),
            expected_offset: 0,
            ingest_metadata: None,
        },
        &mut actor,
        &mut ctx,
    );

    assert!(result.is_ok());
}

#[test]
fn should_reject_begin_session_without_write_permission() {
    let (mut actor, mut ctx) = make_actor_context("realm1", "area1", "orders");
    let session = make_session_with_read_only();

    let result = session.begin_session(
        StreamMessage::Begin {
            family_id: *ctx.address().family(),
            route: Route::new("stream://realm1/area1/orders/append"),
            expected_offset: 0,
            ingest_metadata: None,
        },
        &mut actor,
        &mut ctx,
    );

    assert!(result.is_err());
    assert!(result.unwrap_err().contains("unauthorized"));
}

#[test]
fn should_allow_read_with_read_permission() {
    let (mut actor, mut ctx) = make_actor_context("realm1", "area1", "orders");
    let session = make_session_with_read_only();

    let result = session.read_stream(
        StreamMessage::Read {
            family_id: *ctx.address().family(),
            route: Route::new("stream://realm1/area1/orders/read"),
            from_offset: 0,
            limit: 10,
            max_bytes: None,
        },
        &mut actor,
        &mut ctx,
    );

    assert!(result.is_ok());
}

#[test]
fn should_reject_read_without_read_permission() {
    let (mut actor, mut ctx) = make_actor_context("realm1", "area1", "orders");
    let session = make_session_with_no_access();

    let result = session.read_stream(
        StreamMessage::Read {
            family_id: *ctx.address().family(),
            route: Route::new("stream://realm1/area1/orders/read"),
            from_offset: 0,
            limit: 10,
            max_bytes: None,
        },
        &mut actor,
        &mut ctx,
    );

    assert!(result.is_err());
    assert!(result.unwrap_err().contains("unauthorized"));
}

#[test]
fn should_enforce_realm_boundary_in_permissions() {
    let (mut actor, mut ctx) = make_actor_context("realm2", "area1", "orders");
    let perms =
        vec![fitz::auth::Permission::parse("stream://realm1/area1/orders/*#write").unwrap()];
    let session = SessionActor::new(
        fitz::session::session::SessionId(10),
        SessionPermissions::from_permissions(perms),
    );

    let result = session.begin_session(
        StreamMessage::Begin {
            family_id: *ctx.address().family(),
            route: Route::new("stream://realm2/area1/orders/append"),
            expected_offset: 0,
            ingest_metadata: None,
        },
        &mut actor,
        &mut ctx,
    );

    assert!(result.is_err());
}

#[test]
fn should_enforce_area_boundary_in_permissions() {
    let (mut actor, mut ctx) = make_actor_context("realm1", "area2", "orders");
    let perms =
        vec![fitz::auth::Permission::parse("stream://realm1/area1/orders/*#write").unwrap()];
    let session = SessionActor::new(
        fitz::session::session::SessionId(11),
        SessionPermissions::from_permissions(perms),
    );

    let result = session.begin_session(
        StreamMessage::Begin {
            family_id: *ctx.address().family(),
            route: Route::new("stream://realm1/area2/orders/append"),
            expected_offset: 0,
            ingest_metadata: None,
        },
        &mut actor,
        &mut ctx,
    );

    assert!(result.is_err());
}

#[test]
fn should_allow_wildcard_permission_for_all_resources() {
    let (mut actor, mut ctx) = make_actor_context("realm1", "area1", "any_resource");
    let perms = vec![fitz::auth::Permission::parse("stream://realm1/area1/**#write").unwrap()];
    let session = SessionActor::new(
        fitz::session::session::SessionId(12),
        SessionPermissions::from_permissions(perms),
    );

    let result = session.begin_session(
        StreamMessage::Begin {
            family_id: *ctx.address().family(),
            route: Route::new("stream://realm1/area1/any_resource/append"),
            expected_offset: 0,
            ingest_metadata: None,
        },
        &mut actor,
        &mut ctx,
    );

    assert!(result.is_ok());
}

#[test]
fn should_reject_second_active_session_on_same_resource() {
    let store = Arc::new(StreamStore::new(create_test_db()));
    let mut actor = make_actor_with_store(store, "realm1", "area1", "orders");

    let session_id = actor.begin_append_session(10, 100, 0, None).unwrap();
    assert_eq!(session_id, 100);

    let error = actor
        .begin_append_session(11, 101, 0, None)
        .expect_err("second session should be rejected");
    assert_eq!(error, "session already active");
    assert!(actor.has_active_session());
}

#[test]
fn should_allow_new_session_after_commit() {
    let store = Arc::new(StreamStore::new(create_test_db()));
    let mut actor = make_actor_with_store(store, "realm1", "area1", "orders");

    actor.begin_append_session(10, 100, 0, None).unwrap();
    actor
        .append_to_session(100, Bytes::from_static(b"event-0"), None)
        .unwrap();
    let commit = actor.commit_session(100, StreamWriteMode::Sync).unwrap();
    assert_eq!(commit.last_resource_offset, 0);

    actor.begin_append_session(10, 101, 1, None).unwrap();
    assert!(actor.has_active_session());
}

#[test]
fn should_allow_new_session_after_rollback() {
    let store = Arc::new(StreamStore::new(create_test_db()));
    let mut actor = make_actor_with_store(store, "realm1", "area1", "orders");

    actor.begin_append_session(10, 100, 0, None).unwrap();
    actor.rollback_session(100).unwrap();

    actor.begin_append_session(10, 101, 0, None).unwrap();
    assert!(actor.has_active_session());
}

#[test]
fn should_reject_stale_expected_offset_after_commit() {
    let store = Arc::new(StreamStore::new(create_test_db()));
    let mut actor = make_actor_with_store(store, "realm1", "area1", "orders");

    actor.begin_append_session(10, 100, 0, None).unwrap();
    actor
        .append_to_session(100, Bytes::from_static(b"event-0"), None)
        .unwrap();
    actor.commit_session(100, StreamWriteMode::Sync).unwrap();

    let error = actor
        .begin_append_session(10, 101, 0, None)
        .expect_err("stale expected offset should be rejected");
    assert_eq!(error, "concurrency conflict");
}

#[test]
fn should_abort_append_session_on_owner_cleanup() {
    let store = Arc::new(StreamStore::new(create_test_db()));
    let mut actor = make_actor_with_store(store, "realm1", "area1", "orders");

    actor.begin_append_session(77, 555, 0, None).unwrap();
    actor
        .append_to_session(555, Bytes::from_static(b"staged"), None)
        .unwrap();

    assert_eq!(actor.cleanup_session(77), Some(555));
    assert!(!actor.has_active_session());
    assert_eq!(
        actor.rollback_session(555).expect_err("stale session should be gone"),
        "session not found"
    );
}

#[test]
fn should_recover_next_offset_from_store_after_restart() {
    let store = Arc::new(StreamStore::new(create_test_db()));
    let mut writer = make_actor_with_store(store.clone(), "realm1", "area1", "orders");

    writer.begin_append_session(10, 100, 0, None).unwrap();
    writer
        .append_to_session(100, Bytes::from_static(b"event-0"), None)
        .unwrap();
    writer.commit_session(100, StreamWriteMode::Sync).unwrap();

    let mut restarted = make_actor_with_store(store, "realm1", "area1", "orders");
    restarted.begin_append_session(10, 101, 1, None).unwrap();
    let metadata = restarted.metadata().unwrap().metadata;
    assert_eq!(metadata.last_resource_offset, Some(0));
}

#[test]
fn should_preserve_staged_session_after_commit_conflict() {
    let store = Arc::new(StreamStore::new(create_test_db()));
    let mut committed_writer = make_actor_with_store(store.clone(), "realm1", "area1", "orders");
    let mut stale_writer = make_actor_with_store(store, "realm1", "area1", "orders");

    committed_writer
        .begin_append_session(10, 100, 0, None)
        .unwrap();
    committed_writer
        .append_to_session(100, Bytes::from_static(b"committed"), None)
        .unwrap();
    committed_writer
        .commit_session(100, StreamWriteMode::Sync)
        .unwrap();

    stale_writer.begin_append_session(20, 200, 0, None).unwrap();
    stale_writer
        .append_to_session(200, Bytes::from_static(b"stale"), None)
        .unwrap();

    let error = stale_writer
        .commit_session(200, StreamWriteMode::Sync)
        .expect_err("stale writer should fail optimistic concurrency");
    assert_eq!(error, "concurrency conflict");
    assert!(stale_writer.has_active_session());

    stale_writer.rollback_session(200).unwrap();
    assert!(!stale_writer.has_active_session());
}
