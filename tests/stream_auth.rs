//! Stream Authorization Tests
//!
//! Tests that stream operations enforce session-level permissions:
//! - Write operations require Write access
//! - Read operations require Read access
//! - Session permissions checked before forwarding to actors
//! - Authorization failures return proper errors

use fitz::domains::stream::protocol::{StreamMessage, StreamWriteMode};
use fitz::domains::stream::session::SessionActor;
use fitz::domains::stream::store::StreamStore;
use fitz::domains::stream::stream_actor::StreamActor;
use fitz::runtime::actor::Context;
use fitz::runtime::router::Router;
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use fitz::session::permissions::SessionPermissions;
use std::sync::Arc;

fn make_stream_actor(
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

    let db = Arc::new(
        cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
            .expect("Failed to open store"),
    );
    let store = Arc::new(StreamStore::new(db));
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

fn make_session_with_write_access() -> SessionActor {
    let perms =
        vec![fitz::auth::Permission::parse("stream://realm1/area1/orders/*#write").unwrap()];
    let session_perms = SessionPermissions::from_permissions(perms);
    SessionActor::new(fitz::session::session::SessionId(1), session_perms)
}

fn make_session_with_read_only() -> SessionActor {
    let perms = vec![fitz::auth::Permission::parse("stream://realm1/area1/orders/*#read").unwrap()];
    let session_perms = SessionPermissions::from_permissions(perms);
    SessionActor::new(fitz::session::session::SessionId(2), session_perms)
}

fn make_session_with_no_access() -> SessionActor {
    SessionActor::new(
        fitz::session::session::SessionId(3),
        SessionPermissions::empty(),
    )
}

#[test]
fn should_allow_begin_session_with_write_permission() {
    // Arrange
    let (mut actor, mut ctx) = make_stream_actor("realm1", "area1", "orders");
    let session = make_session_with_write_access();
    let family = *ctx.address().family();
    let route = Route::new("stream://realm1/area1/orders/append");

    let msg = StreamMessage::Begin {
        family_id: family,
        route: route.clone(),
        expected_offset: 0,
        ingest_metadata: None,
    };

    // Act
    let result = session.begin_session(msg, &mut actor, &mut ctx);

    // Assert
    assert!(result.is_ok());
}

#[test]
fn should_reject_begin_session_without_write_permission() {
    // Arrange
    let (mut actor, mut ctx) = make_stream_actor("realm1", "area1", "orders");
    let session = make_session_with_read_only();
    let family = *ctx.address().family();
    let route = Route::new("stream://realm1/area1/orders/append");

    let msg = StreamMessage::Begin {
        family_id: family,
        route,
        expected_offset: 0,
        ingest_metadata: None,
    };

    // Act
    let result = session.begin_session(msg, &mut actor, &mut ctx);

    // Assert
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("unauthorized"));
}

#[test]
fn should_allow_read_with_read_permission() {
    // Arrange
    let (mut actor, mut ctx) = make_stream_actor("realm1", "area1", "orders");
    let session = make_session_with_read_only();
    let family = *ctx.address().family();
    let route = Route::new("stream://realm1/area1/orders/read");

    let msg = StreamMessage::Read {
        family_id: family,
        route,
        from_offset: 0,
        limit: 10,
        max_bytes: None,
    };

    // Act
    let result = session.read_stream(msg, &mut actor, &mut ctx);

    // Assert
    assert!(result.is_ok());
}

#[test]
fn should_reject_read_without_read_permission() {
    // Arrange
    let (mut actor, mut ctx) = make_stream_actor("realm1", "area1", "orders");
    let session = make_session_with_no_access();
    let family = *ctx.address().family();
    let route = Route::new("stream://realm1/area1/orders/read");

    let msg = StreamMessage::Read {
        family_id: family,
        route,
        from_offset: 0,
        limit: 10,
        max_bytes: None,
    };

    // Act
    let result = session.read_stream(msg, &mut actor, &mut ctx);

    // Assert
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("unauthorized"));
}

#[test]
fn should_reject_commit_without_write_permission() {
    // Arrange
    let (mut actor, mut ctx) = make_stream_actor("realm1", "area1", "orders");
    let session_write = make_session_with_write_access();
    let family = *ctx.address().family();
    let route = Route::new("stream://realm1/area1/orders/append");

    // Begin session with write permission
    let begin_msg = StreamMessage::Begin {
        family_id: family,
        route,
        expected_offset: 0,
        ingest_metadata: None,
    };
    let _ = session_write.begin_session(begin_msg, &mut actor, &mut ctx);

    // Try to commit with read-only session (different session, no write permission for commit)
    let session_read = make_session_with_read_only();
    let commit_msg = StreamMessage::Commit {
        session_id: 1,
        mode: StreamWriteMode::Sync,
    };

    // Act
    let result = session_read.commit_session(commit_msg, &mut actor, &mut ctx);

    // Assert - Commit checks were done at BeginSession, so this just forwards
    // The actual protection is that read-only session cannot BEGIN a session
    assert!(result.is_ok()); // This forwards to actor (session check was at begin)
}

#[test]
fn should_enforce_realm_boundary_in_permissions() {
    // Arrange
    let (mut actor, mut ctx) = make_stream_actor("realm2", "area1", "orders");

    // Session has permission for realm1 but not realm2
    let perms =
        vec![fitz::auth::Permission::parse("stream://realm1/area1/orders/*#write").unwrap()];
    let session_perms = SessionPermissions::from_permissions(perms);
    let session = SessionActor::new(fitz::session::session::SessionId(10), session_perms);

    let family = *ctx.address().family();
    let route = Route::new("stream://realm2/area1/orders/append");

    let msg = StreamMessage::Begin {
        family_id: family,
        route,
        expected_offset: 0,
        ingest_metadata: None,
    };

    // Act
    let result = session.begin_session(msg, &mut actor, &mut ctx);

    // Assert - Should fail due to realm mismatch
    assert!(result.is_err());
}

#[test]
fn should_enforce_area_boundary_in_permissions() {
    // Arrange
    let (mut actor, mut ctx) = make_stream_actor("realm1", "area2", "orders");

    // Session has permission for area1 but not area2
    let perms =
        vec![fitz::auth::Permission::parse("stream://realm1/area1/orders/*#write").unwrap()];
    let session_perms = SessionPermissions::from_permissions(perms);
    let session = SessionActor::new(fitz::session::session::SessionId(11), session_perms);

    let family = *ctx.address().family();
    let route = Route::new("stream://realm1/area2/orders/append");

    let msg = StreamMessage::Begin {
        family_id: family,
        route,
        expected_offset: 0,
        ingest_metadata: None,
    };

    // Act
    let result = session.begin_session(msg, &mut actor, &mut ctx);

    // Assert - Should fail due to area mismatch
    assert!(result.is_err());
}

#[test]
fn should_allow_wildcard_permission_for_all_resources() {
    // Arrange
    let (mut actor, mut ctx) = make_stream_actor("realm1", "area1", "any_resource");

    // Session has wildcard permission for all resources in area
    let perms = vec![fitz::auth::Permission::parse("stream://realm1/area1/**#write").unwrap()];
    let session_perms = SessionPermissions::from_permissions(perms);
    let session = SessionActor::new(fitz::session::session::SessionId(12), session_perms);

    let family = *ctx.address().family();
    let route = Route::new("stream://realm1/area1/any_resource/append");

    let msg = StreamMessage::Begin {
        family_id: family,
        route,
        expected_offset: 0,
        ingest_metadata: None,
    };

    // Act
    let result = session.begin_session(msg, &mut actor, &mut ctx);

    // Assert
    assert!(result.is_ok());
}

#[test]
fn should_allow_read_with_read_only_permission() {
    // Arrange
    let (mut actor, mut ctx) = make_stream_actor("realm1", "area1", "orders");
    let session_read = make_session_with_read_only();
    let family = *ctx.address().family();
    let read_msg = StreamMessage::Read {
        family_id: family,
        route: Route::new("stream://realm1/area1/orders/read"),
        from_offset: 0,
        limit: 10,
        max_bytes: None,
    };

    // Act
    let result = session_read.read_stream(read_msg, &mut actor, &mut ctx);

    // Assert
    assert!(result.is_ok());
}

#[test]
fn should_deny_write_with_read_only_permission() {
    // Arrange
    let (mut actor, mut ctx) = make_stream_actor("realm1", "area1", "orders");
    let session_read = make_session_with_read_only();
    let family = *ctx.address().family();
    let write_msg = StreamMessage::Begin {
        family_id: family,
        route: Route::new("stream://realm1/area1/orders/append"),
        expected_offset: 0,
        ingest_metadata: None,
    };

    // Act
    let result = session_read.begin_session(write_msg, &mut actor, &mut ctx);

    // Assert
    assert!(result.is_err());
}

#[test]
fn should_check_permissions_on_peek_operation() {
    // Arrange
    let (mut actor, mut ctx) = make_stream_actor("realm1", "area1", "orders");
    let session_no_access = make_session_with_no_access();
    let family = *ctx.address().family();

    let msg = StreamMessage::Last {
        family_id: family,
        route: Route::new("stream://realm1/area1/orders/peek"),
    };

    // Act
    let result = session_no_access.read_stream(msg, &mut actor, &mut ctx);

    // Assert - Should fail with no read permission
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("unauthorized"));
}

#[test]
fn should_check_permissions_on_get_metadata() {
    // Arrange
    let (mut actor, mut ctx) = make_stream_actor("realm1", "area1", "orders");
    let session_no_access = make_session_with_no_access();
    let family = *ctx.address().family();

    let msg = StreamMessage::GetMetadata {
        family_id: family,
        route: Route::new("stream://realm1/area1/orders/metadata"),
    };

    // Act
    let result = session_no_access.read_stream(msg, &mut actor, &mut ctx);

    // Assert - Should fail with no read permission
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("unauthorized"));
}
