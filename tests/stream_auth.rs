use bytes::Bytes;
use fitz::auth::Permission;
use fitz::domains::stream::actor::{StreamActor, StreamMessage};
use fitz::prelude::Actor;
use fitz::runtime::actor::Context;
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use fitz::session::permissions::SessionPermissions;
use fitz::session::session::{SessionActor, SessionId};
use std::sync::Arc;

// This file tests Stream authorization: verifies that SessionActor properly enforces
// permissions for stream operations (append, read) before allowing access.
// It MUST NOT test implementation details.

fn make_ctx() -> Context<StreamActor> {
    let router = Arc::new(fitz::runtime::router::Router::new());
    let addr = RouteAddress::new(
        RouteFamily::new(1),
        Route::new("stream://acme/orders/checkout"),
    );
    Context::new(addr, router)
}

#[test]
fn should_reject_append_without_write_permission() {
    // Arrange
    let mut actor = StreamActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    let session = SessionActor::new(SessionId(1), SessionPermissions::empty());

    let msg = StreamMessage::Append {
        family_id: RouteFamily::new(1),
        route: Route::new("stream://acme/orders/checkout/append"),
        resource_offset: 0,
        body: Bytes::from(b"order-placed-event".to_vec()),
        metadata: None,
    };

    // Act
    let result = session.append_stream(msg, &mut actor, &mut ctx);

    // Assert
    assert!(result.is_err());
}

#[test]
fn should_allow_append_with_write_permission() {
    // Arrange
    let mut actor = StreamActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    let perms = vec![Permission::parse("stream://acme/orders/**#write").unwrap()];
    let session_perms = SessionPermissions::from_permissions(perms);
    let session = SessionActor::new(SessionId(1), session_perms);

    let msg = StreamMessage::Append {
        family_id: RouteFamily::new(1),
        route: Route::new("stream://acme/orders/checkout/append"),
        resource_offset: 0,
        body: Bytes::from(b"order-placed-event".to_vec()),
        metadata: None,
    };

    // Act
    let result = session.append_stream(msg, &mut actor, &mut ctx);

    // Assert
    assert!(result.is_ok());
}

#[test]
fn should_reject_read_without_read_permission() {
    // Arrange
    let mut actor = StreamActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    let session = SessionActor::new(SessionId(1), SessionPermissions::empty());

    let msg = StreamMessage::Read {
        family_id: RouteFamily::new(1),
        route: Route::new("stream://acme/orders/checkout/read"),
        from_offset: 0,
        limit: 100,
    };

    // Act
    let result = session.read_stream(msg, &mut actor, &mut ctx);

    // Assert
    assert!(result.is_err());
}

#[test]
fn should_allow_read_with_read_permission() {
    // Arrange
    let mut actor = StreamActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    let perms = vec![Permission::parse("stream://acme/orders/**#read").unwrap()];
    let session_perms = SessionPermissions::from_permissions(perms);
    let session = SessionActor::new(SessionId(1), session_perms);

    let msg = StreamMessage::Read {
        family_id: RouteFamily::new(1),
        route: Route::new("stream://acme/orders/checkout/read"),
        from_offset: 0,
        limit: 100,
    };

    // Act
    let result = session.read_stream(msg, &mut actor, &mut ctx);

    // Assert
    assert!(result.is_ok());
}

#[test]
fn should_enforce_realm_boundary_for_write_permission() {
    // Arrange
    let mut actor = StreamActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    // Permission only for realm "acme"
    let perms = vec![Permission::parse("stream://acme/orders/**#write").unwrap()];
    let session_perms = SessionPermissions::from_permissions(perms);
    let session = SessionActor::new(SessionId(1), session_perms);

    // Try to write to different realm
    let msg = StreamMessage::Append {
        family_id: RouteFamily::new(1),
        route: Route::new("stream://other-realm/orders/checkout/append"),
        resource_offset: 0,
        body: Bytes::from(b"unauthorized-event".to_vec()),
        metadata: None,
    };

    // Act
    let result = session.append_stream(msg, &mut actor, &mut ctx);

    // Assert
    assert!(result.is_err());
}

#[test]
fn should_enforce_area_boundary_for_read_permission() {
    // Arrange
    let mut actor = StreamActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    // Permission only for area "orders"
    let perms = vec![Permission::parse("stream://acme/orders/**#read").unwrap()];
    let session_perms = SessionPermissions::from_permissions(perms);
    let session = SessionActor::new(SessionId(1), session_perms);

    // Try to read from different area
    let msg = StreamMessage::Read {
        family_id: RouteFamily::new(1),
        route: Route::new("stream://acme/inventory/stock/read"),
        from_offset: 0,
        limit: 100,
    };

    // Act
    let result = session.read_stream(msg, &mut actor, &mut ctx);

    // Assert
    assert!(result.is_err());
}

#[test]
fn should_allow_wildcard_realm_permission_for_any_realm() {
    // Arrange
    let mut actor = StreamActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    // Wildcard permission for all realms
    let perms = vec![Permission::parse("stream://*/*/**#write").unwrap()];
    let session_perms = SessionPermissions::from_permissions(perms);
    let session = SessionActor::new(SessionId(1), session_perms);

    let msg = StreamMessage::Append {
        family_id: RouteFamily::new(1),
        route: Route::new("stream://any-realm/any-area/resource/append"),
        resource_offset: 0,
        body: Bytes::from(b"event".to_vec()),
        metadata: None,
    };

    // Act
    let result = session.append_stream(msg, &mut actor, &mut ctx);

    // Assert
    assert!(result.is_ok());
}

#[test]
fn should_require_write_permission_for_append_not_read() {
    // Arrange
    let mut actor = StreamActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    // Only read permission
    let perms = vec![Permission::parse("stream://acme/orders/**#read").unwrap()];
    let session_perms = SessionPermissions::from_permissions(perms);
    let session = SessionActor::new(SessionId(1), session_perms);

    let msg = StreamMessage::Append {
        family_id: RouteFamily::new(1),
        route: Route::new("stream://acme/orders/checkout/append"),
        resource_offset: 0,
        body: Bytes::from(b"order-event".to_vec()),
        metadata: None,
    };

    // Act
    let result = session.append_stream(msg, &mut actor, &mut ctx);

    // Assert
    assert!(result.is_err());
}

#[test]
fn should_allow_admin_permission_for_append_operation() {
    // Arrange
    let mut actor = StreamActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    let perms = vec![Permission::parse("stream://acme/**#admin").unwrap()];
    let session_perms = SessionPermissions::from_permissions(perms);
    let session = SessionActor::new(SessionId(1), session_perms);

    let append_msg = StreamMessage::Append {
        family_id: RouteFamily::new(1),
        route: Route::new("stream://acme/orders/checkout/append"),
        resource_offset: 0,
        body: Bytes::from(b"event".to_vec()),
        metadata: None,
    };

    // Act
    let result = session.append_stream(append_msg, &mut actor, &mut ctx);

    // Assert
    assert!(result.is_ok());
}

#[test]
fn should_allow_admin_permission_for_read_operation() {
    // Arrange
    let mut actor = StreamActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    let perms = vec![Permission::parse("stream://acme/**#admin").unwrap()];
    let session_perms = SessionPermissions::from_permissions(perms);
    let session = SessionActor::new(SessionId(1), session_perms);

    let read_msg = StreamMessage::Read {
        family_id: RouteFamily::new(1),
        route: Route::new("stream://acme/orders/checkout/read"),
        from_offset: 0,
        limit: 100,
    };

    // Act
    let result = session.read_stream(read_msg, &mut actor, &mut ctx);

    // Assert
    assert!(result.is_ok());
}

#[test]
fn should_enforce_resource_level_permission_for_specific_stream() {
    // Arrange
    let mut actor = StreamActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    // Permission only for specific resource
    let perms = vec![Permission::parse("stream://acme/orders/checkout#write").unwrap()];
    let session_perms = SessionPermissions::from_permissions(perms);
    let session = SessionActor::new(SessionId(1), session_perms);

    // Try to write to different resource in same area
    let msg = StreamMessage::Append {
        family_id: RouteFamily::new(1),
        route: Route::new("stream://acme/orders/payment/append"),
        resource_offset: 0,
        body: Bytes::from(b"payment-event".to_vec()),
        metadata: None,
    };

    // Act
    let result = session.append_stream(msg, &mut actor, &mut ctx);

    // Assert
    assert!(result.is_err());
}
