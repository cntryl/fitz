use bytes::Bytes;
use fitz::domains::notice::route_actor::NoticeRouteActor;
use fitz::domains::notice::session::SessionActor;
use fitz::runtime::actor::Context;
use fitz::runtime::router::Router;
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use fitz::session::permissions::SessionPermissions;
use fitz::testkit::notice::TestSink;
use std::sync::Arc;

// Integration-style tests ensuring unauthenticated/unauthorized actions are rejected

#[test]
fn should_reject_unauthenticated_subscribe() {
    // Arrange
    let router = Router::new();
    let sink = Arc::new(TestSink::new());
    let subscriber = RouteAddress::new(RouteFamily::new(1), Route::new("notify://realm/area/sub"));
    router.register(subscriber.clone(), sink.clone());

    let mut notice = NoticeRouteActor::new(RouteFamily::new(1));
    let mut ctx = Context::new(subscriber.clone(), Arc::new(router));

    let session = SessionActor::new(
        fitz::session::session::SessionId(42),
        SessionPermissions::empty(),
    );

    // Act
    let res = session.subscribe(
        RouteFamily::new(1),
        Route::new("notify://realm/area/orders/*"),
        &mut notice,
        &mut ctx,
    );

    // Assert
    assert!(res.is_err());
    assert_eq!(notice.subscription_count(), 0);
    assert_eq!(sink.count(), 0);
}

#[test]
fn should_reject_unauthorized_publish() {
    // Arrange
    let router = Router::new();
    let sink = Arc::new(TestSink::new());
    let subscriber =
        RouteAddress::new(RouteFamily::new(1), Route::new("notify://prod/orders/recv"));
    router.register(subscriber.clone(), sink.clone());

    let mut notice = NoticeRouteActor::new(RouteFamily::new(1));
    let mut ctx = Context::new(subscriber.clone(), Arc::new(router));

    // Session has read permission only
    let perms = vec![fitz::auth::Permission::parse("notice://prod/orders/**#read").unwrap()];
    let session_perms = SessionPermissions::from_permissions(perms);
    let session = SessionActor::new(fitz::session::session::SessionId(43), session_perms);

    // Act: Publish requires write access; expect Err and no delivery
    let res = session.publish(
        RouteFamily::new(1),
        Route::new("notify://prod/orders/create"),
        Bytes::from("payload"),
        &mut notice,
        &mut ctx,
    );

    // Assert
    assert!(res.is_err());
    // Since publish is unauthorized, nothing should be delivered
    assert_eq!(sink.count(), 0);
}
