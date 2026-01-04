use crate::domains::notification::protocol::{PublishMessage, SubscribeMessage};
use crate::domains::notification::route_actor::NoticeRouteActor;
use crate::runtime::actor::{Context, Actor};
use crate::session::permissions::SessionPermissions;
use crate::runtime::routing::Route;
use crate::auth::Access;
use crate::session::session::SessionId;
use crate::runtime::routing::RouteFamily;

/// Lightweight SessionActor helpers for the notification domain.
///
/// Responsibilities:
/// - Enforce session-level authorization for subscribe/publish operations
/// - Forward authorized operations to the NoticeRouteActor
///
/// This is intentionally small: a full actor system is out-of-scope for this change,
/// but tests rely on the SessionActor's semantic enforcement.
pub struct SessionActor {
    pub session_id: SessionId,
    pub permissions: SessionPermissions,
}

impl SessionActor {
    pub fn new(session_id: SessionId, permissions: SessionPermissions) -> Self {
        Self {
            session_id,
            permissions,
        }
    }

    /// Attempt to subscribe. Returns Err if authorization fails.
    pub fn subscribe(
        &self,
        family: RouteFamily,
        pattern: Route,
        notice_actor: &mut NoticeRouteActor,
        ctx: &mut Context<NoticeRouteActor>,
    ) -> Result<(), String> {
        // Subscribe requires read access to the pattern's route
        if !self.permissions.allows(&pattern, Access::Read) {
            return Err("unauthorized: subscribe".to_string());
        }

        let msg = SubscribeMessage::new(family, pattern, self.session_id, ctx.address().clone());
        notice_actor.receive(crate::domains::notification::protocol::NotificationMessage::Subscribe(msg), ctx);
        Ok(())
    }

    /// Attempt to publish; returns Err if authorization fails.
    pub fn publish(
        &self,
        family: RouteFamily,
        route: Route,
        payload: bytes::Bytes,
        notice_actor: &mut NoticeRouteActor,
        ctx: &mut Context<NoticeRouteActor>,
    ) -> Result<(), String> {
        if !self.permissions.allows(&route, Access::Write) {
            return Err("unauthorized: publish".to_string());
        }

        let msg = PublishMessage::new(family, route, payload);
        notice_actor.receive(crate::domains::notification::protocol::NotificationMessage::Publish(msg), ctx);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::actor::Context;
    use crate::runtime::router::Router;
    use crate::runtime::routing::{Route, RouteFamily, RouteAddress};
    use crate::session::permissions::SessionPermissions;
    use crate::auth::Permission;
    use bytes::Bytes;
    use std::sync::Arc;
    use crate::domains::notification::route_actor::NoticeRouteActor;

    fn make_ctx() -> Context<NoticeRouteActor> {
        let router = Router::new();
        let subscriber = RouteAddress::new(RouteFamily::new(1), Route::new("notify://realm/subscriber"));
        Context::new(subscriber, Arc::new(router))
    }

    #[test]
    fn should_reject_unauthenticated_subscribe() {
        // Arrange
        let mut actor = NoticeRouteActor::new(RouteFamily::new(1));
        let mut ctx = make_ctx();

        let session = SessionActor::new(SessionId(1), SessionPermissions::empty());

        // Act
        let res = session.subscribe(
            RouteFamily::new(1),
            Route::new("notify://realm/area/orders/*"),
            &mut actor,
            &mut ctx,
        );

        // Assert
        assert!(res.is_err());
        assert_eq!(actor.subscription_count(), 0);
    }

    #[test]
    fn should_reject_unauthorized_publish() {
        // Arrange
        let router = Router::new();
        let subscriber = RouteAddress::new(RouteFamily::new(1), Route::new("notify://realm/subscriber"));
        let mut actor = NoticeRouteActor::new(RouteFamily::new(1));
        let mut ctx = Context::new(subscriber.clone(), Arc::new(router.clone()));

        // Create a session with a read-only permission on a different prefix
        let perms = vec![Permission::parse("notice://prod/orders/**#read").unwrap()];
        let session_perms = SessionPermissions::from_permissions(perms);
        let session = SessionActor::new(SessionId(1), session_perms);

        // Act
        let res = session.publish(
            RouteFamily::new(1),
            Route::new("notify://prod/orders/create"),
            Bytes::from("hi"),
            &mut actor,
            &mut ctx,
        );

        // Assert: Permission is read-only, but publish requires write
        assert!(res.is_err());
        assert_eq!(actor.subscription_count(), 0);
    }
}
