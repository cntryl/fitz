use crate::auth::Access;
use crate::domains::queue::protocol::QueueMessage;
use crate::domains::queue::QueueActor;
use crate::runtime::actor::{Actor, Context};
use crate::runtime::routing::Route;
use crate::runtime::routing::RouteFamily;
use crate::session::permissions::SessionPermissions;
use crate::session::session::SessionId;
use bytes::Bytes;

/// Lightweight SessionActor helpers for the queue domain.
///
/// Responsibilities:
/// - Enforce session-level authorization for queue operations
/// - Forward authorized operations to the QueueActor
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

    /// Attempt to send a message. Returns Err if authorization fails.
    pub fn send(
        &self,
        family: RouteFamily,
        route: Route,
        body: Bytes,
        delay_seconds: Option<u64>,
        queue_actor: &mut QueueActor,
        ctx: &mut Context<QueueActor>,
    ) -> Result<(), String> {
        // Extract base route (strip /send suffix if present)
        let base_route = Self::extract_base_route(&route);

        // Send requires write access (adding messages is a write operation)
        if !self.permissions.allows(&base_route, Access::Write) {
            return Err("unauthorized: send".to_string());
        }

        let msg = QueueMessage::Send {
            family_id: family,
            route,
            body,
            delay_seconds,
        };
        queue_actor.receive(msg, ctx);
        Ok(())
    }

    /// Attempt to receive messages. Returns Err if authorization fails.
    #[allow(clippy::too_many_arguments)]
    pub fn receive(
        &self,
        family: RouteFamily,
        route: Route,
        lease_seconds: u64,
        batch_size: Option<usize>,
        wait_seconds: Option<u64>,
        queue_actor: &mut QueueActor,
        ctx: &mut Context<QueueActor>,
    ) -> Result<(), String> {
        // Extract base route (strip /receive suffix if present)
        let base_route = Self::extract_base_route(&route);

        // Receive requires read access (consuming messages is a read operation)
        if !self.permissions.allows(&base_route, Access::Read) {
            return Err("unauthorized: receive".to_string());
        }

        let msg = QueueMessage::Receive {
            family_id: family,
            route,
            lease_seconds,
            batch_size,
            wait_seconds,
        };
        queue_actor.receive(msg, ctx);
        Ok(())
    }

    /// Attempt to extend a message lease. Returns Err if authorization fails.
    #[allow(clippy::too_many_arguments)]
    pub fn extend(
        &self,
        family: RouteFamily,
        route: Route,
        id: crate::domains::queue::protocol::MessageId,
        token: u64,
        lease_seconds: u64,
        queue_actor: &mut QueueActor,
        ctx: &mut Context<QueueActor>,
    ) -> Result<(), String> {
        // Extract base route (strip /extend suffix if present)
        let base_route = Self::extract_base_route(&route);

        // Extend requires write access (modifying lease state is a write operation)
        if !self.permissions.allows(&base_route, Access::Write) {
            return Err("unauthorized: extend".to_string());
        }

        let msg = QueueMessage::Extend {
            family_id: family,
            route,
            id,
            token,
            lease_seconds,
        };
        queue_actor.receive(msg, ctx);
        Ok(())
    }

    /// Attempt to acknowledge a message. Returns Err if authorization fails.
    pub fn ack(
        &self,
        family: RouteFamily,
        route: Route,
        id: crate::domains::queue::protocol::MessageId,
        token: u64,
        queue_actor: &mut QueueActor,
        ctx: &mut Context<QueueActor>,
    ) -> Result<(), String> {
        // Extract base route (strip /ack suffix if present)
        let base_route = Self::extract_base_route(&route);

        // Ack requires write access (deleting messages is a write operation)
        if !self.permissions.allows(&base_route, Access::Write) {
            return Err("unauthorized: ack".to_string());
        }

        let msg = QueueMessage::Ack {
            family_id: family,
            route,
            id,
            token,
        };
        queue_actor.receive(msg, ctx);
        Ok(())
    }

    /// Helper to extract base route by stripping known operation suffixes.
    /// For queue operations, the base route is the resource path without the operation.
    /// e.g., "queue://realm/area/jobs/send" -> "queue://realm/area/jobs"
    fn extract_base_route(route: &Route) -> Route {
        let path = route.as_str();
        let base = path
            .strip_suffix("/send")
            .or_else(|| path.strip_suffix("/receive"))
            .or_else(|| path.strip_suffix("/extend"))
            .or_else(|| path.strip_suffix("/ack"))
            .unwrap_or(path);
        Route::new(base)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::Permission;
    use crate::domains::queue::protocol::QueueKey;
    use crate::runtime::actor::Context;
    use crate::runtime::router::Router;
    use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
    use crate::session::permissions::SessionPermissions;
    use std::sync::Arc;

    fn make_ctx() -> Context<QueueActor> {
        let router = Router::new();
        let addr = RouteAddress::new(RouteFamily::new(1), Route::new("queue://realm/area/jobs"));
        Context::new(addr, Arc::new(router))
    }

    fn make_queue_actor() -> QueueActor {
        let store = Arc::new(
            cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
                .expect("Failed to open Midge"),
        );
        let queue_key = QueueKey {
            family: RouteFamily::new(1),
            realm: "realm".to_string(),
            area: "area".to_string(),
            resource: "jobs".to_string(),
        };
        QueueActor::new(
            RouteFamily::new(1),
            queue_key,
            store,
            None,
            crate::utils::idempotency::global_dedup_store(),
        )
    }

    #[test]
    fn should_reject_unauthenticated_send() {
        // Arrange
        let session = SessionActor::new(SessionId(1), SessionPermissions::from_permissions(vec![]));
        let mut actor = make_queue_actor();
        let mut ctx = make_ctx();

        // Act
        let result = session.send(
            RouteFamily::new(1),
            Route::new("queue://realm/area/jobs/send"),
            Bytes::from("test"),
            None,
            &mut actor,
            &mut ctx,
        );

        // Assert
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "unauthorized: send");
    }

    #[test]
    fn should_allow_authorized_send() {
        // Arrange
        let session = SessionActor::new(
            SessionId(1),
            SessionPermissions::from_permissions(vec![Permission::parse(
                "queue://realm/area/jobs#write",
            )
            .unwrap()]),
        );
        let mut actor = make_queue_actor();
        let mut ctx = make_ctx();

        // Act
        let result = session.send(
            RouteFamily::new(1),
            Route::new("queue://realm/area/jobs/send"),
            Bytes::from("test"),
            None,
            &mut actor,
            &mut ctx,
        );

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_reject_unauthorized_reserve() {
        // Arrange
        let session = SessionActor::new(
            SessionId(1),
            SessionPermissions::from_permissions(vec![Permission::parse(
                "queue://realm/area/other#read",
            )
            .unwrap()]),
        );
        let mut actor = make_queue_actor();
        let mut ctx = make_ctx();

        // Act
        let result = session.receive(
            RouteFamily::new(1),
            Route::new("queue://realm/area/jobs/reserve"),
            30,
            Some(10),
            None,
            &mut actor,
            &mut ctx,
        );

        // Assert
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "unauthorized: receive");
    }

    #[test]
    fn should_allow_authorized_reserve_with_read_permission() {
        // Arrange
        let session = SessionActor::new(
            SessionId(1),
            SessionPermissions::from_permissions(vec![Permission::parse(
                "queue://realm/area/jobs#read",
            )
            .unwrap()]),
        );
        let mut actor = make_queue_actor();
        let mut ctx = make_ctx();

        // Act
        let result = session.receive(
            RouteFamily::new(1),
            Route::new("queue://realm/area/jobs/receive"),
            30,
            Some(10),
            None,
            &mut actor,
            &mut ctx,
        );

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_reject_unauthorized_extend() {
        // Arrange
        let session = SessionActor::new(
            SessionId(1),
            SessionPermissions::from_permissions(vec![Permission::parse(
                "queue://realm/area/jobs#read",
            )
            .unwrap()]),
        );
        let mut actor = make_queue_actor();
        let mut ctx = make_ctx();

        // Act
        let result = session.extend(
            RouteFamily::new(1),
            Route::new("queue://realm/area/jobs/extend"),
            crate::domains::queue::protocol::MessageId::new(1),
            12345,
            60,
            &mut actor,
            &mut ctx,
        );

        // Assert
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "unauthorized: extend");
    }

    #[test]
    fn should_reject_unauthorized_complete() {
        // Arrange
        let session = SessionActor::new(
            SessionId(1),
            SessionPermissions::from_permissions(vec![Permission::parse(
                "queue://realm/area/jobs#read",
            )
            .unwrap()]),
        );
        let mut actor = make_queue_actor();
        let mut ctx = make_ctx();

        // Act
        let result = session.ack(
            RouteFamily::new(1),
            Route::new("queue://realm/area/jobs/ack"),
            crate::domains::queue::protocol::MessageId::new(1),
            12345,
            &mut actor,
            &mut ctx,
        );

        // Assert
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "unauthorized: ack");
    }

    #[test]
    fn should_allow_authorized_complete_with_write_permission() {
        // Arrange
        let session = SessionActor::new(
            SessionId(1),
            SessionPermissions::from_permissions(vec![Permission::parse(
                "queue://realm/area/jobs#write",
            )
            .unwrap()]),
        );
        let mut actor = make_queue_actor();
        let mut ctx = make_ctx();

        // Act
        let result = session.ack(
            RouteFamily::new(1),
            Route::new("queue://realm/area/jobs/ack"),
            crate::domains::queue::protocol::MessageId::new(1),
            12345,
            &mut actor,
            &mut ctx,
        );

        // Assert
        assert!(result.is_ok());
    }
}
