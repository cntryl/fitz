use crate::domains::queue::protocol::QueueMessage;
use crate::domains::queue::queue_actor::QueueActor;
use crate::runtime::actor::{Context, Actor};
use crate::session::permissions::SessionPermissions;
use crate::runtime::routing::Route;
use crate::auth::Access;
use crate::session::session::SessionId;
use crate::runtime::routing::RouteFamily;
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

    /// Attempt to enqueue a message. Returns Err if authorization fails.
    pub fn enqueue(
        &self,
        family: RouteFamily,
        route: Route,
        body: Bytes,
        delay_seconds: Option<u64>,
        queue_actor: &mut QueueActor,
        ctx: &mut Context<QueueActor>,
    ) -> Result<(), String> {
        // Enqueue requires write access (adding messages is a write operation)
        if !self.permissions.allows(&route, Access::Write) {
            return Err("unauthorized: enqueue".to_string());
        }

        let msg = QueueMessage::Enqueue {
            family_id: family,
            route,
            body,
            delay_seconds,
        };
        queue_actor.receive(msg, ctx);
        Ok(())
    }

    /// Attempt to reserve messages. Returns Err if authorization fails.
    #[allow(clippy::too_many_arguments)]
    pub fn reserve(
        &self,
        family: RouteFamily,
        route: Route,
        lease_seconds: u64,
        batch_size: Option<usize>,
        wait_seconds: Option<u64>,
        queue_actor: &mut QueueActor,
        ctx: &mut Context<QueueActor>,
    ) -> Result<(), String> {
        // Reserve requires read access (consuming messages is a read operation)
        if !self.permissions.allows(&route, Access::Read) {
            return Err("unauthorized: reserve".to_string());
        }

        let msg = QueueMessage::Reserve {
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
        // Extend requires write access (modifying lease state is a write operation)
        if !self.permissions.allows(&route, Access::Write) {
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

    /// Attempt to complete a message. Returns Err if authorization fails.
    pub fn complete(
        &self,
        family: RouteFamily,
        route: Route,
        id: crate::domains::queue::protocol::MessageId,
        token: u64,
        queue_actor: &mut QueueActor,
        ctx: &mut Context<QueueActor>,
    ) -> Result<(), String> {
        // Complete requires write access (deleting messages is a write operation)
        if !self.permissions.allows(&route, Access::Write) {
            return Err("unauthorized: complete".to_string());
        }

        let msg = QueueMessage::Complete {
            family_id: family,
            route,
            id,
            token,
        };
        queue_actor.receive(msg, ctx);
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
    use crate::domains::queue::protocol::QueueKey;
    use std::sync::Arc;
    use tempfile;

    fn make_ctx() -> Context<QueueActor> {
        let router = Router::new();
        let addr = RouteAddress::new(RouteFamily::new(1), Route::new("queue://realm/area/jobs"));
        Context::new(addr, Arc::new(router))
    }

    fn make_queue_actor() -> QueueActor {
        let temp_dir = tempfile::tempdir().unwrap();
        let store = Arc::new(
            cntryl_midge::MidgeEngine::open(temp_dir.path().join("test.db"))
                .expect("Failed to open Midge"),
        );
        let queue_key = QueueKey {
            family: RouteFamily::new(1),
            realm: "realm".to_string(),
            area: "area".to_string(),
            resource: "jobs".to_string(),
        };
        QueueActor::new(RouteFamily::new(1), queue_key, store, None)
    }

    #[test]
    fn should_reject_unauthenticated_enqueue() {
        // Arrange
        let session = SessionActor::new(
            SessionId(1),
            SessionPermissions::from_permissions(vec![]),
        );
        let mut actor = make_queue_actor();
        let mut ctx = make_ctx();

        // Act
        let result = session.enqueue(
            RouteFamily::new(1),
            Route::new("queue://realm/area/jobs/enqueue"),
            Bytes::from("test"),
            None,
            &mut actor,
            &mut ctx,
        );

        // Assert
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "unauthorized: enqueue");
    }

    #[test]
    fn should_allow_authorized_enqueue() {
        // Arrange
        let session = SessionActor::new(
            SessionId(1),
            SessionPermissions::from_permissions(vec![
                Permission::parse("queue://realm/area/jobs#write").unwrap(),
            ]),
        );
        let mut actor = make_queue_actor();
        let mut ctx = make_ctx();

        // Act
        let result = session.enqueue(
            RouteFamily::new(1),
            Route::new("queue://realm/area/jobs/enqueue"),
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
            SessionPermissions::from_permissions(vec![
                Permission::parse("queue://realm/area/other#read").unwrap(),
            ]),
        );
        let mut actor = make_queue_actor();
        let mut ctx = make_ctx();

        // Act
        let result = session.reserve(
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
        assert_eq!(result.unwrap_err(), "unauthorized: reserve");
    }

    #[test]
    fn should_allow_authorized_reserve_with_read_permission() {
        // Arrange
        let session = SessionActor::new(
            SessionId(1),
            SessionPermissions::from_permissions(vec![
                Permission::parse("queue://realm/area/jobs#read").unwrap(),
            ]),
        );
        let mut actor = make_queue_actor();
        let mut ctx = make_ctx();

        // Act
        let result = session.reserve(
            RouteFamily::new(1),
            Route::new("queue://realm/area/jobs/reserve"),
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
            SessionPermissions::from_permissions(vec![
                Permission::parse("queue://realm/area/jobs#read").unwrap(),
            ]),
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
            SessionPermissions::from_permissions(vec![
                Permission::parse("queue://realm/area/jobs#read").unwrap(),
            ]),
        );
        let mut actor = make_queue_actor();
        let mut ctx = make_ctx();

        // Act
        let result = session.complete(
            RouteFamily::new(1),
            Route::new("queue://realm/area/jobs/complete"),
            crate::domains::queue::protocol::MessageId::new(1),
            12345,
            &mut actor,
            &mut ctx,
        );

        // Assert
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "unauthorized: complete");
    }

    #[test]
    fn should_allow_authorized_complete_with_write_permission() {
        // Arrange
        let session = SessionActor::new(
            SessionId(1),
            SessionPermissions::from_permissions(vec![
                Permission::parse("queue://realm/area/jobs#write").unwrap(),
            ]),
        );
        let mut actor = make_queue_actor();
        let mut ctx = make_ctx();

        // Act
        let result = session.complete(
            RouteFamily::new(1),
            Route::new("queue://realm/area/jobs/complete"),
            crate::domains::queue::protocol::MessageId::new(1),
            12345,
            &mut actor,
            &mut ctx,
        );

        // Assert
        assert!(result.is_ok());
    }
}
