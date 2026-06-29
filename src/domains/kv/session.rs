//! KV domain session authorization helpers.
//!
//! Responsibilities:
//! - Enforce session-level authorization for KV operations
//! - Forward authorized operations to the KvActor
//!
//! Authorization is checked using the realm field from KvMessage::Begin,
//! which is mapped to a route pattern for permission checking.

use crate::auth::Access;
use crate::domains::kv::actor::KvActor;
use crate::domains::kv::protocol::{KvMessage, KvResponse, TxMode};
use crate::runtime::routing::Route;
use crate::session::permissions::SessionPermissions;
use crate::session::session::SessionId;

/// Lightweight SessionActor helpers for the KV domain.
///
/// Responsibilities:
/// - Enforce session-level authorization for KV operations
/// - Forward authorized operations to the KvActor
pub struct SessionActor {
    pub session_id: SessionId,
    pub permissions: SessionPermissions,
}

impl SessionActor {
    #[must_use]
    pub fn new(session_id: SessionId, permissions: SessionPermissions) -> Self {
        Self {
            session_id,
            permissions,
        }
    }

    /// Attempt to begin a KV transaction. Returns Err if authorization fails or actor returns error.
    pub fn begin(&self, msg: KvMessage, kv_actor: &mut KvActor) -> Result<(), String> {
        if let KvMessage::Begin {
            ref realm, mode, ..
        } = msg
        {
            // Extract realm-based route for authorization check
            // Format: "kv://realm" for basic realm-level authorization
            let route = Route::new(format!("kv://{realm}"));

            // Authorization policy: **write implies readwrite**, **read implies readonly**.
            // Authorization depends on transaction mode:
            // - ReadOnly: requires Read OR Write permission
            // - ReadWrite: requires Write permission
            match mode {
                TxMode::ReadOnly => {
                    if !self.permissions.allows(&route, Access::Read)
                        && !self.permissions.allows(&route, Access::Write)
                    {
                        return Err(format!("unauthorized: realm '{realm}'"));
                    }
                }
                TxMode::ReadWrite => {
                    if !self.permissions.allows(&route, Access::Write) {
                        return Err(format!(
                            "unauthorized: write access required for realm '{realm}'"
                        ));
                    }
                }
            }

            // Forward to actor and check for errors
            let response = kv_actor.handle(msg);
            match response {
                KvResponse::BeginOk { .. } => Ok(()),
                KvResponse::Error { error } => Err(format!("kv error: {error}")),
                _ => Ok(()),
            }
        } else {
            Err("invalid message type for begin".to_string())
        }
    }

    /* #[cfg(test)]
    mod tests {
        use super::*;
        use crate::auth::{Access, Permission};
        use crate::domains::kv::actor::KvActor;
        use crate::testkit::create_test_engine_with_cfs;
        use crate::session::session::SessionId;

        #[test]
        fn should_reject_read_only_session_begin_read_write() {
            // Arrange
            let p = Permission::parse("kv://acme#read").unwrap();
            let perms = SessionPermissions::from_permissions(vec![p]);
            let actor = SessionActor::new(SessionId(1), perms);
            let mut kv = KvActor::new(create_test_engine_with_cfs(vec![1]));
            let msg = KvMessage::Begin {
                route_family: crate::runtime::routing::RouteFamily::new(1),
                realm: "acme".to_string(),
                area: "kv".to_string(),
                resource: "table1".to_string(),
                mode: TxMode::ReadWrite,
                write_options: cntryl_midge::WriteOptions::buffered(),
            };

            // Act
            let res = actor.begin(msg, &mut kv);

            // Assert
            assert!(res.is_err());
            assert!(res.unwrap_err().contains("unauthorized"));
        }

        #[test]
        fn should_allow_read_only_session_begin_read_only() {
            // Arrange
            let p = Permission::parse("kv://acme#read").unwrap();
            let perms = SessionPermissions::from_permissions(vec![p]);
            let actor = SessionActor::new(SessionId(1), perms);
            let mut kv = KvActor::new(create_test_engine_with_cfs(vec![1]));
            let msg = KvMessage::Begin {
                route_family: crate::runtime::routing::RouteFamily::new(1),
                realm: "acme".to_string(),
                area: "kv".to_string(),
                resource: "table1".to_string(),
                mode: TxMode::ReadOnly,
                write_options: cntryl_midge::WriteOptions::buffered(),
            };

            // Act
            let res = actor.begin(msg, &mut kv);

            // Assert
            assert!(res.is_ok());
        }

        #[test]
        fn should_allow_write_session_begin_read_write() {
            // Arrange
            let p = Permission::parse("kv://acme#write").unwrap();
            let perms = SessionPermissions::from_permissions(vec![p]);
            let actor = SessionActor::new(SessionId(1), perms);
            let mut kv = KvActor::new(create_test_engine_with_cfs(vec![1]));
            let msg = KvMessage::Begin {
                route_family: crate::runtime::routing::RouteFamily::new(1),
                realm: "acme".to_string(),
                area: "kv".to_string(),
                resource: "table1".to_string(),
                mode: TxMode::ReadWrite,
                write_options: cntryl_midge::WriteOptions::buffered(),
            };

            // Act
            let res = actor.begin(msg, &mut kv);

            // Assert
            assert!(res.is_ok());
        }
    }

     */

    /// Forward subsequent KV operations (after begin).
    /// Realm authorization was already checked at begin time.
    pub fn operation(&self, kv_actor: &mut KvActor, msg: KvMessage) -> Result<(), String> {
        kv_actor.handle(msg);
        Ok(())
    }
}
