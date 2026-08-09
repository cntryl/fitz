//! KV domain session authorization helpers.
//!
//! Responsibilities:
//! - Enforce session-level authorization for KV operations
//! - Forward authorized operations to the `KvActor`
//!
//! Authorization is checked using the realm field from `KvMessage::Begin`,
//! which is mapped to a route pattern for permission checking.

use crate::auth::Access;
use crate::domains::kv::actor::KvActor;
use crate::domains::kv::protocol::{KvMessage, KvResponse, TxMode};
use crate::runtime::routing::Route;
use crate::session::permissions::SessionPermissions;
use crate::session::session::SessionId;

/// Lightweight `SessionActor` helpers for the KV domain.
/// See the module documentation for its authorization and forwarding responsibilities.
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

    /// Attempt to begin a KV transaction.
    ///
    /// # Errors
    ///
    /// Returns an error when authorization fails, the message is not `Begin`, or
    /// the actor returns [`KvResponse::Error`].
    pub fn begin(&self, msg: KvMessage, kv_actor: &mut KvActor) -> Result<(), String> {
        if let KvMessage::Begin {
            ref scope, mode, ..
        } = msg
        {
            let realm = &scope.realm;
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
                KvResponse::Error { error } => Err(format!("kv error: {error}")),
                _ => Ok(()),
            }
        } else {
            Err("invalid message type for begin".to_string())
        }
    }

    /// Forward subsequent KV operations (after begin).
    /// Realm authorization was already checked at begin time.
    ///
    pub fn operation(&self, kv_actor: &mut KvActor, msg: KvMessage) -> KvResponse {
        kv_actor.handle(msg)
    }
}
