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
use crate::domains::kv::protocol::KvMessage;
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
    pub fn new(session_id: SessionId, permissions: SessionPermissions) -> Self {
        Self {
            session_id,
            permissions,
        }
    }

    /// Attempt to begin a KV transaction. Returns Err if authorization fails.
    pub fn begin(&self, msg: KvMessage, kv_actor: &mut KvActor) -> Result<(), String> {
        if let KvMessage::Begin { ref realm, .. } = msg {
            // Extract realm-based route for authorization check
            // Format: "kv://realm" for basic realm-level authorization
            let route = Route::new(format!("kv://{}", realm));

            // Check that session is authorized for this realm
            // (either read or write permission is sufficient for access)
            if !self.permissions.allows(&route, Access::Read)
                && !self.permissions.allows(&route, Access::Write)
            {
                return Err(format!("unauthorized: realm '{}'", realm));
            }

            kv_actor.handle(msg);
            Ok(())
        } else {
            Err("invalid message type for begin".to_string())
        }
    }

    /// Forward subsequent KV operations (after begin).
    /// Realm authorization was already checked at begin time.
    pub fn operation(&self, kv_actor: &mut KvActor, msg: KvMessage) -> Result<(), String> {
        kv_actor.handle(msg);
        Ok(())
    }
}
