//! KV domain session authorization helpers.
//!
//! The KV domain is simpler than others: it has no explicit routes in messages.
//! Instead, authorization happens at the HTTP/WS transport layer based on the
//! realm/area/resource URI segments. This module is intentionally a stub
//! for future expansion if authorization becomes more fine-grained.

use crate::domains::kv::actor::KvActor;
use crate::domains::kv::protocol::KvMessage;
use crate::session::permissions::SessionPermissions;
use crate::session::session::SessionId;

/// Lightweight SessionActor helpers for the KV domain.
///
/// Currently acts as a pass-through for domain operations.
/// Authorization is enforced at the HTTP/WS transport layer.
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

    /// Forward a message to the KV actor.
    /// Authorization is handled at transport layer.
    pub fn forward(&self, kv_actor: &mut KvActor, msg: KvMessage) {
        kv_actor.handle(msg);
    }
}
