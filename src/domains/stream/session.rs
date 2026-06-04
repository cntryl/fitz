//! Session integration for stream operations

use crate::auth::Access;
use crate::domains::stream::protocol::StreamMessage;
use crate::domains::stream::StreamActor;
use crate::prelude::Actor;
use crate::runtime::actor::Context;
use crate::session::permissions::SessionPermissions;
use crate::session::session::SessionId;

/// Lightweight SessionActor helpers for the stream domain.
///
/// Responsibilities:
/// - Enforce session-level authorization for stream operations
/// - Forward authorized operations to the StreamActor
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

    /// Begin append session (requires write permission)
    pub fn begin_session(
        &self,
        msg: StreamMessage,
        actor: &mut StreamActor,
        ctx: &mut Context<StreamActor>,
    ) -> Result<(), String> {
        if let StreamMessage::Begin { ref route, .. } = msg {
            if !self.permissions.allows(route, Access::Write) {
                return Err("unauthorized: begin session".to_string());
            }

            actor.receive(msg, ctx);
            Ok(())
        } else {
            Err("invalid message type".to_string())
        }
    }

    /// Append to session (requires write permission)
    pub fn append_to_session(
        &self,
        msg: StreamMessage,
        actor: &mut StreamActor,
        ctx: &mut Context<StreamActor>,
    ) -> Result<(), String> {
        if let StreamMessage::Append { .. } = msg {
            // Permission was checked when session was begun
            actor.receive(msg, ctx);
            Ok(())
        } else {
            Err("invalid message type".to_string())
        }
    }

    /// Commit session (requires write permission)
    pub fn commit_session(
        &self,
        msg: StreamMessage,
        actor: &mut StreamActor,
        ctx: &mut Context<StreamActor>,
    ) -> Result<(), String> {
        if let StreamMessage::Commit { .. } = msg {
            // Permission was checked when session was begun
            actor.receive(msg, ctx);
            Ok(())
        } else {
            Err("invalid message type".to_string())
        }
    }

    /// Rollback session
    pub fn abort_session(
        &self,
        msg: StreamMessage,
        actor: &mut StreamActor,
        ctx: &mut Context<StreamActor>,
    ) -> Result<(), String> {
        if let StreamMessage::Rollback { .. } = msg {
            actor.receive(msg, ctx);
            Ok(())
        } else {
            Err("invalid message type".to_string())
        }
    }

    /// Read from stream (requires read permission)
    pub fn read_stream(
        &self,
        msg: StreamMessage,
        actor: &mut StreamActor,
        ctx: &mut Context<StreamActor>,
    ) -> Result<(), String> {
        let route = match &msg {
            StreamMessage::Read { route, .. } => route,
            StreamMessage::Last { route, .. } => route,
            StreamMessage::GetMetadata { route, .. } => route,
            _ => return Err("invalid message type".to_string()),
        };

        if !self.permissions.allows(route, Access::Read) {
            return Err("unauthorized: read".to_string());
        }

        actor.receive(msg, ctx);
        Ok(())
    }
}
