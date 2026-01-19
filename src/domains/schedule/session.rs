//! Schedule domain session authorization helpers.
//!
//! Responsibilities:
//! - Enforce session-level authorization for schedule operations
//! - Forward authorized operations to the ScheduleActor

use crate::auth::Access;
use crate::domains::schedule::actor::ScheduleActor;
use crate::domains::schedule::ScheduleMessage;
use crate::runtime::actor::{Actor, Context};
use crate::runtime::routing::Route;
use crate::session::permissions::SessionPermissions;
use crate::session::session::SessionId;

/// Lightweight SessionActor helpers for the schedule domain.
///
/// Responsibilities:
/// - Enforce session-level authorization for schedule operations
/// - Forward authorized operations to the ScheduleActor
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

    /// Attempt to schedule a job. Returns Err if authorization fails.
    pub fn schedule(
        &self,
        route: Route,
        msg: ScheduleMessage,
        actor: &mut ScheduleActor,
        ctx: &mut Context<ScheduleActor>,
    ) -> Result<(), String> {
        // Schedule requires write access (creating a scheduled job is a write operation)
        if !self.permissions.allows(&route, Access::Write) {
            return Err("unauthorized: schedule".to_string());
        }

        actor.receive(msg, ctx);
        Ok(())
    }

    /// Attempt to cancel a scheduled job. Returns Err if authorization fails.
    pub fn cancel(
        &self,
        route: Route,
        msg: ScheduleMessage,
        actor: &mut ScheduleActor,
        ctx: &mut Context<ScheduleActor>,
    ) -> Result<(), String> {
        // Cancel requires write access (modifying a scheduled job is a write operation)
        if !self.permissions.allows(&route, Access::Write) {
            return Err("unauthorized: cancel".to_string());
        }

        actor.receive(msg, ctx);
        Ok(())
    }

    /// Attempt to get job status. Returns Err if authorization fails.
    pub fn status(
        &self,
        route: Route,
        msg: ScheduleMessage,
        actor: &mut ScheduleActor,
        ctx: &mut Context<ScheduleActor>,
    ) -> Result<(), String> {
        // Status query requires read access
        if !self.permissions.allows(&route, Access::Read) {
            return Err("unauthorized: status".to_string());
        }

        actor.receive(msg, ctx);
        Ok(())
    }
}
