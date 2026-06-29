//! Schedule domain session authorization helpers.
//!
//! Responsibilities:
//! - Enforce session-level authorization for schedule operations
//! - Validate permissions before forwarding to ScheduleActor

use crate::auth::Access;
use crate::domains::schedule::ScheduleMessage;
use crate::runtime::routing::Route;
use crate::session::permissions::SessionPermissions;
use crate::session::session::SessionId;

/// Lightweight SessionActor helpers for the schedule domain.
///
/// Responsibilities:
/// - Enforce session-level authorization for schedule operations
/// - Validate route access based on operation type
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

    /// Validate authorization for a schedule operation
    pub fn authorize(&self, route: &Route, msg: &ScheduleMessage) -> Result<(), String> {
        match msg {
            ScheduleMessage::Create { .. }
            | ScheduleMessage::CreateBatch { .. }
            | ScheduleMessage::Cancel { .. } => {
                // Create/Cancel requires write access
                if !self.permissions.allows(route, Access::Write) {
                    return Err("unauthorized: write access required".to_string());
                }
            }
            ScheduleMessage::List { .. } => {
                // List requires read access
                if !self.permissions.allows(route, Access::Read) {
                    return Err("unauthorized: read access required".to_string());
                }
            }
            ScheduleMessage::Subscribe { .. }
            | ScheduleMessage::Unsubscribe { .. }
            | ScheduleMessage::UnsubscribeAll { .. } => {
                // Subscribe requires read access (receiving notifications)
                if !self.permissions.allows(route, Access::Read) {
                    return Err("unauthorized: read access required".to_string());
                }
            }
        }
        Ok(())
    }
}
