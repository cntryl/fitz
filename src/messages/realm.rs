//! RealmMsg messages.

use crate::actor::ActorRef;
use super::session::SessionMsg;

/// Messages for RealmActor.
#[derive(Debug)]
pub enum RealmMsg {
    /// Subscribe a session to a route.
    Subscribe { route: String, session: ActorRef<SessionMsg>, connection_id: u64 },
    /// Unsubscribe a session.
    Unsubscribe { route: String, connection_id: u64 },
}
