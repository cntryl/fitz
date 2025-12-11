//! RouterActor messages.

use crate::actor::ActorRef;

/// Messages for RouterActor.
#[derive(Debug)]
pub enum RouterMsg {
    /// Register a route handler.
    RegisterRoute { route: String },
    /// Unregister a route.
    UnregisterRoute { route: String },
}
