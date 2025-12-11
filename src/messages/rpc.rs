//! RpcMsg messages.

use crate::actor::ActorRef;

/// Messages for RpcActor.
#[derive(Debug)]
pub enum RpcMsg {
    /// Invoke an RPC.
    Invoke { realm: String, area: String, operation: String, payload: Vec<u8> },
}
