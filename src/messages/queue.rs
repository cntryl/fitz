//! QueueMsg messages.

use crate::actor::ActorRef;

/// Messages for QueueActor.
#[derive(Debug)]
pub enum QueueMsg {
    /// Enqueue a message.
    Enqueue { realm: String, area: String, queue_name: String, payload: Vec<u8> },
}
