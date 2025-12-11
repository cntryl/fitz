//! StreamMsg messages.

use crate::actor::ActorRef;

/// Messages for StreamActor.
#[derive(Debug)]
pub enum StreamMsg {
    /// Append to a stream.
    Append { realm: String, area: String, stream_name: String, payload: Vec<u8> },
}
