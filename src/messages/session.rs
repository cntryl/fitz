//! SessionActor messages.

use crate::actor::ActorRef;

/// Messages for SessionActor.
#[derive(Debug)]
pub enum SessionMsg {
    /// New TLV frame arrived from transport.
    InboundFrame { frame_type: u16, payload: Vec<u8> },

    /// Send outbound frame to client.
    OutboundFrame { frame_type: u16, payload: Vec<u8> },

    /// Connection closed.
    ConnectionClosed,
}
