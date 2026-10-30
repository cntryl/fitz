//! Message identifiers and the decoded message container for wire format 2.
//!
//! The 15-byte header codec lives in [`crate::protocol::wire2::header`]; domain
//! payload fields are encoded with [`crate::protocol::payload_codec`].

use bytes::Bytes;
use std::fmt;

/// Message type identifier: a verb wire code or a control identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MessageType(pub u16);

impl MessageType {
    /// Client to broker, first message on a connection.
    pub const CONNECT: MessageType = MessageType(super::wire2::control::CONNECT);
    /// Broker to client, answers an accepted `CONNECT`.
    pub const HELLO: MessageType = MessageType(super::wire2::control::HELLO);
    /// Broker to client, answers a refused `CONNECT`; the connection then closes.
    pub const CONNECT_REJECTED: MessageType = MessageType(super::wire2::control::CONNECT_REJECTED);
    /// Broker to client at any time after `HELLO`: drain, forced close, or a limit change.
    pub const GOAWAY: MessageType = MessageType(super::wire2::control::GOAWAY);

    #[inline]
    #[must_use]
    pub fn new(value: u16) -> Self {
        Self(value)
    }

    #[inline]
    #[must_use]
    pub fn as_u16(&self) -> u16 {
        self.0
    }
}

impl fmt::Display for MessageType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "msg_type({})", self.0)
    }
}

/// One decoded message: its type and its value bytes.
#[derive(Debug, Clone)]
pub struct WireMessage {
    pub msg_type: MessageType,
    pub value: Bytes,
}

impl WireMessage {
    #[must_use]
    pub fn new(msg_type: MessageType, value: Bytes) -> Self {
        Self { msg_type, value }
    }

    /// The message type.
    #[must_use]
    pub fn msg_type(&self) -> MessageType {
        self.msg_type
    }

    /// Borrow the value bytes.
    #[must_use]
    pub fn value(&self) -> &[u8] {
        &self.value
    }

    /// The type and a borrow of the value, for zero-copy routing.
    #[must_use]
    pub fn as_ref(&self) -> (MessageType, &[u8]) {
        (self.msg_type, &self.value)
    }
}
