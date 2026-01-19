//! Common imports and prelude

pub use crate::runtime::envelope::{Envelope, MessageId};
pub use crate::runtime::*;

/// Default HTTP/WebSocket listen port
pub const DEFAULT_HTTP_PORT: u16 = 4090;

/// Default TCP (length-prefixed) listen port
pub const DEFAULT_TCP_PORT: u16 = 4091;

