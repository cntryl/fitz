//! Wire format 3 protocol surface.
//!
//! Wire 3 retains the released fixed header, message registry, correlation
//! rules, and core payload layouts. The new extension envelope is opt-in per
//! message through header flag bit 2.

pub mod extensions;
pub mod header;

/// The only wire generation accepted by this broker build.
pub const WIRE_VERSION: u16 = 3;

/// Per-publish queue TTL in hours, carried by the ENQUEUE extension.
pub const QUEUE_MESSAGE_TTL_CAPABILITY_ID: u32 = 1;
/// Relinquish a queue reservation, with an optional visibility delay.
pub const QUEUE_NACK_CAPABILITY_ID: u32 = 2;
pub const BROKER_CAPABILITY_IDS: &[u32] =
    &[QUEUE_MESSAGE_TTL_CAPABILITY_ID, QUEUE_NACK_CAPABILITY_ID];

/// Legacy capability bit fields remain zero; Wire 3 capabilities use stable
/// numeric IDs in the CONNECT and HELLO extension field registry.
pub const BROKER_CAPABILITIES: u32 = 0;

pub mod control {
    pub use crate::protocol::wire2::control::*;
}

pub mod catalog {
    pub use crate::protocol::wire2::catalog::*;
}

pub mod envelope {
    pub use crate::protocol::wire2::envelope::*;
}

pub mod handshake {
    pub use crate::protocol::wire2::handshake::*;
}
