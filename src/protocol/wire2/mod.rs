// LAYER: PROTOCOL (Transport-Agnostic)
//! Wire format 2: the 0.3.0 format intended to freeze as 1.0.0.
//!
//! Defined by `docs/development/wire-format-1.0-design.md`. One message per
//! transport frame, a fixed 15-byte header, a structured handshake, and one
//! response envelope whose error carries a code, a class, and an effect.
//!
//! These codecs are pure functions over byte slices. They own no I/O and no
//! session state, so every SDK can be checked against the same golden vectors.

pub mod catalog;
pub mod envelope;
pub mod handshake;
pub mod header;

/// The wire version these codecs implement.
pub const WIRE_VERSION: u16 = 2;

/// Capability bits this broker implements. Wire format 2 ships none: every
/// behavior in the design is base protocol.
pub const BROKER_CAPABILITIES: u32 = 0;

/// Largest message value the broker accepts or frames until the limit is
/// wired through configuration.
pub const DEFAULT_MAX_MESSAGE_BYTES: u32 = 1024 * 1024;

/// Control message identifiers. Verb identifiers for the domains stay as they
/// are in the manifest; only the control block changes in wire format 2.
pub mod control {
    /// Client to broker, first message on a connection.
    pub const CONNECT: u16 = 1;
    /// Broker to client, answers an accepted `CONNECT`.
    pub const HELLO: u16 = 2;
    /// Broker to client, answers a refused `CONNECT`; the connection then closes.
    pub const CONNECT_REJECTED: u16 = 3;
    /// Broker to client at any time after `HELLO`: drain, forced close, or a limit change.
    pub const GOAWAY: u16 = 4;
}
