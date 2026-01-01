// LAYER: PROTOCOL (Transport-Agnostic)
//! Protocol helpers shared by transports and session logic.
//!
//! This layer provides:
//! - TLV (Tag-Length-Value) wire format encoding/decoding
//! - Logical channel definitions
//! - Multiplexing helpers
//!
//! This layer is transport-agnostic and contains no Tokio, no routing, no domain logic.

pub mod frame;
pub mod mux;
pub mod tlv;

pub use frame::{ChannelId, FrameError};
pub use mux::{ChannelMessage, Mux, MuxError, TypeMapping};
pub use tlv::{MessageType, TlvDecoder, TlvEncoder, TlvError, TlvRecord};
