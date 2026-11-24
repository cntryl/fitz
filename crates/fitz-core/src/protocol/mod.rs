// LAYER: PROTOCOL (Transport-Agnostic)
//! Protocol helpers shared by transports and session logic.
//!
//! This layer provides:
//! - Message identifiers and the decoded message container ([`message`]); the header codec is [`wire2::header`]
//! - Payload codec ([`payload_codec`]) for domain message bodies (sequential typed fields)
//! - Logical channel definitions
//! - Multiplexing helpers
//!
//! This layer is transport-agnostic and contains no Tokio, no routing, no domain logic.

pub mod auth_route;
pub mod error_codes;
pub mod frame;
pub mod frame_context;
pub mod manifest;
pub mod message;
pub mod mux;
pub mod payload_codec;
#[doc(hidden)]
pub mod test_support;
pub mod wire2;
pub mod wire3;

pub use error_codes::{
    kv as error_kv, lease as error_lease, notice as error_notice, queue as error_queue,
    rpc as error_rpc, schedule as error_schedule, stream as error_stream,
};
pub use frame::{ChannelId, FrameError};
pub use frame_context::FrameContext;
pub use message::{MessageType, WireMessage};
pub use mux::{ChannelMessage, Mux, MuxError, TypeMapping};
pub use payload_codec::{PayloadDecoder, PayloadEncoder};
