// LAYER: PROTOCOL (Transport-Agnostic)
//! Protocol helpers shared by transports and session logic.
//!
//! This layer provides:
//! - Frame TLV (type-length-value) wire format ([`tlv`]) for multiplexed frames
//! - Payload codec ([`payload_codec`]) for domain message bodies (sequential typed fields)
//! - Logical channel definitions
//! - Multiplexing helpers
//! - Domain-specific codecs (KV, etc)
//!
//! This layer is transport-agnostic and contains no Tokio, no routing, no domain logic.

pub mod error_codes;
pub mod frame;
pub mod frame_context;
pub mod kv_codec;
pub mod lease_codec;
pub mod manifest;
pub mod mux;
pub mod notice_codec;
pub mod payload_codec;
pub mod queue_codec;
pub mod rpc_codec;
pub mod schedule_codec;
pub mod stream_codec;
#[cfg(test)]
pub(crate) mod test_support;
pub mod tlv;

pub use error_codes::{
    kv as error_kv, lease as error_lease, notice as error_notice, queue as error_queue,
    rpc as error_rpc, schedule as error_schedule, stream as error_stream,
};
pub use frame::{ChannelId, FrameError};
pub use frame_context::FrameContext;
pub use kv_codec as kv;
pub use lease_codec as lease;
pub use mux::{ChannelMessage, Mux, MuxError, TypeMapping};
pub use notice_codec as notice;
pub use payload_codec::{PayloadDecoder, PayloadEncoder};
pub use queue_codec as queue;
pub use rpc_codec as rpc;
pub use schedule_codec as schedule;
pub use stream_codec as stream;
pub use tlv::{MessageType, TlvDecoder, TlvEncoder, TlvError, TlvRecord};
