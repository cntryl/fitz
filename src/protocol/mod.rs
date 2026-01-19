// LAYER: PROTOCOL (Transport-Agnostic)
//! Protocol helpers shared by transports and session logic.
//!
//! This layer provides:
//! - TLV (Tag-Length-Value) wire format encoding/decoding
//! - Logical channel definitions
//! - Multiplexing helpers
//! - Domain-specific codecs (KV, etc)
//!
//! This layer is transport-agnostic and contains no Tokio, no routing, no domain logic.

pub mod frame;
pub mod frame_context;
pub mod kv_codec;
pub mod queue_codec;
pub mod notice_codec;
pub mod stream_codec;
pub mod rpc_codec;
pub mod lease_codec;
pub mod schedule_codec;
pub mod mux;
pub mod tlv;
pub mod codec_trait;
pub mod tlv_codec;

pub use frame::{ChannelId, FrameError};
pub use frame_context::FrameContext;
pub use kv_codec as kv;
pub use queue_codec as queue;
pub use notice_codec as notice;
pub use stream_codec as stream;
pub use rpc_codec as rpc;
pub use lease_codec as lease;
pub use schedule_codec as schedule;
pub use mux::{ChannelMessage, Mux, MuxError, TypeMapping};
pub use tlv::{MessageType, TlvDecoder, TlvEncoder, TlvError, TlvRecord};
pub use codec_trait::{CodecBuilder, DomainCodec, DomainResponse};
pub use tlv_codec::{TlvDecoder as SimpleDecoder, TlvEncoder as SimpleEncoder};
