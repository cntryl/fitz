//! Frame context: Associates TLV frame metadata with router Envelope
//!
//! # Purpose
//!
//! The transport layer extracts session_id, channel_id, and msg_type from the TLV frame,
//! but the Envelope struct doesn't carry this metadata. This module defines FrameContext
//! which combines the transport metadata needed by domain handlers.
//!
//! # Design
//!
//! FrameContext is stored as the payload of an Envelope when a frame arrives:
//! - Transport has: session_id, channel_id, msg_type, raw bytes
//! - FrameContext wraps these and is boxed as Envelope payload
//! - Domain sinks receive Envelope, extract FrameContext, and can access all needed info

use crate::protocol::frame::ChannelId;
use crate::protocol::tlv::MessageType;
use bytes::Bytes;

/// Frame context: Transport metadata for domain handlers
///
/// Created at the ingress boundary when a transport frame arrives.
/// Stored as the payload of an Envelope for routing.
/// This allows domain sinks to access the original frame metadata
/// needed for TLV parsing and session correlation.
#[derive(Clone)]
pub struct FrameContext {
    /// Session ID from transport
    pub session_id: u64,
    /// Channel ID from transport (e.g., Data, Control, Priority)
    pub channel_id: ChannelId,
    /// Message type from TLV header
    pub msg_type: MessageType,
    /// Raw TLV payload bytes
    pub payload: Bytes,
}

impl FrameContext {
    /// Create a new frame context from transport metadata
    pub fn new(
        session_id: u64,
        channel_id: ChannelId,
        msg_type: MessageType,
        payload: Bytes,
    ) -> Self {
        Self {
            session_id,
            channel_id,
            msg_type,
            payload,
        }
    }
}

impl std::fmt::Debug for FrameContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FrameContext")
            .field("session_id", &self.session_id)
            .field("channel_id", &self.channel_id)
            .field("msg_type", &self.msg_type)
            .field("payload_len", &self.payload.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_frame_context() {
        // Arrange

        // Act
        let ctx = FrameContext::new(
            123,
            ChannelId::Pub,
            MessageType::new(103), // KV GET operation
            Bytes::from("test payload"),
        );

        // Assert
        assert_eq!(ctx.session_id, 123);
        assert_eq!(ctx.channel_id, ChannelId::Pub);
        assert_eq!(ctx.msg_type.as_u16(), 103);
        assert_eq!(ctx.payload.len(), 12);
    }
}
