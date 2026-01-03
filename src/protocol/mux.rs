//! Multiplexing layer for logical channels
//!
//! This module maps TLV message types to logical channels and enforces
//! per-channel backpressure via simple counters.

use crate::protocol::frame::ChannelId;
use crate::protocol::tlv::{MessageType, TlvRecord};
use bytes::Bytes;
use std::fmt;
use std::sync::Arc;

impl ChannelId {
    /// Number of channels (stable)
    pub const COUNT: usize = 6;

    #[inline(always)]
    pub fn idx(self) -> usize {
        self as usize
    }
}

/// Message routed to a channel
#[derive(Debug, Clone)]
pub struct ChannelMessage {
    pub channel: ChannelId,
    pub msg_type: MessageType,
    pub payload: Bytes,
}

impl ChannelMessage {
    pub fn new(channel: ChannelId, msg_type: MessageType, payload: Bytes) -> Self {
        Self {
            channel,
            msg_type,
            payload,
        }
    }
}

/// Mapping errors
#[derive(Debug, Clone)]
pub enum MuxError {
    UnknownMessageType(u16),
    ChannelFull(ChannelId),
}

impl fmt::Display for MuxError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownMessageType(t) => write!(f, "unknown message type: {}", t),
            Self::ChannelFull(ch) => write!(f, "channel full: {}", ch),
        }
    }
}

impl std::error::Error for MuxError {}

/// Type-to-channel mapping with optional overrides.
///
/// Overrides are stored in a small, shared vector (Arc) to keep hot-path reads lock-free
/// and cache-friendly. Default routing is an inline range match.
#[derive(Debug, Clone)]
pub struct TypeMapping {
    overrides: Arc<Vec<(u16, ChannelId)>>,
}

impl Default for TypeMapping {
    fn default() -> Self {
        Self { overrides: Arc::new(Vec::new()) }
    }
}

impl TypeMapping {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register an override for a specific message type. This replaces the shared vector
    /// with a new Arc so hot-path readers can continue without locks.
    pub fn register(&mut self, msg_type: u16, channel: ChannelId) {
        let mut vec = (*self.overrides).clone();
        // replace or insert
        if let Some(entry) = vec.iter_mut().find(|e| e.0 == msg_type) {
            entry.1 = channel;
        } else {
            vec.push((msg_type, channel));
        }
        self.overrides = Arc::new(vec);
    }

    /// Get channel for msg_type. Fast path: if no overrides, do only a range check.
    #[inline(always)]
    pub fn get_channel(&self, msg_type: u16) -> Option<ChannelId> {
        // If overrides exist, do a small linear scan first (overrides rare and small)
        if !self.overrides.is_empty() {
            for &(t, ch) in self.overrides.iter() {
                if t == msg_type {
                    return Some(ch);
                }
            }
        }

        match msg_type {
            0..=99 => Some(ChannelId::Control),
            100..=199 => Some(ChannelId::Pub),
            200..=299 => Some(ChannelId::Sub),
            300..=399 => Some(ChannelId::Rpc),
            400..=499 => Some(ChannelId::Lease),
            _ => None,
        }
    }
}

/// Multiplexer enforcing per-channel backpressure
pub struct Mux {
    type_mapping: TypeMapping,
    // Fixed-size arrays for capacities and counters to avoid HashMap lookups on hot path
    capacities: [usize; ChannelId::COUNT],
    counters: [usize; ChannelId::COUNT],
}

/// Borrowed, zero-copy routing result
pub struct ChannelRef<'a> {
    pub channel: ChannelId,
    pub msg_type: MessageType,
    pub payload: &'a [u8],
}

impl Mux {
    pub fn new(channel_capacity: usize) -> Self {
        let mut capacities = [0usize; ChannelId::COUNT];
        let mut counters = [0usize; ChannelId::COUNT];

        for channel in ChannelId::all() {
            capacities[channel.idx()] = channel_capacity;
            counters[channel.idx()] = 0;
        }

        Self {
            type_mapping: TypeMapping::new(),
            capacities,
            counters,
        }
    }

    pub fn with_mapping(channel_capacity: usize, mapping: TypeMapping) -> Self {
        let mut mux = Self::new(channel_capacity);
        mux.type_mapping = mapping;
        mux
    }

    /// Zero-copy hot path: route a msg by type and payload slice. No allocation.
    #[inline(always)]
    pub fn route_ref<'a>(&mut self, msg_type: MessageType, payload: &'a [u8]) -> Result<ChannelRef<'a>, MuxError> {
        let t = msg_type.as_u16();
        let channel = self
            .type_mapping
            .get_channel(t)
            .ok_or(MuxError::UnknownMessageType(t))?;

        let idx = channel.idx();
        let counter = &mut self.counters[idx];
        let capacity = self.capacities[idx];
        if *counter >= capacity {
            return Err(MuxError::ChannelFull(channel));
        }
        *counter += 1;

        Ok(ChannelRef { channel, msg_type, payload })
    }

    /// Convenience owning API: consumes a `TlvRecord` and returns an owned `ChannelMessage`.
    pub fn route(&mut self, record: TlvRecord) -> Result<ChannelMessage, MuxError> {
        // extract msg_type first to avoid partial moves
        let mt = record.msg_type();
        let payload = record.value;
        let channel = self
            .type_mapping
            .get_channel(mt.as_u16())
            .ok_or(MuxError::UnknownMessageType(mt.as_u16()))?;

        let idx = channel.idx();
        let counter = &mut self.counters[idx];
        let capacity = self.capacities[idx];
        if *counter >= capacity {
            return Err(MuxError::ChannelFull(channel));
        }
        *counter += 1;

        Ok(ChannelMessage::new(channel, mt, payload))
    }

    pub fn release(&mut self, channel: ChannelId) {
        let idx = channel.idx();
        if self.counters[idx] > 0 {
            self.counters[idx] -= 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::tlv::{TlvDecoder, TlvEncoder};

    #[test]
    fn should_map_default_ranges() {
        let mapping = TypeMapping::new();
        assert_eq!(mapping.get_channel(50), Some(ChannelId::Control));
        assert_eq!(mapping.get_channel(150), Some(ChannelId::Pub));
        assert_eq!(mapping.get_channel(250), Some(ChannelId::Sub));
        assert_eq!(mapping.get_channel(350), Some(ChannelId::Rpc));
        assert_eq!(mapping.get_channel(450), Some(ChannelId::Lease));
        assert_eq!(mapping.get_channel(999), None);
    }

    #[test]
    fn should_route_to_channel() {
        // Arrange
        let mut mux = Mux::new(2);
        let mut encoder = TlvEncoder::new();
        encoder.encode(MessageType::new(100), b"payload");
        let data = encoder.finish();
        let decoder = TlvDecoder::new();
        let (record, _) = decoder.decode_one(&data).unwrap();

        // Act
        let msg = mux.route(record).unwrap();

        // Assert
        assert_eq!(msg.channel, ChannelId::Pub);
        mux.release(ChannelId::Pub);
        // Zero-copy path
        let mut mux2 = Mux::new(2);
        let (mt, slice, _) = decoder.decode_one_ref(&data).unwrap();
        let cref = mux2.route_ref(mt, slice).unwrap();
        assert_eq!(cref.channel, ChannelId::Pub);
        mux2.release(cref.channel);    }

    #[test]
    fn should_track_backpressure() {
        let mut mux = Mux::new(1);
        let mut encoder = TlvEncoder::new();
        encoder.encode(MessageType::new(100), b"value");
        let data = encoder.finish();
        let decoder = TlvDecoder::new();
        let (record, _) = decoder.decode_one(&data).unwrap();

        let _ = mux.route(record.clone()).unwrap();
        assert!(matches!(
            mux.route(record),
            Err(MuxError::ChannelFull(ChannelId::Pub))
        ));
    }
}
