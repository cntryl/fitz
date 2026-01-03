//! Multiplexing layer for logical channels
//!
//! This module maps TLV message types to logical channels and enforces
//! per-channel backpressure via simple counters.

use crate::protocol::frame::ChannelId;
use crate::protocol::tlv::{MessageType, TlvRecord};
use bytes::Bytes;
use std::collections::HashMap;
use std::fmt;

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

/// Type-to-channel mapping with optional overrides
#[derive(Debug, Default, Clone)]
pub struct TypeMapping {
    overrides: HashMap<u16, ChannelId>,
}

impl TypeMapping {
    pub fn new() -> Self {
        Self {
            overrides: HashMap::new(),
        }
    }

    pub fn register(&mut self, msg_type: u16, channel: ChannelId) {
        self.overrides.insert(msg_type, channel);
    }

    pub fn get_channel(&self, msg_type: u16) -> Option<ChannelId> {
        if let Some(&channel) = self.overrides.get(&msg_type) {
            return Some(channel);
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
    capacities: HashMap<ChannelId, usize>,
    counters: HashMap<ChannelId, usize>,
}

impl Mux {
    pub fn new(channel_capacity: usize) -> Self {
        let mut capacities = HashMap::new();
        let mut counters = HashMap::new();

        for channel in ChannelId::all() {
            capacities.insert(channel, channel_capacity);
            counters.insert(channel, 0);
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

    pub fn route(&mut self, record: TlvRecord) -> Result<ChannelMessage, MuxError> {
        let channel = self
            .type_mapping
            .get_channel(record.msg_type().as_u16())
            .ok_or(MuxError::UnknownMessageType(record.msg_type().as_u16()))?;

        let counter = self.counters.get_mut(&channel).expect("channel missing");
        let capacity = self.capacities[&channel];
        if *counter >= capacity {
            return Err(MuxError::ChannelFull(channel));
        }

        *counter += 1;
        Ok(ChannelMessage::new(
            channel,
            record.msg_type(),
            record.value,
        ))
    }

    pub fn release(&mut self, channel: ChannelId) {
        if let Some(counter) = self.counters.get_mut(&channel) {
            if *counter > 0 {
                *counter -= 1;
            }
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
    }

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
