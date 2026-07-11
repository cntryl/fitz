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

    #[inline]
    #[must_use]
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
            Self::UnknownMessageType(t) => write!(f, "unknown message type: {t}"),
            Self::ChannelFull(ch) => write!(f, "channel full: {ch}"),
        }
    }
}

impl std::error::Error for MuxError {}

/// Type-to-channel mapping with optional overrides.
///
/// Overrides are stored in a small, shared vector (Arc) to keep hot-path reads lock-free
/// and cache-friendly. Default routing comes from the exact protocol manifest.
#[derive(Debug, Clone)]
pub struct TypeMapping {
    overrides: Arc<Vec<(u16, ChannelId)>>,
}

impl Default for TypeMapping {
    fn default() -> Self {
        Self {
            overrides: Arc::new(Vec::new()),
        }
    }
}

impl TypeMapping {
    #[must_use]
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

    /// Get the manifest channel for `msg_type`, honoring an explicit override.
    #[inline]
    #[must_use]
    pub fn get_channel(&self, msg_type: u16) -> Option<ChannelId> {
        // If overrides exist, do a small linear scan first (overrides rare and small)
        if !self.overrides.is_empty() {
            for &(t, ch) in self.overrides.iter() {
                if t == msg_type {
                    return Some(ch);
                }
            }
        }

        let entry = crate::protocol::manifest::entry(MessageType::new(msg_type))?;
        match entry.decoder {
            crate::protocol::manifest::ManifestDecoder::Control => Some(ChannelId::Control),
            crate::protocol::manifest::ManifestDecoder::Kv
            | crate::protocol::manifest::ManifestDecoder::Notice => Some(ChannelId::Pub),
            crate::protocol::manifest::ManifestDecoder::Queue
            | crate::protocol::manifest::ManifestDecoder::Stream => Some(ChannelId::Sub),
            crate::protocol::manifest::ManifestDecoder::Rpc => Some(ChannelId::Rpc),
            crate::protocol::manifest::ManifestDecoder::Lease => Some(ChannelId::Lease),
            crate::protocol::manifest::ManifestDecoder::Schedule => Some(ChannelId::Internal),
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

/// RAII grant that releases channel capacity when dropped.
///
/// This type borrows the `Mux` mutably for its lifetime to guarantee the
/// grant is released to the same mux instance. Holding a `ChannelGrant` will
/// prevent other mutable operations on the same `Mux` until it is dropped.
pub struct ChannelGrant<'a> {
    mux: &'a mut Mux,
    channel: ChannelId,
    released: bool,
}

impl ChannelGrant<'_> {
    /// Explicitly consume the grant and release capacity early.
    pub fn release(mut self) {
        if !self.released {
            self.mux.release(self.channel);
            self.released = true;
        }
        // Prevent Drop from running now that we've already released
        std::mem::forget(self);
    }
}

impl Drop for ChannelGrant<'_> {
    fn drop(&mut self) {
        if !self.released {
            // Release capacity back to mux on drop.
            self.mux.release(self.channel);
            self.released = true;
        }
    }
}

impl Mux {
    #[must_use]
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

    #[must_use]
    pub fn with_mapping(channel_capacity: usize, mapping: TypeMapping) -> Self {
        let mut mux = Self::new(channel_capacity);
        mux.type_mapping = mapping;
        mux
    }

    /// Zero-copy hot path: route a msg by type and payload slice. No allocation.
    ///
    /// # Errors
    ///
    /// Returns [`MuxError::UnknownMessageType`] when no channel mapping exists
    /// for `msg_type`, or [`MuxError::ChannelFull`] when the target channel is
    /// already at capacity.
    #[inline]
    pub fn route_ref<'a>(
        &mut self,
        msg_type: MessageType,
        payload: &'a [u8],
    ) -> Result<ChannelRef<'a>, MuxError> {
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

        Ok(ChannelRef {
            channel,
            msg_type,
            payload,
        })
    }

    /// Route and return an RAII grant that will release capacity when dropped.
    ///
    /// # Errors
    ///
    /// Returns [`MuxError::UnknownMessageType`] when no channel mapping exists
    /// for `msg_type`, or [`MuxError::ChannelFull`] when the target channel is
    /// already at capacity.
    pub fn route_grant<'a>(
        &'a mut self,
        msg_type: MessageType,
        payload: &'a [u8],
    ) -> Result<(ChannelRef<'a>, ChannelGrant<'a>), MuxError> {
        let cref = self.route_ref(msg_type, payload)?;
        let grant = ChannelGrant {
            mux: self,
            channel: cref.channel,
            released: false,
        };
        Ok((cref, grant))
    }

    /// Convenience owning API: consumes a `TlvRecord` and returns an owned `ChannelMessage`.
    ///
    /// # Errors
    ///
    /// Returns [`MuxError::UnknownMessageType`] when no channel mapping exists
    /// for the record message type, or [`MuxError::ChannelFull`] when the
    /// target channel is already at capacity.
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

    /// Inspect current occupancy for a channel (useful for tests and benches)
    #[must_use]
    pub fn occupancy(&self, channel: ChannelId) -> usize {
        self.counters[channel.idx()]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::tlv::{TlvDecoder, TlvEncoder};

    #[test]
    fn should_map_manifest_message_ids() {
        // Arrange
        let mapping = TypeMapping::new();

        // Act
        let c1 = mapping.get_channel(1);
        let c2 = mapping.get_channel(100);
        let c3 = mapping.get_channel(200);
        let c4 = mapping.get_channel(300);
        let c5 = mapping.get_channel(400);
        let c6 = mapping.get_channel(504);
        let c7 = mapping.get_channel(505);
        let c8 = mapping.get_channel(600);
        let c9 = mapping.get_channel(700);
        let c10 = mapping.get_channel(999);

        // Assert
        assert_eq!(c1, Some(ChannelId::Control));
        assert_eq!(c2, Some(ChannelId::Pub));
        assert_eq!(c3, Some(ChannelId::Sub));
        assert_eq!(c4, Some(ChannelId::Rpc));
        assert_eq!(c5, Some(ChannelId::Lease));
        assert_eq!(c6, Some(ChannelId::Pub)); // notice server response
        assert_eq!(c7, None); // reserved gap
        assert_eq!(c8, Some(ChannelId::Sub)); // stream
        assert_eq!(c9, Some(ChannelId::Internal)); // schedule
        assert_eq!(c10, None);
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
        mux2.release(cref.channel);

        // RAII grant path
        let mut mux3 = Mux::new(1);
        let (mt, slice, _) = decoder.decode_one_ref(&data).unwrap();
        {
            let (_cref, _grant) = mux3.route_grant(mt, slice).unwrap();
            // grant held; cannot call other mux methods while borrowed
        }
        // grant dropped, now routing should succeed
        assert!(mux3.route_ref(mt, slice).is_ok());

        // Explicit release
        let mut mux4 = Mux::new(1);
        let (mt, slice, _) = decoder.decode_one_ref(&data).unwrap();
        let (_cref, grant) = mux4.route_grant(mt, slice).unwrap();
        // Explicit release
        grant.release();
        assert!(mux4.route_ref(mt, slice).is_ok());
    }

    #[test]
    fn should_track_backpressure() {
        // Arrange
        let mut mux = Mux::new(1);
        let mut encoder = TlvEncoder::new();
        encoder.encode(MessageType::new(100), b"value");
        let data = encoder.finish();
        let decoder = TlvDecoder::new();
        let (record, _) = decoder.decode_one(&data).unwrap();

        // Act
        let _ = mux.route(record.clone()).unwrap();
        let res = mux.route(record);

        // Assert
        assert!(matches!(res, Err(MuxError::ChannelFull(ChannelId::Pub))));
    }
}
