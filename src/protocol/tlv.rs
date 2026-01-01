//! TLV (Type-Length-Value) codec for protocol messages
//!
//! TLV frames are decoded into typed records without routing decisions.

use bytes::{Bytes, BytesMut, BufMut};
use std::fmt;

/// Message type identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MessageType(pub u16);

impl MessageType {
    pub const ESCAPE_MARKER: u8 = 0xFF;
    pub const MAX_SINGLE_BYTE: u16 = 0xFE;

    pub fn new(value: u16) -> Self {
        Self(value)
    }

    pub fn as_u16(&self) -> u16 {
        self.0
    }

    pub fn is_single_byte(&self) -> bool {
        self.0 <= Self::MAX_SINGLE_BYTE
    }

    pub fn encoded_type_len(&self) -> usize {
        if self.is_single_byte() { 1 } else { 3 }
    }

    pub fn encoded_size(&self, value_len: usize) -> usize {
        self.encoded_type_len() + 4 + value_len
    }
}

impl fmt::Display for MessageType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "msg_type({})", self.0)
    }
}

/// TLV record
#[derive(Debug, Clone)]
pub struct TlvRecord {
    pub msg_type: MessageType,
    pub value: Bytes,
}

impl TlvRecord {
    pub fn new(msg_type: MessageType, value: Bytes) -> Self {
        Self { msg_type, value }
    }

    pub fn msg_type(&self) -> MessageType {
        self.msg_type
    }

    pub fn value(&self) -> &[u8] {
        &self.value
    }
}

/// TLV decoding error
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TlvError {
    /// Missing type field
    IncompleteType,
    /// Missing length field
    IncompleteLength,
    /// Missing value bytes
    IncompleteValue { needed: usize, available: usize },
    /// Invalid escape encoding
    InvalidTypeEncoding,
    /// Value length too large
    LengthTooLarge(u32),
    /// Buffer empty
    EmptyFrame,
}

impl fmt::Display for TlvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IncompleteType => write!(f, "incomplete type field"),
            Self::IncompleteLength => write!(f, "incomplete length field"),
            Self::IncompleteValue { needed, available } => write!(
                f,
                "incomplete value: need {} bytes, have {}",
                needed, available
            ),
            Self::InvalidTypeEncoding => write!(f, "invalid type encoding"),
            Self::LengthTooLarge(len) => write!(f, "value length too large: {}", len),
            Self::EmptyFrame => write!(f, "empty frame"),
        }
    }
}

impl std::error::Error for TlvError {}

/// TLV decoder
#[derive(Clone)]
pub struct TlvDecoder {
    max_value_len: u32,
}

impl TlvDecoder {
    pub fn new() -> Self {
        Self { max_value_len: 256 * 1024 * 1024 }
    }

    pub fn with_max_len(max_value_len: u32) -> Self {
        Self { max_value_len }
    }

    pub fn decode_one(&self, input: &[u8]) -> Result<(TlvRecord, usize), TlvError> {
        if input.is_empty() {
            return Err(TlvError::EmptyFrame);
        }

        let mut offset = 0;
        let msg_type = if input[offset] == MessageType::ESCAPE_MARKER {
            if input.len() < offset + 3 {
                return Err(TlvError::IncompleteType);
            }
            let type_bytes = &input[offset + 1..offset + 3];
            let value = u16::from_be_bytes([type_bytes[0], type_bytes[1]]);
            offset += 3;
            MessageType(value)
        } else {
            let value = input[offset] as u16;
            offset += 1;
            MessageType(value)
        };

        if input.len() < offset + 4 {
            return Err(TlvError::IncompleteLength);
        }
        let len_bytes = &input[offset..offset + 4];
        let value_len = u32::from_be_bytes([len_bytes[0], len_bytes[1], len_bytes[2], len_bytes[3]]) as usize;
        offset += 4;

        if value_len as u32 > self.max_value_len {
            return Err(TlvError::LengthTooLarge(value_len as u32));
        }

        if input.len() < offset + value_len {
            return Err(TlvError::IncompleteValue {
                needed: offset + value_len,
                available: input.len(),
            });
        }

        let value = Bytes::copy_from_slice(&input[offset..offset + value_len]);
        offset += value_len;

        Ok((TlvRecord::new(msg_type, value), offset))
    }

    pub fn decode_all(&self, input: &[u8]) -> Result<Vec<TlvRecord>, TlvError> {
        let mut offset = 0;
        let mut records = Vec::new();

        while offset < input.len() {
            let (record, consumed) = self.decode_one(&input[offset..])?;
            records.push(record);
            offset += consumed;
        }

        Ok(records)
    }
}

impl Default for TlvDecoder {
    fn default() -> Self {
        Self::new()
    }
}

/// TLV encoder for testing/debug
pub struct TlvEncoder {
    buffer: BytesMut,
}

impl TlvEncoder {
    pub fn new() -> Self {
        Self { buffer: BytesMut::with_capacity(512) }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self { buffer: BytesMut::with_capacity(capacity) }
    }

    pub fn encode(&mut self, msg_type: MessageType, value: &[u8]) {
        if msg_type.is_single_byte() {
            self.buffer.put_u8(msg_type.0 as u8);
        } else {
            self.buffer.put_u8(MessageType::ESCAPE_MARKER);
            self.buffer.extend_from_slice(&msg_type.0.to_be_bytes());
        }

        let len_bytes = (value.len() as u32).to_be_bytes();
        self.buffer.extend_from_slice(&len_bytes);
        self.buffer.extend_from_slice(value);
    }

    pub fn finish(self) -> Bytes {
        self.buffer.freeze()
    }

    pub fn clear(&mut self) {
        self.buffer.clear();
    }
}

impl Default for TlvEncoder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_single_byte_type() {
        let mut encoder = TlvEncoder::new();
        encoder.encode(MessageType::new(42), b"hello");
        let data = encoder.finish();

        let decoder = TlvDecoder::new();
        let (record, consumed) = decoder.decode_one(&data).unwrap();

        assert_eq!(record.msg_type().as_u16(), 42);
        assert_eq!(record.value(), b"hello");
        assert_eq!(consumed, 1 + 4 + 5);
    }

    #[test]
    fn decode_multiple_records() {
        let mut encoder = TlvEncoder::new();
        encoder.encode(MessageType::new(1), b"first");
        encoder.encode(MessageType::new(2), b"second");
        let data = encoder.finish();

        let decoder = TlvDecoder::new();
        let records = decoder.decode_all(&data).unwrap();

        assert_eq!(records.len(), 2);
        assert_eq!(records[0].msg_type().as_u16(), 1);
        assert_eq!(records[1].msg_type().as_u16(), 2);
    }

    #[test]
    fn incomplete_value() {
        let mut encoder = TlvEncoder::new();
        encoder.encode(MessageType::new(1), b"hello");
        let mut data = encoder.finish().to_vec();
        data.truncate(7);

        let decoder = TlvDecoder::new();
        assert!(matches!(decoder.decode_one(&data), Err(TlvError::IncompleteValue { .. })));
    }

    #[test]
    fn oversized_value() {
        let mut encoder = TlvEncoder::new();
        encoder.encode(MessageType::new(1), b"data");
        let mut data = encoder.finish().to_vec();
        data[1..5].copy_from_slice(&1_000_000_000u32.to_be_bytes());

        let decoder = TlvDecoder::with_max_len(1024);
        assert!(matches!(decoder.decode_one(&data), Err(TlvError::LengthTooLarge(_))));
    }
}
