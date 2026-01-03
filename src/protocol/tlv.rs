//! TLV (Type-Length-Value) codec for protocol messages
//!
//! TLV frames are decoded into typed records without routing decisions.

use bytes::{BufMut, Bytes, BytesMut};
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
        if self.is_single_byte() {
            1
        } else {
            3
        }
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

/// Zero-copy reference to a TLV record (borrows the input buffer)
#[derive(Debug, Clone, Copy)]
pub struct TlvRef<'a> {
    pub ty: MessageType,
    pub value: &'a [u8],
}

/// Zero-copy reference to a TLV record (borrows the input buffer)


impl TlvRecord {
    pub fn new(msg_type: MessageType, value: Bytes) -> Self {
        Self { msg_type, value }
    }

    /// Return the message type (copy, u16 wrapper)
    pub fn msg_type(&self) -> MessageType {
        self.msg_type
    }

    /// Borrow the value as a slice for quick inspection
    pub fn value(&self) -> &[u8] {
        &self.value
    }

    /// Convert to a zero-copy tuple (msg_type, &[u8]). This copies no data but borrows the inner bytes.
    pub fn as_ref(&self) -> (MessageType, &[u8]) {
        (self.msg_type, &self.value)
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
        Self {
            max_value_len: 256 * 1024 * 1024,
        }
    }

    pub fn with_max_len(max_value_len: u32) -> Self {
        Self { max_value_len }
    }

    /// Zero-copy decode: returns (MessageType, value_slice, consumed_bytes)
    /// No allocations, suitable for hot-path routing.
    #[inline]
    pub fn decode_one_ref<'a>(&self, input: &'a [u8]) -> Result<(MessageType, &'a [u8], usize), TlvError> {
        if input.is_empty() {
            return Err(TlvError::EmptyFrame);
        }

        let mut offset: usize = 0;

        // Parse type (single-byte or escape + two-byte BE u16)
        let msg_type = if input[offset] == MessageType::ESCAPE_MARKER {
            // need marker + 2 bytes
            if input.len() < offset + 3 {
                return Err(TlvError::IncompleteType);
            }
            let hi = input[offset + 1];
            let lo = input[offset + 2];
            let value = u16::from_be_bytes([hi, lo]);
            // Validate escape encoding: escaped value must be > MAX_SINGLE_BYTE
            if value <= MessageType::MAX_SINGLE_BYTE {
                return Err(TlvError::InvalidTypeEncoding);
            }
            offset += 3; // marker + 2 bytes
            MessageType(value)
        } else {
            let value = input[offset] as u16;
            offset += 1;
            MessageType(value)
        };

        // Read length (4 bytes BE)
        if input.len() < offset + 4 {
            return Err(TlvError::IncompleteLength);
        }
        let len = u32::from_be_bytes([
            input[offset],
            input[offset + 1],
            input[offset + 2],
            input[offset + 3],
        ]) as usize;
        offset += 4;

        if (len as u32) > self.max_value_len {
            return Err(TlvError::LengthTooLarge(len as u32));
        }

        if input.len() < offset + len {
            return Err(TlvError::IncompleteValue {
                needed: offset + len,
                available: input.len(),
            });
        }

        let value_slice = &input[offset..offset + len];
        offset += len;

        Ok((msg_type, value_slice, offset))
    }

    /// Owned decode convenience wrapper that copies value bytes into a `Bytes`.
    #[inline]
    pub fn decode_one(&self, input: &[u8]) -> Result<(TlvRecord, usize), TlvError> {
        let (msg_type, slice, consumed) = self.decode_one_ref(input)?;
        Ok((TlvRecord::new(msg_type, Bytes::copy_from_slice(slice)), consumed))
    }

    /// Decode all into a provided vector to reuse allocation. Returns number of records appended.
    pub fn decode_into(&self, input: &[u8], out: &mut Vec<TlvRecord>) -> Result<usize, TlvError> {
        let mut offset = 0usize;
        let mut count = 0usize;
        while offset < input.len() {
            let (msg_type, slice, consumed) = self.decode_one_ref(&input[offset..])?;
            out.push(TlvRecord::new(msg_type, Bytes::copy_from_slice(slice)));
            offset += consumed;
            count += 1;
        }
        Ok(count)
    }

    /// Collect zero-copy refs into user-provided vector. No allocations beyond the Vec buffer itself.
    pub fn decode_refs_into<'a>(&self, input: &'a [u8], out: &mut Vec<TlvRef<'a>>) -> Result<usize, TlvError> {
        let mut offset = 0usize;
        let mut count = 0usize;
        while offset < input.len() {
            let (msg_type, slice, consumed) = self.decode_one_ref(&input[offset..])?;
            out.push(TlvRef { ty: msg_type, value: slice });
            offset += consumed;
            count += 1;
        }
        Ok(count)
    }

    /// Decode all convenience API (allocating)
    pub fn decode_all(&self, input: &[u8]) -> Result<Vec<TlvRecord>, TlvError> {
        let mut vec = Vec::new();
        self.decode_into(input, &mut vec)?;
        Ok(vec)
    }

    /// Iterator over zero-copy decoded records. Yields `Ok((MessageType, &value))` or an `Err` on first failure.
    pub fn iter<'a>(&'a self, input: &'a [u8]) -> TlvDecoderIter<'a> {
        TlvDecoderIter { decoder: self, buf: input, offset: 0 }
    }
}

/// Streaming zero-copy iterator for TLV frames.
pub struct TlvDecoderIter<'a> {
    decoder: &'a TlvDecoder,
    buf: &'a [u8],
    offset: usize,
}

impl<'a> Iterator for TlvDecoderIter<'a> {
    type Item = Result<(MessageType, &'a [u8]), TlvError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset >= self.buf.len() {
            return None;
        }

        match self.decoder.decode_one_ref(&self.buf[self.offset..]) {
            Ok((t, slice, consumed)) => {
                self.offset += consumed;
                Some(Ok((t, slice)))
            }
            Err(e) => Some(Err(e)),
        }
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
        Self {
            buffer: BytesMut::with_capacity(512),
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buffer: BytesMut::with_capacity(capacity),
        }
    }

    /// Encode a record. Inline for hot-path performance.
    #[inline(always)]
    pub fn encode(&mut self, msg_type: MessageType, value: &[u8]) {
        // If msg_type is in single-byte range, write a single u8; otherwise write escape marker + BE u16
        if msg_type.is_single_byte() {
            self.buffer.put_u8(msg_type.0 as u8);
        } else {
            // Validate that encoded two-byte forms are actually > MAX_SINGLE_BYTE
            if msg_type.0 <= MessageType::MAX_SINGLE_BYTE {
                // This should never happen for well-formed MessageType; be defensive
                // For performance we don't return Result here; we simply encode with escape marker
            }
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

        // zero-copy variant
        let (mt, slice, cons) = decoder.decode_one_ref(&data).unwrap();
        assert_eq!(mt.as_u16(), 42);
        assert_eq!(slice, b"hello");
        assert_eq!(cons, consumed);
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

        // iterator zero-copy
        let mut it = decoder.iter(&data);
        let a = it.next().unwrap().unwrap();
        assert_eq!(a.0.as_u16(), 1);
        assert_eq!(a.1, b"first");
        let b = it.next().unwrap().unwrap();
        assert_eq!(b.0.as_u16(), 2);
        assert_eq!(b.1, b"second");
        assert!(it.next().is_none());
    }

    #[test]
    fn decode_into_and_decode_all_consistency() {
        let mut encoder = TlvEncoder::new();
        encoder.encode(MessageType::new(3), b"foo");
        encoder.encode(MessageType::new(4), b"bar");
        let data = encoder.finish();

        let decoder = TlvDecoder::new();
        let mut vec = Vec::new();
        let n = decoder.decode_into(&data, &mut vec).unwrap();
        assert_eq!(n, 2);
        assert_eq!(vec[0].msg_type().as_u16(), 3);
        assert_eq!(vec[1].msg_type().as_u16(), 4);

        let all = decoder.decode_all(&data).unwrap();
        assert_eq!(all[0].msg_type().as_u16(), 3);
        assert_eq!(all[1].msg_type().as_u16(), 4);
    }

    #[test]
    fn decode_refs_into_zero_copy() {
        let mut encoder = TlvEncoder::new();
        encoder.encode(MessageType::new(7), b"hello");
        encoder.encode(MessageType::new(8), b"world");
        let data = encoder.finish();

        let decoder = TlvDecoder::new();
        let mut out: Vec<TlvRef> = Vec::new();
        let n = decoder.decode_refs_into(&data, &mut out).unwrap();
        assert_eq!(n, 2);
        assert_eq!(out[0].ty.as_u16(), 7);
        assert_eq!(out[1].ty.as_u16(), 8);
        assert_eq!(out[0].value, b"hello");
        assert_eq!(out[1].value, b"world");
    }

    #[test]
    fn incomplete_value() {
        let mut encoder = TlvEncoder::new();
        encoder.encode(MessageType::new(1), b"hello");
        let mut data = encoder.finish().to_vec();
        data.truncate(7);

        let decoder = TlvDecoder::new();
        assert!(matches!(
            decoder.decode_one(&data),
            Err(TlvError::IncompleteValue { .. })
        ));
    }

    #[test]
    fn oversized_value() {
        let mut encoder = TlvEncoder::new();
        encoder.encode(MessageType::new(1), b"data");
        let mut data = encoder.finish().to_vec();
        data[1..5].copy_from_slice(&1_000_000_000u32.to_be_bytes());

        let decoder = TlvDecoder::with_max_len(1024);
        assert!(matches!(
            decoder.decode_one(&data),
            Err(TlvError::LengthTooLarge(_))
        ));
    }

    #[test]
    fn invalid_escape_encoding() {
        // Build an escaped type whose value is <= MAX_SINGLE_BYTE (invalid)
        let mut data = Vec::new();
        data.push(MessageType::ESCAPE_MARKER);
        // encode 42 as two-byte BE => invalid, should be rejected
        data.extend_from_slice(&42u16.to_be_bytes());
        // write length 0
        data.extend_from_slice(&(0u32.to_be_bytes()));

        let decoder = TlvDecoder::new();
        assert!(matches!(decoder.decode_one_ref(&data), Err(TlvError::InvalidTypeEncoding)));
        assert!(matches!(decoder.decode_one(&data), Err(TlvError::InvalidTypeEncoding)));
    }
}
