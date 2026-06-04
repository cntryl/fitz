//! Frame-level TLV (Type-Length-Value) codec for the wire protocol.
//!
//! Encodes/decodes multiplexed protocol frames: each record has a message type (u16),
//! 2-byte length, and value bytes. Used for CONNECT, RPC, notice, stream, etc. on the wire.
//! For encoding domain message *payloads* (sequential typed fields), see [`crate::protocol::payload_codec`].

use bytes::{BufMut, Bytes, BytesMut};
use std::fmt;

/// Message type identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MessageType(pub u16);

impl MessageType {
    pub const ESCAPE_MARKER: u8 = 0xFF;
    pub const MAX_SINGLE_BYTE: u16 = 0xFE;

    pub const CONNECT: MessageType = MessageType(1); // control connect message

    #[inline]
    pub fn new(value: u16) -> Self {
        Self(value)
    }

    #[inline]
    pub fn as_u16(&self) -> u16 {
        self.0
    }

    #[inline]
    pub fn is_single_byte(&self) -> bool {
        self.0 <= Self::MAX_SINGLE_BYTE
    }

    #[inline]
    pub fn encoded_type_len(&self) -> usize {
        if self.is_single_byte() {
            1
        } else {
            3
        }
    }

    #[inline]
    pub fn encoded_size(&self, value_len: usize) -> usize {
        self.encoded_type_len() + 2 + value_len
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
    /// Duplicate tag within a single frame
    DuplicateTag(u16),
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
            Self::DuplicateTag(tag) => write!(f, "duplicate tag in frame: {}", tag),
            Self::EmptyFrame => write!(f, "empty frame"),
        }
    }
}

impl std::error::Error for TlvError {}

/// O(1) duplicate-tag tracker for u16 message tags without heap allocation.
struct SeenTags {
    bits: [u64; 1024],
}

impl SeenTags {
    #[inline]
    fn new() -> Self {
        Self { bits: [0; 1024] }
    }

    /// Returns true if this is the first time `tag` is seen.
    #[inline]
    fn insert(&mut self, tag: u16) -> bool {
        let idx = (tag >> 6) as usize;
        let mask = 1u64 << (tag & 63);
        let slot = &mut self.bits[idx];
        let is_new = (*slot & mask) == 0;
        if is_new {
            *slot |= mask;
        }
        is_new
    }
}

/// TLV decoder
#[derive(Clone)]
pub struct TlvDecoder {
    max_value_len: u32,
}

impl TlvDecoder {
    pub fn new() -> Self {
        Self {
            // Default to u16 maximum to match TLV value size invariant (<= 65535 bytes)
            max_value_len: u16::MAX as u32,
        }
    }

    pub fn with_max_len(max_value_len: u32) -> Self {
        Self { max_value_len }
    }

    /// Zero-copy decode: returns (MessageType, value_slice, consumed_bytes)
    /// No allocations, suitable for hot-path routing.
    #[inline]
    pub fn decode_one_ref<'a>(
        &self,
        input: &'a [u8],
    ) -> Result<(MessageType, &'a [u8], usize), TlvError> {
        let input_len = input.len();
        if input_len == 0 {
            return Err(TlvError::EmptyFrame);
        }

        let mut offset: usize = 0;

        // Parse type (single-byte or escape + two-byte BE u16)
        let msg_type = if input[offset] == MessageType::ESCAPE_MARKER {
            // need marker + 2 bytes = 3 total for type
            if input_len < 3 {
                return Err(TlvError::IncompleteType);
            }
            let hi = input[offset + 1];
            let lo = input[offset + 2];
            let value = u16::from_be_bytes([hi, lo]);
            // Escaped values must be outside the single-byte range.
            if value <= MessageType::MAX_SINGLE_BYTE {
                return Err(TlvError::InvalidTypeEncoding);
            }
            offset += 3;
            MessageType(value)
        } else {
            let value = input[offset] as u16;
            offset += 1;
            MessageType(value)
        };

        // Read length (2 bytes BE) - need at least offset + 2 bytes.
        if input_len < offset + 2 {
            return Err(TlvError::IncompleteLength);
        }
        let value_len = u16::from_be_bytes([input[offset], input[offset + 1]]) as usize;

        if (value_len as u32) > self.max_value_len {
            return Err(TlvError::LengthTooLarge(value_len as u32));
        }

        offset += 2;
        let end_offset = offset + value_len;
        // Preserve the public contract: report absolute bytes needed/available.
        if input_len < end_offset {
            return Err(TlvError::IncompleteValue {
                needed: end_offset,
                available: input_len,
            });
        }

        let value_slice = &input[offset..end_offset];

        Ok((msg_type, value_slice, end_offset))
    }

    /// Owned decode convenience wrapper that copies value bytes into a `Bytes`.
    #[inline]
    pub fn decode_one(&self, input: &[u8]) -> Result<(TlvRecord, usize), TlvError> {
        let (msg_type, slice, consumed) = self.decode_one_ref(input)?;
        Ok((
            TlvRecord::new(msg_type, Bytes::copy_from_slice(slice)),
            consumed,
        ))
    }

    /// Decode all into a provided vector to reuse allocation. Returns number of records appended.
    pub fn decode_into(&self, input: &[u8], out: &mut Vec<TlvRecord>) -> Result<usize, TlvError> {
        let mut offset = 0usize;
        let mut count = 0usize;
        let mut seen = SeenTags::new();
        out.reserve(input.len() / 3);
        while offset < input.len() {
            let (msg_type, slice, consumed) = self.decode_one_ref(&input[offset..])?;
            if !seen.insert(msg_type.as_u16()) {
                return Err(TlvError::DuplicateTag(msg_type.as_u16()));
            }
            out.push(TlvRecord::new(msg_type, Bytes::copy_from_slice(slice)));
            offset += consumed;
            count += 1;
        }
        Ok(count)
    }

    /// Collect zero-copy refs into user-provided vector. No allocations beyond the Vec buffer itself.
    pub fn decode_refs_into<'a>(
        &self,
        input: &'a [u8],
        out: &mut Vec<TlvRef<'a>>,
    ) -> Result<usize, TlvError> {
        let mut offset = 0usize;
        let mut count = 0usize;
        let mut seen = SeenTags::new();
        out.reserve(input.len() / 3);
        while offset < input.len() {
            let (msg_type, slice, consumed) = self.decode_one_ref(&input[offset..])?;
            if !seen.insert(msg_type.as_u16()) {
                return Err(TlvError::DuplicateTag(msg_type.as_u16()));
            }
            out.push(TlvRef {
                ty: msg_type,
                value: slice,
            });
            offset += consumed;
            count += 1;
        }
        Ok(count)
    }

    /// Decode all convenience API (allocating)
    pub fn decode_all(&self, input: &[u8]) -> Result<Vec<TlvRecord>, TlvError> {
        let mut vec = Vec::with_capacity(input.len() / 3);
        self.decode_into(input, &mut vec)?;
        Ok(vec)
    }

    /// Iterator over zero-copy decoded records. Yields `Ok((MessageType, &value))` or an `Err` on first failure.
    pub fn iter<'a>(&'a self, input: &'a [u8]) -> TlvDecoderIter<'a> {
        TlvDecoderIter {
            decoder: self,
            buf: input,
            offset: 0,
            seen: SeenTags::new(),
        }
    }
}

/// Streaming zero-copy iterator for TLV frames.
pub struct TlvDecoderIter<'a> {
    decoder: &'a TlvDecoder,
    buf: &'a [u8],
    offset: usize,
    seen: SeenTags,
}

impl<'a> Iterator for TlvDecoderIter<'a> {
    type Item = Result<(MessageType, &'a [u8]), TlvError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset >= self.buf.len() {
            return None;
        }

        match self.decoder.decode_one_ref(&self.buf[self.offset..]) {
            Ok((t, slice, consumed)) => {
                // Duplicate tag check for iterator mode
                if !self.seen.insert(t.as_u16()) {
                    return Some(Err(TlvError::DuplicateTag(t.as_u16())));
                }
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
    /// Create a new encoder with a default buffer capacity.
    ///
    /// PERF: In hot paths, prefer reusing a single encoder via `clear()`
    /// instead of constructing a new encoder for every frame.
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
    #[inline]
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

        let len = value.len();
        // enforce u16 limit at encode time for safety
        if len > u16::MAX as usize {
            // For encoder (tests/debug), panic; production callers should chunk before encoding
            panic!("TLV value too large: {} bytes (max {})", len, u16::MAX);
        }
        let len_bytes = (len as u16).to_be_bytes();
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
    fn should_decode_single_byte_type() {
        // Arrange
        let mut encoder = TlvEncoder::new();
        encoder.encode(MessageType::new(42), b"hello");
        let data = encoder.finish();

        // Act
        let decoder = TlvDecoder::new();
        let (record, consumed) = decoder.decode_one(&data).unwrap();

        // Assert
        assert_eq!(record.msg_type().as_u16(), 42);
        assert_eq!(record.value(), b"hello");
        assert_eq!(consumed, 1 + 2 + 5);

        // zero-copy variant
        let (mt, slice, cons) = decoder.decode_one_ref(&data).unwrap();
        assert_eq!(mt.as_u16(), 42);
        assert_eq!(slice, b"hello");
        assert_eq!(cons, consumed);
    }

    #[test]
    fn should_decode_multiple_records() {
        // Arrange
        let mut encoder = TlvEncoder::new();
        encoder.encode(MessageType::new(1), b"first");
        encoder.encode(MessageType::new(2), b"second");
        let data = encoder.finish();

        // Act
        let decoder = TlvDecoder::new();
        let records = decoder.decode_all(&data).unwrap();

        // Assert
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
    fn should_reject_duplicate_tags() {
        // Arrange
        let mut encoder = TlvEncoder::new();
        encoder.encode(MessageType::new(1), b"a");
        encoder.encode(MessageType::new(1), b"b");
        let data = encoder.finish();

        // Act
        let decoder = TlvDecoder::new();
        let res = decoder.decode_all(&data);
        println!("duplicate test res = {:?}", res);

        // Assert
        assert!(matches!(res, Err(TlvError::DuplicateTag(1))));
    }

    #[test]
    fn should_decode_into() {
        // Arrange
        let mut encoder = TlvEncoder::new();
        encoder.encode(MessageType::new(3), b"foo");
        encoder.encode(MessageType::new(4), b"bar");
        let data = encoder.finish();

        // Act
        let decoder = TlvDecoder::new();
        let mut vec = Vec::new();
        let n = decoder.decode_into(&data, &mut vec).unwrap();

        // Assert
        assert_eq!(n, 2);
        assert_eq!(vec[0].msg_type().as_u16(), 3);
        assert_eq!(vec[1].msg_type().as_u16(), 4);
    }

    #[test]
    fn should_decode_all_consistently() {
        // Arrange
        let mut encoder = TlvEncoder::new();
        encoder.encode(MessageType::new(3), b"foo");
        encoder.encode(MessageType::new(4), b"bar");
        let data = encoder.finish();

        // Act
        let decoder = TlvDecoder::new();
        let all = decoder.decode_all(&data).unwrap();

        // Assert
        assert_eq!(all[0].msg_type().as_u16(), 3);
        assert_eq!(all[1].msg_type().as_u16(), 4);
    }

    #[test]
    fn should_decode_refs_into_zero_copy() {
        // Arrange
        let mut encoder = TlvEncoder::new();
        encoder.encode(MessageType::new(7), b"hello");
        encoder.encode(MessageType::new(8), b"world");
        let data = encoder.finish();

        // Act
        let decoder = TlvDecoder::new();
        let mut out: Vec<TlvRef> = Vec::new();
        let n = decoder.decode_refs_into(&data, &mut out).unwrap();

        // Assert
        assert_eq!(n, 2);
        assert_eq!(out[0].ty.as_u16(), 7);
        assert_eq!(out[1].ty.as_u16(), 8);
        assert_eq!(out[0].value, b"hello");
        assert_eq!(out[1].value, b"world");
    }

    #[test]
    fn should_fail_on_incomplete_value() {
        // Arrange
        let mut encoder = TlvEncoder::new();
        encoder.encode(MessageType::new(1), b"hello");
        let mut data = encoder.finish().to_vec();
        data.truncate(7);

        // Act
        let decoder = TlvDecoder::new();
        let res = decoder.decode_one(&data);

        // Assert
        assert!(matches!(res, Err(TlvError::IncompleteValue { .. })));
    }

    #[test]
    fn should_fail_on_oversized_value() {
        // Arrange
        let mut encoder = TlvEncoder::new();
        encoder.encode(MessageType::new(1), b"data");
        let mut data = encoder.finish().to_vec();
        // set length to 2000 (u16) which is > decoder max 1024
        data[1..3].copy_from_slice(&2000u16.to_be_bytes());

        // Act
        let decoder = TlvDecoder::with_max_len(1024);
        let res = decoder.decode_one(&data);

        // Assert
        assert!(matches!(res, Err(TlvError::LengthTooLarge(_))));
    }

    #[test]
    fn should_reject_invalid_escape_encoding() {
        // Arrange: Build an escaped type whose value is <= MAX_SINGLE_BYTE (invalid)
        let mut data = Vec::new();
        data.push(MessageType::ESCAPE_MARKER);
        // encode 42 as two-byte BE => invalid, should be rejected
        data.extend_from_slice(&42u16.to_be_bytes());
        // write length 0
        data.extend_from_slice(&(0u16.to_be_bytes()));

        // Act
        let decoder = TlvDecoder::new();
        let res_ref = decoder.decode_one_ref(&data);
        let res = decoder.decode_one(&data);

        // Assert
        assert!(matches!(res_ref, Err(TlvError::InvalidTypeEncoding)));
        assert!(matches!(res, Err(TlvError::InvalidTypeEncoding)));
    }

    #[test]
    fn should_decode_zero_length_value() {
        // Arrange: type=1, length=0, no value bytes
        let data = vec![0x01, 0x00, 0x00];

        // Act
        let decoder = TlvDecoder::new();
        let res = decoder.decode_one_ref(&data);

        // Assert
        let (msg_type, value, consumed) = res.expect("should decode zero-length value");
        assert_eq!(msg_type.as_u16(), 1);
        assert!(value.is_empty());
        assert_eq!(consumed, 3);
    }

    #[test]
    fn should_decode_zero_length_at_end_of_input() {
        // Arrange: multiple records, last one has zero length
        let mut encoder = TlvEncoder::new();
        encoder.encode(MessageType::new(1), b"hello");
        encoder.encode(MessageType::new(2), b"");
        let data = encoder.finish().to_vec();

        // Act
        let decoder = TlvDecoder::new();
        let (msg_type1, val1, consumed1) = decoder.decode_one_ref(&data).expect("first record");
        let remainder = &data[consumed1..];
        let (msg_type2, val2, consumed2) =
            decoder.decode_one_ref(remainder).expect("second record");

        // Assert
        assert_eq!(msg_type1.as_u16(), 1);
        assert_eq!(val1, b"hello");
        assert_eq!(msg_type2.as_u16(), 2);
        assert!(val2.is_empty());
        assert_eq!(consumed2, 3); // type(1) + len(2) + zero value(0)
    }

    #[test]
    fn should_report_correct_bytes_needed_on_truncated_value() {
        // Arrange: type=1, length=5, but only 3 bytes provided
        let data = vec![0x01, 0x00, 0x05, 0xAA, 0xBB, 0xCC]; // only 3 of 5 value bytes

        // Act
        let decoder = TlvDecoder::new();
        let res = decoder.decode_one_ref(&data);

        // Assert
        assert!(matches!(
            res,
            Err(TlvError::IncompleteValue {
                needed: 8,
                available: 6
            })
        ));
    }
}
