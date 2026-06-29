//! Sequential payload encoding for domain message bodies.
//!
//! This is **not** TLV (tag-length-value). It encodes/decodes a fixed order of
//! typed fields (scalars, length-prefixed strings/bytes, optionals). Used by
//! domain codecs (RPC, lease, schedule, notice, stream, etc.) to serialize
//! the body of messages. For the wire-level frame format (real TLV), see [`crate::protocol::tlv`].

use bytes::BufMut;
use std::ops::Range;

/// Encoder for sequential typed fields in message payloads.
pub struct PayloadEncoder {
    buf: Vec<u8>,
}

impl Default for PayloadEncoder {
    fn default() -> Self {
        Self::new()
    }
}

impl PayloadEncoder {
    /// Create a new encoder
    #[must_use]
    pub fn new() -> Self {
        Self { buf: Vec::new() }
    }

    /// Create encoder with pre-allocated capacity (reduces reallocations when reusing)
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buf: Vec::with_capacity(capacity),
        }
    }

    /// Clear the buffer for reuse. Use after `finish()` or when discarding content.
    /// Call sites can hold one encoder and call clear() then encode again to avoid allocating a new encoder.
    pub fn clear(&mut self) {
        self.buf.clear();
    }

    /// Return the current buffer contents and reset the encoder for reuse (no copy).
    /// Caller gets ownership of the Vec; encoder is left with an empty buffer.
    pub fn finish(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.buf)
    }

    /// Encode a u8 scalar
    pub fn put_u8(&mut self, val: u8) {
        self.buf.put_u8(val);
    }

    /// Encode a u16 scalar
    pub fn put_u16(&mut self, val: u16) {
        self.buf.put_u16(val);
    }

    /// Encode a u32 scalar
    pub fn put_u32(&mut self, val: u32) {
        self.buf.put_u32(val);
    }

    /// Encode a u64 scalar
    pub fn put_u64(&mut self, val: u64) {
        self.buf.put_u64(val);
    }

    /// Encode a string with length prefix
    pub fn put_string(&mut self, val: &str) {
        self.buf.put_u32(val.len() as u32);
        self.buf.put_slice(val.as_bytes());
    }

    /// Encode bytes with length prefix
    pub fn put_bytes(&mut self, val: &[u8]) {
        self.buf.put_u32(val.len() as u32);
        self.buf.put_slice(val);
    }

    /// Encode an optional value (1-byte flag, then value if present)
    pub fn put_optional_u64(&mut self, val: Option<u64>) {
        match val {
            Some(v) => {
                self.buf.put_u8(1);
                self.buf.put_u64(v);
            }
            None => {
                self.buf.put_u8(0);
            }
        }
    }

    /// Encode an optional string
    pub fn put_optional_string(&mut self, val: Option<&str>) {
        match val {
            Some(s) => {
                self.buf.put_u8(1);
                self.buf.put_u32(s.len() as u32);
                self.buf.put_slice(s.as_bytes());
            }
            None => {
                self.buf.put_u8(0);
            }
        }
    }
}

/// Decoder for sequential typed fields with bounds checking.
pub struct PayloadDecoder<'a> {
    payload: &'a [u8],
    offset: usize,
}

impl<'a> PayloadDecoder<'a> {
    /// Create a new decoder
    #[must_use]
    pub fn new(payload: &'a [u8]) -> Self {
        Self { payload, offset: 0 }
    }

    /// Get remaining bytes
    #[must_use]
    pub fn remaining(&self) -> usize {
        self.payload.len().saturating_sub(self.offset)
    }

    /// Get the current decoder offset.
    #[must_use]
    pub fn offset(&self) -> usize {
        self.offset
    }

    /// Peek the next u8 without advancing.
    pub fn peek_u8(&self) -> Result<u8, String> {
        if self.offset + 1 > self.payload.len() {
            return Err("Incomplete u8".to_string());
        }
        Ok(self.payload[self.offset])
    }

    /// Decode a u8 scalar
    pub fn get_u8(&mut self) -> Result<u8, String> {
        if self.offset + 1 > self.payload.len() {
            return Err("Incomplete u8".to_string());
        }
        let val = self.payload[self.offset];
        self.offset += 1;
        Ok(val)
    }

    /// Decode a u16 scalar
    pub fn get_u16(&mut self) -> Result<u16, String> {
        if self.offset + 2 > self.payload.len() {
            return Err("Incomplete u16".to_string());
        }
        let val = u16::from_be_bytes([self.payload[self.offset], self.payload[self.offset + 1]]);
        self.offset += 2;
        Ok(val)
    }

    /// Decode a u32 scalar
    pub fn get_u32(&mut self) -> Result<u32, String> {
        if self.offset + 4 > self.payload.len() {
            return Err("Incomplete u32".to_string());
        }
        let val = u32::from_be_bytes([
            self.payload[self.offset],
            self.payload[self.offset + 1],
            self.payload[self.offset + 2],
            self.payload[self.offset + 3],
        ]);
        self.offset += 4;
        Ok(val)
    }

    /// Decode a u64 scalar
    pub fn get_u64(&mut self) -> Result<u64, String> {
        if self.offset + 8 > self.payload.len() {
            return Err("Incomplete u64".to_string());
        }
        let val = u64::from_be_bytes([
            self.payload[self.offset],
            self.payload[self.offset + 1],
            self.payload[self.offset + 2],
            self.payload[self.offset + 3],
            self.payload[self.offset + 4],
            self.payload[self.offset + 5],
            self.payload[self.offset + 6],
            self.payload[self.offset + 7],
        ]);
        self.offset += 8;
        Ok(val)
    }

    /// Decode a string with length prefix (borrowed; no allocation).
    /// Use when the caller can use `&str` or will do a single `.to_string()` for owned.
    pub fn get_string_ref(&mut self) -> Result<&'a str, String> {
        let len = self.get_u32()? as usize;
        if self.offset + len > self.payload.len() {
            return Err("Incomplete string data".to_string());
        }
        let slice = &self.payload[self.offset..self.offset + len];
        let s = std::str::from_utf8(slice).map_err(|_| "Invalid UTF-8 in string".to_string())?;
        self.offset += len;
        Ok(s)
    }

    /// Decode a string with length prefix (owned; one allocation).
    pub fn get_string(&mut self) -> Result<String, String> {
        self.get_string_ref().map(|s| s.to_string())
    }

    /// Decode bytes with length prefix
    pub fn get_bytes(&mut self) -> Result<bytes::Bytes, String> {
        let len = self.get_u32()? as usize;
        if self.offset + len > self.payload.len() {
            return Err("Incomplete bytes data".to_string());
        }
        let data = bytes::Bytes::copy_from_slice(&self.payload[self.offset..self.offset + len]);
        self.offset += len;
        Ok(data)
    }

    /// Decode a byte range with length prefix without allocating.
    ///
    /// Returns the range within the original payload slice that contains the bytes.
    pub fn get_bytes_range(&mut self) -> Result<Range<usize>, String> {
        let len = self.get_u32()? as usize;
        if self.offset + len > self.payload.len() {
            return Err("Incomplete bytes data".to_string());
        }

        let start = self.offset;
        self.offset += len;
        Ok(start..self.offset)
    }

    /// Skip bytes with length prefix without allocating.
    pub fn skip_bytes(&mut self) -> Result<(), String> {
        let len = self.get_u32()? as usize;
        if self.offset + len > self.payload.len() {
            return Err("Incomplete bytes data".to_string());
        }
        self.offset += len;
        Ok(())
    }

    /// Decode an optional u64
    pub fn get_optional_u64(&mut self) -> Result<Option<u64>, String> {
        let flag = self.get_u8()?;
        if flag == 1 {
            self.get_u64().map(Some)
        } else {
            Ok(None)
        }
    }

    /// Decode an optional string
    pub fn get_optional_string(&mut self) -> Result<Option<String>, String> {
        let flag = self.get_u8()?;
        if flag == 1 {
            self.get_string().map(Some)
        } else {
            Ok(None)
        }
    }

    /// Decode optional bytes
    pub fn get_optional_bytes(&mut self) -> Result<Option<bytes::Bytes>, String> {
        let flag = self.get_u8()?;
        if flag == 1 {
            self.get_bytes().map(Some)
        } else {
            Ok(None)
        }
    }

    /// Skip optional bytes without allocating.
    pub fn skip_optional_bytes(&mut self) -> Result<(), String> {
        let flag = self.get_u8()?;
        if flag == 1 {
            self.skip_bytes()
        } else {
            Ok(())
        }
    }

    /// Check if we've consumed all input
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.offset == self.payload.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn should_roundtrip_scalars() {
        // Arrange
        let mut enc = PayloadEncoder::new();
        enc.put_u8(42);
        enc.put_u16(1000);
        enc.put_u32(100_000);
        enc.put_u64(9_999_999_999);

        // Act
        let buf = enc.finish();
        let mut dec = PayloadDecoder::new(&buf);

        // Assert
        assert_eq!(dec.get_u8().unwrap(), 42);
        assert_eq!(dec.get_u16().unwrap(), 1000);
        assert_eq!(dec.get_u32().unwrap(), 100_000);
        assert_eq!(dec.get_u64().unwrap(), 9_999_999_999);
        assert!(dec.is_complete());
    }

    #[test]
    fn should_roundtrip_strings() {
        // Arrange
        let mut enc = PayloadEncoder::new();
        enc.put_string("hello");
        enc.put_string("world");

        // Act
        let buf = enc.finish();
        let mut dec = PayloadDecoder::new(&buf);

        // Assert
        assert_eq!(dec.get_string().unwrap(), "hello");
        assert_eq!(dec.get_string().unwrap(), "world");
        assert!(dec.is_complete());
    }

    #[test]
    fn should_roundtrip_bytes() {
        // Arrange
        let mut enc = PayloadEncoder::new();
        enc.put_bytes(b"test data");

        // Act
        let buf = enc.finish();
        let mut dec = PayloadDecoder::new(&buf);

        // Assert
        assert_eq!(dec.get_bytes().unwrap(), Bytes::from("test data"));
        assert!(dec.is_complete());
    }

    #[test]
    fn should_return_byte_range_without_allocating() {
        // Arrange
        let mut enc = PayloadEncoder::new();
        enc.put_bytes(b"test data");

        // Act
        let buf = enc.finish();
        let mut dec = PayloadDecoder::new(&buf);
        let range = dec.get_bytes_range().unwrap();

        // Assert
        assert_eq!(&buf[range], b"test data");
        assert!(dec.is_complete());
    }

    #[test]
    fn should_roundtrip_optional() {
        // Arrange
        let mut enc = PayloadEncoder::new();
        enc.put_optional_u64(Some(42));
        enc.put_optional_u64(None);
        enc.put_optional_string(Some("hello"));
        enc.put_optional_string(None);

        // Act
        let buf = enc.finish();
        let mut dec = PayloadDecoder::new(&buf);

        // Assert
        assert_eq!(dec.get_optional_u64().unwrap(), Some(42));
        assert_eq!(dec.get_optional_u64().unwrap(), None);
        assert_eq!(
            dec.get_optional_string().unwrap(),
            Some("hello".to_string())
        );
        assert_eq!(dec.get_optional_string().unwrap(), None);
        assert!(dec.is_complete());
    }

    #[test]
    fn should_error_on_incomplete_data() {
        // Arrange
        let buf = vec![1, 2]; // incomplete u32

        // Act
        let mut dec = PayloadDecoder::new(&buf);

        // Assert
        assert!(dec.get_u32().is_err());
    }

    #[test]
    fn should_validate_utf8() {
        // Arrange
        let mut enc = PayloadEncoder::new();
        enc.put_u32(4); // length
        let mut buf = enc.finish();
        buf.extend_from_slice(&[255, 255, 255, 255]); // invalid UTF-8

        // Act
        let mut dec = PayloadDecoder::new(&buf);

        // Assert
        assert!(dec.get_string().is_err());
    }
}
