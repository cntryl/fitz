//! Shared TLV encoding/decoding utilities
//!
//! Common helpers used across all domain codecs.

use bytes::BufMut;

/// Helper for encoding TLV (Tag-Length-Value) format
pub struct TlvEncoder {
    buf: Vec<u8>,
}

impl Default for TlvEncoder {
    fn default() -> Self {
        Self::new()
    }
}

impl TlvEncoder {
    /// Create a new encoder
    pub fn new() -> Self {
        Self { buf: Vec::new() }
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

    /// Finish encoding and return bytes
    pub fn finish(self) -> Vec<u8> {
        self.buf
    }
}

/// Helper for decoding TLV format with bounds checking
pub struct TlvDecoder<'a> {
    payload: &'a [u8],
    offset: usize,
}

impl<'a> TlvDecoder<'a> {
    /// Create a new decoder
    pub fn new(payload: &'a [u8]) -> Self {
        Self { payload, offset: 0 }
    }

    /// Get remaining bytes
    pub fn remaining(&self) -> usize {
        self.payload.len().saturating_sub(self.offset)
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

    /// Decode a string with length prefix
    pub fn get_string(&mut self) -> Result<String, String> {
        let len = self.get_u32()? as usize;
        if self.offset + len > self.payload.len() {
            return Err("Incomplete string data".to_string());
        }
        let s = String::from_utf8(self.payload[self.offset..self.offset + len].to_vec())
            .map_err(|_| "Invalid UTF-8 in string".to_string())?;
        self.offset += len;
        Ok(s)
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

    /// Check if we've consumed all input
    pub fn is_complete(&self) -> bool {
        self.offset == self.payload.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn should_encode_and_decode_scalars() {
        let mut enc = TlvEncoder::new();
        enc.put_u8(42);
        enc.put_u16(1000);
        enc.put_u32(100000);
        enc.put_u64(9999999999);

        let buf = enc.finish();
        let mut dec = TlvDecoder::new(&buf);

        assert_eq!(dec.get_u8().unwrap(), 42);
        assert_eq!(dec.get_u16().unwrap(), 1000);
        assert_eq!(dec.get_u32().unwrap(), 100000);
        assert_eq!(dec.get_u64().unwrap(), 9999999999);
        assert!(dec.is_complete());
    }

    #[test]
    fn should_encode_and_decode_strings() {
        let mut enc = TlvEncoder::new();
        enc.put_string("hello");
        enc.put_string("world");

        let buf = enc.finish();
        let mut dec = TlvDecoder::new(&buf);

        assert_eq!(dec.get_string().unwrap(), "hello");
        assert_eq!(dec.get_string().unwrap(), "world");
        assert!(dec.is_complete());
    }

    #[test]
    fn should_encode_and_decode_bytes() {
        let mut enc = TlvEncoder::new();
        enc.put_bytes(b"test data");

        let buf = enc.finish();
        let mut dec = TlvDecoder::new(&buf);

        assert_eq!(dec.get_bytes().unwrap(), Bytes::from("test data"));
        assert!(dec.is_complete());
    }

    #[test]
    fn should_encode_and_decode_optional() {
        let mut enc = TlvEncoder::new();
        enc.put_optional_u64(Some(42));
        enc.put_optional_u64(None);
        enc.put_optional_string(Some("hello"));
        enc.put_optional_string(None);

        let buf = enc.finish();
        let mut dec = TlvDecoder::new(&buf);

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
        let buf = vec![1, 2]; // incomplete u32
        let mut dec = TlvDecoder::new(&buf);

        assert!(dec.get_u32().is_err());
    }

    #[test]
    fn should_validate_utf8() {
        let mut enc = TlvEncoder::new();
        enc.put_u32(4); // length
        let mut buf = enc.finish();
        buf.extend_from_slice(&[255, 255, 255, 255]); // invalid UTF-8

        let mut dec = TlvDecoder::new(&buf);
        assert!(dec.get_string().is_err());
    }
}
