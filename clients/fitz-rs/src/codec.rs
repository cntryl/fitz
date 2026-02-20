//! Payload codec: sequential typed fields for domain message bodies.
//!
//! Matches the server's payload_codec format (not TLV; fixed-order typed fields).
//! Frame-level encode/decode is in `encode_message_frame` / `decode_message_frame`.

use crate::error::{FitzError, Result};

/// Encoder for message payloads (sequential typed fields).
pub struct PayloadEncoder {
    buf: Vec<u8>,
}

impl PayloadEncoder {
    pub fn new() -> Self {
        Self { buf: Vec::new() }
    }

    pub fn put_u8(&mut self, value: u8) -> &mut Self {
        self.buf.push(value);
        self
    }

    pub fn put_u16(&mut self, value: u16) -> &mut Self {
        self.buf.extend_from_slice(&value.to_be_bytes());
        self
    }

    pub fn put_u32(&mut self, value: u32) -> &mut Self {
        self.buf.extend_from_slice(&value.to_be_bytes());
        self
    }

    pub fn put_u64(&mut self, value: u64) -> &mut Self {
        self.buf.extend_from_slice(&value.to_be_bytes());
        self
    }

    pub fn put_bytes(&mut self, bytes: &[u8]) -> &mut Self {
        self.buf.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
        self.buf.extend_from_slice(bytes);
        self
    }

    pub fn put_string(&mut self, s: &str) -> &mut Self {
        let bytes = s.as_bytes();
        self.buf.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
        self.buf.extend_from_slice(bytes);
        self
    }

    pub fn finish(self) -> Vec<u8> {
        self.buf
    }
}

impl Default for PayloadEncoder {
    fn default() -> Self {
        Self::new()
    }
}

/// Decoder for message payloads (sequential typed fields).
pub struct PayloadDecoder<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> PayloadDecoder<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn check_len(&self, n: usize) -> Result<()> {
        if self.pos + n > self.buf.len() {
            return Err(FitzError::Codec("Not enough bytes".to_string()));
        }
        Ok(())
    }

    pub fn get_u8(&mut self) -> Result<u8> {
        self.check_len(1)?;
        let value = self.buf[self.pos];
        self.pos += 1;
        Ok(value)
    }

    pub fn get_u16(&mut self) -> Result<u16> {
        self.check_len(2)?;
        let bytes = &self.buf[self.pos..self.pos + 2];
        self.pos += 2;
        Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
    }

    pub fn get_u32(&mut self) -> Result<u32> {
        self.check_len(4)?;
        let bytes = &self.buf[self.pos..self.pos + 4];
        self.pos += 4;
        Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    pub fn get_u64(&mut self) -> Result<u64> {
        self.check_len(8)?;
        let bytes = &self.buf[self.pos..self.pos + 8];
        self.pos += 8;
        Ok(u64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    pub fn get_bytes(&mut self) -> Result<Vec<u8>> {
        let len = self.get_u32()? as usize;
        self.check_len(len)?;
        let bytes = self.buf[self.pos..self.pos + len].to_vec();
        self.pos += len;
        Ok(bytes)
    }

    pub fn get_string(&mut self) -> Result<String> {
        let bytes = self.get_bytes()?;
        String::from_utf8(bytes).map_err(|_| FitzError::Codec("Invalid UTF-8".to_string()))
    }

    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    pub fn is_empty(&self) -> bool {
        self.pos >= self.buf.len()
    }

    pub fn get_remaining(&mut self) -> &'a [u8] {
        let slice = &self.buf[self.pos..];
        self.pos = self.buf.len();
        slice
    }
}

/// Encode a message frame: [u16 msg_type][payload]
pub fn encode_message_frame(msg_type: u16, payload: &[u8]) -> Vec<u8> {
    let mut frame = Vec::new();
    
    // Encode message type (multi-byte for type ≥ 255)
    if msg_type < 255 {
        frame.push(msg_type as u8);
    } else {
        frame.push(0xFF);
        frame.extend_from_slice(&msg_type.to_be_bytes());
    }
    
    // Encode length (u16 BE)
    frame.extend_from_slice(&(payload.len() as u16).to_be_bytes());
    
    // Append payload
    frame.extend_from_slice(payload);
    
    frame
}

/// Decode a message frame: returns (msg_type, payload_start)
pub fn decode_message_frame(buf: &[u8]) -> Result<(u16, usize)> {
    if buf.is_empty() {
        return Err(FitzError::Codec("Empty frame".to_string()));
    }

    let (msg_type, header_len) = if buf[0] == 0xFF {
        if buf.len() < 3 {
            return Err(FitzError::Codec("Incomplete multi-byte message type".to_string()));
        }
        let msg_type = u16::from_be_bytes([buf[1], buf[2]]);
        (msg_type, 3)
    } else {
        (buf[0] as u16, 1)
    };

    if buf.len() < header_len + 2 {
        return Err(FitzError::Codec("Incomplete length field".to_string()));
    }

    let _len = u16::from_be_bytes([buf[header_len], buf[header_len + 1]]) as usize;
    let payload_start = header_len + 2;

    Ok((msg_type, payload_start))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_encode_and_decode_u64() {
        let mut enc = PayloadEncoder::new();
        enc.put_u64(0x0123456789ABCDEF);
        let buf = enc.finish();

        let mut dec = PayloadDecoder::new(&buf);
        let value = dec.get_u64().unwrap();
        assert_eq!(value, 0x0123456789ABCDEF);
    }

    #[test]
    fn should_encode_and_decode_string() {
        let mut enc = PayloadEncoder::new();
        enc.put_string("hello");
        let buf = enc.finish();

        let mut dec = PayloadDecoder::new(&buf);
        let value = dec.get_string().unwrap();
        assert_eq!(value, "hello");
    }

    #[test]
    fn should_encode_and_decode_bytes() {
        let mut enc = PayloadEncoder::new();
        enc.put_bytes(b"data");
        let buf = enc.finish();

        let mut dec = PayloadDecoder::new(&buf);
        let value = dec.get_bytes().unwrap();
        assert_eq!(value, b"data");
    }

    #[test]
    fn should_encode_message_frame_single_byte_type() {
        let frame = encode_message_frame(100, b"hello");
        assert_eq!(frame[0], 100);
        assert_eq!(frame[1], 0);
        assert_eq!(frame[2], 5); // length
    }

    #[test]
    fn should_decode_message_frame() {
        let frame = encode_message_frame(100, b"hello");
        let (msg_type, payload_start) = decode_message_frame(&frame).unwrap();
        assert_eq!(msg_type, 100);
        assert_eq!(&frame[payload_start..], b"hello");
    }
}
