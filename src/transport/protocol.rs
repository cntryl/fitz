//! TLV (Type-Length-Value) wire protocol.
//!
//! Frame format: [type:u16][len:u32][value:bytes]
//! - type: 16-bit frame type identifier
//! - len: 32-bit payload length (big-endian)
//! - value: payload bytes

use std::io::{self, Read, Write};

/// TLV frame header size (2 + 4 = 6 bytes).
pub const TLV_HEADER_SIZE: usize = 6;

/// Maximum frame size (16MB).
pub const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;

/// TLV frame.
#[derive(Debug, Clone)]
pub struct TlvFrame {
    pub frame_type: u16,
    pub payload: Vec<u8>,
}

impl TlvFrame {
    /// Create a new TLV frame.
    pub fn new(frame_type: u16, payload: Vec<u8>) -> Self {
        Self {
            frame_type,
            payload,
        }
    }

    /// Encode the frame to bytes.
    pub fn encode(&self) -> Vec<u8> {
        let len = self.payload.len() as u32;
        let mut buf = Vec::with_capacity(TLV_HEADER_SIZE + self.payload.len());
        buf.extend_from_slice(&self.frame_type.to_be_bytes());
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(&self.payload);
        buf
    }

    /// Decode a frame from a reader.
    pub fn decode<R: Read>(reader: &mut R) -> io::Result<Self> {
        // Read header (type + length)
        let mut header = [0u8; TLV_HEADER_SIZE];
        reader.read_exact(&mut header)?;

        let frame_type = u16::from_be_bytes([header[0], header[1]]);
        let len = u32::from_be_bytes([header[2], header[3], header[4], header[5]]) as usize;

        if len > MAX_FRAME_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("frame too large: {} bytes", len),
            ));
        }

        // Read payload
        let mut payload = vec![0u8; len];
        reader.read_exact(&mut payload)?;

        Ok(Self {
            frame_type,
            payload,
        })
    }

    /// Write frame to a writer.
    pub fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        let encoded = self.encode();
        writer.write_all(&encoded)?;
        writer.flush()
    }
}

/// TLV codec for streaming.
pub struct TlvCodec {
    /// Partial frame buffer for incomplete reads.
    buffer: Vec<u8>,
}

impl TlvCodec {
    /// Create a new TLV codec.
    pub fn new() -> Self {
        Self {
            buffer: Vec::with_capacity(8192),
        }
    }

    /// Feed bytes into the codec and extract complete frames.
    pub fn feed(&mut self, data: &[u8]) -> Vec<TlvFrame> {
        self.buffer.extend_from_slice(data);
        let mut frames = Vec::new();

        while self.buffer.len() >= TLV_HEADER_SIZE {
            // Parse header
            let frame_type = u16::from_be_bytes([self.buffer[0], self.buffer[1]]);
            let len = u32::from_be_bytes([
                self.buffer[2],
                self.buffer[3],
                self.buffer[4],
                self.buffer[5],
            ]) as usize;

            // Check if we have the full frame
            if self.buffer.len() < TLV_HEADER_SIZE + len {
                // Incomplete frame, wait for more data
                break;
            }

            // Extract payload
            let payload = self.buffer[TLV_HEADER_SIZE..TLV_HEADER_SIZE + len].to_vec();
            frames.push(TlvFrame {
                frame_type,
                payload,
            });

            // Remove processed frame from buffer
            self.buffer.drain(0..TLV_HEADER_SIZE + len);
        }

        frames
    }

    /// Clear the internal buffer.
    pub fn clear(&mut self) {
        self.buffer.clear();
    }
}

impl Default for TlvCodec {
    fn default() -> Self {
        Self::new()
    }
}

/// Common TLV frame types.
pub mod frame_types {
    // Session
    pub const SESSION_HELLO: u16 = 0x0001;
    pub const SESSION_AUTH: u16 = 0x0002;
    pub const SESSION_CLOSE: u16 = 0x0003;

    // Streams
    pub const STREAM_APPEND: u16 = 0x0100;
    pub const STREAM_SUBSCRIBE: u16 = 0x0101;
    pub const STREAM_DATA: u16 = 0x0102;
    pub const STREAM_UNSUBSCRIBE: u16 = 0x0103;

    // Queues
    pub const QUEUE_ENQUEUE: u16 = 0x0200;
    pub const QUEUE_DEQUEUE: u16 = 0x0201;
    pub const QUEUE_ACK: u16 = 0x0202;
    pub const QUEUE_NACK: u16 = 0x0203;
    pub const QUEUE_MESSAGE: u16 = 0x0204;

    // RPC
    pub const RPC_REGISTER: u16 = 0x0300;
    pub const RPC_INVOKE: u16 = 0x0301;
    pub const RPC_RESPONSE: u16 = 0x0302;
    pub const RPC_ERROR: u16 = 0x0303;

    // Lease
    pub const LEASE_ACQUIRE: u16 = 0x0400;
    pub const LEASE_RENEW: u16 = 0x0401;
    pub const LEASE_SURRENDER: u16 = 0x0402;
    pub const LEASE_GRANTED: u16 = 0x0403;
    pub const LEASE_REVOKED: u16 = 0x0404;

    // KV
    pub const KV_PUT: u16 = 0x0500;
    pub const KV_GET: u16 = 0x0501;
    pub const KV_DELETE: u16 = 0x0502;
    pub const KV_RESPONSE: u16 = 0x0503;

    // Realm/Pub-Sub
    pub const REALM_SUBSCRIBE: u16 = 0x0600;
    pub const REALM_UNSUBSCRIBE: u16 = 0x0601;
    pub const REALM_PUBLISH: u16 = 0x0602;
    pub const REALM_MESSAGE: u16 = 0x0603;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_encode_and_decode_frame() {
        // Arrange
        let frame = TlvFrame::new(0x1234, vec![1, 2, 3, 4, 5]);

        // Act
        let encoded = frame.encode();
        let mut cursor = std::io::Cursor::new(encoded);
        let decoded = TlvFrame::decode(&mut cursor).unwrap();

        // Assert
        assert_eq!(decoded.frame_type, frame.frame_type);
        assert_eq!(decoded.payload, frame.payload);
    }

    #[test]
    fn should_feed_and_extract_frames() {
        // Arrange
        let mut codec = TlvCodec::new();
        let frame1 = TlvFrame::new(0x0001, vec![10, 20]);
        let frame2 = TlvFrame::new(0x0002, vec![30, 40, 50]);
        let mut data = frame1.encode();
        data.extend_from_slice(&frame2.encode());

        // Act
        let frames = codec.feed(&data);

        // Assert
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].frame_type, 0x0001);
        assert_eq!(frames[1].frame_type, 0x0002);
    }
}
