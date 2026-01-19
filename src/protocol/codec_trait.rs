//! Common codec interface for all domains
//!
//! All domain codecs should implement this trait to ensure consistent
//! behavior across the system.

use crate::protocol::frame_context::FrameContext;

/// Standard codec interface for domain message parsing and encoding
///
/// Each domain codec must implement this trait to:
/// 1. Parse incoming TLV-encoded messages
/// 2. Encode outgoing responses
/// 3. Support synchronous, deterministic processing
///
/// # Implementation Pattern
///
/// ```ignore
/// pub fn parse_request(
///     ctx: &FrameContext,
///     payload: &[u8],
/// ) -> Result<DomainMessage, String> {
///     let mut dec = TlvDecoder::new(payload);
///     match ctx.msg_type {
///         100 => parse_operation_a(&mut dec),
///         101 => parse_operation_b(&mut dec),
///         _ => Err(format!("Unknown operation: {}", ctx.msg_type)),
///     }
/// }
///
/// pub fn encode_response(response: &DomainResponse) -> Vec<u8> {
///     let mut enc = TlvEncoder::new();
///     match response {
///         DomainResponse::Ok => { /* encode ok */ },
///         DomainResponse::Error(e) => { /* encode error */ },
///     }
///     enc.finish()
/// }
/// ```
pub trait DomainCodec {
    /// The message type this codec handles
    type Message;

    /// The response type this codec handles
    type Response;

    /// Parse an incoming message from TLV bytes
    ///
    /// Must be deterministic and thread-safe.
    /// Should validate all inputs and return clear error messages.
    fn parse(&self, ctx: &FrameContext, payload: &[u8]) -> Result<Self::Message, String>;

    /// Encode an outgoing response to TLV bytes
    ///
    /// Must be deterministic and produce valid TLV format.
    fn encode(&self, response: &Self::Response) -> Vec<u8>;
}

/// Common response envelope for all domains
///
/// Domains can return specialized responses, but all should support:
/// - Ok: Operation succeeded with optional payload
/// - Error: Operation failed with error message
#[derive(Debug, Clone)]
pub enum DomainResponse {
    /// Operation succeeded with optional data
    Ok(Option<Vec<u8>>),

    /// Operation failed with error message
    Error(String),

    /// Custom response (domain-specific)
    Custom(Vec<u8>),
}

/// Codec builder pattern for consistent initialization
///
/// # Example
///
/// ```ignore
/// let codec = CodecBuilder::new()
///     .with_family(1)
///     .with_max_payload(1024 * 1024)
///     .build::<MyDomainCodec>()?;
/// ```
pub struct CodecBuilder {
    family: u16,
    max_payload: usize,
}

impl Default for CodecBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl CodecBuilder {
    pub fn new() -> Self {
        Self {
            family: 0,
            max_payload: 64 * 1024 * 1024, // 64 MB default
        }
    }

    pub fn with_family(mut self, family: u16) -> Self {
        self.family = family;
        self
    }

    pub fn with_max_payload(mut self, size: usize) -> Self {
        self.max_payload = size;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_codec_builder() {
        let _builder = CodecBuilder::new()
            .with_family(1)
            .with_max_payload(1024);
    }
}
