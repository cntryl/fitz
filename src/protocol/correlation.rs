// LAYER: PROTOCOL (Transport-Agnostic)
//! Frame-level request correlation and the capability advertisement that
//! enables it.
//!
//! # Why this is not a domain payload field
//!
//! Only RPC carried a correlation identifier, inside its own payload. Every
//! other verb was identified on the wire by its `MessageType` alone, so a
//! client could not tell which of several outstanding same-type requests a
//! response answered. Carrying the identifier in a separate record keeps every
//! existing domain payload byte-for-byte unchanged and costs 11 bytes only on
//! the requests that opt in.
//!
//! # Correctness, not telemetry
//!
//! Architectural Law 7 forbids observability from defining behavior. The
//! correlation is deliberately *not* telemetry: it is load-bearing for
//! request/response matching, and is specified as protocol correctness. It must
//! never be sampled, dropped under load, or disabled by an observability
//! setting.
//!
//! # Zero is reserved
//!
//! `0` is the absent value, so a decoded correlation is always `NonZeroU64` and
//! no downstream code needs a "is this really set?" check.

use std::num::NonZeroU64;

/// Wire width of a correlation value.
pub const CORRELATION_BYTES_LEN: usize = 8;

/// Protocol version advertised by this broker.
pub const PROTOCOL_VERSION: u16 = 1;

/// Broker accepts `CORRELATE` records and echoes `CORRELATED` on responses.
pub const CAP_CORRELATION: u32 = 1 << 0;

/// Every capability this broker implements.
pub const SUPPORTED_CAPABILITIES: u32 = CAP_CORRELATION;

/// Why a correlation record could not be accepted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CorrelationError {
    /// Value was not exactly `CORRELATION_BYTES_LEN` bytes.
    MalformedLength(usize),
    /// Value decoded to the reserved absent sentinel.
    ReservedZero,
}

impl std::fmt::Display for CorrelationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MalformedLength(len) => {
                write!(
                    f,
                    "malformed CORRELATE: expected {CORRELATION_BYTES_LEN} bytes, got {len}"
                )
            }
            Self::ReservedZero => write!(f, "CORRELATE id 0 is reserved"),
        }
    }
}

impl std::error::Error for CorrelationError {}

/// Decode a `CORRELATE` / `CORRELATED` record value.
///
/// # Errors
///
/// Returns [`CorrelationError`] when the value is the wrong width or is the
/// reserved zero sentinel.
pub fn decode(value: &[u8]) -> Result<NonZeroU64, CorrelationError> {
    let bytes: [u8; CORRELATION_BYTES_LEN] = value
        .try_into()
        .map_err(|_| CorrelationError::MalformedLength(value.len()))?;
    NonZeroU64::new(u64::from_be_bytes(bytes)).ok_or(CorrelationError::ReservedZero)
}

/// Encode a correlation value.
#[must_use]
pub fn encode(correlation: NonZeroU64) -> [u8; CORRELATION_BYTES_LEN] {
    correlation.get().to_be_bytes()
}

/// Encode a `SERVER_HELLO` body.
///
/// Readers MUST ignore trailing bytes so a later version can append fields
/// without burning another message id.
#[must_use]
pub fn encode_server_hello(version: u16, capabilities: u32) -> Vec<u8> {
    let mut out = Vec::with_capacity(6);
    out.extend_from_slice(&version.to_be_bytes());
    out.extend_from_slice(&capabilities.to_be_bytes());
    out
}

/// Decode a `SERVER_HELLO` body, ignoring any trailing forward-compatible bytes.
#[must_use]
pub fn decode_server_hello(value: &[u8]) -> Option<(u16, u32)> {
    if value.len() < 6 {
        return None;
    }
    let version = u16::from_be_bytes([value[0], value[1]]);
    let capabilities = u32::from_be_bytes([value[2], value[3], value[4], value[5]]);
    Some((version, capabilities))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_round_trip_a_correlation_value() {
        // Arrange
        let correlation = NonZeroU64::new(0x0102_0304_0506_0708).expect("non-zero");

        // Act
        let encoded = encode(correlation);
        let decoded = decode(&encoded).expect("decode");

        // Assert
        assert_eq!(encoded, [1, 2, 3, 4, 5, 6, 7, 8]);
        assert_eq!(decoded, correlation);
    }

    #[test]
    fn should_reject_zero_as_the_reserved_absent_value() {
        // Arrange
        let value = 0u64.to_be_bytes();

        // Act
        let result = decode(&value);

        // Assert
        assert_eq!(result, Err(CorrelationError::ReservedZero));
    }

    #[test]
    fn should_reject_a_value_that_is_not_eight_bytes() {
        // Arrange
        // Act
        // Assert: both short and long values are malformed, so a truncated or
        // padded record can never be silently read as a valid correlation.
        assert_eq!(
            decode(&[1, 2, 3]),
            Err(CorrelationError::MalformedLength(3))
        );
        assert_eq!(
            decode(&[0, 0, 0, 0, 0, 0, 0, 1, 9]),
            Err(CorrelationError::MalformedLength(9))
        );
    }

    #[test]
    fn should_round_trip_a_server_hello_body() {
        // Arrange
        // Act
        let encoded = encode_server_hello(PROTOCOL_VERSION, SUPPORTED_CAPABILITIES);
        let decoded = decode_server_hello(&encoded).expect("decode");

        // Assert
        assert_eq!(encoded.len(), 6);
        assert_eq!(decoded, (PROTOCOL_VERSION, SUPPORTED_CAPABILITIES));
    }

    #[test]
    fn should_ignore_trailing_server_hello_bytes_for_forward_compatibility() {
        // Arrange
        // A later broker may append fields. This broker must read the prefix it
        // understands rather than reject the frame, or the version after this
        // one cannot extend the handshake without a new message id.
        let mut body = encode_server_hello(2, CAP_CORRELATION);
        body.extend_from_slice(&[0xAB, 0xCD]);

        // Act
        let decoded = decode_server_hello(&body).expect("decode");

        // Assert
        assert_eq!(decoded, (2, CAP_CORRELATION));
    }

    #[test]
    fn should_reject_a_truncated_server_hello_body() {
        // Arrange
        let body = [0u8; 5];

        // Act
        let decoded = decode_server_hello(&body);

        // Assert
        assert!(decoded.is_none());
    }
}
