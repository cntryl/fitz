//! The fixed 15-byte message header and the exact-length message it frames.

use std::num::NonZeroU64;

/// Encoded size of every header.
pub const HEADER_LEN: usize = 15;

/// Bit 0 of `flags`: the sender wants a status frame for a message that is
/// otherwise fire-and-forget (Notice `PUBLISH`).
pub const FLAG_ACK_REQUESTED: u8 = 0b0000_0001;

const RESERVED_FLAGS: u8 = !FLAG_ACK_REQUESTED;

/// Header flags. Reserved bits must be zero on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct HeaderFlags(u8);

impl HeaderFlags {
    #[must_use]
    pub const fn none() -> Self {
        Self(0)
    }

    #[must_use]
    pub const fn ack_requested() -> Self {
        Self(FLAG_ACK_REQUESTED)
    }

    #[must_use]
    pub const fn is_ack_requested(self) -> bool {
        self.0 & FLAG_ACK_REQUESTED != 0
    }

    #[must_use]
    pub const fn bits(self) -> u8 {
        self.0
    }

    /// # Errors
    ///
    /// Returns the offending bits when any reserved bit is set.
    pub const fn from_bits(bits: u8) -> Result<Self, u8> {
        if bits & RESERVED_FLAGS == 0 {
            Ok(Self(bits))
        } else {
            Err(bits & RESERVED_FLAGS)
        }
    }
}

/// The header fields a client chooses per message: the correlation that ties
/// answers back to the request, and the flags.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct HeaderLabels {
    pub correlation: Option<NonZeroU64>,
    pub flags: HeaderFlags,
}

impl HeaderLabels {
    /// Uncorrelated, no flags: a notification or control message.
    #[must_use]
    pub const fn none() -> Self {
        Self {
            correlation: None,
            flags: HeaderFlags::none(),
        }
    }

    /// Correlated, no flags.
    #[must_use]
    pub const fn correlated(correlation: Option<NonZeroU64>) -> Self {
        Self {
            correlation,
            flags: HeaderFlags::none(),
        }
    }
}

/// One message header: who it is for, whether it answers something, and how
/// long the value is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MessageHeader {
    pub message_type: u16,
    pub flags: HeaderFlags,
    /// `None` on notifications and control messages; `Some` on every request
    /// and on every frame that answers one.
    pub correlation: Option<NonZeroU64>,
    pub length: u32,
}

/// Why a frame could not be read as a message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageError {
    /// Fewer than [`HEADER_LEN`] bytes.
    HeaderTruncated { got: usize },
    /// A reserved flag bit was set.
    ReservedFlags(u8),
    /// The declared length exceeds the negotiated limit.
    TooLarge { length: u32, max: u32 },
    /// The frame holds a different number of value bytes than the header declares.
    LengthMismatch { declared: u32, actual: usize },
}

impl std::fmt::Display for MessageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::HeaderTruncated { got } => {
                write!(f, "message header needs {HEADER_LEN} bytes, got {got}")
            }
            Self::ReservedFlags(bits) => write!(f, "reserved header flags set: {bits:#010b}"),
            Self::TooLarge { length, max } => {
                write!(
                    f,
                    "message length {length} exceeds the negotiated limit {max}"
                )
            }
            Self::LengthMismatch { declared, actual } => {
                write!(
                    f,
                    "header declares {declared} value bytes, frame carries {actual}"
                )
            }
        }
    }
}

impl std::error::Error for MessageError {}

impl MessageHeader {
    #[must_use]
    pub fn encode(&self) -> [u8; HEADER_LEN] {
        let mut out = [0_u8; HEADER_LEN];
        out[0..2].copy_from_slice(&self.message_type.to_be_bytes());
        out[2] = self.flags.bits();
        let correlation = self.correlation.map_or(0, NonZeroU64::get);
        out[3..11].copy_from_slice(&correlation.to_be_bytes());
        out[11..15].copy_from_slice(&self.length.to_be_bytes());
        out
    }

    /// Read the header at the start of `frame`. Does not look at the value.
    ///
    /// # Errors
    ///
    /// Returns [`MessageError::HeaderTruncated`] or [`MessageError::ReservedFlags`].
    pub fn decode(frame: &[u8]) -> Result<Self, MessageError> {
        if frame.len() < HEADER_LEN {
            return Err(MessageError::HeaderTruncated { got: frame.len() });
        }
        let message_type = u16::from_be_bytes([frame[0], frame[1]]);
        let flags = HeaderFlags::from_bits(frame[2]).map_err(MessageError::ReservedFlags)?;
        let mut correlation = [0_u8; 8];
        correlation.copy_from_slice(&frame[3..11]);
        let mut length = [0_u8; 4];
        length.copy_from_slice(&frame[11..15]);
        Ok(Self {
            message_type,
            flags,
            correlation: NonZeroU64::new(u64::from_be_bytes(correlation)),
            length: u32::from_be_bytes(length),
        })
    }
}

/// Encode a complete message: header followed by exactly `value`.
///
/// # Panics
///
/// Panics if `value` is longer than `u32::MAX` bytes, which no transport can
/// carry; callers enforce the negotiated limit before encoding.
#[must_use]
pub fn encode_message(
    message_type: u16,
    flags: HeaderFlags,
    correlation: Option<NonZeroU64>,
    value: &[u8],
) -> Vec<u8> {
    let length = u32::try_from(value.len()).expect("message value fits the u32 length field");
    let header = MessageHeader {
        message_type,
        flags,
        correlation,
        length,
    };
    let mut out = Vec::with_capacity(HEADER_LEN + value.len());
    out.extend_from_slice(&header.encode());
    out.extend_from_slice(value);
    out
}

/// Split one transport frame into its header and value, enforcing the
/// negotiated `max_message_bytes` and the exact-length rule.
///
/// # Errors
///
/// Every variant of [`MessageError`]. A frame that fails here is answered with
/// a protocol-block error, never with a connection close.
pub fn decode_message(
    frame: &[u8],
    max_message_bytes: u32,
) -> Result<(MessageHeader, &[u8]), MessageError> {
    let header = MessageHeader::decode(frame)?;
    if header.length > max_message_bytes {
        return Err(MessageError::TooLarge {
            length: header.length,
            max: max_message_bytes,
        });
    }
    let value = &frame[HEADER_LEN..];
    let declared = usize::try_from(header.length).unwrap_or(usize::MAX);
    if value.len() != declared {
        return Err(MessageError::LengthMismatch {
            declared: header.length,
            actual: value.len(),
        });
    }
    Ok((header, value))
}

#[cfg(test)]
mod tests {
    use super::*;

    const LIMIT: u32 = 1024;

    fn correlation(value: u64) -> Option<NonZeroU64> {
        NonZeroU64::new(value)
    }

    #[test]
    fn should_round_trip_a_correlated_request_header() {
        // Arrange
        let value = b"payload";

        // Act
        let frame = encode_message(104, HeaderFlags::none(), correlation(42), value);
        let (header, decoded_value) = decode_message(&frame, LIMIT).expect("decode");

        // Assert
        assert_eq!(frame.len(), HEADER_LEN + value.len());
        assert_eq!(header.message_type, 104);
        assert_eq!(header.correlation, correlation(42));
        assert_eq!(header.length, 7);
        assert_eq!(decoded_value, value);
    }

    #[test]
    fn should_encode_the_documented_byte_layout() {
        // Arrange
        let header = MessageHeader {
            message_type: 0x0102,
            flags: HeaderFlags::ack_requested(),
            correlation: correlation(0x0A0B_0C0D_0E0F_1011),
            length: 0x0000_0203,
        };

        // Act
        let bytes = header.encode();

        // Assert
        assert_eq!(
            bytes,
            [
                0x01, 0x02, 0x01, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10, 0x11, 0x00, 0x00, 0x02,
                0x03
            ]
        );
    }

    #[test]
    fn should_treat_zero_correlation_as_absent() {
        // Arrange
        let frame = encode_message(504, HeaderFlags::none(), None, b"");

        // Act
        let (header, _) = decode_message(&frame, LIMIT).expect("decode");

        // Assert
        assert_eq!(header.correlation, None);
        assert_eq!(&frame[3..11], &[0; 8]);
    }

    #[test]
    fn should_reject_reserved_flag_bits_without_reading_the_value() {
        // Arrange
        let mut frame = encode_message(100, HeaderFlags::none(), correlation(1), b"x");
        frame[2] = 0b1000_0010;

        // Act
        let error = decode_message(&frame, LIMIT).expect_err("reserved bits");

        // Assert
        assert_eq!(error, MessageError::ReservedFlags(0b1000_0010));
    }

    #[test]
    fn should_reject_a_truncated_header() {
        // Arrange
        let frame = [0_u8; HEADER_LEN - 1];

        // Act
        let error = MessageHeader::decode(&frame).expect_err("truncated");

        // Assert
        assert_eq!(error, MessageError::HeaderTruncated { got: 14 });
    }

    #[test]
    fn should_reject_a_length_over_the_negotiated_limit_before_checking_bytes() {
        // Arrange
        let mut frame = encode_message(604, HeaderFlags::none(), correlation(7), b"abc");
        frame[11..15].copy_from_slice(&(LIMIT + 1).to_be_bytes());

        // Act
        let error = decode_message(&frame, LIMIT).expect_err("too large");

        // Assert
        assert_eq!(
            error,
            MessageError::TooLarge {
                length: LIMIT + 1,
                max: LIMIT
            }
        );
    }

    #[test]
    fn should_reject_a_frame_whose_value_length_disagrees_with_the_header() {
        // Arrange
        let mut frame = encode_message(200, HeaderFlags::none(), correlation(9), b"abc");
        frame.push(0xFF);

        // Act
        let error = decode_message(&frame, LIMIT).expect_err("length mismatch");

        // Assert
        assert_eq!(
            error,
            MessageError::LengthMismatch {
                declared: 3,
                actual: 4
            }
        );
    }

    #[test]
    fn should_keep_the_ack_flag_through_a_round_trip() {
        // Arrange
        let frame = encode_message(500, HeaderFlags::ack_requested(), correlation(3), b"evt");

        // Act
        let (header, _) = decode_message(&frame, LIMIT).expect("decode");

        // Assert
        assert!(header.flags.is_ack_requested());
    }
}
