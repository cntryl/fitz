//! Wire 3 uses the released fixed header with flag bit 2 assigned to payload
//! extensions. The Wire 2 decoder remains unchanged and continues to reject
//! that bit.

pub use crate::protocol::wire2::header::{
    encode_message, HeaderFlags, HeaderLabels, MessageError, MessageHeader, FLAG_ACK_REQUESTED,
    FLAG_EXTENSIONS_PRESENT, FLAG_RESPONSE_FINAL, HEADER_LEN,
};

pub use crate::protocol::wire2::header::decode_message_wire3 as decode_message;

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroU64;

    #[test]
    fn should_count_the_extension_envelope_inside_message_limit() {
        // Arrange
        let payload = [0, 0, 0, 0, 0, 8, 0, 0, 0, 4, b'd', b'a', b't', b'a'];
        let frame = encode_message(
            104,
            HeaderFlags::extensions_present(),
            NonZeroU64::new(1),
            &payload,
        );

        // Act
        let too_small = decode_message(
            &frame,
            u32::try_from(HEADER_LEN + payload.len() - 1).expect("frame length fits u32"),
        );
        let exact = decode_message(
            &frame,
            u32::try_from(HEADER_LEN + payload.len()).expect("frame length fits u32"),
        );

        // Assert
        assert!(matches!(too_small, Err(MessageError::TooLarge { .. })));
        assert!(exact.is_ok());
    }
}
