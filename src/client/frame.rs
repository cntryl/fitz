//! Frame size validation and UTF-8 helpers for client-side code.
//!
//! These helpers do not decode or validate a complete Wire2 message.

use std::str;

/// Frame size limits
#[derive(Clone, Debug)]
pub struct FrameLimits {
    /// Maximum frame size in bytes (default matches Wire2's default message limit)
    pub max_frame_size: usize,

    /// Maximum buffer size for frame assembly (default is five times the frame limit)
    pub max_buffer_size: usize,
}

impl FrameLimits {
    #[must_use]
    pub fn new(max_frame: usize, max_buffer: usize) -> Self {
        Self {
            max_frame_size: max_frame,
            max_buffer_size: max_buffer,
        }
    }
}

impl Default for FrameLimits {
    fn default() -> Self {
        let max_frame_size = crate::protocol::wire2::DEFAULT_MAX_MESSAGE_BYTES as usize;
        Self {
            max_frame_size,
            max_buffer_size: max_frame_size * 5,
        }
    }
}

/// Frame validation result
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FrameValidation {
    /// Frame is valid
    Valid,

    /// Frame exceeds size limit
    TooLarge { size: usize, limit: usize },

    /// Invalid UTF-8 in string field
    InvalidUtf8,
}

/// Validate frame before processing
#[must_use]
pub fn validate_frame(data: &[u8], limits: &FrameLimits) -> FrameValidation {
    // Check frame size
    if data.len() > limits.max_frame_size {
        return FrameValidation::TooLarge {
            size: data.len(),
            limit: limits.max_frame_size,
        };
    }

    FrameValidation::Valid
}

/// Validate a UTF-8 string field.
///
/// # Errors
///
/// Returns [`FrameValidation::InvalidUtf8`] when `bytes` is not valid UTF-8.
pub fn validate_utf8(bytes: &[u8]) -> Result<&str, FrameValidation> {
    str::from_utf8(bytes).map_err(|_| FrameValidation::InvalidUtf8)
}

/// Validate UTF-8 and return an owned [`String`].
///
/// # Errors
///
/// Returns [`FrameValidation::InvalidUtf8`] when `bytes` is not valid UTF-8.
pub fn validate_utf8_owned(bytes: Vec<u8>) -> Result<String, FrameValidation> {
    String::from_utf8(bytes).map_err(|_| FrameValidation::InvalidUtf8)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_accept_valid_frames() {
        let limits = FrameLimits::default();
        let data = b"test frame data";
        assert_eq!(validate_frame(data, &limits), FrameValidation::Valid);
    }

    #[test]
    fn should_default_to_wire2_message_limit() {
        // Arrange
        let limits = FrameLimits::default();

        // Act
        // Assert
        assert_eq!(
            limits.max_frame_size,
            crate::protocol::wire2::DEFAULT_MAX_MESSAGE_BYTES as usize
        );
        assert_eq!(limits.max_buffer_size, limits.max_frame_size * 5);
    }

    #[test]
    fn should_reject_oversized_frames() {
        // Arrange
        let limits = FrameLimits {
            max_frame_size: 100,
            max_buffer_size: 500,
        };
        let data = vec![0u8; 200];

        // Act
        match validate_frame(&data, &limits) {
            FrameValidation::TooLarge { size, limit } => {
                // Assert
                assert_eq!(size, 200);
                assert_eq!(limit, 100);
            }
            _ => panic!("Expected TooLarge"),
        }
    }

    #[test]
    fn should_validate_utf8() {
        // Arrange
        let valid = b"hello";

        // Act
        assert!(validate_utf8(valid).is_ok());

        // Assert
        let invalid = b"\xFF\xFE";
        assert_eq!(validate_utf8(invalid), Err(FrameValidation::InvalidUtf8));
    }

    #[test]
    fn should_validate_utf8_owned() {
        // Arrange
        let valid = "hello".as_bytes().to_vec();

        // Act
        assert!(validate_utf8_owned(valid).is_ok());

        // Assert
        let invalid = vec![0xFF, 0xFE];
        assert_eq!(
            validate_utf8_owned(invalid),
            Err(FrameValidation::InvalidUtf8)
        );
    }
}
