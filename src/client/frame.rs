//! Frame validation and protocol safety
//!
//! Validates frames before processing to prevent:
//! - Frame too large (buffering DoS)
//! - Invalid UTF-8 (protocol violation)
//! - Malformed TLV (protocol violation)

use std::str;

/// Frame size limits
#[derive(Clone, Debug)]
pub struct FrameLimits {
    /// Maximum frame size in bytes (default 100 MB)
    pub max_frame_size: usize,
    
    /// Maximum buffer size for frame assembly (default 500 MB)
    pub max_buffer_size: usize,
}

impl FrameLimits {
    pub fn new(max_frame: usize, max_buffer: usize) -> Self {
        Self {
            max_frame_size: max_frame,
            max_buffer_size: max_buffer,
        }
    }
}

impl Default for FrameLimits {
    fn default() -> Self {
        Self {
            max_frame_size: 100 * 1024 * 1024,  // 100 MB
            max_buffer_size: 500 * 1024 * 1024, // 500 MB
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
    
    /// Malformed TLV encoding
    MalformedTlv(String),
}

/// Validate frame before processing
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

/// Validate UTF-8 string (e.g., in TLV fields)
pub fn validate_utf8(bytes: &[u8]) -> Result<&str, FrameValidation> {
    str::from_utf8(bytes).map_err(|_| FrameValidation::InvalidUtf8)
}

/// Validate UTF-8 and return owned string
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
    fn should_reject_oversized_frames() {
        let limits = FrameLimits {
            max_frame_size: 100,
            max_buffer_size: 500,
        };
        let data = vec![0u8; 200];
        
        match validate_frame(&data, &limits) {
            FrameValidation::TooLarge { size, limit } => {
                assert_eq!(size, 200);
                assert_eq!(limit, 100);
            }
            _ => panic!("Expected TooLarge"),
        }
    }

    #[test]
    fn should_validate_utf8() {
        let valid = b"hello";
        assert!(validate_utf8(valid).is_ok());
        
        let invalid = b"\xFF\xFE";
        assert_eq!(validate_utf8(invalid), Err(FrameValidation::InvalidUtf8));
    }

    #[test]
    fn should_validate_utf8_owned() {
        let valid = "hello".as_bytes().to_vec();
        assert!(validate_utf8_owned(valid).is_ok());
        
        let invalid = vec![0xFF, 0xFE];
        assert_eq!(validate_utf8_owned(invalid), Err(FrameValidation::InvalidUtf8));
    }
}
