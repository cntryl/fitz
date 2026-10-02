//! Optional client-reported metadata sent after capability negotiation.

use crate::protocol::payload_codec::{PayloadDecoder, PayloadEncoder};

/// Maximum UTF-8 byte length of the reported service name.
pub const MAX_SERVICE_NAME_BYTES: usize = 128;

/// Encode one service name for [`crate::protocol::tlv::MessageType::SESSION_METADATA`].
#[must_use]
pub fn encode_service_name(service_name: &str) -> Vec<u8> {
    let mut encoder = PayloadEncoder::with_capacity(4 + service_name.len());
    encoder.put_string(service_name);
    encoder.finish()
}

/// Decode and validate a service name from a session metadata message.
///
/// # Errors
///
/// Returns an error when the payload is malformed, the name is empty, contains
/// control characters, or exceeds [`MAX_SERVICE_NAME_BYTES`].
pub fn decode_service_name(payload: &[u8]) -> Result<String, String> {
    let mut decoder = PayloadDecoder::new(payload);
    let service_name = decoder.get_string()?;
    if !decoder.is_complete() {
        return Err("session metadata contains trailing bytes".to_string());
    }

    let service_name = service_name.trim();
    if service_name.is_empty() {
        return Err("service name must not be empty".to_string());
    }
    if service_name.len() > MAX_SERVICE_NAME_BYTES {
        return Err(format!(
            "service name exceeds {MAX_SERVICE_NAME_BYTES} UTF-8 bytes"
        ));
    }
    if service_name.chars().any(char::is_control) {
        return Err("service name must not contain control characters".to_string());
    }

    Ok(service_name.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_round_trip_service_name() {
        // Arrange
        let service_name = "orders-worker";
        let payload = encode_service_name(service_name);

        // Act
        let decoded = decode_service_name(&payload).expect("valid service name");

        // Assert
        assert_eq!(decoded, service_name);
    }

    #[test]
    fn should_reject_oversized_service_name() {
        // Arrange
        let service_name = "x".repeat(MAX_SERVICE_NAME_BYTES + 1);
        let payload = encode_service_name(&service_name);

        // Act
        let result = decode_service_name(&payload);

        // Assert
        assert!(result.unwrap_err().contains("exceeds"));
    }
}
