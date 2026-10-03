//! Response decoding shared by Stream transport benchmarks.

/// Decode the error code and message from a Stream response payload.
///
/// # Errors
///
/// Returns an error when the payload is not a valid error envelope.
pub fn decode_stream_error_payload(payload: &[u8]) -> Result<(u16, String), String> {
    use crate::protocol::payload_codec::PayloadDecoder;

    let mut decoder = PayloadDecoder::new(payload);
    if !matches!(decoder.get_u8()?, 1 | 2) {
        return Err("Payload is not a Stream error envelope".to_string());
    }
    let code = u16::try_from(decoder.get_u32()?)
        .map_err(|_| "Stream error code exceeds u16 range".to_string())?;
    let message = decoder.get_string()?;
    if !decoder.is_complete() {
        return Err("Trailing data in Stream error envelope".to_string());
    }
    Ok((code, message))
}

/// Retry only a known conflict or a request rejected before actor admission.
#[must_use]
pub fn is_retryable_stream_commit_error(code: u16) -> bool {
    use crate::protocol::error_codes::stream;

    matches!(code, stream::ERR_CONCURRENCY_CONFLICT | stream::ERR_BUSY)
}

#[cfg(test)]
mod tests {
    use super::{decode_stream_error_payload, is_retryable_stream_commit_error};
    use crate::domains::stream::StreamClientResponseBody;
    use crate::protocol::stream_codec::encode_response;

    #[test]
    fn should_decode_versioned_stream_commit_error() {
        // Arrange
        let payload = encode_response(
            602,
            &StreamClientResponseBody::Error("concurrency conflict".into()),
        );

        // Act
        let decoded = decode_stream_error_payload(&payload);

        // Assert
        assert_eq!(decoded, Ok((2001, "concurrency conflict".into())));
    }

    #[test]
    fn should_not_retry_indeterminate_stream_commit_timeout() {
        // Arrange
        let payload = encode_response(
            602,
            &StreamClientResponseBody::Error(
                "domain timeout: request outcome unknown, do not blindly retry".into(),
            ),
        );
        let (code, _) = decode_stream_error_payload(&payload).expect("coded Stream error");

        // Act
        let retryable = is_retryable_stream_commit_error(code);

        // Assert
        assert_eq!(code, 2012);
        assert!(!retryable);
    }

    #[test]
    fn should_retry_stream_commit_conflict() {
        // Arrange
        let code = crate::protocol::error_codes::stream::ERR_CONCURRENCY_CONFLICT;

        // Act
        let retryable = is_retryable_stream_commit_error(code);

        // Assert
        assert!(retryable);
    }

    #[test]
    fn should_retry_pre_admission_stream_busy() {
        // Arrange
        let code = crate::protocol::error_codes::stream::ERR_BUSY;

        // Act
        let retryable = is_retryable_stream_commit_error(code);

        // Assert
        assert!(retryable);
    }
}
