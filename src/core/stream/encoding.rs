//! TLV encoding/decoding for stream events

use super::types::StreamEvent;
use crate::protocol::frame::{build_tlv, find_tlv};
use crate::protocol::tags::*;

/// Encode a StreamEvent to TLV format
///
/// Format:
/// - TAG_SEQ (0x24): sequence (u64 BE)
/// - TAG_AREA_SEQ (0xB0): area_seq (u64 BE, optional)
/// - TAG_BODY (0x22): body (bytes)
/// - TAG_METADATA (0xA3): metadata (bytes, optional)
/// - TAG_TIMESTAMP (0xB1): created_at (u64 BE)
/// - TAG_STREAM_END (0x25): is_end flag (empty TLV)
pub fn encode_event(event: &StreamEvent) -> Vec<u8> {
    let mut buf = Vec::new();

    // TAG_SEQ: sequence
    build_tlv(TAG_SEQ, &event.sequence.to_be_bytes(), &mut buf);

    // TAG_AREA_SEQ: area_seq (if present)
    if let Some(area_seq) = event.area_seq {
        build_tlv(TAG_AREA_SEQ, &area_seq.to_be_bytes(), &mut buf);
    }

    // TAG_BODY: body
    build_tlv(TAG_BODY, &event.body, &mut buf);

    // TAG_METADATA: metadata (if present)
    if let Some(ref metadata) = event.metadata {
        build_tlv(TAG_METADATA, metadata, &mut buf);
    }

    // TAG_TIMESTAMP: created_at
    build_tlv(TAG_TIMESTAMP, &event.created_at.to_be_bytes(), &mut buf);

    // TAG_STREAM_END: is_end flag (empty TLV)
    if event.is_end {
        build_tlv(TAG_STREAM_END, &[], &mut buf);
    }

    buf
}

/// Decode a StreamEvent from TLV format
pub fn decode_event(bytes: &[u8]) -> Result<StreamEvent, String> {
    // Parse TAG_SEQ (required)
    let sequence = find_tlv(bytes, TAG_SEQ)
        .and_then(|b| {
            if b.len() == 8 {
                Some(u64::from_be_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]))
            } else {
                None
            }
        })
        .ok_or("Missing or invalid TAG_SEQ")?;

    // Parse TAG_AREA_SEQ (optional)
    let area_seq = find_tlv(bytes, TAG_AREA_SEQ).and_then(|b| {
        if b.len() == 8 {
            Some(u64::from_be_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]))
        } else {
            None
        }
    });

    // Parse TAG_BODY (required)
    let body = find_tlv(bytes, TAG_BODY)
        .map(|b| b.to_vec())
        .ok_or("Missing TAG_BODY")?;

    // Parse TAG_METADATA (optional)
    let metadata = find_tlv(bytes, TAG_METADATA).map(|b| b.to_vec());

    // Parse TAG_TIMESTAMP (required)
    let created_at = find_tlv(bytes, TAG_TIMESTAMP)
        .and_then(|b| {
            if b.len() == 8 {
                Some(u64::from_be_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]))
            } else {
                None
            }
        })
        .ok_or("Missing or invalid TAG_TIMESTAMP")?;

    // Parse TAG_STREAM_END (optional flag)
    let is_end = find_tlv(bytes, TAG_STREAM_END).is_some();

    // For now, we don't encode resource in TLV, so use empty string
    // This should be set by the context when decoding
    let resource = String::new();

    Ok(StreamEvent {
        sequence,
        resource,
        area_seq,
        body,
        metadata,
        created_at,
        is_end,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_encode_and_decode_minimal_event() {
        // Arrange
        let event = StreamEvent {
            sequence: 42,
            resource: "test-resource".to_string(),
            area_seq: None,
            body: b"test body".to_vec(),
            metadata: None,
            created_at: 1699401600,
            is_end: false,
        };

        // Act
        let encoded = encode_event(&event);
        let decoded = decode_event(&encoded).unwrap();

        // Assert
        assert_eq!(decoded.sequence, event.sequence);
        // Note: resource is not encoded in TLV, set by service context
        assert_eq!(decoded.resource, ""); // decode_event returns empty resource
        assert_eq!(decoded.area_seq, event.area_seq);
        assert_eq!(decoded.body, event.body);
        assert_eq!(decoded.metadata, event.metadata);
        assert_eq!(decoded.created_at, event.created_at);
        assert_eq!(decoded.is_end, event.is_end);
    }

    #[test]
    fn should_encode_and_decode_full_event() {
        // Arrange
        let event = StreamEvent {
            sequence: 123,
            resource: "test-resource".to_string(),
            area_seq: Some(456),
            body: b"full body with metadata".to_vec(),
            metadata: Some(b"metadata".to_vec()),
            created_at: 1699401600,
            is_end: true,
        };

        // Act
        let encoded = encode_event(&event);
        let decoded = decode_event(&encoded).unwrap();

        // Assert
        assert_eq!(decoded.sequence, event.sequence);
        // Note: resource is not encoded in TLV, set by service context
        assert_eq!(decoded.resource, ""); // decode_event returns empty resource
        assert_eq!(decoded.area_seq, event.area_seq);
        assert_eq!(decoded.body, event.body);
        assert_eq!(decoded.metadata, event.metadata);
        assert_eq!(decoded.created_at, event.created_at);
        assert_eq!(decoded.is_end, event.is_end);
    }

    #[test]
    fn should_decode_event_with_stream_end_flag() {
        // Arrange
        let event = StreamEvent {
            sequence: 5,
            resource: "test-resource".to_string(),
            area_seq: None,
            body: b"final event".to_vec(),
            metadata: None,
            created_at: 1699401600,
            is_end: true,
        };

        // Act
        let encoded = encode_event(&event);
        let decoded = decode_event(&encoded).unwrap();

        // Assert
        assert!(decoded.is_end);
        assert_eq!(decoded.sequence, event.sequence);
        assert_eq!(decoded.resource, ""); // resource not encoded
        assert_eq!(decoded.body, event.body);
    }

    #[test]
    fn should_fail_decoding_without_required_tags() {
        // Arrange
        let empty = Vec::new();

        // Act
        let result = decode_event(&empty);

        // Assert
        assert!(result.is_err());
    }
}
