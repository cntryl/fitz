//! RPC domain TLV encoding/decoding utilities
//!
//! Handles parsing of TLV-encoded request payloads and building TLV-encoded responses
//! for RPC operations (request/reply patterns).

use crate::protocol::tags::{
    TAG_BODY, TAG_ERR_MSG, TAG_ID, TAG_ROUTE, TAG_ROUTE_REPLY, TAG_SEQ, TAG_STREAM_END,
    TAG_SUBSCRIBE, TAG_UNSUBSCRIBE,
};
use smallvec::SmallVec;

/// Response buffer optimized for typical RPC frames (<64 bytes for control messages)
/// Uses stack allocation to avoid heap overhead for small messages
#[allow(dead_code)]
type ResponseBuf = SmallVec<[u8; 64]>;

/// RPC operation type
#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum RpcOperation {
    Subscribe,
    Unsubscribe,
    Request,
    Reply,
}

/// Parsed TLV payload for RPC operations
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct RpcTlvPayload {
    pub operation: RpcOperation,
    pub body: Option<Vec<u8>>,
    pub correlation_id: Option<String>,
    pub reply_route: Option<String>,
    pub seq: Option<u64>,
    pub is_stream_end: bool,
}

/// Parse TLV payload in a single pass
/// Returns descriptive errors on malformed input
#[allow(dead_code)]
pub fn parse_tlv_payload(payload: &[u8]) -> Result<RpcTlvPayload, String> {
    let mut has_subscribe = false;
    let mut has_unsubscribe = false;
    let mut body: Option<Vec<u8>> = None;
    let mut correlation_id: Option<String> = None;
    let mut reply_route: Option<String> = None;
    let mut seq: Option<u64> = None;
    let mut is_stream_end = false;

    let mut offset = 0;
    while offset + 2 <= payload.len() {
        let tag = payload[offset];
        let length = payload[offset + 1] as usize;
        let value_start = offset + 2;

        // Validate that we have enough bytes for the advertised length
        if value_start + length > payload.len() {
            return Err(format!(
                "Malformed TLV at offset {}: tag {} claims {} bytes but only {} available",
                offset,
                tag,
                length,
                payload.len() - value_start
            ));
        }

        match tag {
            TAG_SUBSCRIBE => has_subscribe = true,
            TAG_UNSUBSCRIBE => has_unsubscribe = true,
            TAG_BODY => body = Some(payload[value_start..value_start + length].to_vec()),
            TAG_ID => {
                if let Ok(s) = std::str::from_utf8(&payload[value_start..value_start + length]) {
                    correlation_id = Some(s.to_string());
                }
            }
            TAG_ROUTE_REPLY => {
                if let Ok(s) = std::str::from_utf8(&payload[value_start..value_start + length]) {
                    reply_route = Some(s.to_string());
                }
            }
            TAG_SEQ => {
                if length == 8 {
                    let mut bytes = [0u8; 8];
                    bytes.copy_from_slice(&payload[value_start..value_start + 8]);
                    seq = Some(u64::from_be_bytes(bytes));
                }
            }
            TAG_STREAM_END => is_stream_end = true,
            _ => {
                // Unknown tag - skip it (forward compatibility)
            }
        }

        offset = value_start + length;
    }

    // Check for trailing garbage bytes
    if offset != payload.len() {
        return Err(format!(
            "TLV parse incomplete: {} trailing bytes after offset {}",
            payload.len() - offset,
            offset
        ));
    }

    // Determine operation type
    let operation = if has_subscribe {
        RpcOperation::Subscribe
    } else if has_unsubscribe {
        RpcOperation::Unsubscribe
    } else if reply_route.is_some() {
        RpcOperation::Request
    } else if body.is_some() {
        RpcOperation::Reply
    } else {
        return Err("Unknown RPC operation: could not determine operation type".to_string());
    };

    Ok(RpcTlvPayload {
        operation,
        body,
        correlation_id,
        reply_route,
        seq,
        is_stream_end,
    })
}

/// Build TLV response for subscription acknowledgment
pub fn build_subscribe_response(route: &str) -> ResponseBuf {
    let mut response = ResponseBuf::new();

    response.push(TAG_ROUTE);
    response.push(route.len() as u8);
    response.extend_from_slice(route.as_bytes());

    response
}

/// Build TLV response for RPC request acknowledgment
pub fn build_request_response(route: &str) -> ResponseBuf {
    let mut response = ResponseBuf::new();

    response.push(TAG_ROUTE);
    response.push(route.len() as u8);
    response.extend_from_slice(route.as_bytes());

    response
}

/// Build TLV error response
pub fn build_error_response(error_msg: &str) -> ResponseBuf {
    let mut response = ResponseBuf::new();

    response.push(TAG_ERR_MSG);
    response.push(error_msg.len() as u8);
    response.extend_from_slice(error_msg.as_bytes());

    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_parse_subscribe_operation() {
        // Arrange
        let payload = vec![TAG_SUBSCRIBE, 0];

        // Act
        let result = parse_tlv_payload(&payload);

        // Assert
        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert_eq!(parsed.operation, RpcOperation::Subscribe);
    }

    #[test]
    fn should_parse_unsubscribe_operation() {
        // Arrange
        let payload = vec![TAG_UNSUBSCRIBE, 0];

        // Act
        let result = parse_tlv_payload(&payload);

        // Assert
        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert_eq!(parsed.operation, RpcOperation::Unsubscribe);
    }

    #[test]
    fn should_parse_rpc_request() {
        // Arrange
        let mut payload = Vec::new();
        payload.push(TAG_ROUTE_REPLY);
        payload.push(10);
        payload.extend_from_slice(b"inbox://id");
        payload.push(TAG_ID);
        payload.push(6);
        payload.extend_from_slice(b"req123");
        payload.push(TAG_BODY);
        payload.push(4);
        payload.extend_from_slice(b"data");

        // Act
        let result = parse_tlv_payload(&payload);

        // Assert
        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert_eq!(parsed.operation, RpcOperation::Request);
        assert_eq!(parsed.reply_route, Some("inbox://id".to_string()));
        assert_eq!(parsed.correlation_id, Some("req123".to_string()));
        assert_eq!(parsed.body, Some(b"data".to_vec()));
    }

    #[test]
    fn should_parse_rpc_reply() {
        // Arrange
        let mut payload = Vec::new();
        payload.push(TAG_ID);
        payload.push(6);
        payload.extend_from_slice(b"req456");
        payload.push(TAG_BODY);
        payload.push(8);
        payload.extend_from_slice(b"response");

        // Act
        let result = parse_tlv_payload(&payload);

        // Assert
        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert_eq!(parsed.operation, RpcOperation::Reply);
        assert_eq!(parsed.correlation_id, Some("req456".to_string()));
        assert_eq!(parsed.body, Some(b"response".to_vec()));
    }

    #[test]
    fn should_parse_streaming_rpc_with_seq() {
        // Arrange
        let mut payload = Vec::new();
        payload.push(TAG_ID);
        payload.push(3);
        payload.extend_from_slice(b"cor");
        payload.push(TAG_SEQ);
        payload.push(8);
        payload.extend_from_slice(&42u64.to_be_bytes());
        payload.push(TAG_BODY);
        payload.push(5);
        payload.extend_from_slice(b"chunk");

        // Act
        let result = parse_tlv_payload(&payload);

        // Assert
        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert_eq!(parsed.seq, Some(42));
        assert_eq!(parsed.body, Some(b"chunk".to_vec()));
    }

    #[test]
    fn should_parse_stream_end_flag() {
        // Arrange
        let mut payload = Vec::new();
        payload.push(TAG_BODY);
        payload.push(4);
        payload.extend_from_slice(b"last");
        payload.push(TAG_STREAM_END);
        payload.push(0);

        // Act
        let result = parse_tlv_payload(&payload);

        // Assert
        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert!(parsed.is_stream_end);
        assert_eq!(parsed.body, Some(b"last".to_vec()));
    }

    #[test]
    fn should_reject_malformed_tlv() {
        // Arrange
        let payload = vec![TAG_BODY, 50]; // Claims 50 bytes but no data

        // Act
        let result = parse_tlv_payload(&payload);

        // Assert
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Malformed TLV"));
    }

    #[test]
    fn should_reject_trailing_bytes() {
        // Arrange
        let payload = vec![TAG_SUBSCRIBE, 0, 0xAB]; // Garbage byte

        // Act
        let result = parse_tlv_payload(&payload);

        // Assert
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("trailing bytes"));
    }

    #[test]
    fn should_build_subscribe_response() {
        // Arrange
        let route = "rpc://realm/service/method";

        // Act
        let response = build_subscribe_response(route);

        // Assert
        assert_eq!(response[0], TAG_ROUTE);
        assert_eq!(response[1], route.len() as u8);
        assert_eq!(&response[2..], route.as_bytes());
    }

    #[test]
    fn should_build_request_response() {
        // Arrange
        let route = "rpc://realm/auth/verify";

        // Act
        let response = build_request_response(route);

        // Assert
        assert_eq!(response[0], TAG_ROUTE);
        assert!(response.len() > 2);
    }

    #[test]
    fn should_build_error_response() {
        // Arrange
        let error_msg = "Handler not found";

        // Act
        let response = build_error_response(error_msg);

        // Assert
        assert_eq!(response[0], TAG_ERR_MSG);
        assert_eq!(response[1], error_msg.len() as u8);
        assert_eq!(&response[2..], error_msg.as_bytes());
    }

    #[test]
    fn should_reject_empty_payload() {
        // Arrange
        let payload = Vec::new();

        // Act
        let result = parse_tlv_payload(&payload);

        // Assert
        assert!(result.is_err());
    }
}
