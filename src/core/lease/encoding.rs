//! Lease domain TLV encoding/decoding utilities
//!
//! Handles parsing of TLV-encoded request payloads and building TLV-encoded responses
//! for lease operations (acquire, renew, surrender).

use crate::core::lease::types::LeaseGrant;
use crate::protocol::tags::{TAG_BODY, TAG_DELIVERY_TOKEN, TAG_ID, TAG_LEASE};

/// Parsed TLV payload for lease operations
#[derive(Debug, Clone)]
pub struct LeaseTlvPayload {
    pub id: Option<String>,
    pub token: Option<String>,
    pub ttl_secs: Option<u32>,
    pub body: Option<Vec<u8>>,
}

/// Parse TLV payload to extract lease operation parameters
pub fn parse_tlv_payload(payload: &[u8]) -> LeaseTlvPayload {
    let mut id = None;
    let mut token = None;
    let mut ttl_secs = None;
    let mut body = None;
    let mut i = 0;

    while i < payload.len() {
        if i + 2 > payload.len() {
            break;
        }

        let tag = payload[i];
        let len = payload[i + 1] as usize;
        i += 2;

        if i + len > payload.len() {
            break;
        }

        let data = &payload[i..i + len];
        i += len;

        match tag {
            TAG_ID => {
                if let Ok(s) = std::str::from_utf8(data) {
                    id = Some(s.to_string());
                }
            }
            TAG_DELIVERY_TOKEN => {
                if let Ok(s) = std::str::from_utf8(data) {
                    token = Some(s.to_string());
                }
            }
            TAG_LEASE => {
                if len == 4 {
                    let bytes = [data[0], data[1], data[2], data[3]];
                    ttl_secs = Some(u32::from_be_bytes(bytes));
                }
            }
            TAG_BODY => {
                body = Some(data.to_vec());
            }
            _ => {} // Ignore unknown tags
        }
    }

    LeaseTlvPayload {
        id,
        token,
        ttl_secs,
        body,
    }
}

/// Build TLV response for lease grant
pub fn build_grant_response(grant: &LeaseGrant) -> Vec<u8> {
    let mut response = Vec::new();

    // TAG_ID
    let id_bytes = grant.id.as_bytes();
    response.push(TAG_ID);
    response.push(id_bytes.len() as u8);
    response.extend_from_slice(id_bytes);

    // TAG_DELIVERY_TOKEN
    let token_bytes = grant.token.as_bytes();
    response.push(TAG_DELIVERY_TOKEN);
    response.push(token_bytes.len() as u8);
    response.extend_from_slice(token_bytes);

    // TAG_LEASE (TTL)
    response.push(TAG_LEASE);
    response.push(4); // u32 is 4 bytes
    response.extend_from_slice(&grant.ttl_secs.to_be_bytes());

    // TAG_BODY (optional)
    if let Some(body) = &grant.body {
        response.push(TAG_BODY);
        response.push(body.len() as u8);
        response.extend_from_slice(body);
    }

    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_parse_acquire_payload() {
        // Arrange
        let mut payload = Vec::new();
        payload.push(TAG_LEASE);
        payload.push(4);
        payload.extend_from_slice(&30u32.to_be_bytes());

        // Act
        let parsed = parse_tlv_payload(&payload);

        // Assert
        assert_eq!(parsed.ttl_secs, Some(30));
        assert_eq!(parsed.id, None);
        assert_eq!(parsed.token, None);
    }

    #[test]
    fn should_parse_renew_payload() {
        // Arrange
        let mut payload = Vec::new();
        payload.push(TAG_ID);
        payload.push(5);
        payload.extend_from_slice(b"id123");
        payload.push(TAG_DELIVERY_TOKEN);
        payload.push(9);
        payload.extend_from_slice(b"token_abc");
        payload.push(TAG_LEASE);
        payload.push(4);
        payload.extend_from_slice(&60u32.to_be_bytes());

        // Act
        let parsed = parse_tlv_payload(&payload);

        // Assert
        assert_eq!(parsed.id, Some("id123".to_string()));
        assert_eq!(parsed.token, Some("token_abc".to_string()));
        assert_eq!(parsed.ttl_secs, Some(60));
    }

    #[test]
    fn should_parse_surrender_payload() {
        // Arrange
        let mut payload = Vec::new();
        payload.push(TAG_ID);
        payload.push(5);
        payload.extend_from_slice(b"id456");
        payload.push(TAG_DELIVERY_TOKEN);
        payload.push(8);
        payload.extend_from_slice(b"token_xy");

        // Act
        let parsed = parse_tlv_payload(&payload);

        // Assert
        assert_eq!(parsed.id, Some("id456".to_string()));
        assert_eq!(parsed.token, Some("token_xy".to_string()));
        assert_eq!(parsed.ttl_secs, None);
    }

    #[test]
    fn should_parse_empty_payload() {
        // Arrange
        let payload = Vec::new();

        // Act
        let parsed = parse_tlv_payload(&payload);

        // Assert
        assert_eq!(parsed.id, None);
        assert_eq!(parsed.token, None);
        assert_eq!(parsed.ttl_secs, None);
        assert_eq!(parsed.body, None);
    }

    #[test]
    fn should_build_grant_response_without_body() {
        // Arrange
        let grant = LeaseGrant {
            id: "grant123".to_string(),
            token: "secure_token".to_string(),
            ttl_secs: 300,
            body: None,
        };

        // Act
        let response = build_grant_response(&grant);

        // Assert
        assert!(response.contains(&TAG_ID));
        assert!(response.contains(&TAG_DELIVERY_TOKEN));
        assert!(response.contains(&TAG_LEASE));
        assert!(!response.contains(&TAG_BODY));
    }

    #[test]
    fn should_build_grant_response_with_body() {
        // Arrange
        let grant = LeaseGrant {
            id: "grant456".to_string(),
            token: "another_token".to_string(),
            ttl_secs: 600,
            body: Some(b"lease_data".to_vec()),
        };

        // Act
        let response = build_grant_response(&grant);

        // Assert
        assert!(response.contains(&TAG_ID));
        assert!(response.contains(&TAG_DELIVERY_TOKEN));
        assert!(response.contains(&TAG_LEASE));
        assert!(response.contains(&TAG_BODY));
        
        // Verify body content is present
        let body_content = b"lease_data";
        let windows = response.windows(body_content.len());
        assert!(windows.into_iter().any(|w| w == body_content));
    }

    #[test]
    fn should_parse_payload_with_body() {
        // Arrange
        let mut payload = Vec::new();
        payload.push(TAG_BODY);
        payload.push(10);
        payload.extend_from_slice(b"test_lease");
        payload.push(TAG_LEASE);
        payload.push(4);
        payload.extend_from_slice(&120u32.to_be_bytes());

        // Act
        let parsed = parse_tlv_payload(&payload);

        // Assert
        assert_eq!(parsed.body, Some(b"test_lease".to_vec()));
        assert_eq!(parsed.ttl_secs, Some(120));
    }

    #[test]
    fn should_handle_malformed_lease_gracefully() {
        // Arrange
        let payload = vec![TAG_LEASE, 3]; // Claims 3 bytes but u32 needs 4

        // Act
        let parsed = parse_tlv_payload(&payload);

        // Assert
        assert_eq!(parsed.ttl_secs, None); // Should not parse malformed data
    }
}
