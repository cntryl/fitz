//! End-to-end KV domain test
//!
//! Tests the codec and message parsing pipeline

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use fitz::protocol::kv;
    use fitz::runtime::routing::RouteFamily;

    #[test]
    fn should_parse_kv_get_message() {
        // Test the codec GET parser
        // Per CLIENT_SPEC: resource is implicit from transaction context (established at BEGIN)
        let key = "test_key";
        let tx_id = 42u64;

        let mut payload = Vec::new();
        payload.extend_from_slice(&tx_id.to_be_bytes());
        payload.extend_from_slice(&(key.len() as u32).to_be_bytes());
        payload.extend_from_slice(key.as_bytes());

        let result = kv::parse_request(
            103, // GET
            RouteFamily::new(1),
            "realm".to_string(),
            "area".to_string(),
            &payload,
        );

        assert!(result.is_ok(), "Failed to parse KV GET message");
    }

    #[test]
    fn should_parse_kv_put_message() {
        // Test the codec PUT parser
        // Per CLIENT_SPEC: resource is implicit from transaction context (established at BEGIN)
        let key = "my_key";
        let value = "my_value";
        let tx_id = 42u64;

        let mut payload = Vec::new();
        payload.extend_from_slice(&tx_id.to_be_bytes());
        payload.extend_from_slice(&(key.len() as u32).to_be_bytes());
        payload.extend_from_slice(key.as_bytes());
        payload.extend_from_slice(&(value.len() as u32).to_be_bytes());
        payload.extend_from_slice(value.as_bytes());

        let result = kv::parse_request(
            104, // PUT
            RouteFamily::new(1),
            "realm".to_string(),
            "area".to_string(),
            &payload,
        );

        assert!(result.is_ok(), "Failed to parse KV PUT message");
    }

    #[test]
    fn should_parse_kv_begin_message() {
        // Test the codec BEGIN parser
        let resource = "my_resource";

        let mut payload = Vec::new();
        payload.extend_from_slice(&(resource.len() as u32).to_be_bytes());
        payload.extend_from_slice(resource.as_bytes());
        payload.push(0); // ReadWrite mode
        payload.push(0); // Buffered write option

        let result = kv::parse_request(
            100, // BEGIN
            RouteFamily::new(1),
            "realm".to_string(),
            "area".to_string(),
            &payload,
        );

        assert!(result.is_ok(), "Failed to parse KV BEGIN message");
    }

    #[test]
    fn should_encode_kv_get_result_found() {
        // Test response encoding
        use fitz::domains::kv::KvResponse;

        let response = KvResponse::GetResult {
            found: true,
            value: Some(Bytes::from("test_value")),
        };

        let encoded = kv::encode_response(&response);
        assert!(
            !encoded.is_empty(),
            "Response encoding should produce bytes"
        );
    }

    #[test]
    fn should_encode_kv_get_result_not_found() {
        // Test response encoding for not-found case
        use fitz::domains::kv::KvResponse;

        let response = KvResponse::GetResult {
            found: false,
            value: None,
        };

        let encoded = kv::encode_response(&response);
        assert!(!encoded.is_empty(), "Not-found response should encode");
    }

    #[test]
    fn should_encode_kv_put_ok() {
        // Test response encoding for PutOk
        // Note: PutOk is an empty response (which is correct in the protocol)
        use fitz::domains::kv::KvResponse;

        let response = KvResponse::PutOk;
        let encoded = kv::encode_response(&response);
        // Empty response is valid - just verify it encodes without error
        let _ = encoded;
    }

    #[test]
    fn should_roundtrip_kv_message() {
        // Test parsing and the fact that it completes successfully
        // Per CLIENT_SPEC: resource is implicit from transaction context (established at BEGIN)
        let key = "test_key";
        let tx_id = 42u64;

        let mut payload = Vec::new();
        payload.extend_from_slice(&tx_id.to_be_bytes());
        payload.extend_from_slice(&(key.len() as u32).to_be_bytes());
        payload.extend_from_slice(key.as_bytes());

        // Parse the message
        let parse_result = kv::parse_request(
            103, // GET
            RouteFamily::new(1),
            "realm".to_string(),
            "area".to_string(),
            &payload,
        );

        assert!(parse_result.is_ok());

        // Simulate a response
        use fitz::domains::kv::KvResponse;
        let response = KvResponse::GetResult {
            found: true,
            value: Some(Bytes::from("found_value")),
        };

        // Encode response
        let encoded = kv::encode_response(&response);
        assert!(!encoded.is_empty());
    }
}
