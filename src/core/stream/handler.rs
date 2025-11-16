// Stream domain handler - routes all stream:// operations

use super::service::StreamService;
use super::types::StreamEvent;
use crate::core::domain::{Domain, DomainContext, DomainResponse};
use crate::protocol::tags::{
    TAG_ASSIGNED_REV, TAG_BODY, TAG_METADATA, TAG_NOTIFICATION, TAG_SEQ, TAG_STREAM_END,
    TAG_WATERMARK,
};
use crate::storage::traits::KvStore;
use std::sync::Arc;
use parking_lot::RwLock;

/// Parsed TLV payload for stream operations
#[derive(Debug, Clone)]
pub struct StreamTlvPayload {
    pub body: Option<Vec<u8>>,
    pub metadata: Option<Vec<u8>>,
    pub is_end: bool,
    pub from_seq: Option<u64>,
    pub limit: Option<usize>,
}

#[derive(Debug)]
pub struct StreamDomain {
    service: Arc<RwLock<StreamService>>,
}

impl StreamDomain {
    pub fn new(kv_store: Arc<dyn KvStore>) -> Self {
        Self {
            service: Arc::new(RwLock::new(StreamService::new(kv_store))),
        }
    }

    /// Get the shared stream service
    pub fn get_service(&self) -> Arc<RwLock<StreamService>> {
        Arc::clone(&self.service)
    }

    /// Subscribe to availability notifications for a route pattern
    /// Returns subscription ID for later unsubscribe
    pub async fn subscribe(
        &self,
        rf: crate::routing::RouteFamilyId,
        route_pattern: String,
        channel_id: u32,
        sender: crate::core::domain::SubSender,
    ) -> Result<u64, String> {
        let mut service = self.service.write();
        Ok(service.subscribe(rf, route_pattern, channel_id, sender))
    }

    /// Unsubscribe from availability notifications
    /// Returns true if subscription was found and removed
    pub async fn unsubscribe(&self, subscription_id: u64) -> Result<bool, String> {
        let mut service = self.service.write();
        Ok(service.unsubscribe(subscription_id))
    }

    /// Parse TLV payload to extract stream operation parameters
    /// Note: For append operations the server will assign `resource_seq` and `created_at`.
    /// Clients may still include TAG_SEQ for read/seek purposes; TAG_SEQ in append
    /// payloads will be ignored.
    fn parse_tlv_payload(payload: &[u8]) -> StreamTlvPayload {
        let mut body = None;
        let mut metadata = None;
        let mut is_end = false;
        let mut from_seq = None;
        let limit = None;
        let mut i = 0;

        while i < payload.len() {
            if i + 2 > payload.len() {
                break;
            }

            let tag = payload[i];
            let len_byte = payload[i + 1];
            i += 2;

            // Handle extended length encoding
            let len = if len_byte == 255 {
                if i + 4 > payload.len() {
                    break;
                }
                let len_bytes = [payload[i], payload[i + 1], payload[i + 2], payload[i + 3]];
                i += 4;
                u32::from_be_bytes(len_bytes) as usize
            } else {
                len_byte as usize
            };

            if i + len > payload.len() {
                break;
            }

            let data = &payload[i..i + len];
            i += len;

            match tag {
                TAG_SEQ => {
                    if len == 8 {
                        let bytes = [
                            data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                        ];
                        let seq = u64::from_be_bytes(bytes);
                        // Treat TAG_SEQ as a "from"/seek value for reads. Do not
                        // treat it as a client-supplied assignment for appends.
                        from_seq = Some(seq);
                    }
                }
                TAG_BODY => {
                    body = Some(data.to_vec());
                }
                TAG_METADATA => {
                    metadata = Some(data.to_vec());
                }
                TAG_STREAM_END => {
                    is_end = true;
                }
                _ => {}
            }
        }

        StreamTlvPayload {
            body,
            metadata,
            is_end,
            from_seq,
            limit,
        }
    }

    /// Build TLV response for append result
    fn build_append_response(
        resource_seq: u64,
        _area_seq_range: Option<std::ops::Range<u64>>,
    ) -> Vec<u8> {
        let mut response = Vec::new();

        response.push(TAG_ASSIGNED_REV);
        response.push(8);
        response.extend_from_slice(&resource_seq.to_be_bytes());

        response
    }

    /// Build TLV response for events
    fn build_events_response(events: Vec<super::types::StreamEvent>) -> Vec<u8> {
        let mut response = Vec::new();

        for event in events {
            // TAG_NOTIFICATION contains the full event TLV
            let event_tlv = super::encoding::encode_event(&event);
            response.push(TAG_NOTIFICATION);
            if event_tlv.len() <= 255 {
                response.push(event_tlv.len() as u8);
                response.extend_from_slice(&event_tlv);
            } else {
                response.push(255);
                response.extend_from_slice(&(event_tlv.len() as u32).to_be_bytes());
                response.extend_from_slice(&event_tlv);
            }
        }

        response
    }

    /// Build TLV response for area read (includes watermark)
    fn build_area_response(events: Vec<super::types::StreamEvent>, watermark: u64) -> Vec<u8> {
        let mut response = Vec::new();

        // TAG_WATERMARK first
        response.push(TAG_WATERMARK);
        response.push(8);
        response.extend_from_slice(&watermark.to_be_bytes());

        // Then TAG_NOTIFICATION for each event
        for event in events {
            let event_tlv = super::encoding::encode_event(&event);
            response.push(TAG_NOTIFICATION);
            if event_tlv.len() <= 255 {
                response.push(event_tlv.len() as u8);
                response.extend_from_slice(&event_tlv);
            } else {
                response.push(255);
                response.extend_from_slice(&(event_tlv.len() as u32).to_be_bytes());
                response.extend_from_slice(&event_tlv);
            }
        }

        response
    }
}

// NOTE: Default impl commented out due to midge KvStore trait changes
// The trait requires ColumnFamilyHandle parameters that this mock doesn't provide
// Mock store implementations are currently not compatible with the midge trait
/*
impl Default for StreamDomain {
    fn default() -> Self {
        use crate::storage::traits::KvTransaction;
        use cntryl_midge::{MidgeError, MidgeResult};
        use bytes::Bytes;

        struct MockStore;
        impl KvStore for MockStore {
            fn put(&self, _key: &[u8], _value: &[u8]) -> MidgeResult<()> {
                Ok(())
            }
            fn get(&self, _key: &[u8]) -> MidgeResult<Option<Bytes>> {
                Ok(None)
            }
            fn delete(&self, _key: &[u8]) -> MidgeResult<()> {
                Ok(())
            }
            fn scan(&self, _start: &[u8], _end: &[u8]) -> MidgeResult<Vec<(Bytes, Bytes)>> {
                Ok(vec![])
            }
            fn begin_transaction(&self) -> MidgeResult<Box<dyn KvTransaction>> {
                Err(MidgeError::InvalidOperation("Transactions not supported in mock".to_string()))
            }
        }

        Self::new(Arc::new(MockStore))
    }
}
*/

impl Domain for StreamDomain {
    fn handle(&self, request: DomainContext) -> DomainResponse {
        let service = self.service.read();

        // Parse operation from route and TLV payload
        let parsed = Self::parse_tlv_payload(&request.payload);
        let body = parsed.body;
        let metadata = parsed.metadata;
        let is_end = parsed.is_end;
        let from_seq = parsed.from_seq;
        let limit = parsed.limit;

            // Extract area and resource from Route
            let area = match &request.route.area {
                Some(a) => a.as_str(),
                None => return DomainResponse::Error("Missing area in route".to_string()),
            };
            let resource = match &request.route.resource {
                Some(r) => r.as_str(),
                None => return DomainResponse::Error("Missing resource in route".to_string()),
            };
            let operation = request.route.operation.as_deref().unwrap_or("append");
            let realm = area; // Use area as realm for now

            match operation {
                "append" => {
                    // Create event from payload
                    let event = StreamEvent {
                        sequence: 0, // Will be assigned by client
                        resource: resource.to_string(),
                        area_seq: None,
                        body: body.unwrap_or_default(),
                        metadata,
                        created_at: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_secs(),
                        is_end,
                    };

                    // Use transaction API with direct writes
                    match service
                        .begin_append(request.route_family, realm, area, resource)
                        
                    {
                        Ok(txn_id) => {
                            match service
                                .append_event(txn_id, request.route_family, event)
                                
                            {
                                Ok(_) => {
                                    match service.commit_append(txn_id, request.route_family)
                                    {
                                        Ok((first_seq, last_seq, _count)) => {
                                            let response = Self::build_append_response(
                                                first_seq,
                                                Some(first_seq..last_seq + 1),
                                            );
                                            DomainResponse::Frame(
                                                crate::protocol::frame::PooledFrame::from_vec(
                                                    response,
                                                ),
                                            )
                                        }
                                        Err(e) => {
                                            let _ = service
                                                .rollback_append(txn_id, request.route_family)
                                                ;
                                            DomainResponse::Error(format!("Commit failed: {}", e))
                                        }
                                    }
                                }
                                Err(e) => {
                                    let _ =
                                        service.rollback_append(txn_id, request.route_family);
                                    DomainResponse::Error(format!("Append failed: {}", e))
                                }
                            }
                        }
                        Err(e) => DomainResponse::Error(format!("Begin transaction failed: {}", e)),
                    }
                }
                "read" => {
                    let from = from_seq.unwrap_or(0);
                    let lim = limit.unwrap_or(100);

                    match service
                        .read(request.route_family, realm, area, resource, from, lim)
                        
                    {
                        Ok(events) => {
                            let response = Self::build_events_response(events);
                            DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(
                                response,
                            ))
                        }
                        Err(e) => DomainResponse::Error(format!("Read failed: {}", e)),
                    }
                }
                "read-area" => {
                    let from = from_seq.unwrap_or(0);
                    let lim = limit.unwrap_or(100);

                    match service
                        .read_area(request.route_family, realm, area, from, lim)
                        
                    {
                        Ok(events) => {
                            // Get watermark for response
                            let watermark = service
                                .get_watermark(request.route_family, realm, area)
                                
                                .unwrap_or(0);
                            let response = Self::build_area_response(events, watermark);
                            DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(
                                response,
                            ))
                        }
                        Err(e) => DomainResponse::Error(format!("Area read failed: {}", e)),
                    }
                }
                _ => DomainResponse::Error(format!("Unknown stream operation: {}", operation)),
            }
    }

    fn cleanup_channel(&self, rf: crate::routing::RouteFamilyId, channel_id: u32) {
        let mut service = self.service.write();
        service.cleanup_channel(rf, channel_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // Parent module imports required TAG_* constants; redundant here and flagged by clippy

    #[test]
    fn should_parse_empty_tlv_payload() {
        // Arrange
        let payload = vec![];

        // Act
        let parsed = StreamDomain::parse_tlv_payload(&payload);
        let body = parsed.body;
        let metadata = parsed.metadata;
        let is_end = parsed.is_end;
        let from_seq = parsed.from_seq;
        let limit = parsed.limit;

        // Assert
        assert!(body.is_none());
        assert!(metadata.is_none());
        assert!(!is_end);
        assert!(from_seq.is_none());
        assert!(limit.is_none());
    }

    #[test]
    fn should_parse_tlv_payload_with_body() {
        // Arrange
        let mut payload = vec![];
        let body_data = b"test body";
        payload.push(TAG_BODY);
        payload.push(body_data.len() as u8);
        payload.extend_from_slice(body_data);

        // Act
        let parsed = StreamDomain::parse_tlv_payload(&payload);
        let body = parsed.body;
        let metadata = parsed.metadata;
        let is_end = parsed.is_end;
        let from_seq = parsed.from_seq;
        let limit = parsed.limit;

        // Assert
        assert_eq!(body, Some(body_data.to_vec()));
        assert!(metadata.is_none());
        assert!(!is_end);
        assert!(from_seq.is_none());
        assert!(limit.is_none());
    }

    #[test]
    fn should_parse_tlv_payload_with_metadata() {
        // Arrange
        let metadata_data = b"test metadata";
        let mut payload = vec![TAG_METADATA, metadata_data.len() as u8];
        payload.extend_from_slice(metadata_data);

        // Act
        let parsed = StreamDomain::parse_tlv_payload(&payload);
        let body = parsed.body;
        let metadata = parsed.metadata;
        let is_end = parsed.is_end;
        let from_seq = parsed.from_seq;
        let limit = parsed.limit;

        // Assert
        assert!(body.is_none());
        assert_eq!(metadata, Some(metadata_data.to_vec()));
        assert!(!is_end);
        assert!(from_seq.is_none());
        assert!(limit.is_none());
    }

    #[test]
    fn should_parse_tlv_payload_with_stream_end() {
        // Arrange
        let payload = vec![TAG_STREAM_END, 0]; // Empty TLV

        // Act
        let parsed = StreamDomain::parse_tlv_payload(&payload);
        let body = parsed.body;
        let metadata = parsed.metadata;
        let is_end = parsed.is_end;
        let from_seq = parsed.from_seq;
        let limit = parsed.limit;

        // Assert
        assert!(body.is_none());
        assert!(metadata.is_none());
        assert!(is_end);
        assert!(from_seq.is_none());
        assert!(limit.is_none());
    }

    #[test]
    fn should_parse_tlv_payload_with_sequence() {
        // Arrange
        let mut payload = vec![];
        let seq = 42u64;
        payload.push(TAG_SEQ);
        payload.push(8);
        payload.extend_from_slice(&seq.to_be_bytes());

        // Act
        let parsed = StreamDomain::parse_tlv_payload(&payload);
        let body = parsed.body;
        let metadata = parsed.metadata;
        let is_end = parsed.is_end;
        let from_seq = parsed.from_seq;
        let limit = parsed.limit;

        // Assert
        assert!(body.is_none());
        assert!(metadata.is_none());
        assert!(!is_end);
        assert_eq!(from_seq, Some(seq));
        assert!(limit.is_none());
    }

    #[test]
    fn should_parse_tlv_payload_with_multiple_tags() {
        // Arrange
        let mut payload = vec![];

        // Add body
        let body_data = b"test body";
        payload.push(TAG_BODY);
        payload.push(body_data.len() as u8);
        payload.extend_from_slice(body_data);

        // Add metadata
        let metadata_data = b"test metadata";
        payload.push(TAG_METADATA);
        payload.push(metadata_data.len() as u8);
        payload.extend_from_slice(metadata_data);

        // Add sequence
        let seq = 123u64;
        payload.push(TAG_SEQ);
        payload.push(8);
        payload.extend_from_slice(&seq.to_be_bytes());

        // Add stream end
        payload.push(TAG_STREAM_END);
        payload.push(0);

        // Act
        let parsed = StreamDomain::parse_tlv_payload(&payload);
        let body = parsed.body;
        let metadata = parsed.metadata;
        let is_end = parsed.is_end;
        let from_seq = parsed.from_seq;
        let limit = parsed.limit;

        // Assert
        assert_eq!(body, Some(body_data.to_vec()));
        assert_eq!(metadata, Some(metadata_data.to_vec()));
        assert!(is_end);
        assert_eq!(from_seq, Some(seq));
        assert!(limit.is_none());
    }

    #[test]
    fn should_handle_extended_length_encoding() {
        // Arrange
        let mut payload = vec![];
        let large_body = vec![1u8; 300]; // Larger than 255 bytes

        payload.push(TAG_BODY);
        payload.push(255); // Extended length marker
        payload.extend_from_slice(&(large_body.len() as u32).to_be_bytes());
        payload.extend_from_slice(&large_body);

        // Act
        let parsed = StreamDomain::parse_tlv_payload(&payload);
        let body = parsed.body;
        let metadata = parsed.metadata;
        let is_end = parsed.is_end;
        let from_seq = parsed.from_seq;
        let limit = parsed.limit;

        // Assert
        assert_eq!(body, Some(large_body));
        assert!(metadata.is_none());
        assert!(!is_end);
        assert!(from_seq.is_none());
        assert!(limit.is_none());
    }

    #[test]
    fn should_build_append_response() {
        // Arrange
        let resource_seq = 42u64;

        // Act
        let response = StreamDomain::build_append_response(resource_seq, None);

        // Assert
        assert_eq!(response[0], TAG_ASSIGNED_REV);
        assert_eq!(response[1], 8);
        let seq_bytes = &response[2..10];
        assert_eq!(
            u64::from_be_bytes(seq_bytes.try_into().unwrap()),
            resource_seq
        );
    }

    #[test]
    fn should_build_events_response() {
        // Arrange
        let events = vec![StreamEvent {
            sequence: 0,
            resource: "test".to_string(),
            area_seq: Some(100),
            body: b"event data".to_vec(),
            metadata: None,
            created_at: 1234567890,
            is_end: false,
        }];

        // Act
        let response = StreamDomain::build_events_response(events.clone());

        // Assert
        // Should start with TAG_NOTIFICATION
        assert_eq!(response[0], TAG_NOTIFICATION);

        // Parse the TLV event back to verify
        let tlv_start = if response[1] == 255 { 6 } else { 2 };
        let tlv_data = &response[tlv_start..];
        let parsed_event = crate::core::stream::encoding::decode_event(tlv_data).unwrap();
        assert_eq!(parsed_event.sequence, events[0].sequence);
        assert_eq!(parsed_event.body, events[0].body);
    }

    #[test]
    fn should_build_area_response() {
        // Arrange
        let events = vec![StreamEvent {
            sequence: 0,
            resource: "test".to_string(),
            area_seq: Some(100),
            body: b"event data".to_vec(),
            metadata: None,
            created_at: 1234567890,
            is_end: false,
        }];
        let watermark = 150u64;

        // Act
        let response = StreamDomain::build_area_response(events.clone(), watermark);

        // Assert
        // Should start with TAG_WATERMARK
        assert_eq!(response[0], TAG_WATERMARK);
        assert_eq!(response[1], 8); // u64 length
        let wm_bytes = &response[2..10];
        assert_eq!(u64::from_be_bytes(wm_bytes.try_into().unwrap()), watermark);

        // Then TAG_NOTIFICATION for the event
        assert_eq!(response[10], TAG_NOTIFICATION);

        // Parse the TLV event back to verify
        let tlv_start = if response[11] == 255 { 16 } else { 12 };
        let tlv_data = &response[tlv_start..];
        let parsed_event = crate::core::stream::encoding::decode_event(tlv_data).unwrap();
        assert_eq!(parsed_event.sequence, events[0].sequence);
        assert_eq!(parsed_event.body, events[0].body);
    }

    #[test]
    fn should_ignore_unknown_tlv_tags() {
        // Arrange
        let mut payload = vec![];
        let body_data = b"test body";

        // Add unknown tag
        payload.push(0xFF);
        payload.push(4);
        payload.extend_from_slice(b"junk");

        // Add known body tag
        payload.push(TAG_BODY);
        payload.push(body_data.len() as u8);
        payload.extend_from_slice(body_data);

        // Act
        let parsed = StreamDomain::parse_tlv_payload(&payload);
        let body = parsed.body;
        let metadata = parsed.metadata;
        let is_end = parsed.is_end;
        let from_seq = parsed.from_seq;
        let limit = parsed.limit;

        // Assert
        assert_eq!(body, Some(body_data.to_vec()));
        assert!(metadata.is_none());
        assert!(!is_end);
        assert!(from_seq.is_none());
        assert!(limit.is_none());
    }

    #[test]
    fn should_handle_malformed_tlv_data_gracefully() {
        // Arrange
        let payload = vec![TAG_BODY, 10]; // Tag + length but no data

        // Act
        let parsed = StreamDomain::parse_tlv_payload(&payload);
        let body = parsed.body;
        let metadata = parsed.metadata;
        let is_end = parsed.is_end;
        let from_seq = parsed.from_seq;
        let limit = parsed.limit;

        // Assert
        assert!(body.is_none());
        assert!(metadata.is_none());
        assert!(!is_end);
        assert!(from_seq.is_none());
        assert!(limit.is_none());
    }
}
