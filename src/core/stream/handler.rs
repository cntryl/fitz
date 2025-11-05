// Stream domain handler - routes all stream:// operations

use super::service::{StreamResponse, StreamService};
use super::types::StreamOperation;
use crate::core::domain::{Domain, DomainRequest, DomainResponse};
use crate::protocol::tags::{TAG_ASSIGNED_REV, TAG_BODY, TAG_ERR_MSG, TAG_METADATA, TAG_SEQ, TAG_STREAM_END};
use crate::storage::traits::KvStore;
use std::sync::Arc;

pub struct StreamDomain {
    service: StreamService,
}

impl StreamDomain {
    pub fn new() -> Self {
        Self {
            service: StreamService::new(),
        }
    }

    /// Parse TLV payload to extract stream operation parameters
    /// Returns: (resource_seq, body, metadata, is_end, from_seq, limit)
    fn parse_tlv_payload(
        payload: &[u8],
    ) -> (
        Option<u64>,
        Option<Vec<u8>>,
        Option<Vec<u8>>,
        bool,
        Option<u64>,
        Option<usize>,
    ) {
        let mut resource_seq = None;
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
                    // TAG_SEQ can be used for both resource_seq and from_seq
                    // For append it's resource_seq, for read it's from_seq
                    if len == 8 {
                        let bytes = [
                            data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                        ];
                        let seq = u64::from_be_bytes(bytes);
                        // We'll set both and let the service decide which to use
                        resource_seq = Some(seq);
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
                    // TAG_STREAM_END is a flag, value doesn't matter
                    is_end = true;
                }
                _ => {} // Ignore unknown tags
            }
        }

        // Extract limit from route if present (not in TLV for now)
        // This is a simplified approach; production might use a TAG_LIMIT
        (resource_seq, body, metadata, is_end, from_seq, limit)
    }

    /// Build TLV response for append result
    fn build_append_response(
        resource_seq: u64,
        _area_seq_range: Option<std::ops::Range<u64>>,
    ) -> Vec<u8> {
        let mut response = Vec::new();

        // TAG_ASSIGNED_REV with resource_seq
        response.push(TAG_ASSIGNED_REV);
        response.push(8); // u64 is 8 bytes
        response.extend_from_slice(&resource_seq.to_be_bytes());

        // If area_seq was assigned, include it
        // For now, we'll just use TAG_ASSIGNED_REV for resource_seq
        // In production, might add TAG_FIRST_ASSIGNED_REV and TAG_ASSIGNED_REV for ranges

        response
    }

    /// Build TLV response for events
    fn build_events_response(events: Vec<super::types::StreamEvent>) -> Vec<u8> {
        let mut response = Vec::new();

        // Encode events as JSON for simplicity (production would use more efficient encoding)
        let json = serde_json::to_vec(&events).unwrap_or_default();

        response.push(TAG_BODY);
        if json.len() <= 255 {
            response.push(json.len() as u8);
            response.extend_from_slice(&json);
        } else {
            // Extended length
            response.push(255);
            let len = json.len() as u32;
            response.extend_from_slice(&len.to_be_bytes());
            response.extend_from_slice(&json);
        }

        response
    }

    /// Build TLV response for area read (includes watermark)
    fn build_area_response(
        events: Vec<super::types::StreamEvent>,
        watermark: u64,
    ) -> Vec<u8> {
        let mut response = Vec::new();

        // Encode as JSON with watermark
        #[derive(serde::Serialize)]
        struct AreaResponse {
            events: Vec<super::types::StreamEvent>,
            watermark: u64,
        }

        let area_resp = AreaResponse { events, watermark };
        let json = serde_json::to_vec(&area_resp).unwrap_or_default();

        response.push(TAG_BODY);
        if json.len() <= 255 {
            response.push(json.len() as u8);
            response.extend_from_slice(&json);
        } else {
            // Extended length
            response.push(255);
            let len = json.len() as u32;
            response.extend_from_slice(&len.to_be_bytes());
            response.extend_from_slice(&json);
        }

        response
    }

    /// Build TLV error response
    fn build_error_response(error: String) -> Vec<u8> {
        let mut response = Vec::new();

        response.push(TAG_ERR_MSG);
        let err_bytes = error.as_bytes();
        if err_bytes.len() <= 255 {
            response.push(err_bytes.len() as u8);
            response.extend_from_slice(err_bytes);
        } else {
            response.push(255);
            response.extend_from_slice(&err_bytes[..255]);
        }

        response
    }
}

impl Default for StreamDomain {
    fn default() -> Self {
        Self::new()
    }
}

impl Domain for StreamDomain {
    fn handle<'a>(
        &'a self,
        request: DomainRequest,
        kv_store: Arc<dyn KvStore>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = DomainResponse> + Send + 'a>> {
        Box::pin(async move {
            // Determine operation from route
            let operation = match StreamOperation::from_route(&request.route) {
                Ok(op) => op,
                Err(err) => {
                    return DomainResponse::Frame(Self::build_error_response(err));
                }
            };

            // Parse TLV payload
            let (resource_seq, body, metadata, is_end, from_seq, limit) =
                Self::parse_tlv_payload(&request.payload);

            // Use route_str for the store operations
            let route_str = &request.route_str;

            // Handle the operation
            let result = self
                .service
                .handle_operation(
                    operation,
                    route_str,
                    resource_seq,
                    body,
                    metadata,
                    is_end,
                    from_seq,
                    limit,
                    kv_store,
                )
                .await;

            // Build response
            match result {
                Ok(StreamResponse::AppendResult(append_result)) => {
                    let response = Self::build_append_response(
                        append_result.resource_seq,
                        append_result.area_seq_range,
                    );
                    DomainResponse::Frame(response)
                }
                Ok(StreamResponse::Events(events)) => {
                    let response = Self::build_events_response(events);
                    DomainResponse::Frame(response)
                }
                Ok(StreamResponse::AreaRead(area_resp)) => {
                    let response =
                        Self::build_area_response(area_resp.events, area_resp.watermark);
                    DomainResponse::Frame(response)
                }
                Err(err) => DomainResponse::Frame(Self::build_error_response(err)),
            }
        })
    }

    fn schemes(&self) -> &[&str] {
        &["stream"]
    }
}
