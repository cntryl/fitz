// Stream domain handler - routes all stream:// operations

use super::service::{StreamOperationParams, StreamResponse, StreamService};
use super::types::StreamOperation;
use crate::core::domain::{Domain, DomainRequest, DomainResponse};
use crate::protocol::tags::{
    TAG_ASSIGNED_REV, TAG_BODY, TAG_ERR_MSG, TAG_METADATA, TAG_SEQ, TAG_STREAM_END,
};
use crate::storage::traits::KvStore;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct StreamDomain {
    service: Arc<RwLock<StreamService>>,
}

impl StreamDomain {
    pub fn new(kv_store: Arc<dyn KvStore>) -> Self {
        Self {
            service: Arc::new(RwLock::new(StreamService::new(kv_store))),
        }
    }

    /// Parse TLV payload to extract stream operation parameters
    /// Note: For append operations the server will assign `resource_seq` and `created_at`.
    /// Clients may still include TAG_SEQ for read/seek purposes; TAG_SEQ in append
    /// payloads will be ignored.
    fn parse_tlv_payload(
        payload: &[u8],
    ) -> (
        Option<Vec<u8>>, // body
        Option<Vec<u8>>, // metadata
        bool,            // is_end
        Option<u64>,     // from_seq
        Option<usize>,   // limit
    ) {
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

        (body, metadata, is_end, from_seq, limit)
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

        let json = serde_json::to_vec(&events).unwrap_or_default();

        response.push(TAG_BODY);
        if json.len() <= 255 {
            response.push(json.len() as u8);
            response.extend_from_slice(&json);
        } else {
            response.push(255);
            let len = json.len() as u32;
            response.extend_from_slice(&len.to_be_bytes());
            response.extend_from_slice(&json);
        }

        response
    }

    /// Build TLV response for area read (includes watermark)
    fn build_area_response(events: Vec<super::types::StreamEvent>, watermark: u64) -> Vec<u8> {
        let mut response = Vec::new();

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

    /// Build TLV subscription response with sequence info
    fn build_subscription_response(
        last_resource_seq: Option<u64>,
        last_area_seq: Option<u64>,
        watermark: Option<u64>,
    ) -> Vec<u8> {
        let mut response = Vec::new();

        // TAG_SEQ for last_resource_seq
        if let Some(seq) = last_resource_seq {
            response.push(TAG_SEQ);
            response.push(8);
            response.extend_from_slice(&seq.to_be_bytes());
        }

        // TAG_AREA_SEQ for last_area_seq (using 0xB0)
        if let Some(area_seq) = last_area_seq {
            response.push(0xB0); // TAG_AREA_SEQ
            response.push(8);
            response.extend_from_slice(&area_seq.to_be_bytes());
        }

        // TAG_METADATA for watermark (reuse metadata tag for watermark)
        if let Some(wm) = watermark {
            response.push(TAG_METADATA);
            response.push(8);
            response.extend_from_slice(&wm.to_be_bytes());
        }

        response
    }

    /// Build TLV response for begin-append operation
    fn build_begin_append_response(first_seq: u64) -> Vec<u8> {
        let mut response = Vec::new();

        // TAG_SEQ for first sequence that will be assigned
        response.push(TAG_SEQ);
        response.push(8);
        response.extend_from_slice(&first_seq.to_be_bytes());

        response
    }

    /// Build TLV response for commit-append operation
    fn build_commit_append_response(first_seq: u64, last_seq: u64, event_count: usize) -> Vec<u8> {
        let mut response = Vec::new();

        // TAG_SEQ for first_seq
        response.push(TAG_SEQ);
        response.push(8);
        response.extend_from_slice(&first_seq.to_be_bytes());

        // TAG_ASSIGNED_REV for last_seq
        response.push(TAG_ASSIGNED_REV);
        response.push(8);
        response.extend_from_slice(&last_seq.to_be_bytes());

        // TAG_METADATA for event_count (using u64 encoding)
        response.push(TAG_METADATA);
        response.push(8);
        response.extend_from_slice(&(event_count as u64).to_be_bytes());

        response
    }

    /// Build TLV response for rollback-append operation
    fn build_rollback_append_response() -> Vec<u8> {
        // Empty response body indicates success
        Vec::new()
    }
}

impl Default for StreamDomain {
    fn default() -> Self {
        use crate::storage::traits::KvTransaction;
        use bytes::Bytes;

        struct MockStore;
        impl KvStore for MockStore {
            fn put(&self, _key: &[u8], _value: &[u8]) -> Result<(), String> {
                Ok(())
            }
            fn get(&self, _key: &[u8]) -> Result<Option<Bytes>, String> {
                Ok(None)
            }
            fn delete(&self, _key: &[u8]) -> Result<(), String> {
                Ok(())
            }
            fn put_batch(&self, _writes: Vec<(Vec<u8>, Vec<u8>)>) -> Result<(), String> {
                Ok(())
            }
            fn delete_batch(&self, _keys: Vec<Vec<u8>>) -> Result<(), String> {
                Ok(())
            }
            fn scan(&self, _start: &[u8], _end: &[u8]) -> Result<Vec<(Bytes, Bytes)>, String> {
                Ok(vec![])
            }
            fn flush(&self) -> Result<(), String> {
                Ok(())
            }
            fn begin_transaction(&self) -> Result<Box<dyn KvTransaction>, String> {
                Err("Transactions not supported in mock".to_string())
            }
        }

        Self::new(Arc::new(MockStore))
    }
}

impl Domain for StreamDomain {
    fn handle<'a>(
        &'a self,
        request: DomainRequest,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = DomainResponse> + Send + 'a>> {
        Box::pin(async move {
            let operation = match StreamOperation::from_route(&request.route) {
                Ok(op) => op,
                Err(err) => {
                    return DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(
                        Self::build_error_response(err),
                    ));
                }
            };

            let (body, metadata, is_end, from_seq, limit) =
                Self::parse_tlv_payload(&request.payload);

            let route_str = &request.route_str;

            let params = StreamOperationParams {
                operation,
                route: route_str,
                channel_id: request.channel_id,
                body,
                metadata,
                is_end,
                from_seq,
                limit,
            };

            let result = {
                let mut service = self.service.write().await;
                service.handle_operation(params).await
            };

            match result {
                Ok(StreamResponse::AppendResult(append_result)) => {
                    let response = Self::build_append_response(
                        append_result.resource_seq,
                        append_result.area_seq_range,
                    );
                    DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(response))
                }
                Ok(StreamResponse::Events(events)) => {
                    let response = Self::build_events_response(events);
                    DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(response))
                }
                Ok(StreamResponse::AreaRead(area_resp)) => {
                    let response =
                        Self::build_area_response(area_resp.events, area_resp.watermark);
                    DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(response))
                }
                Ok(StreamResponse::Subscription(sub_info)) => {
                    let response = Self::build_subscription_response(
                        sub_info.last_resource_seq,
                        sub_info.last_area_seq,
                        sub_info.watermark,
                    );
                    DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(response))
                }
                Ok(StreamResponse::BeginAppendOk { first_seq }) => {
                    let response = Self::build_begin_append_response(first_seq);
                    DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(response))
                }
                Ok(StreamResponse::CommitAppendOk {
                    first_seq,
                    last_seq,
                    event_count,
                }) => {
                    let response = Self::build_commit_append_response(first_seq, last_seq, event_count);
                    DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(response))
                }
                Ok(StreamResponse::RollbackAppendOk) => {
                    let response = Self::build_rollback_append_response();
                    DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(response))
                }
                Err(err) => DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(
                    Self::build_error_response(err),
                )),
            }
        })
    }

    fn schemes(&self) -> &[&str] {
        &["stream"]
    }

    fn cleanup_channel<'a>(
        &'a self,
        channel_id: u32,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            let mut service = self.service.write().await;
            service.cleanup_channel(channel_id).await;
        })
    }
}
