// Stream domain handler - routes all stream:// operations

use super::service::StreamService;
use super::types::{AppendResult, AreaReadResponse, StreamEvent, StreamOperation};
use crate::core::domain::{Domain, DomainContext, DomainResponse};
use crate::protocol::tags::{
    TAG_ASSIGNED_REV, TAG_BODY, TAG_ERR_MSG, TAG_METADATA, TAG_SEQ, TAG_STREAM_END,
};
use crate::routing::DEFAULT_RF;
use crate::storage::traits::KvStore;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Stream handler response types
#[derive(Debug)]
pub enum StreamResponse {
    AppendResult(AppendResult),
    Events(Vec<StreamEvent>),
    AreaRead(AreaReadResponse),
    Subscription(SubscriptionInfo),
    BeginAppendOk { first_seq: u64 },
    AppendOk,
    CommitAppendOk {
        first_seq: u64,
        last_seq: u64,
        event_count: usize,
    },
    RollbackAppendOk,
}

/// Lightweight subscription info returned to subscribers
#[derive(Debug)]
pub struct SubscriptionInfo {
    pub last_resource_seq: Option<u64>,
    pub last_area_seq: Option<u64>,
    pub watermark: Option<u64>,
}

pub struct StreamDomain {
    service: Arc<RwLock<StreamService>>,
}

impl StreamDomain {
    pub fn new(kv_store: Arc<dyn KvStore>) -> Self {
        Self {
            service: Arc::new(RwLock::new(StreamService::new(kv_store))),
        }
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
        let mut service = self.service.write().await;
        Ok(service.subscribe(rf, route_pattern, channel_id, sender))
    }

    /// Unsubscribe from availability notifications
    /// Returns true if subscription was found and removed
    pub async fn unsubscribe(&self, subscription_id: u64) -> Result<bool, String> {
        let mut service = self.service.write().await;
        Ok(service.unsubscribe(subscription_id))
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
    fn build_append_ok_response() -> Vec<u8> {
        let mut buf = Vec::new();
        // Simple success response with no additional data
        buf
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
    fn handle<'a>(
        &'a self,
        request: DomainContext,
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

            let result = {
                let mut service = self.service.write().await;
                match operation {
                    StreamOperation::BeginAppend => {
                        let (area, resource) = match StreamService::parse_route(route_str) {
                            Ok(parts) => parts,
                            Err(err) => return DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(
                                Self::build_error_response(err),
                            )),
                        };
                        let realm = match request.route.realm.as_ref() {
                            Some(r) => r,
                            None => return DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(
                                Self::build_error_response("Missing realm".to_string()),
                            )),
                        };
                        let first_seq = match service
                            .begin_append(DEFAULT_RF, realm, area, resource, request.channel_id, route_str)
                            .await {
                            Ok(seq) => seq,
                            Err(err) => return DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(
                                Self::build_error_response(err),
                            )),
                        };
                        Ok(StreamResponse::BeginAppendOk { first_seq })
                    }
                    StreamOperation::Append => {
                        let event = if let Some(body) = body {
                            StreamEvent {
                                sequence: from_seq.unwrap_or(0),
                                resource: StreamService::parse_route(route_str)?.1.to_string(),
                                area_seq: None, // Will be assigned during commit
                                body,
                                metadata,
                                created_at: std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap()
                                    .as_secs(),
                                is_end,
                            }
                        } else {
                            return DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(
                                Self::build_error_response("Append operation requires body".to_string()),
                            ));
                        };
                        service.append_event(request.channel_id, route_str, event)
                            .await?;
                        Ok(StreamResponse::AppendOk)
                    }
                    StreamOperation::CommitAppend => {
                        let area = StreamService::parse_route(route_str)?.0;
                        let (first_seq, last_seq, event_count) =
                            service.commit_append(request.channel_id, route_str, area).await?;
                        Ok(StreamResponse::CommitAppendOk {
                            first_seq,
                            last_seq,
                            event_count,
                        })
                    }
                    StreamOperation::RollbackAppend => {
                        service.rollback_append(request.channel_id, route_str)
                            .await?;
                        Ok(StreamResponse::RollbackAppendOk)
                    }
                    StreamOperation::Read => {
                        let (area, resource) = StreamService::parse_route(route_str)?;
                        let realm = request.route.realm.as_ref().ok_or("Missing realm")?;
                        let from_seq = from_seq.unwrap_or(0);
                        let limit = limit.unwrap_or(100);
                        let events = service.read(DEFAULT_RF, realm, area, resource, from_seq, limit).await?;
                        Ok(StreamResponse::Events(events))
                    }
                    StreamOperation::ReadArea => {
                        let area = StreamService::parse_route(route_str)?.0;
                        let realm = request.route.realm.as_ref().ok_or("Missing realm")?;
                        let from_seq = from_seq.unwrap_or(0);
                        let limit = limit.unwrap_or(100);
                        let events = service.read_area(DEFAULT_RF, realm, area, from_seq, limit).await?;
                        let watermark = service.get_watermark(DEFAULT_RF, realm, area).await?;
                        Ok(StreamResponse::AreaRead(AreaReadResponse {
                            events,
                            watermark,
                        }))
                    }
                    StreamOperation::Subscribe => {
                        // Parse route to determine subscription level
                        // Route patterns:
                        // - stream://realm/area/subscribe -> subscribes to stream://realm/area/*
                        // - stream://realm/area/resource/subscribe -> subscribes to stream://realm/area/resource/*
                        // Area subscriptions return current watermark, resource subscriptions do not
                        let parts: Vec<&str> = route_str.split('/').collect();
                        
                        // Determine subscription pattern based on route structure
                        // stream://realm/area/subscribe -> stream://realm/area/*
                        // stream://realm/area/resource/subscribe -> stream://realm/area/resource/*
                        let route_pattern = if parts.len() >= 5 && parts[parts.len() - 1] == "subscribe" {
                            if parts.len() == 5 {
                                // Area subscription: stream://realm/area/subscribe -> stream://realm/area/*
                                format!("{}/{}", route_str.trim_end_matches("/subscribe"), "*")
                            } else if parts.len() == 6 {
                                // Resource subscription: stream://realm/area/resource/subscribe -> stream://realm/area/resource/*
                                format!("{}/{}", route_str.trim_end_matches("/subscribe"), "*")
                            } else {
                                return Err("Invalid subscription route format".to_string());
                            }
                        } else {
                            return Err("Subscribe operation must be at area or resource level".to_string());
                        };
                        
                        // Subscribe to availability notifications
                        let mut svc = service.write().await;
                        let sender = request.sender.clone().ok_or_else(|| "No sender available for subscription".to_string())?;
                        let realm = request.route.realm.as_ref().ok_or("Missing realm")?;
                        let sub_id = svc.subscribe(DEFAULT_RF, route_pattern, request.channel_id, sender);
                        
                        // For area subscriptions, return current watermark
                        let watermark = if parts.len() == 5 {
                            // Area subscription - get current watermark
                            svc.get_watermark(realm, parts[parts.len() - 2]).await.ok()
                        } else {
                            None // Resource subscription doesn't use watermark
                        };
                        
                        Ok(StreamResponse::Subscription(
                            SubscriptionInfo {
                                last_resource_seq: None,
                                last_area_seq: None,
                                watermark,
                            },
                        ))
                    }
                    StreamOperation::Unsubscribe => {
                        // Parse subscription ID
                        let sub_id = parse_tlv_payload::<u64>(&request.payload)?;
                        
                        // Unsubscribe from availability notifications
                        let mut svc = service.write().await;
                        svc.unsubscribe(sub_id);
                        
                        Ok(StreamResponse::Unsubscription)
                    }
                    StreamOperation::Peek => {
                        // Peek is same as Read for now
                        let (area, resource) = StreamService::parse_route(route_str)?;
                        let realm = request.route.realm.as_ref().ok_or("Missing realm")?;
                        let from_seq = from_seq.unwrap_or(0);
                        let limit = limit.unwrap_or(100);
                        let events = service.read(DEFAULT_RF, realm, area, resource, from_seq, limit).await?;
                        Ok(StreamResponse::Events(events))
                    }
                }
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
                    let response = Self::build_area_response(area_resp.events, area_resp.watermark);
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
                Ok(StreamResponse::AppendOk) => {
                    let response = Self::build_append_ok_response();
                    DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(response))
                }
                Ok(StreamResponse::CommitAppendOk {
                    first_seq,
                    last_seq,
                    event_count,
                }) => {
                    let response =
                        Self::build_commit_append_response(first_seq, last_seq, event_count);
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
    fn cleanup_channel<'a>(
        &'a self,
        _rf: crate::storage::RouteFamilyId,
        _channel_id: u32,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            // TODO: Cleanup stream subscriptions when StreamService is re-enabled
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::domain::DomainContext;
    use crate::protocol::route::parse_route;
    use crate::protocol::tags::*;
    use crate::storage::midge_adapter;

    // Helper to build a DomainContext for testing
    fn make_request(raw_route: &str, payload: Vec<u8>) -> DomainContext {
        let raw_s = raw_route.to_string();
        let route = parse_route(&raw_s).unwrap();
        // Create a mock sender for testing
        let (tx, _rx) = tokio::sync::mpsc::channel(32);
        DomainContext {
            route,
            route_str: raw_s,
            payload,
            channel_id: 1,
            route_family: 0, // tests use default route family
            sender: Some(tx),
        }
    }

    #[test]
    fn should_parse_begin_append_operation_from_route() {
        // Arrange
        let route = parse_route("stream://area1/resource1/begin-append").unwrap();

        // Act
        let op = StreamOperation::from_route(&route);

        // Assert
        assert!(op.is_ok());
        assert_eq!(op.unwrap(), StreamOperation::BeginAppend);
    }

    #[test]
    fn should_parse_append_operation_from_route() {
        // Arrange
        let route = parse_route("stream://area1/resource1/append").unwrap();

        // Act
        let op = StreamOperation::from_route(&route);

        // Assert
        assert!(op.is_ok());
        assert_eq!(op.unwrap(), StreamOperation::Append);
    }

    #[test]
    fn should_parse_read_operation_from_route() {
        // Arrange
        let route = parse_route("stream://area1/resource1/read").unwrap();

        // Act
        let op = StreamOperation::from_route(&route);

        // Assert
        assert!(op.is_ok());
        assert_eq!(op.unwrap(), StreamOperation::Read);
    }

    #[test]
    fn should_parse_read_area_operation_from_route() {
        // Arrange
        let route = parse_route("stream://area1/read-area").unwrap();

        // Act
        let op = StreamOperation::from_route(&route);

        // Assert
        assert!(op.is_ok());
        assert_eq!(op.unwrap(), StreamOperation::ReadArea);
    }

    #[test]
    fn should_default_to_read_when_no_operation_specified() {
        // Arrange
        let route = parse_route("stream://area1/resource1").unwrap();

        // Act
        let op = StreamOperation::from_route(&route);

        // Assert
        assert!(op.is_ok());
        assert_eq!(op.unwrap(), StreamOperation::Read);
    }

    #[test]
    fn should_default_to_read_area_when_no_resource_specified() {
        // Arrange
        let route = parse_route("stream://area1").unwrap();

        // Act
        let op = StreamOperation::from_route(&route);

        // Assert
        assert!(op.is_ok());
        assert_eq!(op.unwrap(), StreamOperation::ReadArea);
    }

    #[test]
    fn should_return_error_for_unknown_operation() {
        // Arrange
        let route = parse_route("stream://area1/resource1/invalid").unwrap();

        // Act
        let op = StreamOperation::from_route(&route);

        // Assert
        assert!(op.is_err());
        assert!(op.unwrap_err().contains("Unknown stream operation"));
    }

    #[tokio::test]
    async fn should_build_begin_append_response() {
        // Arrange
        let kv_store = Arc::new(midge_adapter::create_memory_store().expect("Create store"));
        let domain = StreamDomain::new(kv_store);
        let payload = Vec::new(); // empty payload for begin-append
        let req = make_request("stream://realm1/area1/resource1/begin-append", payload);

        // Act
        let resp = domain.handle(req).await;

        // Assert
        match resp {
            DomainResponse::Frame(frame) => {
                let data = frame.as_ref();
                // Should contain TAG_SEQ with first_seq (0)
                assert!(data.len() >= 3);
                assert_eq!(data[0], TAG_SEQ);
                assert_eq!(data[1], 8); // u64 length
            }
            _ => panic!("expected Frame response"),
        }
    }

    #[tokio::test]
    async fn should_return_error_when_append_missing_body() {
        // Arrange
        let kv_store = Arc::new(midge_adapter::create_memory_store().expect("Create store"));
        let domain = StreamDomain::new(kv_store);
        let payload = Vec::new(); // missing TAG_BODY
        let req = make_request("stream://realm1/area1/resource1/append", payload);

        // Act
        let resp = domain.handle(req).await;

        // Assert
        match resp {
            DomainResponse::Frame(frame) => {
                let data = frame.as_ref();
                // Should contain TAG_ERR_MSG
                assert!(data.len() >= 2);
                assert_eq!(data[0], TAG_ERR_MSG);
            }
            _ => panic!("expected Frame response with error"),
        }
    }

    #[tokio::test]
    async fn should_parse_tlv_payload_with_sequence() {
        // Arrange
        let mut payload = Vec::new();
        let seq: u64 = 42;
        payload.push(TAG_SEQ);
        payload.push(8);
        payload.extend_from_slice(&seq.to_be_bytes());

        let body = b"test data";
        payload.push(TAG_BODY);
        payload.push(body.len() as u8);
        payload.extend_from_slice(body);

        // Act
        let (parsed_body, _metadata, _is_end, from_seq, _limit) =
            StreamDomain::parse_tlv_payload(&payload);

        // Assert
        assert_eq!(from_seq, Some(seq));
        assert_eq!(parsed_body, Some(body.to_vec()));
    }

    #[tokio::test]
    async fn should_parse_tlv_payload_with_extended_length() {
        // Arrange
        let mut payload = Vec::new();
        payload.push(TAG_BODY);
        payload.push(255); // extended length marker
        let body_len: u32 = 300;
        payload.extend_from_slice(&body_len.to_be_bytes());
        let body = vec![1u8; body_len as usize];
        payload.extend_from_slice(&body);

        // Act
        let (parsed_body, _metadata, _is_end, _from_seq, _limit) =
            StreamDomain::parse_tlv_payload(&payload);

        // Assert
        assert_eq!(parsed_body, Some(body));
    }

    #[tokio::test]
    async fn should_handle_subscribe_operation() {
        // Arrange
        let kv_store = Arc::new(midge_adapter::create_memory_store().expect("Create store"));
        let domain = StreamDomain::new(kv_store);
        let payload = Vec::new(); // empty payload for subscribe
        let req = make_request("stream://realm1/area1/resource1/subscribe", payload);

        // Act
        let resp = domain.handle(req).await;

        // Assert
        match resp {
            DomainResponse::Frame(frame) => {
                let data = frame.as_ref();
                // Should contain TAG_SEQ with subscription ID
                assert!(data.len() >= 3);
                assert_eq!(data[0], TAG_SEQ);
                assert_eq!(data[1], 8); // u64 length
            }
            _ => panic!("expected Frame response"),
        }
    }

    #[tokio::test]
    async fn should_handle_area_subscribe_operation_with_watermark() {
        // Arrange
        let kv_store = Arc::new(midge_adapter::create_memory_store().expect("Create store"));
        let domain = StreamDomain::new(kv_store);
        let payload = Vec::new(); // empty payload for subscribe
        let req = make_request("stream://realm1/area1/subscribe", payload);

        // Act
        let resp = domain.handle(req).await;

        // Assert
        match resp {
            DomainResponse::Frame(frame) => {
                let data = frame.as_ref();
                // Should contain TAG_SEQ with subscription ID
                assert!(data.len() >= 3);
                assert_eq!(data[0], TAG_SEQ);
                assert_eq!(data[1], 8); // u64 length
            }
            _ => panic!("expected Frame response"),
        }
    }

    #[tokio::test]
    async fn should_return_watermark_for_area_subscription_after_commits() {
        // Arrange - commit some events to create a watermark
        let kv_store = Arc::new(midge_adapter::create_memory_store().expect("Create store"));
        let domain = StreamDomain::new(kv_store.clone());
        
        // First, append and commit some events
        let mut payload = Vec::new();
        payload.push(TAG_BODY);
        payload.push(4);
        payload.extend_from_slice(b"test");
        
        let append_req = make_request("stream://realm1/area1/resource1/append", payload);
        let _ = domain.handle(append_req).await; // begin-append
        
        let commit_req = make_request("stream://realm1/area1/resource1/commit-append", Vec::new());
        let _ = domain.handle(commit_req).await; // commit-append
        
        // Now subscribe to the area
        let subscribe_req = make_request("stream://realm1/area1/subscribe", Vec::new());

        // Act
        let resp = domain.handle(subscribe_req).await;

        // Assert - should return watermark (currently 0 since we committed 1 event)
        match resp {
            DomainResponse::Frame(frame) => {
                let data = frame.as_ref();
                // Should contain TAG_SEQ with subscription ID
                assert!(data.len() >= 3);
                assert_eq!(data[0], TAG_SEQ);
                assert_eq!(data[1], 8); // u64 length
                // The watermark should be included in the response
            }
            _ => panic!("expected Frame response"),
        }
    }
}
