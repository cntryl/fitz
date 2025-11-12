// Control domain handler - routes all control:// operations

use super::service::ControlService;
use super::types::ControlOperation;
use crate::core::domain::{Domain, DomainContext, DomainResponse};
use crate::core::notice::NoticeService;
use crate::protocol::tags::{TAG_BODY, TAG_ERR_MSG, TAG_ID, TAG_ROUTE};
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct ControlDomain {
    service: ControlService,
    // Control domain uses notice service for pub/sub
    notice_service: Arc<RwLock<NoticeService>>,
}

impl ControlDomain {
    pub fn new() -> Self {
        Self {
            service: ControlService::new("default-node".to_string()),
            notice_service: Arc::new(RwLock::new(NoticeService::new())),
        }
    }

    pub fn with_node_id(node_id: String) -> Self {
        Self {
            service: ControlService::new(node_id),
            notice_service: Arc::new(RwLock::new(NoticeService::new())),
        }
    }

    pub fn with_notice_service(notice_service: Arc<RwLock<NoticeService>>) -> Self {
        Self {
            service: ControlService::new("default-node".to_string()),
            notice_service,
        }
    }

    /// Parse TLV-encoded payload to extract body
    fn parse_tlv_body(&self, payload: &[u8]) -> Option<Vec<u8>> {
        self.find_tlv(payload, TAG_BODY)
    }

    /// Extract TLV value by tag
    fn find_tlv(&self, payload: &[u8], tag: u8) -> Option<Vec<u8>> {
        let mut offset = 0;
        while offset + 2 <= payload.len() {
            let t = payload[offset];
            let length = payload[offset + 1] as usize;
            offset += 2;

            if offset + length > payload.len() {
                break;
            }

            if t == tag {
                return Some(payload[offset..offset + length].to_vec());
            }

            offset += length;
        }
        None
    }

    /// Build TLV-encoded response
    fn build_tlv_response(&self, route: &str, msg_id: Option<&str>, body: &[u8]) -> Vec<u8> {
        let mut response = Vec::new();

        // TAG_ROUTE
        let route_bytes = route.as_bytes();
        response.push(TAG_ROUTE);
        response.push(route_bytes.len() as u8);
        response.extend_from_slice(route_bytes);

        // TAG_ID (if present)
        if let Some(id) = msg_id {
            let id_bytes = id.as_bytes();
            response.push(TAG_ID);
            response.push(id_bytes.len() as u8);
            response.extend_from_slice(id_bytes);
        }

        // TAG_BODY
        response.push(TAG_BODY);
        response.push(body.len() as u8);
        response.extend_from_slice(body);

        response
    }

    /// Build TLV-encoded error response
    fn build_error_response(&self, error_msg: &str) -> Vec<u8> {
        let mut response = Vec::new();

        // TAG_ERR_MSG
        let msg_bytes = error_msg.as_bytes();
        response.push(TAG_ERR_MSG);
        response.push(msg_bytes.len() as u8);
        response.extend_from_slice(msg_bytes);

        response
    }
}

impl Default for ControlDomain {
    fn default() -> Self {
        Self::new()
    }
}

impl Domain for ControlDomain {
    fn handle<'a>(
        &'a self,
        request: DomainContext,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = DomainResponse> + Send + 'a>> {
        Box::pin(async move {
            // Parse body from TLV payload
            let body = match self.parse_tlv_body(&request.payload) {
                Some(b) => b,
                None => {
                    let error_response = self.build_error_response("Missing body in request");
                    return DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(error_response));
                }
            };

            // Determine operation from route
            let operation = match ControlOperation::from_route(&request.route_str) {
                Ok(op) => op,
                Err(e) => {
                    let error_response = self.build_error_response(&e);
                    return DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(error_response));
                }
            };

            // Handle the control operation
            match self.service.handle_operation(operation, &body).await {
                Ok(response_body) => {
                    // Dispatch to subscribers (pub/sub pattern)
                    let msg_id_string = self
                        .find_tlv(&request.payload, TAG_ID)
                        .and_then(|b| std::str::from_utf8(&b).ok().map(|s| s.to_string()));
                    let msg_id = msg_id_string.as_deref();

                    let mut notice_service = self.notice_service.write().await;
                    let (_delivered, _failed) = notice_service.publish(
                        &request.route_str,
                        msg_id,
                        &response_body,
                    );
                    drop(notice_service);

                    // Build TLV-encoded response
                    // Echo the body back for pub/sub pattern
                    let response =
                        self.build_tlv_response(&request.route_str, None, &response_body);
                    DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(response))
                }
                Err(err) => {
                    let error_response = self.build_error_response(&err);
                    DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(error_response))
                }
            }
        })
    }

    /// Cleanup all subscriptions for a channel (delegates to notice service)
    fn cleanup_channel<'a>(
        &'a self,
        _rf: crate::storage::RouteFamilyId,
        channel_id: u32,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            let mut service = self.notice_service.write().await;
            service.cleanup_channel(channel_id)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::route::Route;

    #[test]
    fn should_parse_tlv_body_correctly() {
        // Arrange
        let domain = ControlDomain::new();
        let mut payload = Vec::new();
        payload.push(TAG_BODY);
        payload.push(5); // length
        payload.extend_from_slice(b"hello");

        // Act
        let result = domain.parse_tlv_body(&payload);

        // Assert
        assert_eq!(result, Some(b"hello".to_vec()));
    }

    #[test]
    fn should_build_tlv_response_with_route_and_body() {
        // Arrange
        let domain = ControlDomain::new();

        // Act
        let response = domain.build_tlv_response("control://heartbeat", None, b"test");

        // Assert
        assert!(!response.is_empty());
        assert_eq!(response[0], TAG_ROUTE);
    }

    #[tokio::test]
    async fn should_handle_heartbeat_operation() {
        // Arrange
        let domain = ControlDomain::new();
        let mut payload = Vec::new();
        payload.push(TAG_BODY);
        let body = b"{\"nodeId\":\"test-node\",\"timestamp\":1234567890}";
        payload.push(body.len() as u8);
        payload.extend_from_slice(body);

        let request = DomainContext {
            route: Route {
                scheme: crate::protocol::route::Scheme::Control,
                realm: None,
                area: None,
                resource: Some("heartbeat".to_string()),
                operation: None,
                raw: "control://heartbeat".to_string(),
            },
            route_str: "control://heartbeat".to_string(),
            payload,
            channel_id: 1,
            route_family: 0,  // tests use default route family
        };

        // Act
        let response = domain.handle(request).await;

        // Assert
        match response {
            DomainResponse::Frame(_frame) => {
                // Success
            }
            _ => panic!("Expected Frame response"),
        }
    }

    #[tokio::test]
    async fn should_handle_shutdown_operation() {
        // Arrange
        let domain = ControlDomain::new();
        let mut payload = Vec::new();
        payload.push(TAG_BODY);
        let body = b"{\"nodeId\":\"test-node\",\"reason\":\"maintenance\"}";
        payload.push(body.len() as u8);
        payload.extend_from_slice(body);

        let request = DomainContext {
            route: Route {
                scheme: crate::protocol::route::Scheme::Control,
                realm: None,
                area: None,
                resource: Some("shutdown".to_string()),
                operation: None,
                raw: "control://shutdown".to_string(),
            },
            route_str: "control://shutdown".to_string(),
            payload,
            channel_id: 1,
            route_family: 0,  // tests use default route family
        };

        // Act
        let response = domain.handle(request).await;

        // Assert
        match response {
            DomainResponse::Frame(_) => {
                // Success
            }
            _ => panic!("Expected Frame response"),
        }
    }

    #[tokio::test]
    async fn should_return_error_when_body_missing() {
        // Arrange
        let domain = ControlDomain::new();
        let payload = Vec::new(); // Empty payload

        let request = DomainContext {
            route: Route {
                scheme: crate::protocol::route::Scheme::Control,
                realm: None,
                area: None,
                resource: Some("heartbeat".to_string()),
                operation: None,
                raw: "control://heartbeat".to_string(),
            },
            route_str: "control://heartbeat".to_string(),
            payload,
            channel_id: 1,
            route_family: 0,  // tests use default route family
        };

        // Act
        let response = domain.handle(request).await;

        // Assert
        match response {
            DomainResponse::Frame(_frame) => {
                // Success - error frame returned
            }
            _ => panic!("Expected Frame response with error"),
        }
    }
}
