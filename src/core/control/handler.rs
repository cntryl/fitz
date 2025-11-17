// Control domain handler - routes all control:// operations

use super::service::ControlService;
use super::types::ControlOperation;
use crate::core::domain::{Domain, DomainContext, DomainResponse};
use crate::core::notice::NoticeService;
use crate::core::parsing::{response, tlv};
use crate::protocol::tags::{TAG_BODY, TAG_ID, TAG_ROUTE};
use std::sync::Arc;
use parking_lot::RwLock;

#[derive(Debug)]
pub struct ControlDomain {
    service: ControlService,
    // Control domain uses notice service for pub/sub
    notice_service: Arc<RwLock<NoticeService>>,
}

impl ControlDomain {
    pub fn new() -> Self {
        Self {
            service: ControlService::new(),
            notice_service: Arc::new(RwLock::new(NoticeService::new())),
        }
    }

    pub fn with_notice_service(notice_service: Arc<RwLock<NoticeService>>) -> Self {
        Self {
            service: ControlService::new(),
            notice_service,
        }
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
}

impl Default for ControlDomain {
    fn default() -> Self {
        Self::new()
    }
}

impl Domain for ControlDomain {
    fn handle(&self, request: DomainContext) -> DomainResponse {
        // Parse body from TLV payload
        let body = match tlv::parse_bytes(&request.payload, TAG_BODY) {
            Some(b) => b,
            None => {
                let error_response = response::error("Missing body in request");
                return DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(
                    error_response,
                ));
            }
        };

        // Determine operation from route
        let operation = match ControlOperation::from_route(&request.route_str) {
            Ok(op) => op,
            Err(e) => {
                let error_response = response::error(&e);
                return DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(
                    error_response,
                ));
            }
        };

        // Handle the control operation
        match self.service.handle_operation(operation, &body) {
            Ok(response_body) => {
                // Dispatch to subscribers (pub/sub pattern)
                let msg_id_string =
                    tlv::parse_string(&request.payload, TAG_ID).map(|s| s.to_string());
                let msg_id = msg_id_string.as_deref();

                let mut notice_service = self.notice_service.write();
                let _ = notice_service.publish(
                    request.route_family,
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
                let error_response = response::error(&err);
                DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(
                    error_response,
                ))
            }
        }
    }

    /// Cleanup all subscriptions for a channel (delegates to notice service)
    fn cleanup_channel(&self, rf: crate::routing::RouteFamilyId, channel_id: u32) {
        let mut service = self.notice_service.write();
        service.cleanup_channel(rf, channel_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::route::Route;

    #[test]
    fn should_parse_tlv_body_correctly() {
        // Arrange
        let mut payload = Vec::new();
        crate::protocol::frame::build_tlv(TAG_BODY, b"hello", &mut payload);

        // Act
        let result = tlv::parse_bytes(&payload, TAG_BODY);

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

    #[test]
    fn should_handle_heartbeat_operation() {
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
            route_family: 0, // tests use default route family
            sender: None,
        };

        // Act
        let response = domain.handle(request);

        // Assert
        match response {
            DomainResponse::Frame(_frame) => {
                // Success
            }
            _ => panic!("Expected Frame response"),
        }
    }

    #[test]
    fn should_handle_shutdown_operation() {
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
            route_family: 0, // tests use default route family
            sender: None,
        };

        // Act
        let response = domain.handle(request);

        // Assert
        match response {
            DomainResponse::Frame(_) => {
                // Success
            }
            _ => panic!("Expected Frame response"),
        }
    }

    #[test]
    fn should_return_error_when_body_missing() {
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
            route_family: 0, // tests use default route family
            sender: None,
        };

        // Act
        let response = domain.handle(request);

        // Assert
        match response {
            DomainResponse::Frame(_frame) => {
                // Success - error frame returned
            }
            _ => panic!("Expected Frame response with error"),
        }
    }
}
