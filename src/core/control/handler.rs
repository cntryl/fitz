// Control domain handler - routes all control:// operations
//
// This handler is the protocol adapter layer between the engine and ControlService.
// Responsibilities:
// - Parse TLV payload to extract control operation body
// - Determine operation from route
// - Call service to process control operation
// - Return NoticeDelivery for pub/sub fanout (engine coordinates)
//
// The handler NO LONGER directly calls the notice service - this is now
// coordinated through the engine to maintain proper domain isolation.

use super::service::ControlService;
use super::types::ControlOperation;
use crate::core::domain::{Domain, DomainContext, DomainResponse};
use crate::core::parsing::{response::ResponseBuilder, tlv};
use crate::protocol::tags::{TAG_BODY, TAG_ID, TAG_ROUTE};
use std::sync::Arc;

#[derive(Debug)]
pub struct ControlDomain {
    service: Arc<ControlService>,
}

impl ControlDomain {
    pub fn new() -> Self {
        Self {
            service: Arc::new(ControlService::new()),
        }
    }

    /// Get the shared control service
    pub fn get_service(&self) -> Arc<ControlService> {
        Arc::clone(&self.service)
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
                return DomainResponse::Error("Missing body in request".to_string());
            }
        };

        // Determine operation from route
        let operation = match ControlOperation::from_route(&request.route_str) {
            Ok(op) => op,
            Err(e) => {
                return DomainResponse::Error(e);
            }
        };

        // Handle the control operation - service processes and returns response body
        match self.service.handle_operation(operation, body) {
            Ok(response_body) => {
                // Build notification frame for pub/sub fanout
                let notification_frame = ResponseBuilder::new()
                    .add_string(TAG_ROUTE, &request.route_str)
                    .add_optional_string(TAG_ID, tlv::parse_string(&request.payload, TAG_ID))
                    .add_bytes(TAG_BODY, &response_body)
                    .build_frame();

                // Build acknowledgment frame for requester (echo back the body)
                let ack_frame = ResponseBuilder::new()
                    .add_bytes(TAG_BODY, &response_body)
                    .build_frame();

                // Return NoticeDelivery - engine will coordinate fanout to subscribers
                // This maintains proper domain isolation - control domain doesn't directly
                // call notice service, instead returns routing instruction to engine
                DomainResponse::NoticeDelivery {
                    subscribers: smallvec::SmallVec::new(), // Engine will fill this in
                    notification_frame,
                    ack_frame: Some(ack_frame),
                }
            }
            Err(err) => DomainResponse::Error(err),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::frame::find_tlv;
    use crate::protocol::route::Route;

    #[test]
    fn should_parse_tlv_body_correctly() {
        // Arrange
        let mut payload = Vec::new();
        crate::protocol::frame::build_tlv(TAG_BODY, b"hello", &mut payload);

        // Act
        let result = find_tlv(&payload, TAG_BODY);

        // Assert
        assert_eq!(result, Some(b"hello" as &[u8]));
    }

    #[test]
    fn should_handle_heartbeat_operation() {
        // Arrange
        let domain = ControlDomain::new();
        let body = b"{\"nodeId\":\"test-node\",\"timestamp\":1234567890}";
        let mut payload = Vec::new();
        crate::protocol::frame::build_tlv(TAG_BODY, body, &mut payload);

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
        };

        // Act
        let response = domain.handle(request);

        // Assert
        match response {
            DomainResponse::NoticeDelivery {
                notification_frame,
                ack_frame,
                ..
            } => {
                // Success - control now returns NoticeDelivery for pub/sub fanout
                assert!(!notification_frame.as_slice().is_empty());
                assert!(ack_frame.is_some());
            }
            DomainResponse::Error(e) => panic!("Got Error instead of NoticeDelivery: {}", e),
            other => panic!("Expected NoticeDelivery response, got {:?}", other),
        }
    }

    #[test]
    fn should_handle_shutdown_operation() {
        // Arrange
        let domain = ControlDomain::new();
        let body = b"{\"nodeId\":\"test-node\",\"reason\":\"maintenance\"}";
        let mut payload = Vec::new();
        crate::protocol::frame::build_tlv(TAG_BODY, body, &mut payload);

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
        };

        // Act
        let response = domain.handle(request);

        // Assert
        match response {
            DomainResponse::NoticeDelivery {
                notification_frame,
                ack_frame,
                ..
            } => {
                // Success - control now returns NoticeDelivery for pub/sub fanout
                assert!(!notification_frame.as_slice().is_empty());
                assert!(ack_frame.is_some());
            }
            _ => panic!("Expected NoticeDelivery response"),
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
        };

        // Act
        let response = domain.handle(request);

        // Assert
        match response {
            DomainResponse::Error(msg) => {
                // Success - error response for missing body
                assert!(msg.contains("Missing body"));
            }
            _ => panic!("Expected Error response"),
        }
    }
}
