// Queue domain handler - routes all queue:// operations

use super::encoding::{
    build_enqueue_response, build_error_response, build_list_response, build_reserve_response,
    build_success_response, parse_tlv_payload,
};
use super::service::QueueService;
use super::types::QueueOperation;
use crate::core::domain::{Domain, DomainContext, DomainResponse};
use crate::storage::traits::KvStore;
use parking_lot::RwLock;
use std::sync::Arc;

#[derive(Debug)]
pub struct QueueDomain {
    service: Arc<RwLock<QueueService>>,
}

impl QueueDomain {
    pub fn new(kv_store: Arc<dyn KvStore>) -> Self {
        Self {
            service: Arc::new(RwLock::new(QueueService::new(kv_store))),
        }
    }

    /// Get the shared queue service
    pub fn get_service(&self) -> Arc<RwLock<QueueService>> {
        Arc::clone(&self.service)
    }

    /// Parse TLV payload to extract queue operation parameters
    fn parse_tlv_payload(payload: &[u8]) -> super::encoding::QueueTlvPayload {
        parse_tlv_payload(payload)
    }

    /// Build TLV response for enqueue operation
    fn build_enqueue_response(message_ids: &[String]) -> Vec<u8> {
        build_enqueue_response(message_ids)
    }

    /// Build TLV response for reserve operation
    fn build_reserve_response(messages: &[(String, Vec<u8>, String)]) -> Vec<u8> {
        build_reserve_response(messages)
    }

    /// Build TLV response for list operation
    fn build_list_response(queues: &[String]) -> Vec<u8> {
        build_list_response(queues)
    }

    /// Build TLV response for successful operations (consume, extend-lease, config)
    fn build_success_response() -> Vec<u8> {
        build_success_response()
    }

    /// Build TLV error response
    fn build_error_response(error_msg: &str) -> Vec<u8> {
        build_error_response(error_msg)
    }
}

impl Domain for QueueDomain {
    fn handle(&self, request: DomainContext) -> DomainResponse {
        // Parse TLV payload
        let parsed = Self::parse_tlv_payload(&request.payload);
        let message_id = parsed.message_id.clone();
        let body = parsed.body.clone();
        let lease_secs = parsed.lease_secs;
        let delivery_token = parsed.delivery_token.clone();
        let ttl_secs = parsed.ttl_secs;
        let _config = parsed.config;

        // Extract realm, area, resource, and operation from Route
        let realm = match &request.route.realm {
            Some(r) => r.as_str(),
            None => return DomainResponse::Error("Missing realm in route".to_string()),
        };
        let area = match &request.route.area {
            Some(a) => a.as_str(),
            None => return DomainResponse::Error("Missing area in route".to_string()),
        };
        let resource = match &request.route.resource {
            Some(r) => r.as_str(),
            None => return DomainResponse::Error("Missing resource in route".to_string()),
        };

        // Determine queue operation
        let queue_operation = match QueueOperation::from_route(&request.route) {
            Ok(op) => op,
            Err(e) => {
                // For malformed or unknown operations return a TLV error frame so
                // the engine and client receive an encoded error (consistent with
                // other domains like `notice` which return error frames).
                // If the operation is missing entirely, return a DomainResponse::Error
                // so callers can inspect that condition explicitly (tests expect this)
                if e.contains("Missing operation") {
                    return DomainResponse::Error(e);
                }
                let response = Self::build_error_response(&e);
                return DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(
                    response,
                ));
            }
        };

        let service = self.service.read();

        match queue_operation {
                QueueOperation::Enqueue => {
                    let message_body = body.unwrap_or_default();
                    let ttl = ttl_secs.unwrap_or(0);
                    let _batch_size = 1; // Default to single message for now, could be extended

                    match service
                        .enqueue(realm, area, resource, message_body, Some(ttl), None)
                    {
                        Ok(message_id) => {
                            let response = Self::build_enqueue_response(&[message_id]);
                            DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(
                                response,
                            ))
                        }
                        Err(e) => {
                            let response = Self::build_error_response(&e);
                            DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(
                                response,
                            ))
                        }
                    }
                }
                QueueOperation::Subscribe => {
                    // TODO: Implement subscribe operation - register for message availability notifications
                    // For now, just return success
                    let response = Self::build_success_response();
                    DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(response))
                }
                QueueOperation::Unsubscribe => {
                    // TODO: Implement unsubscribe operation - remove message availability notifications
                    // For now, just return success
                    let response = Self::build_success_response();
                    DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(response))
                }
                QueueOperation::Reserve => {
                    let lease_duration = lease_secs.unwrap_or(30);
                    let batch_size = 10; // Default batch size

                    match service
                        .receive(realm, area, resource, batch_size, lease_duration)
                    {
                        Ok(messages) => {
                            let message_data: Vec<(String, Vec<u8>, String)> = messages
                                .into_iter()
                                .map(|msg| (msg.id, msg.body, msg.lease_owner.unwrap_or_default()))
                                .collect();
                            let response = Self::build_reserve_response(&message_data);
                            DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(
                                response,
                            ))
                        }
                        Err(e) => {
                            let response = Self::build_error_response(&e);
                            DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(
                                response,
                            ))
                        }
                    }
                }
                QueueOperation::ExtendLease => {
                    let msg_id = message_id.unwrap_or_default();
                    let token = delivery_token.unwrap_or_default();
                    let additional_secs = lease_secs.unwrap_or(30);

                    match service
                        .extend_lease(realm, area, resource, &msg_id, &token, additional_secs)
                    {
                        Ok(()) => {
                            let response = Self::build_success_response();
                            DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(
                                response,
                            ))
                        }
                        Err(e) => {
                            let response = Self::build_error_response(&e);
                            DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(
                                response,
                            ))
                        }
                    }
                }
                QueueOperation::Consume => {
                    let msg_id = message_id.unwrap_or_default();
                    let token = delivery_token.unwrap_or_default();

                    match service
                        .complete(realm, area, resource, &msg_id, &token)
                    {
                        Ok(()) => {
                            let response = Self::build_success_response();
                            DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(
                                response,
                            ))
                        }
                        Err(e) => {
                            let response = Self::build_error_response(&e);
                            DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(
                                response,
                            ))
                        }
                    }
                }
                QueueOperation::Nack => {
                    let msg_id = message_id.unwrap_or_default();
                    let token = delivery_token.unwrap_or_default();

                    match service.nack(realm, area, resource, &msg_id, &token) {
                        Ok(()) => {
                            let response = Self::build_success_response();
                            DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(
                                response,
                            ))
                        }
                        Err(e) => {
                            let response = Self::build_error_response(&e);
                            DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(
                                response,
                            ))
                        }
                    }
                }
                QueueOperation::Requeue => {
                    let msg_id = message_id.unwrap_or_default();
                    let token = delivery_token.unwrap_or_default();

                    match service
                        .requeue(realm, area, resource, &msg_id, &token)
                    {
                        Ok(()) => {
                            let response = Self::build_success_response();
                            DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(
                                response,
                            ))
                        }
                        Err(e) => {
                            let response = Self::build_error_response(&e);
                            DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(
                                response,
                            ))
                        }
                    }
                }
                QueueOperation::Get => {
                    // Get operation not supported in no-peek design
                    let response = Self::build_error_response("Get operation not supported");
                    DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(response))
                }
                QueueOperation::List => {
                    // Handle different list patterns:
                    // queue://realm/area/*/list -> list all queues in realm/area
                    // queue://realm/*/*/list -> list all queues in realm
                    if resource == "*" && area != "*" {
                        // List all queues in this realm/area scope
                        let queues = service
                            .list_queues_in_scope(realm, area)
                            .unwrap_or_else(|_| Vec::new());
                        let response = Self::build_list_response(&queues);
                        DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(
                            response,
                        ))
                    } else if resource == "*" && area == "*" {
                        // List all queues in this realm (realm/*/*/list)
                        let queues = service
                            .list_queues_in_realm(realm)
                            .unwrap_or_else(|_| Vec::new());
                        let response = Self::build_list_response(&queues);
                        DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(
                            response,
                        ))
                    } else {
                        // List specific queue route
                        let route = format!("{}/{}/{}", realm, area, resource);
                        let queues = vec![route];
                        let response = Self::build_list_response(&queues);
                        DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(
                            response,
                        ))
                    }
                }
                QueueOperation::Config => {
                    // TODO: Implement config operation
                    let response = Self::build_success_response();
                    DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(response))
                }
        }
    }

    fn cleanup_channel(&self, _rf: crate::routing::RouteFamilyId, _channel_id: u32) {
        // Queue domain doesn't maintain per-channel state currently
        // This could be extended to clean up channel-specific leases if needed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::domain::{DomainContext, DomainResponse};
    use crate::protocol::route::parse_route;
    use crate::storage::midge_adapter::create_memory_store;

    fn create_test_handler() -> QueueDomain {
        let kv_store = create_memory_store().expect("Failed to create memory store");
        QueueDomain::new(kv_store)
    }

    fn create_test_context(route_str: &str, payload: Vec<u8>) -> DomainContext {
        let route = parse_route(route_str).expect("Failed to parse route");
        DomainContext {
            route,
            route_str: route_str.to_string(),
            payload,
            channel_id: 1,
            route_family: 0,
            sender: None,
        }
    }

    #[test]
    fn should_parse_resource_path_with_operation() {
        // Arrange
        let handler = create_test_handler();
        let context = create_test_context("queue://test_realm/test_area/resource1/list", vec![]);

        // Act
        let result = handler.handle(context);

        // Assert
        match result {
            DomainResponse::Frame(_) | DomainResponse::Ok => {
                // Should succeed
            }
            DomainResponse::Error(e) => panic!("Expected success, got error: {}", e),
        }
    }

    #[test]
    fn should_handle_enqueue_operation() {
        // Arrange
        let handler = create_test_handler();
        let payload = vec![0x01, 0x04, b'b', b'o', b'd', b'y']; // TAG_BODY with "body"
        let context =
            create_test_context("queue://test_realm/test_area/resource1/enqueue", payload);

        // Act
        let result = handler.handle(context);

        // Assert
        match result {
            DomainResponse::Frame(_) | DomainResponse::Ok => {
                // Should succeed
            }
            DomainResponse::Error(e) => panic!("Expected success, got error: {}", e),
        }
    }

    #[test]
    fn should_handle_receive_operation() {
        // Arrange
        let handler = create_test_handler();
        let context = create_test_context("queue://test_realm/test_area/resource1/receive", vec![]);

        // Act
        let result = handler.handle(context);

        // Assert
        match result {
            DomainResponse::Frame(_) | DomainResponse::Ok => {
                // Should succeed
            }
            DomainResponse::Error(e) => panic!("Expected success, got error: {}", e),
        }
    }

    #[test]
    fn should_handle_list_operation_for_specific_queue() {
        // Arrange
        let handler = create_test_handler();
        let context = create_test_context("queue://test_realm/test_area/resource1/list", vec![]);

        // Act
        let result = handler.handle(context);

        // Assert
        match result {
            DomainResponse::Frame(_) | DomainResponse::Ok => {
                // Should succeed
            }
            DomainResponse::Error(e) => panic!("Expected success, got error: {}", e),
        }
    }

    #[test]
    fn should_handle_list_operation_for_area_wildcard() {
        // Arrange
        let handler = create_test_handler();
        let context = create_test_context("queue://test_realm/test_area/*/list", vec![]);

        // Act
        let result = handler.handle(context);

        // Assert
        match result {
            DomainResponse::Frame(_) | DomainResponse::Ok => {
                // Should succeed
            }
            DomainResponse::Error(e) => panic!("Expected success, got error: {}", e),
        }
    }

    #[test]
    fn should_handle_list_operation_for_realm_wildcard() {
        // Arrange
        let handler = create_test_handler();
        let context = create_test_context("queue://test_realm/*/*/list", vec![]);

        // Act
        let result = handler.handle(context);

        // Assert
        match result {
            DomainResponse::Frame(_) | DomainResponse::Ok => {
                // Should succeed
            }
            DomainResponse::Error(e) => panic!("Expected success, got error: {}", e),
        }
    }

    #[test]
    fn should_handle_unknown_operation() {
        // Arrange
        let handler = create_test_handler();
        let context = create_test_context("queue://test_realm/test_area/resource1/unknown", vec![]);

        // Act
        let result = handler.handle(context);

        // Assert
        match result {
            DomainResponse::Frame(_) | DomainResponse::Ok => {
                // Should return error response for unknown operation
            }
            DomainResponse::Error(e) => panic!("Expected frame response with error, got: {}", e),
        }
    }

    #[test]
    fn should_reject_missing_area() {
        // Arrange
        let handler = create_test_handler();
        let mut context =
            create_test_context("queue://test_realm/test_area/resource1/list", vec![]);
        context.route.area = None;

        // Act
        let result = handler.handle(context);

        // Assert
        match result {
            DomainResponse::Error(e) => assert!(e.contains("Missing area")),
            _ => panic!("Expected error for missing area"),
        }
    }

    #[test]
    fn should_reject_missing_resource() {
        // Arrange
        let handler = create_test_handler();
        let mut context =
            create_test_context("queue://test_realm/test_area/resource1/list", vec![]);
        context.route.resource = None;

        // Act
        let result = handler.handle(context);

        // Assert
        match result {
            DomainResponse::Error(e) => assert!(e.contains("Missing resource")),
            _ => panic!("Expected error for missing resource"),
        }
    }

    #[test]
    fn should_reject_invalid_resource_path_format() {
        // Arrange
        let handler = create_test_handler();
        let context =
            create_test_context("queue://test_realm/test_area/invalid/path/format", vec![]);

        // Act
        let result = handler.handle(context);

        // Assert
        match result {
            DomainResponse::Frame(_) => {
                // Should return error frame for unknown operation
            }
            _ => panic!("Expected frame response with error for unknown operation"),
        }
    }

    #[tokio::test]
    async fn should_reject_missing_operation_in_resource_path() {
        // Arrange
        let handler = create_test_handler().await;
        let context = create_test_context("queue://test_realm/test_area/resource1", vec![]);

        // Act
        let result = handler.handle(context).await;

        // Assert
        match result {
            DomainResponse::Error(e) => assert!(e.contains("Missing operation")),
            _ => panic!("Expected error for missing operation"),
        }
    }

    #[test]
    fn should_parse_tlv_payload_with_minimal_data() {
        // Arrange
        let payload = vec![];

        // Act
        let parsed = QueueDomain::parse_tlv_payload(&payload);
        let message_id = parsed.message_id;
        let body = parsed.body;
        let lease_secs = parsed.lease_secs;
        let delivery_token = parsed.delivery_token;
        let ttl_secs = parsed.ttl_secs;
        let config = parsed.config;

        // Assert
        assert!(message_id.is_none());
        assert!(body.is_none());
        assert!(lease_secs.is_none());
        assert!(delivery_token.is_none());
        assert!(ttl_secs.is_none());
        assert!(config.is_none());
    }

    #[test]
    fn should_build_success_response() {
        // Arrange
        // No setup needed for success response

        // Act
        let response = QueueDomain::build_success_response();

        // Assert
        assert!(response.is_empty());
    }

    #[test]
    fn should_build_error_response() {
        // Arrange
        let error_msg = "Test error";

        // Act
        let response = QueueDomain::build_error_response(error_msg);

        // Assert
        assert!(!response.is_empty());
        assert_eq!(response[0], 0x41); // TAG_ERR_MSG
        assert_eq!(response[1] as usize, error_msg.len());
        assert_eq!(&response[2..], error_msg.as_bytes());
    }

    #[test]
    fn should_build_enqueue_response() {
        // Arrange
        let message_ids = vec!["msg1".to_string(), "msg2".to_string()];

        // Act
        let response = QueueDomain::build_enqueue_response(&message_ids);

        // Assert
        assert!(!response.is_empty());
        // Should contain TAG_ID for each message
        assert_eq!(response[0], 0x21); // TAG_ID
    }

    #[test]
    fn should_build_list_response() {
        // Arrange
        let queues = vec!["queue1".to_string(), "queue2".to_string()];

        // Act
        let response = QueueDomain::build_list_response(&queues);

        // Assert
        assert!(!response.is_empty());
        // Should contain TAG_ID for each queue
        assert_eq!(response[0], 0x21); // TAG_ID
    }

    #[test]
    fn should_build_reserve_response() {
        // Arrange
        let messages = vec![
            ("msg1".to_string(), b"body1".to_vec(), "token1".to_string()),
            ("msg2".to_string(), b"body2".to_vec(), "token2".to_string()),
        ];

        // Act
        let response = QueueDomain::build_reserve_response(&messages);

        // Assert
        assert!(!response.is_empty());
        // Response should contain TLV data for messages
    }
}
