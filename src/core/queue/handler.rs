// Queue domain handler - routes all queue:// operations

use super::encoding::{
    build_enqueue_response, build_error_response, build_list_response, build_reserve_response,
    build_success_response, parse_tlv_payload,
};
use super::service::QueueService;
use super::types::{QueueConfig, QueueStats};
use crate::core::domain::{Domain, DomainContext, DomainResponse};
use crate::storage::traits::KvStore;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Queue handler response types
#[derive(Debug)]
pub enum QueueResponse {
    SubscribeOk,
    UnsubscribeOk,
    ReceiveOk {
        messages: Vec<(String, Vec<u8>, String)>, // (id, body, delivery_token)
    },
    ExtendOk {
        extended_count: usize,
    },
    AckOk {
        acked_count: usize,
    },
    NackOk {
        nacked_count: usize,
    },
    RequeueOk {
        requeued_count: usize,
    },
    ListOk {
        queues: Vec<String>, // List of available queue routes
    },
    ConfigOk,
    Stats(QueueStats),
}

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
    fn parse_tlv_payload(
        payload: &[u8],
    ) -> (
        Option<String>,      // message_id
        Option<Vec<u8>>,     // body
        Option<u32>,         // lease_secs
        Option<String>,      // delivery_token
        Option<u64>,         // ttl_secs
        Option<QueueConfig>, // config
    ) {
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
    fn handle<'a>(
        &'a self,
        request: DomainContext,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = DomainResponse> + Send + 'a>> {
        Box::pin(async move {
            // Parse TLV payload
            let (message_id, body, lease_secs, delivery_token, ttl_secs, _config) =
                Self::parse_tlv_payload(&request.payload);

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
            let operation = match &request.route.operation {
                Some(o) => o.as_str(),
                None => return DomainResponse::Error("Missing operation in route".to_string()),
            };

            let service = self.service.read().await;

            match operation {
                "enqueue" => {
                    let message_body = body.unwrap_or_default();
                    let ttl = ttl_secs.unwrap_or(0);
                    let batch_size = 1; // Default to single message for now, could be extended

                    // TODO: Implement batch enqueue operation
                    // For now, return a placeholder response with single message ID
                    let message_ids = vec![format!(
                        "msg_{}",
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_nanos()
                    )];
                    let response = Self::build_enqueue_response(&message_ids);
                    DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(response))
                }
                "subscribe" => {
                    // TODO: Implement subscribe operation - register for message availability notifications
                    // For now, just return success
                    let response = Self::build_success_response();
                    DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(response))
                }
                "unsubscribe" => {
                    // TODO: Implement unsubscribe operation - remove message availability notifications
                    // For now, just return success
                    let response = Self::build_success_response();
                    DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(response))
                }
                "receive" => {
                    let lease_duration = lease_secs.unwrap_or(30);
                    let batch_size = 10; // Default batch size

                    // TODO: Implement receive batch operation using service.reserve_batch()
                    // For now, return empty batch
                    let messages = Vec::new();
                    let response = Self::build_reserve_response(&messages);
                    DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(response))
                }
                "extend" => {
                    let _msg_id = message_id.unwrap_or_default();
                    let _token = delivery_token.unwrap_or_default();
                    let _additional_secs = lease_secs.unwrap_or(30);

                    // TODO: Implement extend operation using service.extend_lease()
                    let response = Self::build_success_response();
                    DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(response))
                }
                "ack" => {
                    let _msg_id = message_id.unwrap_or_default();
                    let _token = delivery_token.unwrap_or_default();

                    // TODO: Implement ack operation using service.consume()
                    let response = Self::build_success_response();
                    DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(response))
                }
                "nack" => {
                    let _msg_id = message_id.unwrap_or_default();
                    let _token = delivery_token.unwrap_or_default();

                    // TODO: Implement nack operation using service.nack()
                    let response = Self::build_success_response();
                    DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(response))
                }
                "requeue" => {
                    let _msg_id = message_id.unwrap_or_default();
                    let _token = delivery_token.unwrap_or_default();

                    // TODO: Implement requeue operation using service.requeue()
                    let response = Self::build_success_response();
                    DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(response))
                }
                "get" => {
                    // Get config for specific queue
                    let route = format!("{}/{}/{}", realm, area, resource);
                    // TODO: Implement get config operation
                    let response = Self::build_success_response();
                    DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(response))
                }
                "list" => {
                    // Handle different list patterns:
                    // queue://realm/area/*/list -> list all queues in realm/area
                    // queue://realm/*/*/list -> list all queues in realm
                    if resource == "*" && area != "*" {
                        // List all queues in this realm/area scope
                        let queues = service
                            .list_queues_in_scope(realm, area)
                            .await
                            .unwrap_or_else(|_| Vec::new());
                        let response = Self::build_list_response(&queues);
                        DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(
                            response,
                        ))
                    } else if resource == "*" && area == "*" {
                        // List all queues in this realm (realm/*/*/list)
                        let queues = service
                            .list_queues_in_realm(realm)
                            .await
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
                "config" => {
                    // TODO: Implement config operation
                    let response = Self::build_success_response();
                    DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(response))
                }
                _ => {
                    let error_response = Self::build_error_response(&format!(
                        "Unknown queue operation: {}",
                        operation
                    ));
                    DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(
                        error_response,
                    ))
                }
            }
        })
    }

    fn cleanup_channel<'a>(
        &'a self,
        _rf: crate::routing::RouteFamilyId,
        _channel_id: u32,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            // Queue domain doesn't maintain per-channel state currently
            // This could be extended to clean up channel-specific leases if needed
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::domain::{DomainContext, DomainResponse};
    use crate::protocol::route::parse_route;
    use crate::storage::midge_adapter::create_memory_store;

    async fn create_test_handler() -> QueueDomain {
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

    #[tokio::test]
    async fn should_parse_resource_path_with_operation() {
        // Arrange
        let handler = create_test_handler().await;
        let context = create_test_context("queue://test_realm/test_area/resource1/list", vec![]);

        // Act
        let result = handler.handle(context).await;

        // Assert
        match result {
            DomainResponse::Frame(_) | DomainResponse::Ok => {
                // Should succeed
            }
            DomainResponse::Error(e) => panic!("Expected success, got error: {}", e),
        }
    }

    #[tokio::test]
    async fn should_handle_enqueue_operation() {
        // Arrange
        let handler = create_test_handler().await;
        let payload = vec![0x01, 0x04, b'b', b'o', b'd', b'y']; // TAG_BODY with "body"
        let context =
            create_test_context("queue://test_realm/test_area/resource1/enqueue", payload);

        // Act
        let result = handler.handle(context).await;

        // Assert
        match result {
            DomainResponse::Frame(_) | DomainResponse::Ok => {
                // Should succeed
            }
            DomainResponse::Error(e) => panic!("Expected success, got error: {}", e),
        }
    }

    #[tokio::test]
    async fn should_handle_receive_operation() {
        // Arrange
        let handler = create_test_handler().await;
        let context = create_test_context("queue://test_realm/test_area/resource1/receive", vec![]);

        // Act
        let result = handler.handle(context).await;

        // Assert
        match result {
            DomainResponse::Frame(_) | DomainResponse::Ok => {
                // Should succeed
            }
            DomainResponse::Error(e) => panic!("Expected success, got error: {}", e),
        }
    }

    #[tokio::test]
    async fn should_handle_list_operation_for_specific_queue() {
        // Arrange
        let handler = create_test_handler().await;
        let context = create_test_context("queue://test_realm/test_area/resource1/list", vec![]);

        // Act
        let result = handler.handle(context).await;

        // Assert
        match result {
            DomainResponse::Frame(_) | DomainResponse::Ok => {
                // Should succeed
            }
            DomainResponse::Error(e) => panic!("Expected success, got error: {}", e),
        }
    }

    #[tokio::test]
    async fn should_handle_list_operation_for_area_wildcard() {
        // Arrange
        let handler = create_test_handler().await;
        let context = create_test_context("queue://test_realm/test_area/*/list", vec![]);

        // Act
        let result = handler.handle(context).await;

        // Assert
        match result {
            DomainResponse::Frame(_) | DomainResponse::Ok => {
                // Should succeed
            }
            DomainResponse::Error(e) => panic!("Expected success, got error: {}", e),
        }
    }

    #[tokio::test]
    async fn should_handle_list_operation_for_realm_wildcard() {
        // Arrange
        let handler = create_test_handler().await;
        let context = create_test_context("queue://test_realm/*/*/list", vec![]);

        // Act
        let result = handler.handle(context).await;

        // Assert
        match result {
            DomainResponse::Frame(_) | DomainResponse::Ok => {
                // Should succeed
            }
            DomainResponse::Error(e) => panic!("Expected success, got error: {}", e),
        }
    }

    #[tokio::test]
    async fn should_handle_unknown_operation() {
        // Arrange
        let handler = create_test_handler().await;
        let context = create_test_context("queue://test_realm/test_area/resource1/unknown", vec![]);

        // Act
        let result = handler.handle(context).await;

        // Assert
        match result {
            DomainResponse::Frame(_) | DomainResponse::Ok => {
                // Should return error response for unknown operation
            }
            DomainResponse::Error(e) => panic!("Expected frame response with error, got: {}", e),
        }
    }

    #[tokio::test]
    async fn should_reject_missing_area() {
        // Arrange
        let handler = create_test_handler().await;
        let mut context =
            create_test_context("queue://test_realm/test_area/resource1/list", vec![]);
        context.route.area = None;

        // Act
        let result = handler.handle(context).await;

        // Assert
        match result {
            DomainResponse::Error(e) => assert!(e.contains("Missing area")),
            _ => panic!("Expected error for missing area"),
        }
    }

    #[tokio::test]
    async fn should_reject_missing_resource() {
        // Arrange
        let handler = create_test_handler().await;
        let mut context =
            create_test_context("queue://test_realm/test_area/resource1/list", vec![]);
        context.route.resource = None;

        // Act
        let result = handler.handle(context).await;

        // Assert
        match result {
            DomainResponse::Error(e) => assert!(e.contains("Missing resource")),
            _ => panic!("Expected error for missing resource"),
        }
    }

    #[tokio::test]
    async fn should_reject_invalid_resource_path_format() {
        // Arrange
        let handler = create_test_handler().await;
        let context =
            create_test_context("queue://test_realm/test_area/invalid/path/format", vec![]);

        // Act
        let result = handler.handle(context).await;

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
        let (message_id, body, lease_secs, delivery_token, ttl_secs, config) =
            QueueDomain::parse_tlv_payload(&payload);

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
