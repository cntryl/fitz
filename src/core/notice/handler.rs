//! Notice domain handler - protocol adapter for notice:// operations
//!
//! ## Handler Responsibilities
//! - Parse TLV payloads for subscribe/unsubscribe/publish operations
//! - Route operations to NoticeService
//! - Build TLV responses using encoding module (SmallVec-optimized)
//! - Single-pass parsing for efficiency
//!
//! ## Architecture
//! - Handler (this) = Protocol adapter (TLV ↔ service calls)
//! - Service = Business logic (subscription management, fanout coordination)
//! - Pure synchronous operation (no async, no I/O)

use super::encoding;
use super::service::NoticeService;
use crate::core::domain::{Domain, DomainContext, DomainResponse};
use crate::protocol::tags::{
    TAG_BODY, TAG_ID, TAG_NO_ACK, TAG_SUBSCRIBE, TAG_UNSUBSCRIBE,
};
use parking_lot::RwLock;
use std::sync::Arc;

#[derive(Debug)]
pub struct NoticeDomain {
    service: Arc<RwLock<NoticeService>>,
}

impl NoticeDomain {
    pub fn new() -> Self {
        Self {
            service: Arc::new(RwLock::new(NoticeService::new())),
        }
    }

    /// Get the shared notice service for use by other domains (e.g., control)
    pub fn get_service(&self) -> Arc<RwLock<NoticeService>> {
        Arc::clone(&self.service)
    }

    /// Parse TLV-encoded payload and extract all relevant fields in one pass
    /// Returns descriptive errors on malformed input instead of silently dropping bytes
    fn parse_tlv_single_pass(&self, payload: &[u8]) -> Result<TlvParseResult, String> {
        let mut has_subscribe = false;
        let mut has_unsubscribe = false;
        let mut body_range: Option<(usize, usize)> = None;
        let mut id_range: Option<(usize, usize)> = None;
        let mut no_ack = false;

        let mut offset = 0;
        while offset + 2 <= payload.len() {
            let tag = payload[offset];
            let length = payload[offset + 1] as usize;
            let value_start = offset + 2;

            // Validate that we have enough bytes for the advertised length
            if value_start + length > payload.len() {
                return Err(format!(
                    "Malformed TLV at offset {}: tag {} claims {} bytes but only {} available",
                    offset,
                    tag,
                    length,
                    payload.len() - value_start
                ));
            }

            match tag {
                TAG_SUBSCRIBE => has_subscribe = true,
                TAG_UNSUBSCRIBE => has_unsubscribe = true,
                TAG_BODY => body_range = Some((value_start, length)),
                TAG_ID => id_range = Some((value_start, length)),
                TAG_NO_ACK => no_ack = true,
                _ => {
                    // Unknown tag - skip it but don't error (forward compatibility)
                }
            }

            offset = value_start + length;
        }

        // Check for trailing garbage bytes
        if offset != payload.len() {
            return Err(format!(
                "TLV parse incomplete: {} trailing bytes after offset {}",
                payload.len() - offset,
                offset
            ));
        }

        let operation = if has_subscribe {
            NoticeOp::Subscribe
        } else if has_unsubscribe {
            NoticeOp::Unsubscribe
        } else if body_range.is_some() {
            NoticeOp::Publish
        } else {
            return Err(
                "Unknown notice operation: no subscribe, unsubscribe, or body tag found"
                    .to_string(),
            );
        };

        Ok(TlvParseResult {
            operation,
            body_range,
            id_range,
            no_ack,
        })
    }
}

struct TlvParseResult {
    operation: NoticeOp,
    body_range: Option<(usize, usize)>,
    id_range: Option<(usize, usize)>,
    no_ack: bool,
}

impl Default for NoticeDomain {
    fn default() -> Self {
        Self::new()
    }
}

enum NoticeOp {
    Subscribe,
    Unsubscribe,
    Publish,
}

impl Domain for NoticeDomain {
    fn handle(&self, request: DomainContext) -> DomainResponse {
        // Parse TLV payload in a single pass (optimization: avoid double-scan)
        let parse_result = match self.parse_tlv_single_pass(&request.payload) {
            Ok(result) => result,
            Err(e) => {
                let error_response = encoding::build_error_response(&e);
                return DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(
                    error_response.to_vec(),
                ));
            }
        };

        match parse_result.operation {
            NoticeOp::Subscribe => {
                // Validate subscription route format
                if let Err(e) =
                    crate::protocol::route::validate_notice_subscription(&request.route_str)
                {
                    let error_response = encoding::build_error_response(e);
                    return DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(
                        error_response.to_vec(),
                    ));
                }

                // Actually subscribe via service
                let mut service = self.service.write();
                let _sub_id = service.subscribe(
                    request.route_family,
                    request.route_str.clone(),
                    request.channel_id,
                );

                let response = encoding::build_ack_response(&request.route_str);
                DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(
                    response.to_vec(),
                ))
            }

            NoticeOp::Unsubscribe => {
                // For unsubscribe, we need subscription ID which isn't in the frame
                // Alternative: cleanup all subscriptions for this channel_id on this route
                // For now, cleanup entire channel (matches typical client disconnect behavior)
                let mut service = self.service.write();
                service.cleanup_channel(request.route_family, request.channel_id);

                let response = encoding::build_ack_response(&request.route_str);
                DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(
                    response.to_vec(),
                ))
            }

            NoticeOp::Publish => {
                // Validate publish route format
                if let Err(e) = crate::protocol::route::validate_notice_publish(&request.route) {
                    let error_response = encoding::build_error_response(e);
                    return DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(
                        error_response.to_vec(),
                    ));
                }

                // Extract body from parsed range (avoid second scan)
                let body = match parse_result.body_range {
                    Some((start, len)) => &request.payload[start..start + len],
                    None => {
                        let error_response = encoding::build_error_response("Missing body in publish");
                        return DomainResponse::Frame(
                            crate::protocol::frame::PooledFrame::from_vec(error_response.to_vec()),
                        );
                    }
                };

                // Extract msg_id from parsed range (avoid second scan)
                let msg_id = parse_result.id_range.and_then(|(start, len)| {
                    std::str::from_utf8(&request.payload[start..start + len]).ok()
                });

                // Dispatch to subscribers - get fanout list
                let service = self.service.read();
                let result =
                    service.publish(request.route_family, &request.route_str, msg_id, body);

                // Build notification frame using encoding module (always needed)
                let notification_frame =
                    encoding::build_notification_frame(&request.route_str, msg_id, body);

                // Build ACK frame with subscriber count unless TAG_NO_ACK is present OR no subscribers
                // Optimization: Skip ACK construction when no one is listening
                let ack_frame_opt = if parse_result.no_ack || result.subscribers.is_empty() {
                    None
                } else {
                    encoding::build_ack_frame_with_count(
                        &request.route_str,
                        msg_id,
                        result.subscribers.len() as u32,
                    )
                };

                // Return fanout delivery instruction
                DomainResponse::NoticeDelivery {
                    subscribers: result.subscribers,
                    notification_frame,
                    ack_frame: ack_frame_opt,
                }
            }
        }
    }

    /// Cleanup all subscriptions for a channel
    fn cleanup_channel(&self, rf: crate::routing::RouteFamilyId, channel_id: u32) {
        let mut service = self.service.write();
        service.cleanup_channel(rf, channel_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::route::Route;
    use crate::protocol::tags::TAG_ERR_MSG;

    #[test]
    fn should_parse_subscribe_operation() {
        // Arrange
        let domain = NoticeDomain::new();
        let payload = vec![TAG_SUBSCRIBE, 0]; // empty value

        // Act
        let result = domain.parse_tlv_single_pass(&payload);

        // Assert
        assert!(result.is_ok());
        if let Ok(parsed) = result {
            assert!(matches!(parsed.operation, NoticeOp::Subscribe));
        }
    }

    #[test]
    fn should_parse_publish_operation() {
        // Arrange
        let domain = NoticeDomain::new();
        let mut payload = Vec::new();
        payload.push(TAG_BODY);
        payload.push(5);
        payload.extend_from_slice(b"hello");

        // Act
        let result = domain.parse_tlv_single_pass(&payload);

        // Assert
        assert!(result.is_ok());
        if let Ok(parsed) = result {
            assert!(matches!(parsed.operation, NoticeOp::Publish));
            assert!(parsed.body_range.is_some());
        }
    }

    #[test]
    fn should_handle_subscribe_request() {
        // Arrange
        let domain = NoticeDomain::new();
        let payload = vec![TAG_SUBSCRIBE, 0];

        let request = DomainContext {
            route: Route {
                scheme: crate::protocol::route::Scheme::Notice,
                realm: None,
                area: None,
                resource: Some("test".to_string()),
                operation: None,
                raw: "notice://test".to_string(),
            },
            route_str: "notice://test".to_string(),
            payload,
            channel_id: 1,
            route_family: 0, // test uses default route family
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
    fn should_handle_publish_request_with_no_subscribers() {
        // Arrange
        let domain = NoticeDomain::new();
        let mut payload = Vec::new();
        payload.push(TAG_ID);
        let id = b"msg-1";
        payload.push(id.len() as u8);
        payload.extend_from_slice(id);
        payload.push(TAG_BODY);
        let body = b"hello world";
        payload.push(body.len() as u8);
        payload.extend_from_slice(body);

        let request = DomainContext {
            route: Route {
                scheme: crate::protocol::route::Scheme::Notice,
                realm: Some("realm1".to_string()),
                area: Some("area1".to_string()),
                resource: Some("resource1".to_string()),
                operation: Some("alerts".to_string()),
                raw: "notice://realm1/area1/resource1/alerts".to_string(),
            },
            route_str: "notice://realm1/area1/resource1/alerts".to_string(),
            payload,
            channel_id: 1,
            route_family: 0, // test uses default route family
        };

        // Act
        let response = domain.handle(request);

        // Assert
        match response {
            DomainResponse::Frame(frame) => {
                assert!(!frame.as_ref().is_empty());
            }
            DomainResponse::NoticeDelivery { ack_frame, subscribers, .. } => {
                // Optimization: no subscribers = no ACK (lazy ACK)
                assert!(subscribers.is_empty());
                assert!(ack_frame.is_none());
            }
            _ => panic!("Expected Frame or NoticeDelivery response"),
        }
    }

    #[test]
    fn should_handle_publish_request_with_subscribers() {
        // Arrange
        let domain = NoticeDomain::new();
        let service = domain.get_service();
        
        // Subscribe first so we have subscribers
        {
            let mut svc = service.write();
            svc.subscribe(0, "notice://realm1/area1/resource1/alerts".to_string(), 99);
        }

        let mut payload = Vec::new();
        payload.push(TAG_ID);
        let id = b"msg-1";
        payload.push(id.len() as u8);
        payload.extend_from_slice(id);
        payload.push(TAG_BODY);
        let body = b"hello world";
        payload.push(body.len() as u8);
        payload.extend_from_slice(body);

        let request = DomainContext {
            route: Route {
                scheme: crate::protocol::route::Scheme::Notice,
                realm: Some("realm1".to_string()),
                area: Some("area1".to_string()),
                resource: Some("resource1".to_string()),
                operation: Some("alerts".to_string()),
                raw: "notice://realm1/area1/resource1/alerts".to_string(),
            },
            route_str: "notice://realm1/area1/resource1/alerts".to_string(),
            payload,
            channel_id: 1,
            route_family: 0, // test uses default route family
        };

        // Act
        let response = domain.handle(request);

        // Assert
        match response {
            DomainResponse::NoticeDelivery { ack_frame, subscribers, .. } => {
                // With subscribers: ACK should be present
                assert!(!subscribers.is_empty());
                match ack_frame {
                    Some(frame) => assert!(!frame.as_ref().is_empty()),
                    None => panic!("Expected ACK frame when subscribers exist"),
                }
            }
            _ => panic!("Expected NoticeDelivery response"),
        }
    }

    #[test]
    fn should_reject_publish_without_complete_route() {
        // Arrange
        let domain = NoticeDomain::new();
        let mut payload = Vec::new();
        payload.push(TAG_BODY);
        let body = b"hello";
        payload.push(body.len() as u8);
        payload.extend_from_slice(body);

        let request = DomainContext {
            route: Route {
                scheme: crate::protocol::route::Scheme::Notice,
                realm: Some("realm1".to_string()),
                area: Some("area1".to_string()),
                resource: Some("resource1".to_string()),
                operation: None, // Missing operation - should fail
                raw: "notice://realm1/area1/resource1".to_string(),
            },
            route_str: "notice://realm1/area1/resource1".to_string(),
            payload,
            channel_id: 1,
            route_family: 0, // test uses default route family
        };

        // Act
        let response = domain.handle(request);

        // Assert
        match response {
            DomainResponse::Frame(frame) => {
                // Should contain error message
                let content = frame.as_ref();
                assert!(!content.is_empty());
                // Check for TAG_ERR_MSG
                assert!(content.contains(&TAG_ERR_MSG));
            }
            _ => panic!("Expected Frame response with error"),
        }
    }

    #[test]
    fn should_reject_subscription_without_realm() {
        // Arrange
        let domain = NoticeDomain::new();
        let payload = vec![TAG_SUBSCRIBE, 0];

        let request = DomainContext {
            route: Route {
                scheme: crate::protocol::route::Scheme::Notice,
                realm: None, // Missing realm - should fail
                area: None,
                resource: None,
                operation: None,
                raw: "notice://*".to_string(),
            },
            route_str: "notice://*".to_string(),
            payload,
            channel_id: 1,
            route_family: 0, // test uses default route family
        };

        // Act
        let response = domain.handle(request);

        // Assert
        match response {
            DomainResponse::Frame(frame) => {
                // Should contain error message
                let content = frame.as_ref();
                assert!(!content.is_empty());
                // Check for TAG_ERR_MSG
                assert!(content.contains(&TAG_ERR_MSG));
            }
            _ => panic!("Expected Frame response with error"),
        }
    }

    #[test]
    fn should_accept_valid_subscription_with_wildcard() {
        // Arrange
        let domain = NoticeDomain::new();
        let payload = vec![TAG_SUBSCRIBE, 0];

        let request = DomainContext {
            route: Route {
                scheme: crate::protocol::route::Scheme::Notice,
                realm: Some("realm1".to_string()),
                area: None,
                resource: None,
                operation: None,
                raw: "notice://realm1/*".to_string(),
            },
            route_str: "notice://realm1/*".to_string(),
            payload,
            channel_id: 1,
            route_family: 0, // test uses default route family
        };

        // Act
        let response = domain.handle(request);

        // Assert
        match response {
            DomainResponse::Frame(frame) => {
                // Should not contain error
                let content = frame.as_ref();
                assert!(!content.contains(&TAG_ERR_MSG));
            }
            _ => panic!("Expected Frame response"),
        }
    }

    #[test]
    fn should_publish_with_no_ack_flag() {
        // Arrange
        let domain = NoticeDomain::new();
        let mut payload = Vec::new();
        payload.push(TAG_NO_ACK);
        payload.push(0);
        payload.push(TAG_BODY);
        let body = b"hello world";
        payload.push(body.len() as u8);
        payload.extend_from_slice(body);

        let request = DomainContext {
            route: Route {
                scheme: crate::protocol::route::Scheme::Notice,
                realm: Some("realm1".to_string()),
                area: Some("area1".to_string()),
                resource: Some("resource1".to_string()),
                operation: Some("alerts".to_string()),
                raw: "notice://realm1/area1/resource1/alerts".to_string(),
            },
            route_str: "notice://realm1/area1/resource1/alerts".to_string(),
            payload,
            channel_id: 1,
            route_family: 0,
        };

        // Act
        let response = domain.handle(request);

        // Assert
        match response {
            DomainResponse::NoticeDelivery { ack_frame, .. } => {
                assert!(ack_frame.is_none());
            }
            _ => panic!("Expected NoticeDelivery response"),
        }
    }
}
