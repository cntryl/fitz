// Notice domain handler - routes all notice:// operations

use super::service::NoticeService;
use crate::core::domain::{Domain, DomainRequest, DomainResponse, SubSender};
use crate::protocol::tags::{
    TAG_BODY, TAG_ERR_MSG, TAG_ID, TAG_ROUTE, TAG_ROUTE_REPLY, TAG_SEQ, TAG_STREAM_END,
    TAG_SUBSCRIBE, TAG_UNSUBSCRIBE,
};
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct NoticeDomain {
    service: Arc<Mutex<NoticeService>>,
}

impl NoticeDomain {
    pub fn new() -> Self {
        Self {
            service: Arc::new(Mutex::new(NoticeService::new())),
        }
    }

    /// Get the shared notice service for use by other domains (e.g., control)
    pub fn get_service(&self) -> Arc<Mutex<NoticeService>> {
        Arc::clone(&self.service)
    }

    /// Subscribe to a route pattern (called by engine)
    pub async fn subscribe(
        &self,
        route_pattern: String,
        channel_id: u32,
        sender: SubSender,
    ) -> u64 {
        let mut service = self.service.lock().await;
        service.subscribe(route_pattern, channel_id, sender)
    }

    /// Unsubscribe by subscription ID (called by engine)
    pub async fn unsubscribe(&self, sub_id: u64) -> bool {
        let mut service = self.service.lock().await;
        service.unsubscribe(sub_id)
    }

    /// Cleanup all subscriptions for a channel (called by engine on disconnect)
    pub async fn cleanup_channel(&self, channel_id: u32) {
        let mut service = self.service.lock().await;
        service.cleanup_channel(channel_id)
    }

    /// Parse TLV-encoded payload to determine operation
    fn parse_operation(&self, payload: &[u8]) -> Result<NoticeOp, String> {
        let mut has_subscribe = false;
        let mut has_unsubscribe = false;
        let mut has_body = false;

        let mut offset = 0;
        while offset + 2 <= payload.len() {
            let tag = payload[offset];
            let length = payload[offset + 1] as usize;
            offset += 2;

            if offset + length > payload.len() {
                break;
            }

            match tag {
                TAG_SUBSCRIBE => has_subscribe = true,
                TAG_UNSUBSCRIBE => has_unsubscribe = true,
                TAG_BODY => has_body = true,
                _ => {}
            }

            offset += length;
        }

        if has_subscribe {
            Ok(NoticeOp::Subscribe)
        } else if has_unsubscribe {
            Ok(NoticeOp::Unsubscribe)
        } else if has_body {
            Ok(NoticeOp::Publish)
        } else {
            Err(
                "Unknown notice operation: no subscribe, unsubscribe, or body tag found"
                    .to_string(),
            )
        }
    }

    /// Extract TLV value by tag
    fn find_tlv<'a>(&self, payload: &'a [u8], tag: u8) -> Option<&'a [u8]> {
        let mut offset = 0;
        while offset + 2 <= payload.len() {
            let t = payload[offset];
            let length = payload[offset + 1] as usize;
            offset += 2;

            if offset + length > payload.len() {
                break;
            }

            if t == tag {
                return Some(&payload[offset..offset + length]);
            }

            offset += length;
        }
        None
    }

    /// Build TLV-encoded response
    fn build_tlv_response(&self, route: &str) -> Vec<u8> {
        let mut response = Vec::new();

        // TAG_ROUTE
        let route_bytes = route.as_bytes();
        response.push(TAG_ROUTE);
        response.push(route_bytes.len() as u8);
        response.extend_from_slice(route_bytes);

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
    fn handle<'a>(
        &'a self,
        request: DomainRequest,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = DomainResponse> + Send + 'a>> {
        Box::pin(async move {
            // Determine operation from TLV tags
            let operation = match self.parse_operation(&request.payload) {
                Ok(op) => op,
                Err(e) => {
                    let error_response = self.build_error_response(&e);
                    return DomainResponse::Frame(error_response);
                }
            };

            match operation {
                NoticeOp::Subscribe => {
                    // Subscribe operation - handled by engine via Subscribe command
                    // Domain just acknowledges
                    let response = self.build_tlv_response(&request.route_str);
                    DomainResponse::Frame(response)
                }

                NoticeOp::Unsubscribe => {
                    // Unsubscribe operation - handled by engine via Unsubscribe command
                    // Domain just acknowledges
                    let response = self.build_tlv_response(&request.route_str);
                    DomainResponse::Frame(response)
                }

                NoticeOp::Publish => {
                    // Publish operation - dispatch to subscribers
                    let body = match self.find_tlv(&request.payload, TAG_BODY) {
                        Some(b) => b,
                        None => {
                            let error_response =
                                self.build_error_response("Missing body in publish");
                            return DomainResponse::Frame(error_response);
                        }
                    };

                    let msg_id = self
                        .find_tlv(&request.payload, TAG_ID)
                        .and_then(|b| std::str::from_utf8(b).ok());
                    let reply_to = self
                        .find_tlv(&request.payload, TAG_ROUTE_REPLY)
                        .and_then(|b| std::str::from_utf8(b).ok());
                    let seq = self.find_tlv(&request.payload, TAG_SEQ).and_then(|b| {
                        if b.len() == 4 {
                            Some(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
                        } else {
                            None
                        }
                    });
                    let end = self.find_tlv(&request.payload, TAG_STREAM_END).is_some();

                    // Dispatch to subscribers
                    let mut service = self.service.lock().await;
                    let (_delivered, _failed) =
                        service.publish(&request.route_str, msg_id, body, reply_to, seq, end);

                    // Return success response with the body echoed back
                    let mut response = Vec::new();
                    response.push(TAG_ROUTE);
                    let route_bytes = request.route_str.as_bytes();
                    response.push(route_bytes.len() as u8);
                    response.extend_from_slice(route_bytes);

                    if let Some(id) = msg_id {
                        response.push(TAG_ID);
                        response.push(id.len() as u8);
                        response.extend_from_slice(id.as_bytes());
                    }

                    response.push(TAG_BODY);
                    response.push(body.len() as u8);
                    response.extend_from_slice(body);

                    DomainResponse::Frame(response)
                }
            }
        })
    }

    fn schemes(&self) -> &[&str] {
        &["notice"]
    }

    /// Subscribe to a route pattern
    fn subscribe<'a>(
        &'a self,
        route: String,
        channel_id: u32,
        sender: crate::core::domain::SubSender,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<u64, String>> + Send + 'a>> {
        Box::pin(async move {
            let mut service = self.service.lock().await;
            Ok(service.subscribe(route, channel_id, sender))
        })
    }

    /// Unsubscribe by subscription ID
    fn unsubscribe<'a>(
        &'a self,
        sub_id: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send + 'a>> {
        Box::pin(async move {
            let mut service = self.service.lock().await;
            service.unsubscribe(sub_id)
        })
    }

    /// Cleanup all subscriptions for a channel
    fn cleanup_channel<'a>(
        &'a self,
        channel_id: u32,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            let mut service = self.service.lock().await;
            service.cleanup_channel(channel_id)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::route::Route;

    #[test]
    fn should_parse_subscribe_operation() {
        // Arrange
        let domain = NoticeDomain::new();
        let payload = vec![TAG_SUBSCRIBE, 0]; // empty value

        // Act
        let result = domain.parse_operation(&payload);

        // Assert
        assert!(result.is_ok());
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
        let result = domain.parse_operation(&payload);

        // Assert
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn should_handle_subscribe_request() {
        // Arrange
        let domain = NoticeDomain::new();
        let payload = vec![TAG_SUBSCRIBE, 0];

        let request = DomainRequest {
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
    async fn should_handle_publish_request() {
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

        let request = DomainRequest {
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
        };

        // Act
        let response = domain.handle(request).await;

        // Assert
        match response {
            DomainResponse::Frame(frame) => {
                assert!(!frame.is_empty());
            }
            _ => panic!("Expected Frame response"),
        }
    }
}
