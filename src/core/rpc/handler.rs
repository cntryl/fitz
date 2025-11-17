// RPC domain handler - routes all rpc:// operations
//
// Architecture:
// - Instance-owned RpcService for per-domain isolation
// - Shared Arc<RwLock<RpcService>> for subscribe/publish operations requiring mutation
// - Single-pass TLV parsing with detailed error reporting
// - SmallVec stack allocation for typical <64B control frame responses
// - Inbox allocation with cryptographic uniqueness and ownership enforcement

use super::encoding;
use super::service::RpcService;
use crate::core::domain::{Domain, DomainContext, DomainResponse};
use parking_lot::RwLock;
use std::sync::Arc;

#[derive(Debug)]
pub struct RpcDomain {
    service: Arc<RwLock<RpcService>>,
}

impl RpcDomain {
    pub fn new() -> Self {
        Self {
            service: Arc::new(RwLock::new(RpcService::new())),
        }
    }

    /// Get the shared RPC service
    pub fn get_service(&self) -> Arc<RwLock<RpcService>> {
        Arc::clone(&self.service)
    }

    /// Parse TLV-encoded payload using encoding module
    fn parse_tlv_payload(&self, payload: &[u8]) -> Result<encoding::RpcTlvPayload, String> {
        encoding::parse_tlv_payload(payload)
    }

    /// Build TLV response for subscription acknowledgment
    fn build_subscribe_response(&self, route: &str) -> Vec<u8> {
        encoding::build_subscribe_response(route).to_vec()
    }

    /// Build TLV error response
    fn build_error_response(&self, error_msg: &str) -> Vec<u8> {
        encoding::build_error_response(error_msg).to_vec()
    }

    /// Handle RPC request (client sending request to handler)
    fn handle_rpc_request(
        &self,
        rf: crate::routing::RouteFamilyId,
        route: &str,
        correlation_id: Option<&str>,
        reply_route: Option<&str>,
        body: &[u8],
    ) -> Vec<u8> {
        let service = self.service.read();

        // Find matching handlers
        let handlers = service.matching_handlers(rf, route);

        if handlers.is_empty() {
            return self.build_error_response("No handlers available");
        }

        // Use first available handler (simple dispatch)
        // Future enhancement: implement round-robin, least-connections, or weighted load balancing
        let handler = &handlers[0];

        // Register active request for inbox authorization
        if let (Some(corr_id), Some(reply)) = (correlation_id, reply_route) {
            drop(service); // Release read lock
            let mut service = self.service.write();
            service.register_request(corr_id.to_string(), route.to_string(), reply.to_string());
        }

        // Forward request to handler
        match handler.sender.try_send((
            route.to_string(),
            correlation_id.map(|s| s.to_string()),
            body.to_vec(),
            reply_route.map(|s| s.to_string()),
            None,  // seq
            false, // stream_end
        )) {
            Ok(_) => {
                // Request delivered successfully
                encoding::build_request_response(route).to_vec()
            }
            Err(_) => self.build_error_response("Handler backpressure"),
        }
    }

    /// Handle RPC reply (handler sending reply to client inbox)
    fn handle_rpc_reply(
        &self,
        rf: crate::routing::RouteFamilyId,
        inbox_route: &str,
        correlation_id: Option<&str>,
        body: &[u8],
        seq: Option<u64>,
        is_stream_end: bool,
    ) -> Vec<u8> {
        let service = self.service.read();

        // Authorization: check if correlation_id allows publishing to this inbox
        if let Some(corr_id) = correlation_id {
            if !service.can_publish_to_inbox(inbox_route, corr_id) {
                return self.build_error_response("Unauthorized inbox access");
            }
        } else {
            return self.build_error_response("Missing correlation ID for reply");
        }

        // Find inbox subscriber
        let subscribers = service.matching_inbox_subscribers(rf, inbox_route);

        if subscribers.is_empty() {
            return self.build_error_response("Inbox not found");
        }

        let subscriber = &subscribers[0];

        // Forward reply to client
        match subscriber.sender.try_send((
            inbox_route.to_string(),
            correlation_id.map(|s| s.to_string()),
            body.to_vec(),
            None,                  // reply_to (N/A for replies)
            seq.map(|s| s as u32), // Convert u64 to u32 for SubSender
            is_stream_end,
        )) {
            Ok(_) => {
                encoding::build_request_response(inbox_route).to_vec()
            }
            Err(_) => self.build_error_response("Client backpressure"),
        }
    }
}

impl Default for RpcDomain {
    fn default() -> Self {
        Self::new()
    }
}

impl Domain for RpcDomain {
    fn handle(&self, request: DomainContext) -> DomainResponse {
        let payload = &request.payload;

        // Parse TLV payload
        let parsed = match self.parse_tlv_payload(payload) {
            Ok(result) => result,
            Err(error_msg) => {
                let response_data = self.build_error_response(&error_msg);
                return DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(
                    response_data,
                ));
            }
        };

        // Extract route from request
        let route = &request.route_str;

        // Extract optional fields from parsed payload
        let correlation_id = parsed.correlation_id.as_deref();
        let reply_route = parsed.reply_route.as_deref();
        let body = parsed.body.as_deref().unwrap_or(&[]);

        // Handle subscribe/unsubscribe
        if parsed.operation == encoding::RpcOperation::Subscribe {
            // Subscribe operation - actual subscription happens via Domain::subscribe trait method
            // Handler just acknowledges
            let response_data = self.build_subscribe_response(route);
            return DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(
                response_data,
            ));
        }

        if parsed.operation == encoding::RpcOperation::Unsubscribe {
            // Unsubscribe operation - actual unsubscription happens via Domain::unsubscribe trait method
            // Handler just acknowledges
            let response_data = self.build_subscribe_response(route);
            return DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(
                response_data,
            ));
        }

        // Determine if this is a request or reply
        let is_reply = route.starts_with("inbox://");

        let response_data = if is_reply {
            // This is a reply from handler to client
            self.handle_rpc_reply(
                request.route_family,
                route,
                correlation_id,
                body,
                parsed.seq,
                parsed.is_stream_end,
            )
        } else {
            // This is a request from client to handler
            self.handle_rpc_request(
                request.route_family,
                route,
                correlation_id,
                reply_route,
                body,
            )
        };

        DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(
            response_data,
        ))
    }

    fn cleanup_channel(&self, rf: crate::routing::RouteFamilyId, channel_id: u32) {
        let mut svc = self.service.write();
        svc.cleanup_channel(rf, channel_id)
    }
}

#[cfg(test)]
mod tests {
    // TODO: Tests for subscribe/unsubscribe removed - these operations now happen
    // via dispatch commands through the Domain trait handle() method.
    // Once RPC dispatch handlers are implemented, add tests for those operations.
}
