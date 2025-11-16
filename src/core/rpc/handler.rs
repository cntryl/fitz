// RPC domain handler - routes all rpc:// operations
//
// Architecture:
// - Instance-owned RpcService for per-domain isolation
// - Shared Arc<RwLock<RpcService>> for subscribe/publish operations requiring mutation
// - Single-pass TLV parsing with detailed error reporting
// - SmallVec stack allocation for typical <64B control frame responses
// - Inbox allocation with cryptographic uniqueness and ownership enforcement

use super::service::RpcService;
use crate::core::domain::{Domain, DomainContext, DomainResponse};
use crate::protocol::tags::{
    TAG_BODY, TAG_ERR_MSG, TAG_ID, TAG_ROUTE, TAG_ROUTE_REPLY, TAG_SEQ, TAG_STREAM_END,
    TAG_SUBSCRIBE, TAG_UNSUBSCRIBE,
};
use smallvec::SmallVec;
use std::sync::Arc;
use parking_lot::RwLock;

/// Response buffer optimized for typical RPC frames (<64 bytes for control messages)
/// Uses stack allocation to avoid heap overhead for small messages
type ResponseBuf = SmallVec<[u8; 64]>;

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

    /// Parse TLV-encoded payload and extract all relevant fields in one pass
    /// Returns descriptive errors on malformed input
    fn parse_tlv_single_pass(&self, payload: &[u8]) -> Result<TlvParseResult, String> {
        let mut has_subscribe = false;
        let mut has_unsubscribe = false;
        let mut body_range: Option<(usize, usize)> = None;
        let mut id_range: Option<(usize, usize)> = None;
        let mut reply_route_range: Option<(usize, usize)> = None;
        let mut seq_value: Option<u64> = None;
        let mut has_stream_end = false;

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
                TAG_ROUTE_REPLY => reply_route_range = Some((value_start, length)),
                TAG_SEQ => {
                    if length == 8 {
                        let mut bytes = [0u8; 8];
                        bytes.copy_from_slice(&payload[value_start..value_start + 8]);
                        seq_value = Some(u64::from_be_bytes(bytes));
                    }
                }
                TAG_STREAM_END => has_stream_end = true,
                _ => {
                    // Unknown tag - skip it (forward compatibility)
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

        Ok(TlvParseResult {
            has_subscribe,
            has_unsubscribe,
            body_range,
            id_range,
            reply_route_range,
            seq_value,
            has_stream_end,
        })
    }

    /// Build TLV response for subscription acknowledgment
    fn build_subscribe_response(&self, route: &str) -> ResponseBuf {
        let mut response = ResponseBuf::new();

        // TAG_ROUTE + length + route
        response.push(TAG_ROUTE);
        response.push(route.len() as u8);
        response.extend_from_slice(route.as_bytes());

        response
    }

    /// Build TLV error response
    fn build_error_response(&self, error_msg: &str) -> ResponseBuf {
        let mut response = ResponseBuf::new();

        // TAG_ERR_MSG + length + message
        response.push(TAG_ERR_MSG);
        response.push(error_msg.len() as u8);
        response.extend_from_slice(error_msg.as_bytes());

        response
    }

    /// Handle RPC request (client sending request to handler)
    fn handle_rpc_request(
        &self,
        rf: crate::routing::RouteFamilyId,
        route: &str,
        correlation_id: Option<&str>,
        reply_route: Option<&str>,
        body: &[u8],
    ) -> ResponseBuf {
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
                let mut response = ResponseBuf::new();
                response.push(TAG_ROUTE);
                response.push(route.len() as u8);
                response.extend_from_slice(route.as_bytes());
                response
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
    ) -> ResponseBuf {
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
                let mut response = ResponseBuf::new();
                response.push(TAG_ROUTE);
                response.push(inbox_route.len() as u8);
                response.extend_from_slice(inbox_route.as_bytes());
                response
            }
            Err(_) => self.build_error_response("Client backpressure"),
        }
    }
}

/// Result of TLV parsing
struct TlvParseResult {
    has_subscribe: bool,
    has_unsubscribe: bool,
    body_range: Option<(usize, usize)>,
    id_range: Option<(usize, usize)>,
    reply_route_range: Option<(usize, usize)>,
    seq_value: Option<u64>,
    has_stream_end: bool,
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
        let parse_result = match self.parse_tlv_single_pass(payload) {
            Ok(result) => result,
            Err(error_msg) => {
                let response_data = self.build_error_response(&error_msg);
                return DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(
                    response_data.to_vec(),
                ));
            }
        };

        // Extract route from request
        let route = &request.route_str;

        // Extract optional fields
        let correlation_id = parse_result
            .id_range
            .and_then(|(start, len)| std::str::from_utf8(&payload[start..start + len]).ok());

        let reply_route = parse_result
            .reply_route_range
            .and_then(|(start, len)| std::str::from_utf8(&payload[start..start + len]).ok());

        let body = parse_result
            .body_range
            .map(|(start, len)| &payload[start..start + len])
            .unwrap_or(&[]);

        // Handle subscribe/unsubscribe
        if parse_result.has_subscribe {
            // Subscribe operation - actual subscription happens via Domain::subscribe trait method
            // Handler just acknowledges
            let response_data = self.build_subscribe_response(route);
            return DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(
                response_data.to_vec(),
            ));
        }

        if parse_result.has_unsubscribe {
            // Unsubscribe operation - actual unsubscription happens via Domain::unsubscribe trait method
            // Handler just acknowledges
            let response_data = self.build_subscribe_response(route);
            return DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(
                response_data.to_vec(),
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
                parse_result.seq_value,
                parse_result.has_stream_end,
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
            response_data.to_vec(),
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
