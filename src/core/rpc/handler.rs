//! RPC domain handler - protocol adapter for rpc:// operations
//!
//! ## Handler Responsibilities
//! - Parse TLV-encoded RPC payloads
//! - Route requests/replies via RpcService
//! - Build TLV responses using encoding module
//! - Register correlation IDs for inbox authorization
//!
//! ## Architecture
//! - Handler (this) = Protocol adapter (TLV ↔ service calls)
//! - Service = Business logic (routing, inbox management, authorization)
//! - Pure synchronous operation (no async, no I/O)

use super::encoding;
use super::service::RpcService;
use crate::core::domain::{Domain, DomainContext, DomainResponse};
use crate::routing::GlobalInternTable;
use parking_lot::RwLock;
use std::sync::Arc;

pub struct RpcDomain {
    service: Arc<RwLock<RpcService>>,
}

impl std::fmt::Debug for RpcDomain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RpcDomain").finish()
    }
}

impl RpcDomain {
    pub fn new(interner: Arc<GlobalInternTable>) -> Self {
        Self {
            service: Arc::new(RwLock::new(RpcService::new(interner))),
        }
    }

    /// Get the shared RPC service
    pub fn get_service(&self) -> Arc<RwLock<RpcService>> {
        Arc::clone(&self.service)
    }

    /// Handle RPC request (client sending request to handler)
    /// Returns routing result - transport layer performs actual delivery
    fn handle_rpc_request(
        &self,
        rf: crate::routing::RouteFamilyId,
        route: &str,
        correlation_id: Option<&str>,
        reply_route: Option<&str>,
        body: &[u8],
    ) -> super::service::RpcDeliveryResult {
        // Register active request for inbox authorization
        if let (Some(corr_id), Some(reply)) = (correlation_id, reply_route) {
            let mut service = self.service.write();
            service.register_request(corr_id.to_string(), route.to_string(), reply.to_string());
        }

        // Route request to handler (pure sync, no I/O)
        let service = self.service.read();
        service.route_request(rf, route, correlation_id, reply_route, body)
    }

    /// Handle RPC reply (handler sending reply to client inbox)
    /// Returns routing result - transport layer performs actual delivery
    fn handle_rpc_reply(
        &self,
        rf: crate::routing::RouteFamilyId,
        inbox_route: &str,
        correlation_id: Option<&str>,
        body: &[u8],
        seq: Option<u64>,
        is_stream_end: bool,
    ) -> super::service::RpcDeliveryResult {
        // Route reply to inbox (pure sync, no I/O)
        let service = self.service.read();
        service.route_reply(rf, inbox_route, correlation_id, body, seq, is_stream_end)
    }
}

impl Default for RpcDomain {
    fn default() -> Self {
        Self::new(Arc::new(GlobalInternTable::new()))
    }
}

impl Domain for RpcDomain {
    fn handle(&self, request: DomainContext) -> DomainResponse {
        let payload = &request.payload;

        // Parse TLV payload
        let parsed = match encoding::parse_tlv_payload(payload) {
            Ok(result) => result,
            Err(error_msg) => {
                let response_data = encoding::build_error_response(&error_msg).to_vec();
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
            let response_data = encoding::build_subscribe_response(route).to_vec();
            return DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(
                response_data,
            ));
        }

        if parsed.operation == encoding::RpcOperation::Unsubscribe {
            // Unsubscribe operation - actual unsubscription happens via Domain::unsubscribe trait method
            // Handler just acknowledges
            let response_data = encoding::build_subscribe_response(route).to_vec();
            return DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(
                response_data,
            ));
        }

        // Determine if this is a request or reply
        let is_reply = route.starts_with("inbox://");

        let delivery_result = if is_reply {
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

        // Convert delivery result to domain response
        match delivery_result {
            super::service::RpcDeliveryResult {
                target: Some((channel_id, message)),
                error: None,
            } => {
                // Success - return delivery instruction with ack
                let ack_frame = crate::protocol::frame::PooledFrame::from_vec(
                    encoding::build_request_response(route).to_vec(),
                );
                DomainResponse::RpcDelivery {
                    target_channel_id: channel_id,
                    message,
                    ack_frame,
                }
            }
            super::service::RpcDeliveryResult {
                error: Some(err_msg),
                ..
            } => {
                // Error - return error frame
                let error_frame = crate::protocol::frame::PooledFrame::from_vec(
                    encoding::build_error_response(&err_msg).to_vec(),
                );
                DomainResponse::Frame(error_frame)
            }
            _ => {
                // Shouldn't happen, but handle gracefully
                let error_frame = crate::protocol::frame::PooledFrame::from_vec(
                    encoding::build_error_response("Internal routing error").to_vec(),
                );
                DomainResponse::Frame(error_frame)
            }
        }
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
