// Domain trait - each domain (kv, queue, stream, etc.) implements this
// to handle all operations for its scheme

use crate::protocol::route::Route;
use crate::routing::RouteFamilyId;

/// Complete context for a domain operation
/// Encapsulates all information needed to handle a request or manage resources
#[derive(Debug, Clone)]
pub struct DomainContext {
    /// Parsed route (scheme, realm, area, resource, etc.)
    pub route: Route,
    /// Raw route string
    pub route_str: String,
    /// Frame payload containing TLV tags - domain parses what it needs
    pub payload: Vec<u8>,
    /// Channel ID from frame (for tracking subscriptions/sessions)
    pub channel_id: u32,
    /// Storage route family (for namespacing/multi-tenant operations)
    pub route_family: RouteFamilyId,
}

/// Response from domain operation
#[derive(Debug)]
pub enum DomainResponse {
    /// Success with no data
    Ok,

    /// Success with frame payload to send back (TLV encoded)
    /// Domains build response frames themselves
    Frame(crate::protocol::frame::PooledFrame),

    /// Error message
    Error(String),

    /// RPC delivery instruction - domain returns routing decision,
    /// transport performs actual delivery with backpressure handling
    RpcDelivery {
        /// Target channel_id to deliver to
        target_channel_id: u32,
        /// Message payload
        message: crate::core::rpc::RpcMessage,
        /// Acknowledgment frame to send back to requester
        ack_frame: crate::protocol::frame::PooledFrame,
    },

    /// Notice fanout delivery - domain returns list of subscribers,
    /// transport performs actual fanout with backpressure handling
    NoticeDelivery {
        /// List of (channel_id, sub_id) tuples to deliver to
        subscribers: smallvec::SmallVec<[(u32, u64); 8]>,
        /// Pre-built notification frame payload (TLV: route + id + body)
        notification_frame: Vec<u8>,
        /// Optional acknowledgment frame to send back to publisher
        ack_frame: Option<crate::protocol::frame::PooledFrame>,
    },
}

/// Domain trait - each domain implements this to handle its operations
/// All domain operations are SYNCHRONOUS - no async, no .await, no tokio primitives
pub trait Domain: Send + Sync + std::fmt::Debug {
    /// Handle a request for this domain (SYNCHRONOUS)
    /// Domain parses TLV tags from context.payload to extract operation details
    /// Returns DomainResponse with TLV-encoded response or error
    ///
    /// Domains that need persistent storage should manage their own KvStore instance
    /// Must NOT use async, .await, tokio::spawn, or async locks
    fn handle(&self, context: DomainContext) -> DomainResponse;

    /// Cleanup all subscriptions and resources for a channel (SYNCHRONOUS)
    /// Called when a channel closes or session ends
    /// Default implementation does nothing
    fn cleanup_channel(&self, _route_family: RouteFamilyId, _channel_id: u32) {}
}
