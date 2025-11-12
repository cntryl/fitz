// Domain trait - each domain (kv, queue, stream, etc.) implements this
// to handle all operations for its scheme

use crate::protocol::route::Route;
use crate::routing::RouteFamilyId;
use tokio::sync::mpsc;

/// Type alias for subscriber channels (used by domains that support pub/sub)
pub type SubSender = mpsc::Sender<(
    String,
    Option<String>,
    Vec<u8>,
    Option<String>,
    Option<u32>,
    bool,
)>;

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
}

/// Domain trait - each domain implements this to handle its operations
pub trait Domain: Send + Sync {
    /// Handle a request for this domain
    /// Domain parses TLV tags from context.payload to extract operation details
    /// Returns DomainResponse with TLV-encoded response or error
    ///
    /// Domains that need persistent storage should manage their own KvStore instance
    fn handle<'a>(
        &'a self,
        context: DomainContext,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = DomainResponse> + Send + 'a>>;

    /// Cleanup all subscriptions and resources for a channel
    /// Called when a channel closes or session ends
    /// Default implementation does nothing
    fn cleanup_channel<'a>(
        &'a self,
        _route_family: RouteFamilyId,
        _channel_id: u32,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {})
    }
}
