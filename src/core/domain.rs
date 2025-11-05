// Domain trait - each domain (kv, queue, stream, etc.) implements this
// to handle all operations for its scheme

use crate::protocol::route::Route;
use crate::storage::traits::KvStore;
use std::sync::Arc;
use tokio::sync::mpsc;

/// Type alias for subscriber channels (used by domains that support pub/sub)
pub type SubSender = mpsc::Sender<(String, Option<String>, Vec<u8>, Option<String>, Option<u32>, bool)>;

/// A message dispatched from engine to a domain handler
#[derive(Debug, Clone)]
pub struct DomainRequest {
    /// Parsed route (scheme, realm, area, resource, etc.)
    pub route: Route,
    /// Raw route string
    pub route_str: String,
    /// Frame payload containing TLV tags - domain parses what it needs
    pub payload: Vec<u8>,
    /// Channel ID from frame (for tracking subscriptions/sessions)
    pub channel_id: u32,
}

/// Response from domain operation
#[derive(Debug)]
pub enum DomainResponse {
    /// Success with no data
    Ok,
    
    /// Success with frame payload to send back (TLV encoded)
    /// Domains build response frames themselves
    Frame(Vec<u8>),
    
    /// Error message
    Error(String),
}

/// Domain trait - each domain implements this to handle its operations
/// Domains that support pub/sub can override the subscription methods
pub trait Domain: Send + Sync {
    /// Handle a request for this domain
    /// Domain parses TLV tags from request.payload to extract operation details
    /// Returns DomainResponse with TLV-encoded response or error
    fn handle<'a>(&'a self, request: DomainRequest, kv_store: Arc<dyn KvStore>) 
        -> std::pin::Pin<Box<dyn std::future::Future<Output = DomainResponse> + Send + 'a>>;
    
    /// Get the scheme(s) this domain handles (e.g., "queue", "kv", "stream")
    fn schemes(&self) -> &[&str];
    
    /// Subscribe to a route pattern (optional, for pub/sub domains)
    /// Returns subscription ID
    /// Default implementation returns an error
    fn subscribe<'a>(
        &'a self,
        _route: String,
        _channel_id: u32,
        _sender: SubSender,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<u64, String>> + Send + 'a>> {
        Box::pin(async move {
            Err("This domain does not support subscriptions".to_string())
        })
    }
    
    /// Unsubscribe by subscription ID (optional, for pub/sub domains)
    /// Returns true if subscription was found and removed
    /// Default implementation returns false
    fn unsubscribe<'a>(
        &'a self,
        _sub_id: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send + 'a>> {
        Box::pin(async move { false })
    }
    
    /// Cleanup all subscriptions for a channel (optional, for pub/sub domains)
    /// Default implementation does nothing
    fn cleanup_channel<'a>(
        &'a self,
        _channel_id: u32,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {})
    }
}


