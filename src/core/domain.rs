// Domain trait - each domain (kv, queue, stream, etc.) implements this
// to handle all operations for its scheme

use crate::protocol::route::Route;
use crate::storage::mem::MemStore;
use std::sync::Arc;
use tokio::sync::Mutex;

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
pub trait Domain: Send + Sync {
    /// Handle a request for this domain
    /// Domain parses TLV tags from request.payload to extract operation details
    /// Returns DomainResponse with TLV-encoded response or error
    fn handle<'a>(&'a self, request: DomainRequest, store: Arc<Mutex<MemStore>>) 
        -> std::pin::Pin<Box<dyn std::future::Future<Output = DomainResponse> + Send + 'a>>;
    
    /// Get the scheme(s) this domain handles (e.g., "queue", "kv", "stream")
    fn schemes(&self) -> &[&str];
}

