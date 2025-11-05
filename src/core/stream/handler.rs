// Stream domain handler - routes all stream:// operations

use crate::core::domain::{Domain, DomainRequest, DomainResponse};
use crate::storage::mem::MemStore;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct StreamDomain;

impl StreamDomain {
    pub fn new() -> Self {
        Self
    }
}

impl Default for StreamDomain {
    fn default() -> Self {
        Self::new()
    }
}

impl Domain for StreamDomain {
    fn handle<'a>(&'a self, _request: DomainRequest, _store: Arc<Mutex<MemStore>>) 
        -> std::pin::Pin<Box<dyn std::future::Future<Output = DomainResponse> + Send + 'a>> {
        Box::pin(async move {
            panic!("StreamDomain::handle not yet implemented")
        })
    }
    
    fn schemes(&self) -> &[&str] {
        &["stream"]
    }
}
