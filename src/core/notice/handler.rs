// Notice domain handler - routes all notice:// operations

use crate::core::domain::{Domain, DomainRequest, DomainResponse};
use crate::storage::mem::MemStore;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct NoticeDomain;

impl NoticeDomain {
    pub fn new() -> Self {
        Self
    }
}

impl Default for NoticeDomain {
    fn default() -> Self {
        Self::new()
    }
}

impl Domain for NoticeDomain {
    fn handle<'a>(&'a self, _request: DomainRequest, _store: Arc<Mutex<MemStore>>) 
        -> std::pin::Pin<Box<dyn std::future::Future<Output = DomainResponse> + Send + 'a>> {
        Box::pin(async move {
            panic!("NoticeDomain::handle not yet implemented")
        })
    }
    
    fn schemes(&self) -> &[&str] {
        &["notice"]
    }
}
