// Control domain handler - routes all control:// operations

use crate::core::domain::{Domain, DomainRequest, DomainResponse};
use crate::storage::mem::MemStore;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct ControlDomain;

impl ControlDomain {
    pub fn new() -> Self {
        Self
    }
}

impl Default for ControlDomain {
    fn default() -> Self {
        Self::new()
    }
}

impl Domain for ControlDomain {
    fn handle<'a>(&'a self, _request: DomainRequest, _store: Arc<Mutex<MemStore>>) 
        -> std::pin::Pin<Box<dyn std::future::Future<Output = DomainResponse> + Send + 'a>> {
        Box::pin(async move {
            panic!("ControlDomain::handle not yet implemented")
        })
    }
    
    fn schemes(&self) -> &[&str] {
        &["control"]
    }
}
