// KV domain handler - routes all kv:// operations

use crate::core::domain::{Domain, DomainRequest, DomainResponse};
use crate::storage::mem::MemStore;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct KvDomain;

impl KvDomain {
    pub fn new() -> Self {
        Self
    }
}

impl Default for KvDomain {
    fn default() -> Self {
        Self::new()
    }
}

impl Domain for KvDomain {
    fn handle<'a>(&'a self, _request: DomainRequest, _store: Arc<Mutex<MemStore>>) 
        -> std::pin::Pin<Box<dyn std::future::Future<Output = DomainResponse> + Send + 'a>> {
        Box::pin(async move {
            panic!("KvDomain::handle not yet implemented")
        })
    }
    
    fn schemes(&self) -> &[&str] {
        &["kv"]
    }
}

