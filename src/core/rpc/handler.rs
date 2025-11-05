// RPC domain handler - routes all rpc:// operations

use crate::core::domain::{Domain, DomainRequest, DomainResponse};
use crate::storage::traits::KvStore;
use std::sync::Arc;

pub struct RpcDomain;

impl RpcDomain {
    pub fn new() -> Self {
        Self
    }
}

impl Default for RpcDomain {
    fn default() -> Self {
        Self::new()
    }
}

impl Domain for RpcDomain {
    fn handle<'a>(&'a self, _request: DomainRequest, _kv_store: Arc<dyn KvStore>) 
        -> std::pin::Pin<Box<dyn std::future::Future<Output = DomainResponse> + Send + 'a>> {
        Box::pin(async move {
            panic!("RpcDomain::handle not yet implemented")
        })
    }
    
    fn schemes(&self) -> &[&str] {
        &["rpc"]
    }
}
