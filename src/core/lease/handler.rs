// Lease domain handler - routes all lease:// operations

use crate::core::domain::{Domain, DomainRequest, DomainResponse};

pub struct LeaseDomain;

impl LeaseDomain {
    pub fn new() -> Self {
        Self
    }
}

impl Default for LeaseDomain {
    fn default() -> Self {
        Self::new()
    }
}

impl Domain for LeaseDomain {
    fn handle<'a>(&'a self, _request: DomainRequest) 
        -> std::pin::Pin<Box<dyn std::future::Future<Output = DomainResponse> + Send + 'a>> {
        Box::pin(async move {
            panic!("LeaseDomain::handle not yet implemented")
        })
    }
    
    fn schemes(&self) -> &[&str] {
        &["lease"]
    }
}
