use parking_lot::Mutex;
use std::sync::Arc;

use crate::runtime::envelope::Envelope;
use crate::runtime::router::{MailboxSink, Router};
use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
use crate::session::session::SessionId;

/// Simple test mailbox sink that records delivered envelopes.
#[derive(Clone)]
pub struct TestSink {
    delivered: Arc<Mutex<Vec<Envelope>>>,
}

impl Default for TestSink {
    fn default() -> Self {
        Self::new()
    }
}

impl TestSink {
    pub fn new() -> Self {
        Self {
            delivered: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn count(&self) -> usize {
        self.delivered.lock().len()
    }
}

impl MailboxSink for TestSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), crate::runtime::router::DeliveryError> {
        self.delivered.lock().push(envelope);
        Ok(())
    }

    fn deliver_high_priority(
        &self,
        envelope: Envelope,
    ) -> Result<(), crate::runtime::router::DeliveryError> {
        // For tests, just deliver to same queue
        self.deliver(envelope)
    }
}

/// Helper builders used by E2E tests
pub fn make_router() -> Router {
    Router::new()
}

/// Build a Route for tests
pub fn route(path: &str) -> Route {
    Route::new(path.to_string())
}

/// Build a RouteAddress in the default test family
pub fn addr(path: &str) -> RouteAddress {
    RouteAddress::new(RouteFamily::new(1), route(path))
}

pub fn session_id(n: u64) -> SessionId {
    SessionId(n)
}
