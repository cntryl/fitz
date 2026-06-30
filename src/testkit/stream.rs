use parking_lot::Mutex;
use std::sync::Arc;

use crate::domains::stream::StreamActor;
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
    #[must_use]
    pub fn new() -> Self {
        Self {
            delivered: Arc::new(Mutex::new(Vec::new())),
        }
    }

    #[must_use]
    pub fn count(&self) -> usize {
        self.delivered.lock().len()
    }

    #[must_use]
    pub fn get_envelopes(&self) -> Vec<&Envelope> {
        // Return empty vec for now since Envelope can't be cloned
        // In actual tests, we can use count() to verify delivery
        vec![]
    }

    pub fn clear(&self) {
        self.delivered.lock().clear();
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

/// Create an in-memory Midge database for stream tests
#[must_use]
pub fn create_test_db() -> Arc<cntryl_midge::Engine> {
    crate::testkit::create_test_engine_with_cfs(vec![1])
}

/// Create a `StreamStore` with in-memory database for tests
#[must_use]
pub fn create_test_store() -> crate::domains::stream::StreamStore {
    create_test_store_with_layout(crate::domains::stream::StreamStorageLayout::default())
}

#[must_use]
pub fn create_test_store_with_layout(
    stream_storage_layout: crate::domains::stream::StreamStorageLayout,
) -> crate::domains::stream::StreamStore {
    crate::domains::stream::StreamStore::with_layout(create_test_db(), stream_storage_layout)
}

/// Create a `StreamActor` for testing with in-memory storage.
///
/// # Arguments
/// * `realm` - Realm name
/// * `area` - Area name
/// * `resource` - Resource name
///
/// # Returns
/// `StreamActor` ready for testing
///
/// # Panics
///
/// Panics if the test `StreamActor` cannot be constructed for the default test
/// family and store.
#[must_use]
pub fn create_test_stream_actor(realm: &str, area: &str, resource: &str) -> StreamActor {
    let family = RouteFamily::new(1);

    let store = Arc::new(create_test_store());
    StreamActor::new(
        family,
        realm.to_string(),
        area.to_string(),
        resource.to_string(),
        store,
    )
    .expect("create test stream actor")
}

/// Helper builders used by stream E2E tests
#[must_use]
pub fn make_router() -> Router {
    Router::new()
}

/// Build a Route for tests
#[must_use]
pub fn route(path: &str) -> Route {
    Route::new(path)
}

/// Build a `RouteAddress` in the default test family
#[must_use]
pub fn addr(path: &str) -> RouteAddress {
    RouteAddress::new(RouteFamily::new(1), route(path))
}

/// Build a `RouteAddress` with specific family
#[must_use]
pub fn addr_with_family(path: &str, family: u64) -> RouteAddress {
    RouteAddress::new(RouteFamily::new(family), route(path))
}

#[must_use]
pub fn session_id(n: u64) -> SessionId {
    SessionId(n)
}

/// Create a stream route for testing
#[must_use]
pub fn stream_route(realm: &str, area: &str, resource: &str) -> String {
    format!("stream://{realm}/{area}/{resource}")
}

/// Create an area-level stream route (wildcard resource)
#[must_use]
pub fn area_stream_route(realm: &str, area: &str) -> String {
    format!("stream://{realm}{area}/*")
}

/// Create a realm-level stream route (wildcard area and resource)
#[must_use]
pub fn realm_stream_route(realm: &str) -> String {
    format!("stream://{realm}/*/*")
}

/// Create append operation route
#[must_use]
pub fn append_route(realm: &str, area: &str, resource: &str) -> String {
    format!("stream://{realm}/{area}/{resource}/append")
}

/// Create read operation route
#[must_use]
pub fn read_route(realm: &str, area: &str, resource: &str) -> String {
    format!("stream://{realm}/{area}/{resource}/read")
}
