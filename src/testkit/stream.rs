use parking_lot::Mutex;
use std::sync::Arc;

use crate::domains::stream::stream_actor::StreamActor;
use crate::domains::stream::area_actor::AreaActor;
use crate::runtime::actor::Context;
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

    pub fn get_envelopes(&self) -> Vec<&Envelope> {
        // Return empty vec for now since Envelope can't be cloned
        // In actual tests, we can use count() to verify delivery
        vec![]
    }

    pub fn clear(&self) {
        self.delivered.lock().clear()
    }
}

impl MailboxSink for TestSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), crate::runtime::router::DeliveryError> {
        self.delivered.lock().push(envelope);
        Ok(())
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), crate::runtime::router::DeliveryError> {
        // For tests, just deliver to same queue
        self.deliver(envelope)
    }
}

/// Create an in-memory Midge database for stream tests
pub fn create_test_db() -> Arc<cntryl_midge::MidgeEngine> {
    use cntryl_midge::MidgeOptions;
    Arc::new(cntryl_midge::MidgeEngine::open(MidgeOptions::default()).expect("create in-memory db"))
}

/// Create a StreamStore with in-memory database for tests
pub fn create_test_store() -> crate::domains::stream::StreamStore {
    crate::domains::stream::StreamStore::new(create_test_db())
}

/// Create a StreamActor for testing with in-memory storage
///
/// # Arguments
/// * `realm` - Realm name
/// * `area` - Area name
/// * `resource` - Resource name
///
/// # Returns
/// Tuple of (StreamActor, Context) ready for testing
///
/// # Example
/// ```ignore
/// let (actor, ctx) = create_test_stream_actor("test", "area", "stream");
/// ```
pub fn create_test_stream_actor(
    realm: &str,
    area: &str,
    resource: &str,
) -> (StreamActor, Context<StreamActor>) {
    let router = Arc::new(Router::new());
    let family = RouteFamily::new(1);
    let addr = RouteAddress::new(
        family,
        Route::new(format!("stream://{}/{}/{}/append", realm, area, resource)),
    );

    let store = Arc::new(create_test_store());
    let actor = StreamActor::new(
        family,
        realm.to_string(),
        area.to_string(),
        resource.to_string(),
        store,
    );
    let ctx = Context::new(addr, router);

    (actor, ctx)
}

/// Create an AreaActor for testing with in-memory storage
///
/// # Arguments
/// * `realm` - Realm name
/// * `area` - Area name
///
/// # Returns
/// Tuple of (AreaActor, Context) ready for testing
///
/// # Example
/// ```ignore
/// let (actor, ctx) = create_test_area_actor("test", "area");
/// ```
pub fn create_test_area_actor(realm: &str, area: &str) -> (AreaActor, Context<AreaActor>) {
    let router = Arc::new(Router::new());
    let family = RouteFamily::new(1);
    let addr = RouteAddress::new(
        family,
        Route::new(format!("stream://{}/{}/__area__", realm, area)),
    );

    let store = Arc::new(create_test_store());
    let actor = AreaActor::new(
        family,
        realm.to_string(),
        area.to_string(),
        store,
    );
    let ctx = Context::new(addr, router);

    (actor, ctx)
}

/// Helper builders used by stream E2E tests
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

/// Build a RouteAddress with specific family
pub fn addr_with_family(path: &str, family: u64) -> RouteAddress {
    RouteAddress::new(RouteFamily::new(family), route(path))
}

pub fn session_id(n: u64) -> SessionId {
    SessionId(n)
}

/// Create a stream route for testing
pub fn stream_route(realm: &str, area: &str, resource: &str) -> String {
    format!("stream://{}/{}/{}", realm, area, resource)
}

/// Create an area-level stream route (wildcard resource)
pub fn area_stream_route(realm: &str, area: &str) -> String {
    format!("stream://{}{}/*", realm, area)
}

/// Create a realm-level stream route (wildcard area and resource)
pub fn realm_stream_route(realm: &str) -> String {
    format!("stream://{}/*/*", realm)
}

/// Create append operation route
pub fn append_route(realm: &str, area: &str, resource: &str) -> String {
    format!("stream://{}/{}/{}/append", realm, area, resource)
}

/// Create read operation route
pub fn read_route(realm: &str, area: &str, resource: &str) -> String {
    format!("stream://{}/{}/{}/read", realm, area, resource)
}
