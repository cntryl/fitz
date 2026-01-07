//! Queue benchmarking helpers

use crate::domains::queue::{QueueActor, QueueKey};
use crate::runtime::routing::RouteFamily;
use super::storage::create_bench_store;

/// Create a QueueActor for benchmarking
///
/// Creates an actor with in-memory storage suitable for performance testing.
///
/// # Arguments
/// * `realm` - Realm name for the queue
/// * `area` - Area name for the queue
/// * `resource` - Resource name for the queue
/// * `max_attempts` - Optional maximum delivery attempts before DLQ
///
/// # Example
/// ```ignore
/// let actor = create_bench_queue_actor("bench", "test", "queue", None);
/// ```
pub fn create_bench_queue_actor(
    realm: &str,
    area: &str,
    resource: &str,
    max_attempts: Option<u32>,
) -> QueueActor {
    let queue_key = QueueKey {
        family: RouteFamily::new(1),
        realm: realm.to_string(),
        area: area.to_string(),
        resource: resource.to_string(),
    };
    
    let store = create_bench_store();
    QueueActor::new(RouteFamily::new(1), queue_key, store, max_attempts)
}
