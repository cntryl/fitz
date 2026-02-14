//! Queue test helpers

use crate::domains::queue::{QueueActor, QueueKey};
use crate::runtime::routing::RouteFamily;

/// Create a QueueActor for testing with in-memory storage
///
/// # Arguments
/// * `realm` - Realm name
/// * `area` - Area name  
/// * `resource` - Resource name
/// * `max_attempts` - Optional max delivery attempts before DLQ
pub fn create_test_queue_actor(
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

    let store = super::stream::create_test_db();
    QueueActor::new(
        RouteFamily::new(1),
        queue_key,
        store,
        max_attempts,
        crate::utils::idempotency::global_dedup_store(),
    )
}
