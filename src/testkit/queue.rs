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
#[must_use]
pub fn create_test_queue_actor(
    realm: &str,
    area: &str,
    resource: &str,
    max_attempts: Option<u32>,
) -> QueueActor {
    let queue_key = QueueKey {
        family: RouteFamily::new(0), // CF=0 for Midge test limitation
        realm: realm.to_string(),
        area: area.to_string(),
        resource: resource.to_string(),
    };

    let store = super::stream::create_test_db();
    QueueActor::new_with_write_options(
        RouteFamily::new(0), // CF=0 for Midge test limitation
        queue_key,
        store,
        max_attempts,
        crate::utils::idempotency::default_dedup_store(),
        cntryl_midge::WriteOptions::best_effort(),
    )
}
