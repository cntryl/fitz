//! Queue benchmarking helpers

use super::storage::{create_bench_store, create_local_bench_store};
use crate::domains::queue::{QueueActor, QueueKey};
use crate::runtime::routing::RouteFamily;

/// Create a QueueActor for benchmarking with buffered writes
///
/// Creates an actor with in-memory storage suitable for performance testing.
/// All queues use buffered writes (intent, not events).
///
/// # Arguments
/// * `realm` - Realm name for the queue
/// * `area` - Area name for the queue
/// * `resource` - Resource name for the queue
/// * `max_attempts` - Optional maximum delivery attempts before DLQ
pub fn create_bench_queue_actor(
    realm: &str,
    area: &str,
    resource: &str,
    max_attempts: Option<u32>,
) -> QueueActor {
    let queue_key = QueueKey {
        // TODO: Use CF=1 once Midge supports explicit CF creation in in-memory mode
        // For now, use CF=0 (default) as a workaround for Midge test limitation
        family: RouteFamily::new(0),
        realm: realm.to_string(),
        area: area.to_string(),
        resource: resource.to_string(),
    };

    let store = create_bench_store();
    // TODO: Use CF=1 once Midge supports explicit CF creation in in-memory mode
    QueueActor::new(RouteFamily::new(0), queue_key, store, max_attempts)
}

/// Create a QueueActor for integration benchmarking with local disk storage
///
/// Creates an actor with local disk-backed storage for realistic production testing.
/// Uses a temporary directory that persists for the lifetime of the returned tuple.
///
/// # Arguments
/// * `realm` - Realm name for the queue
/// * `area` - Area name for the queue
/// * `resource` - Resource name for the queue
/// * `max_attempts` - Optional maximum delivery attempts before DLQ
///
/// # Returns
///
/// A tuple of (QueueActor, TempDir). The TempDir must be kept alive for the
/// lifetime of the actor, otherwise the directory will be deleted.
pub fn create_local_bench_queue_actor(
    realm: &str,
    area: &str,
    resource: &str,
    max_attempts: Option<u32>,
) -> (QueueActor, tempfile::TempDir) {
    let queue_key = QueueKey {
        family: RouteFamily::new(0),
        realm: realm.to_string(),
        area: area.to_string(),
        resource: resource.to_string(),
    };

    let (store, temp_dir) = create_local_bench_store();
    let actor = QueueActor::new(RouteFamily::new(0), queue_key, store, max_attempts);
    (actor, temp_dir)
}
