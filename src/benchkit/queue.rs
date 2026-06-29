//! Queue benchmarking helpers

use super::storage::{create_bench_store, create_local_bench_store};
use crate::domains::queue::{QueueActor, QueueKey};
use crate::runtime::routing::RouteFamily;

/// Create a QueueActor for benchmarking with ephemeral in-memory commits.
///
/// Creates an actor with in-memory storage suitable for performance testing.
/// In-memory engines are already ephemeral, so the queue can use best-effort
/// commits to avoid WAL work that cannot survive process exit.
///
/// # Arguments
/// * `realm` - Realm name for the queue
/// * `area` - Area name for the queue
/// * `resource` - Resource name for the queue
/// * `max_attempts` - Optional maximum delivery attempts before DLQ
#[must_use]
pub fn create_bench_queue_actor(
    realm: &str,
    area: &str,
    resource: &str,
    max_attempts: Option<u32>,
) -> QueueActor {
    let family = RouteFamily::new(1);
    let queue_key = QueueKey {
        family,
        realm: realm.to_string(),
        area: area.to_string(),
        resource: resource.to_string(),
    };

    let store = create_bench_store();
    QueueActor::new_with_write_options(
        family,
        queue_key,
        store,
        max_attempts,
        crate::utils::idempotency::default_dedup_store(),
        cntryl_midge::WriteOptions::best_effort(),
    )
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
#[must_use]
pub fn create_local_bench_queue_actor(
    realm: &str,
    area: &str,
    resource: &str,
    max_attempts: Option<u32>,
) -> (QueueActor, tempfile::TempDir) {
    let family = RouteFamily::new(1);
    let queue_key = QueueKey {
        family,
        realm: realm.to_string(),
        area: area.to_string(),
        resource: resource.to_string(),
    };

    let (store, temp_dir) = create_local_bench_store();
    let actor = QueueActor::new(
        family,
        queue_key,
        store,
        max_attempts,
        crate::utils::idempotency::default_dedup_store(),
    );
    (actor, temp_dir)
}
