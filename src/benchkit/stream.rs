//! Stream benchmarking helpers

use super::storage::{create_bench_store, create_local_bench_store};
use crate::domains::stream::stream_actor::StreamActor;
use crate::domains::stream::StreamStore;
use crate::runtime::actor::Context;
use crate::runtime::router::Router;
use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
use std::sync::Arc;

/// Create a StreamActor and its context for benchmarking
///
/// Creates an actor with in-memory storage suitable for performance testing.
///
/// # Arguments
/// * `realm` - Realm name for the stream
/// * `area` - Area name for the stream  
/// * `resource` - Resource name for the stream
///
/// # Returns
/// Tuple of (StreamActor, Context) ready for benchmarking
pub fn create_bench_stream_actor(
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

    let db = create_bench_store();
    let store = Arc::new(StreamStore::new(db));
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

/// Create a StreamActor and its context for integration benchmarking with local disk storage
///
/// Creates an actor with local disk-backed storage for realistic production testing.
///
/// # Arguments
/// * `realm` - Realm name for the stream
/// * `area` - Area name for the stream  
/// * `resource` - Resource name for the stream
///
/// # Returns
/// Tuple of (StreamActor, Context, TempDir). The TempDir must be kept alive for the
/// lifetime of the actor, otherwise the directory will be deleted.
pub fn create_local_bench_stream_actor(
    realm: &str,
    area: &str,
    resource: &str,
) -> (StreamActor, Context<StreamActor>, tempfile::TempDir) {
    let router = Arc::new(Router::new());
    let family = RouteFamily::new(1);
    let addr = RouteAddress::new(
        family,
        Route::new(format!("stream://{}/{}/{}/append", realm, area, resource)),
    );

    let (db, temp_dir) = create_local_bench_store();
    let store = Arc::new(StreamStore::new(db));
    let actor = StreamActor::new(
        family,
        realm.to_string(),
        area.to_string(),
        resource.to_string(),
        store,
    );
    let ctx = Context::new(addr, router);

    (actor, ctx, temp_dir)
}

/// Generate deterministic event payloads for benchmarking
///
/// Creates a vector of payloads with deterministic content suitable for
/// reproducible benchmark results.
///
/// # Arguments
/// * `count` - Number of payloads to generate
/// * `size` - Size of each payload in bytes
///
/// # Returns
/// Vector of byte arrays, each containing the event index in the first 8 bytes
pub fn create_bench_event_payloads(count: usize, size: usize) -> Vec<Vec<u8>> {
    (0..count)
        .map(|i| {
            let mut payload = vec![0u8; size];
            // Deterministic pattern: include event index in payload
            payload[..8.min(size)].copy_from_slice(&(i as u64).to_le_bytes()[..8.min(size)]);
            payload
        })
        .collect()
}
