//! Stream benchmarking helpers

use crate::domains::stream::stream_actor::StreamActor;
use crate::domains::stream::StreamStore;
use crate::runtime::actor::Context;
use crate::runtime::router::Router;
use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
use std::sync::Arc;
use super::storage::create_bench_store;

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
///
/// # Example
/// ```ignore
/// let (actor, ctx) = create_bench_stream_actor("bench-realm", "bench-area", "bench-stream");
/// ```
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
///
/// # Example
/// ```ignore
/// let payloads = create_bench_event_payloads(1000, 256);
/// ```
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
