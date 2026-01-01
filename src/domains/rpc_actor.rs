//! RpcActor: In-flight RPC, timeouts, and correlation.
//!
//! One actor per realm or global.
//! RpcActor handles:
//! - RPC request correlation (ID → waiting session)
//! - Timeout tracking and cancellation
//! - Response fanout to waiting clients
//! - Metrics on latency and errors

pub struct RpcActor {
    // TODO: Implement
}
