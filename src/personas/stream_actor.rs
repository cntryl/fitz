//! StreamActor: Subscriptions, fanout, and ephemeral cursors.
//!
//! One actor per stream.
//! StreamActor handles:
//! - Subscription registration and cleanup
//! - Stream fanout to subscribers
//! - Ephemeral cursor tracking (not persisted)
//! - Backpressure on slow subscribers

pub struct StreamActor {
    // TODO: Implement
}
