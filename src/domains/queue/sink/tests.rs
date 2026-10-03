use super::*;
use crate::dispatch::protocol::frame_context::FrameContext;
use crate::domains::queue::actor::QUEUE_ACTOR_REPLY_TIMEOUT;
use crate::domains::queue::QueueKey;
use crate::runtime::{DeliveryError, Envelope, MailboxSink, Router};
use model::{QueueFamilyState, QUEUE_ACTOR_IDLE_TTL, QUEUE_IDLE_SWEEP_BATCH_SIZE};
use std::sync::Arc;
use std::time::{Duration, Instant};

mod actor_delivery;
mod admin_observation;
mod routing_watch_and_admin;
use routing_watch_and_admin::*;
mod cleanup_and_eviction;
mod correctness;
mod fault_injection;

#[test]
fn should_advance_queue_maintenance_deadlines_only_when_due() {
    // Arrange
    let start = Instant::now();
    let mut clock =
        super::maintenance_clock::QueueMaintenanceClock::new(start, Some(Duration::from_secs(2)));

    // Act
    let idle_due = clock.idle_sweep_due(start);
    let dedup_due = clock.dedup_sweep_due(start);
    let flush_early = clock.fast_flush_due(start);
    let flush_due = clock.fast_flush_due(start + Duration::from_secs(2));

    // Assert
    assert!(idle_due);
    assert!(dedup_due);
    assert!(!flush_early);
    assert!(flush_due);
    assert!(!clock.idle_sweep_due(start));
    assert!(!clock.dedup_sweep_due(start));
    assert!(!clock.fast_flush_due(start + Duration::from_secs(2)));
}
