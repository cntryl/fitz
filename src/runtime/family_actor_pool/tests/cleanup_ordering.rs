use super::*;
use crate::runtime::routing::{session_inbox_address, Route, RouteAddress};
use crate::runtime::{
    CleanedUpSessions, DeliveryError, Envelope, MailboxSink, Router, SessionCleanup, SessionScoped,
};

struct ProbeState {
    cleaned_up: CleanedUpSessions,
}

impl SessionScoped for ProbeState {
    fn cleaned_up_sessions(&mut self) -> &mut CleanedUpSessions {
        &mut self.cleaned_up
    }

    fn release_session_resources(&mut self, _session_id: u64) {}
}

struct PoolSink(FamilyActorIngress<Envelope>);

impl MailboxSink for PoolSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.0
            .try_enqueue(
                *envelope.destination().family(),
                FamilyActorLane::Normal,
                envelope,
            )
            .map_err(family_actor_enqueue_error_to_delivery_error)
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.0
            .try_enqueue(
                *envelope.destination().family(),
                FamilyActorLane::Control,
                envelope,
            )
            .map_err(family_actor_enqueue_error_to_delivery_error)
    }
}

#[test]
fn should_reject_stale_session_work_after_cleanup_marker_capacity_is_exceeded() {
    // Arrange
    let target = family(1);
    let destination = RouteAddress::new(target, Route::new("rpc://probe"));
    let mut pool = FamilyActorPool::new(&[target]).expect("pool");
    let mut shard = pool.take_shard(0).expect("shard");
    let router = Router::new();
    router.register(destination.clone(), Arc::new(PoolSink(pool.ingress())));
    let mut state = ProbeState {
        cleaned_up: CleanedUpSessions::new(4),
    };
    router
        .route(Envelope::from_route(
            session_inbox_address(target, 1),
            destination.clone(),
            1_u64,
        ))
        .expect("queue stale normal request");
    router
        .route_high_priority(Envelope::new(
            destination.clone(),
            SessionCleanup { session_id: 1 },
        ))
        .expect("queue cleanup");

    // Act
    for session_id in 2..100 {
        let work = shard.try_next().expect("control work");
        assert_eq!(work.lane, FamilyActorLane::Control);
        assert!(state.handle_cleanup_envelope(&work.message));
        router
            .route_high_priority(Envelope::new(
                destination.clone(),
                SessionCleanup { session_id },
            ))
            .expect("replenish control work");
    }
    assert!(state.handle_cleanup_envelope(&shard.try_next().expect("last cleanup").message));
    let work = shard.try_next().expect("normal work");
    let rejected = state.is_cleaned_up_request(1, &work.message);

    // Assert
    assert_eq!(work.lane, FamilyActorLane::Normal);
    assert!(
        !state.is_cleaned_up_session(1),
        "recent marker must actually be evicted"
    );
    assert!(
        rejected,
        "closed work must stay invalid independently of mailbox/history capacities"
    );
}
