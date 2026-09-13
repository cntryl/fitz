//! Fault-injection tests for the Lease ACQUIRE/RELEASE ownership-mutation
//! and waiter-promotion boundary crossings described in the 2026-09
//! correctness audit's `notice_lease` slice.

use super::*;

struct FailingSink;

impl crate::runtime::MailboxSink for FailingSink {
    fn deliver(&self, _envelope: Envelope) -> Result<(), crate::runtime::DeliveryError> {
        Err(crate::runtime::DeliveryError::ActorStopped)
    }

    fn deliver_high_priority(
        &self,
        envelope: Envelope,
    ) -> Result<(), crate::runtime::DeliveryError> {
        self.deliver(envelope)
    }
}

#[test]
fn should_leave_lease_unheld_when_direct_acquire_grant_reply_reports_actor_stopped() {
    // Arrange
    let family = RouteFamily::new(1);
    let session_id = 7;
    let route = "lease://acme/locks/undeliverable-actor-stopped";
    let client = RouteAddress::new(family, Route::new("inbox://session/7"));
    let destination = RouteAddress::new(family, Route::new(route));
    let router = Arc::new(Router::new());
    // Unlike the existing MailboxFull-based rollback coverage, this client's
    // sink always fails with ActorStopped.
    router.register(client.clone(), Arc::new(FailingSink));
    let sink = LeaseDomain::new(
        router,
        crate::control::admin::read_model::AdminReadModel::new(),
    );

    // Act
    sink.deliver(Envelope::from_route(
        client,
        destination,
        FrameContext::new(
            session_id,
            ChannelId::Sub,
            MessageType::new(400),
            encode_lease_acquire(route, "", 30),
            family,
        ),
    ))
    .expect("deliver acquire");
    wait_for_lease_count(&sink, 0);

    // Assert
    // Nobody is left holding a lease that its owner never learned
    // about.
    assert_eq!(sink.lease_count(), 0);
}

#[test]
fn should_leave_lease_held_when_release_is_attempted_by_a_non_owner_session() {
    // Arrange
    let family = RouteFamily::new(1);
    let owner_session_id = 7;
    let other_session_id = 8;
    let route = "lease://acme/locks/release-non-owner";
    let key = lease_key(family, route);
    let owner_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let other_address = RouteAddress::new(family, Route::new("inbox://session/8"));
    let destination = RouteAddress::new(family, Route::new(route));
    let router = Arc::new(Router::new());
    let owner_mailbox = Arc::new(Mailbox::new(8));
    let other_mailbox = Arc::new(Mailbox::new(8));
    router.register(owner_address.clone(), owner_mailbox.clone());
    router.register(other_address.clone(), other_mailbox.clone());
    let sink = LeaseDomain::new(
        router,
        crate::control::admin::read_model::AdminReadModel::new(),
    );
    sink.deliver(Envelope::from_route(
        owner_address,
        destination.clone(),
        FrameContext::new(
            owner_session_id,
            ChannelId::Sub,
            MessageType::new(400),
            encode_lease_acquire(route, "shared", 30),
            family,
        ),
    ))
    .expect("deliver acquire");
    let _acquire_ack = receive_envelope(&owner_mailbox, "acquire ack");
    assert_eq!(sink.lease_count(), 1);

    // Act
    // A different session, even reusing the same client-supplied
    // owner_id string, is scoped by session and so is not the owner.
    sink.deliver(Envelope::from_route(
        other_address,
        destination,
        FrameContext::new(
            other_session_id,
            ChannelId::Sub,
            MessageType::new(402),
            encode_lease_release(route, "shared", 1),
            family,
        ),
    ))
    .expect("deliver release");
    let _release_ack = receive_envelope(&other_mailbox, "release ack");

    // Assert
    // The lease is untouched and still tracked against its real
    // owner.
    assert_eq!(sink.lease_count(), 1);
    assert!(sink.session_leases_contain_for_tests(owner_session_id, &key));
}

#[test]
fn should_leave_lease_held_when_release_carries_a_stale_fencing_token() {
    // Arrange
    let family = RouteFamily::new(1);
    let owner_session_id = 7;
    let route = "lease://acme/locks/release-stale-token";
    let key = lease_key(family, route);
    let owner_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let destination = RouteAddress::new(family, Route::new(route));
    let router = Arc::new(Router::new());
    let owner_mailbox = Arc::new(Mailbox::new(8));
    router.register(owner_address.clone(), owner_mailbox.clone());
    let sink = LeaseDomain::new(
        router,
        crate::control::admin::read_model::AdminReadModel::new(),
    );
    sink.deliver(Envelope::from_route(
        owner_address.clone(),
        destination.clone(),
        FrameContext::new(
            owner_session_id,
            ChannelId::Sub,
            MessageType::new(400),
            encode_lease_acquire(route, "owner", 30),
            family,
        ),
    ))
    .expect("deliver acquire");
    let _acquire_ack = receive_envelope(&owner_mailbox, "acquire ack");
    assert_eq!(sink.lease_count(), 1);

    // Act
    // The correct owner/session, but a fencing token that does not
    // match the live one (e.g. replayed from a superseded acquisition).
    sink.deliver(Envelope::from_route(
        owner_address,
        destination,
        FrameContext::new(
            owner_session_id,
            ChannelId::Sub,
            MessageType::new(402),
            encode_lease_release(route, "owner", u64::MAX),
            family,
        ),
    ))
    .expect("deliver release");
    let _release_ack = receive_envelope(&owner_mailbox, "release ack");

    // Assert
    // Fencing rejects the stale-token release and ownership is
    // unaffected.
    assert_eq!(sink.lease_count(), 1);
    assert!(sink.session_leases_contain_for_tests(owner_session_id, &key));
}
