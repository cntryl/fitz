use super::*;
use crate::domains::queue::{QueueActor, QueueResponse};
use std::num::NonZeroU32;

fn key(family: RouteFamily) -> QueueKey {
    QueueKey {
        family,
        realm: "shared".into(),
        area: "jobs".into(),
        resource: "attempts".into(),
    }
}

fn configured_sink(limit: Option<NonZeroU32>) -> QueueDomain {
    new_queue_domain_sink(
        crate::testkit::create_test_engine_with_cfs(vec![1, 2]),
        Arc::new(Router::new()),
        crate::control::admin::read_model::AdminReadModel::new(),
    )
    .with_max_attempts(limit)
    .with_max_message_bytes(1024 * 1024)
    .with_fast_flush_interval(None)
}

fn expire_one_delivery(actor: &mut QueueActor) {
    let response = actor.handle_receive_for_session(7, 0, Some(1));
    assert!(matches!(response, QueueResponse::Received { messages } if messages.len() == 1));
    actor.process_expired_timers();
}

#[test]
fn should_apply_attempt_limit_to_each_route_family_after_builder_rebuilds() {
    // Arrange
    let sink = configured_sink(NonZeroU32::new(1));

    // Act
    let dead_letters = [1, 2].map(|family| {
        let family = RouteFamily::new(family);
        sink.inspect_family_for_tests(family, move |state| {
            state
                .with_actor(&key(family), |actor| {
                    actor.handle_send(Bytes::from_static(b"job"), None);
                    expire_one_delivery(actor);
                    actor.admin_dead_letters().len()
                })
                .unwrap()
                .0
        })
    });

    // Assert
    assert_eq!(dead_letters, [1, 1]);
}

#[test]
fn should_keep_registry_retries_unlimited_when_limit_is_omitted() {
    // Arrange
    let sink = configured_sink(None);
    let family = RouteFamily::new(1);

    // Act
    let counts = sink.inspect_family_for_tests(family, move |state| {
        state
            .with_actor(&key(family), |actor| {
                actor.handle_send(Bytes::from_static(b"job"), None);
                for _ in 0..4 {
                    expire_one_delivery(actor);
                }
                (actor.ready_len(), actor.admin_dead_letters().len())
            })
            .unwrap()
            .0
    });

    // Assert
    assert_eq!(counts, (1, 0));
}

#[test]
fn should_retain_attempt_limit_after_idle_actor_eviction() {
    // Arrange
    let sink = configured_sink(NonZeroU32::new(2));
    let family = RouteFamily::new(1);
    sink.inspect_family_for_tests(family, move |state| {
        state
            .with_actor(&key(family), |actor| {
                actor.handle_send(Bytes::from_static(b"job"), None);
                expire_one_delivery(actor);
            })
            .unwrap();
        state.actor_registry.age_all_for_tests(
            Instant::now()
                .checked_sub(QUEUE_ACTOR_IDLE_TTL + Duration::from_secs(1))
                .unwrap(),
        );
        state.sweep_idle_actors_at(Instant::now());
        assert!(state.actor_registry.is_empty());
    });

    // Act
    let (dead_letters, created, _) = sink.inspect_family_for_tests(family, move |state| {
        state
            .with_actor(&key(family), |actor| {
                expire_one_delivery(actor);
                actor.admin_dead_letters().len()
            })
            .unwrap()
    });

    // Assert
    assert!(created);
    assert_eq!(dead_letters, 1);
}

#[test]
fn should_retain_attempt_limit_after_unknown_outcome_reconstruction() {
    // Arrange
    let sink = configured_sink(NonZeroU32::new(1));
    let family = RouteFamily::new(1);
    sink.inspect_family_for_tests(family, move |state| {
        crate::domains::queue::actor::recovery_store::QueueStore::fail_next_commit_after_apply_for_tests(
            cntryl_midge::MidgeError::OutcomeUnknown("enqueue applied".into()),
        );
        let (_, _, reconstructed) = state.with_actor(&key(family), |actor| {
            actor.handle_send(Bytes::from_static(b"job"), None)
        }).unwrap();
        assert!(reconstructed);
    });

    // Act
    let dead_letters = sink.inspect_family_for_tests(family, move |state| {
        state
            .with_actor(&key(family), |actor| {
                expire_one_delivery(actor);
                actor.admin_dead_letters().len()
            })
            .unwrap()
            .0
    });

    // Assert
    assert_eq!(dead_letters, 1);
}

#[test]
fn should_complete_a_configured_delivery_before_it_can_be_dead_lettered() {
    // Arrange
    let sink = configured_sink(NonZeroU32::new(1));
    let family = RouteFamily::new(1);

    // Act
    let counts = sink.inspect_family_for_tests(family, move |state| {
        state
            .with_actor(&key(family), |actor| {
                actor.handle_send(Bytes::from_static(b"job"), None);
                let QueueResponse::Received { messages } =
                    actor.handle_receive_for_session(7, 30, Some(1))
                else {
                    panic!("reserved message");
                };
                assert_eq!(
                    actor.handle_ack_for_session(7, messages[0].id, messages[0].token),
                    QueueResponse::Acked
                );
                actor.process_expired_timers();
                (actor.ready_len(), actor.admin_dead_letters().len())
            })
            .unwrap()
            .0
    });

    // Assert
    assert_eq!(counts, (0, 0));
}
