use super::*;
use crate::domains::stream::protocol::{
    BatchCommitted, StreamCoordinationMessage, StreamWriteMode,
};
use crate::prelude::Actor;

fn old_batch() -> StreamCoordinationMessage {
    StreamCoordinationMessage::BatchCommitted(BatchCommitted {
        first_area_offset: 0,
        last_area_offset: 0,
        first_realm_offset: 0,
        last_realm_offset: 0,
        first_global_offset: 0,
        last_global_offset: 0,
    })
}

#[test]
fn should_not_restore_an_area_watermark_from_a_pre_drop_coordinator() {
    // Arrange
    let context = setup_test_context();
    seed_committed_stream_route(&context, "stream://acme/logs/events", 1, b"old");
    let family = context.family;

    // Act
    let persisted = context
        .sink
        .inspect_family_for_tests(family, move |runtime| {
            runtime.service_watermark_timers();
            let key = super::super::model::StreamAreaScope {
                family,
                realm: "acme".into(),
                area: "logs".into(),
            };
            let (mut actor, mut actor_context) =
                runtime.watermark_coordinators.area.remove(&key).unwrap();
            let scope =
                crate::domains::stream::StreamDropScope::parse(family, "stream://acme/logs/*")
                    .unwrap();
            runtime
                .core
                .stream_store
                .drop_scope(&scope, StreamWriteMode::Sync)
                .unwrap();
            actor.receive(old_batch(), &mut actor_context);
            runtime
                .core
                .stream_store
                .get_persisted_area_watermark(family.as_u64(), "acme", "logs")
                .unwrap()
        });

    // Assert
    assert_eq!(persisted, None);
}

#[test]
fn should_not_restore_a_realm_watermark_from_a_pre_drop_coordinator() {
    // Arrange
    let context = setup_test_context();
    seed_committed_stream_route(&context, "stream://acme/logs/events", 1, b"old");
    let family = context.family;

    // Act
    let persisted = context
        .sink
        .inspect_family_for_tests(family, move |runtime| {
            runtime.service_watermark_timers();
            let key = super::super::model::StreamRealmScope {
                family,
                realm: "acme".into(),
            };
            let (mut actor, mut actor_context) =
                runtime.watermark_coordinators.realm.remove(&key).unwrap();
            let scope =
                crate::domains::stream::StreamDropScope::parse(family, "stream://acme/*").unwrap();
            runtime
                .core
                .stream_store
                .drop_scope(&scope, StreamWriteMode::Sync)
                .unwrap();
            actor.receive(old_batch(), &mut actor_context);
            runtime
                .core
                .stream_store
                .get_persisted_realm_watermark(family.as_u64(), "acme")
                .unwrap()
        });

    // Assert
    assert_eq!(persisted, None);
}

#[test]
fn should_preserve_queued_realm_coordination_when_an_area_is_dropped() {
    // Arrange
    let context = setup_test_context();
    seed_committed_stream_route(&context, "stream://acme/logs/events", 1, b"old");
    let family = context.family;

    // Act
    let targets = context
        .sink
        .inspect_family_for_tests(family, move |runtime| {
            let StreamCoordinationMessage::BatchCommitted(batch) = old_batch();
            for area in ["logs", "other"] {
                runtime.enqueue_watermark_commit(super::super::model::WatermarkCommit {
                    family,
                    realm: "acme".into(),
                    area: area.into(),
                    batch: batch.clone(),
                    targets: super::super::model::WatermarkTargets::both(),
                });
            }
            let request = crate::domains::stream::StreamDropRequest {
                scope: crate::domains::stream::StreamDropScope::parse(
                    family,
                    "stream://acme/logs/*",
                )
                .unwrap(),
                mode: StreamWriteMode::Sync,
            };
            let envelope = Envelope::new(
                RouteAddress::new(family, Route::new("stream://acme/logs/*")),
                (),
            );
            let meta = crate::runtime::ClientFrameMeta::new(
                TEST_CLIENT_SESSION_ID,
                crate::runtime::ClientChannel::Pub,
                610,
                family,
            );
            runtime.handle_drop_frame(&envelope, meta, None, &request);
            runtime
                .pending_watermark_commits
                .iter()
                .map(|commit| {
                    (
                        commit.area.clone(),
                        commit.targets.area,
                        commit.targets.realm,
                    )
                })
                .collect::<Vec<_>>()
        });

    // Assert
    assert_eq!(
        targets,
        vec![("logs".into(), false, true), ("other".into(), true, true)]
    );
}
