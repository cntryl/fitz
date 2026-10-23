use super::*;
use crate::domains::stream::metrics::METRIC_WATERMARK_COORDINATION_DROPS_TOTAL;
use crate::domains::stream::sink::model::StreamFamilyRuntime;
use crate::domains::stream::sink::model::WatermarkCommit;
use crate::domains::stream::StreamReadItem;

fn fresh_area_coordinator(
    state: &mut StreamFamilyRuntime,
    family: RouteFamily,
    realm: &str,
    area: &str,
) {
    let scope = crate::domains::stream::sink::model::StreamAreaScope {
        family,
        realm: realm.to_owned(),
        area: area.to_owned(),
    };
    let address = crate::runtime::routing::RouteAddress::new(
        family,
        Route::new(format!(
            "stream://{realm}/{area}/{}",
            crate::domains::stream::INTERNAL_AREA_SEGMENT
        )),
    );
    state.watermark_coordinators.area.insert(
        scope,
        (
            crate::domains::stream::area_actor::AreaActor::new(
                family,
                realm.to_owned(),
                area.to_owned(),
                state.core.stream_store.clone(),
                state.core.observability.durable_metrics(),
            ),
            crate::runtime::actor::Context::new(address, state.watermark_router.clone()),
        ),
    );
}

fn fresh_realm_coordinator(state: &mut StreamFamilyRuntime, family: RouteFamily, realm: &str) {
    let scope = crate::domains::stream::sink::model::StreamRealmScope {
        family,
        realm: realm.to_owned(),
    };
    let address = crate::runtime::routing::RouteAddress::new(
        family,
        Route::new(format!(
            "stream://{realm}/{}",
            crate::domains::stream::INTERNAL_REALM_SEGMENT
        )),
    );
    state.watermark_coordinators.realm.insert(
        scope,
        (
            crate::domains::stream::realm_actor::RealmActor::new(
                family,
                realm.to_owned(),
                state.core.stream_store.clone(),
                state.core.observability.durable_metrics(),
            ),
            crate::runtime::actor::Context::new(address, state.watermark_router.clone()),
        ),
    );
}

/// A committed range that starts past the watermark cannot be folded in, so
/// the coordinator keeps it and is no longer idle.
fn gapped_batch() -> crate::domains::stream::protocol::BatchCommitted {
    crate::domains::stream::protocol::BatchCommitted {
        first_area_offset: 5,
        last_area_offset: 5,
        first_realm_offset: 5,
        last_realm_offset: 5,
        first_global_offset: 5,
        last_global_offset: 5,
    }
}

#[test]
fn should_evict_an_idle_area_coordinator_to_track_a_new_area_at_the_cap() {
    // Arrange
    let metrics = crate::observability::metrics::MetricsCollector::new();
    let context = setup_test_context_with_metrics(Some(&metrics));
    let drops_before = metrics.counter_get(METRIC_WATERMARK_COORDINATION_DROPS_TOTAL);
    let family = context.family;
    context.sink.inspect_family_for_tests(family, move |state| {
        for index in 0..crate::domains::stream::MAX_WATERMARK_COORDINATORS {
            fresh_area_coordinator(state, family, "capacity", &format!("area-{index}"));
        }
    });

    // Act
    seed_committed_stream_route(
        &context,
        "stream://capacity/area-overflow/resource",
        1,
        b"tracked",
    );

    // Assert
    assert_eq!(
        context
            .sink
            .get_watermark_for_tests(family, "capacity", "area-overflow")
            .expect("area watermark"),
        0
    );
    let (retained, tracks_overflow) = context.sink.inspect_family_for_tests(family, move |state| {
        let scope = crate::domains::stream::sink::model::StreamAreaScope {
            family,
            realm: "capacity".to_owned(),
            area: "area-overflow".to_owned(),
        };
        (
            state.watermark_coordinators.area.len(),
            state.watermark_coordinators.area.contains_key(&scope),
        )
    });
    assert_eq!(retained, crate::domains::stream::MAX_WATERMARK_COORDINATORS);
    assert!(
        tracks_overflow,
        "the new area must take an idle coordinator's slot"
    );
    assert_eq!(
        metrics.counter_get(METRIC_WATERMARK_COORDINATION_DROPS_TOTAL),
        drops_before
    );
}

#[test]
fn should_evict_an_idle_realm_coordinator_to_track_a_new_realm_at_the_cap() {
    // Arrange
    let metrics = crate::observability::metrics::MetricsCollector::new();
    let context = setup_test_context_with_metrics(Some(&metrics));
    let drops_before = metrics.counter_get(METRIC_WATERMARK_COORDINATION_DROPS_TOTAL);
    let family = context.family;
    context.sink.inspect_family_for_tests(family, move |state| {
        for index in 0..crate::domains::stream::MAX_WATERMARK_COORDINATORS {
            fresh_realm_coordinator(state, family, &format!("realm-{index}"));
        }
    });

    // Act
    seed_committed_stream_route(
        &context,
        "stream://realm-overflow/events/resource",
        1,
        b"tracked",
    );

    // Assert
    let (retained, tracks_overflow) = context.sink.inspect_family_for_tests(family, move |state| {
        let scope = crate::domains::stream::sink::model::StreamRealmScope {
            family,
            realm: "realm-overflow".to_owned(),
        };
        (
            state.watermark_coordinators.realm.len(),
            state.watermark_coordinators.realm.contains_key(&scope),
        )
    });
    assert_eq!(retained, crate::domains::stream::MAX_WATERMARK_COORDINATORS);
    assert!(
        tracks_overflow,
        "the new realm must take an idle coordinator's slot"
    );
    assert_eq!(
        metrics.counter_get(METRIC_WATERMARK_COORDINATION_DROPS_TOTAL),
        drops_before
    );
}

#[test]
fn should_keep_committed_area_history_visible_after_coordinator_limit() {
    // Arrange
    let metrics = crate::observability::metrics::MetricsCollector::new();
    let context = setup_test_context_with_metrics(Some(&metrics));
    let drops_before = metrics.counter_get(METRIC_WATERMARK_COORDINATION_DROPS_TOTAL);

    // Act
    for index in 0..=crate::domains::stream::MAX_WATERMARK_COORDINATORS {
        let route = format!("stream://capacity/area-{index}/resource");
        seed_committed_stream_route(&context, &route, 1, b"committed");
    }
    let family = context.family;
    context.sink.inspect_family_for_tests(family, move |state| {
        // Seeding above may already have evicted idle coordinators; start from
        // a known full set of busy ones.
        state.watermark_coordinators.area.clear();
        for index in 0..crate::domains::stream::MAX_WATERMARK_COORDINATORS {
            let area = format!("area-{index}");
            let scope = crate::domains::stream::sink::model::StreamAreaScope {
                family,
                realm: "capacity".to_owned(),
                area: area.clone(),
            };
            let address = crate::runtime::routing::RouteAddress::new(
                family,
                Route::new(format!(
                    "stream://capacity/{area}/{}",
                    crate::domains::stream::INTERNAL_AREA_SEGMENT
                )),
            );
            state.watermark_coordinators.area.insert(
                scope,
                (
                    crate::domains::stream::area_actor::AreaActor::new(
                        family,
                        "capacity".to_owned(),
                        area.clone(),
                        state.core.stream_store.clone(),
                        state.core.observability.durable_metrics(),
                    ),
                    crate::runtime::actor::Context::new(address, state.watermark_router.clone()),
                ),
            );
            // Hold an unfoldable range so no coordinator is idle and the cap
            // must drop rather than evict.
            state.notify_area_batch_committed(family, "capacity", &area, &gapped_batch());
        }
        state.notify_area_batch_committed(
            family,
            "capacity",
            "area-overflow",
            &crate::domains::stream::protocol::BatchCommitted {
                first_area_offset: 0,
                last_area_offset: 0,
                first_realm_offset: 0,
                last_realm_offset: 0,
                first_global_offset: 0,
                last_global_offset: 0,
            },
        );
    });

    // Assert
    let areas_with_history = (0..=crate::domains::stream::MAX_WATERMARK_COORDINATORS)
        .filter(|index| {
            context
                .sink
                .read_area_records_for_tests(
                    context.family,
                    "capacity",
                    &format!("area-{index}"),
                    0,
                    1,
                )
                .expect("read committed area history")
                .iter()
                .any(|item| matches!(item, StreamReadItem::Event(_)))
        })
        .count();
    assert_eq!(
        areas_with_history,
        crate::domains::stream::MAX_WATERMARK_COORDINATORS + 1
    );
    let retained_coordinators = context
        .sink
        .inspect_family_for_tests(context.family, |state| {
            state.watermark_coordinators.area.len()
        });
    assert_eq!(
        retained_coordinators,
        crate::domains::stream::MAX_WATERMARK_COORDINATORS
    );
    assert!(
        metrics.counter_get(METRIC_WATERMARK_COORDINATION_DROPS_TOTAL) > drops_before,
        "coordinator count {retained_coordinators}, metric before {drops_before}, after {}",
        metrics.counter_get(METRIC_WATERMARK_COORDINATION_DROPS_TOTAL)
    );
}

#[test]
fn should_keep_committed_history_visible_after_pending_coordination_queue_overflow() {
    // Arrange
    let metrics = crate::observability::metrics::MetricsCollector::new();
    let context = setup_test_context_with_metrics(Some(&metrics));
    let drops_before = metrics.counter_get(METRIC_WATERMARK_COORDINATION_DROPS_TOTAL);
    let family = context.family;

    // Act
    context.sink.inspect_family_for_tests(family, move |state| {
        for _ in 0..crate::runtime::FAMILY_ACTOR_NORMAL_LANE_CAPACITY {
            state.enqueue_watermark_commit(WatermarkCommit {
                family,
                realm: "capacity".to_owned(),
                area: "overflow".to_owned(),
                batch: crate::domains::stream::protocol::BatchCommitted {
                    first_area_offset: 0,
                    last_area_offset: 0,
                    first_realm_offset: 0,
                    last_realm_offset: 0,
                    first_global_offset: 0,
                    last_global_offset: 0,
                },
            });
        }
        state.enqueue_watermark_commit(WatermarkCommit {
            family,
            realm: "capacity".to_owned(),
            area: "dropped".to_owned(),
            batch: crate::domains::stream::protocol::BatchCommitted {
                first_area_offset: 1,
                last_area_offset: 1,
                first_realm_offset: 1,
                last_realm_offset: 1,
                first_global_offset: 1,
                last_global_offset: 1,
            },
        });
    });
    seed_committed_stream_route(
        &context,
        "stream://capacity/overflow/resource",
        1,
        b"durable",
    );

    // Assert
    assert!(context
        .sink
        .read_area_records_for_tests(family, "capacity", "overflow", 0, 1)
        .expect("read committed area history")
        .iter()
        .any(|item| matches!(item, StreamReadItem::Event(_))));
    assert!(metrics.counter_get(METRIC_WATERMARK_COORDINATION_DROPS_TOTAL) > drops_before);
    // The commit delivery services coordination, so the overflowed backlog is
    // drained on the request path instead of waiting for an idle tick.
    let pending_commits = context
        .sink
        .inspect_family_for_tests(family, |state| state.pending_watermark_commits.len());
    assert_eq!(pending_commits, 0);
}
