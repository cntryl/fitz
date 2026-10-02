use super::*;
use crate::domains::stream::metrics::METRIC_WATERMARK_COORDINATION_DROPS_TOTAL;
use crate::domains::stream::sink::model::WatermarkCommit;
use crate::domains::stream::StreamReadItem;

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
                        area,
                        state.core.stream_store.clone(),
                        state.core.observability.durable_metrics(),
                    ),
                    crate::runtime::actor::Context::new(address, state.watermark_router.clone()),
                ),
            );
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
    let pending_commits = context
        .sink
        .inspect_family_for_tests(family, |state| state.pending_watermark_commits.len());
    assert_eq!(
        pending_commits,
        crate::runtime::FAMILY_ACTOR_NORMAL_LANE_CAPACITY
    );
}
