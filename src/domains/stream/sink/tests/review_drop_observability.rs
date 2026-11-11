use super::*;

#[test]
fn should_forget_only_reset_area_metrics_after_stream_drop() {
    // Arrange
    let context = setup_test_context();
    seed_committed_stream_route(&context, "stream://acme/logs/events", 4, b"old");
    seed_committed_stream_route(&context, "stream://acme/audit/events", 2, b"retained");
    context.sink.sync_admin_snapshot();
    let metrics = context.sink.config.durable_metrics.clone();
    let before = metrics.snapshot();
    assert!(before
        .area_watermarks
        .iter()
        .any(|item| item.area == "logs"));

    // Act
    assert_eq!(
        drop::drop_stream(&context, "stream://acme/logs/*").as_ref(),
        &[0]
    );
    let after = metrics.snapshot();

    // Assert
    assert!(!after.area_watermarks.iter().any(|item| item.area == "logs"));
    assert_eq!(after.realm_watermarks, before.realm_watermarks);
    assert_eq!(
        after
            .area_watermarks
            .iter()
            .find(|item| item.area == "audit"),
        before
            .area_watermarks
            .iter()
            .find(|item| item.area == "audit")
    );
}

#[test]
fn should_forget_reset_realm_metrics_after_stream_drop() {
    // Arrange
    let context = setup_test_context();
    seed_committed_stream_route(&context, "stream://acme/logs/events", 4, b"old");
    seed_committed_stream_route(&context, "stream://other/logs/events", 2, b"retained");
    context.sink.sync_admin_snapshot();
    let metrics = context.sink.config.durable_metrics.clone();

    // Act
    assert_eq!(
        drop::drop_stream(&context, "stream://acme/*").as_ref(),
        &[0]
    );
    let after = metrics.snapshot();

    // Assert
    assert!(!after
        .realm_watermarks
        .iter()
        .any(|item| item.realm == "acme"));
    assert!(!after
        .area_watermarks
        .iter()
        .any(|item| item.realm == "acme"));
    assert!(after
        .realm_watermarks
        .iter()
        .any(|item| item.realm == "other"));
    assert!(after
        .area_watermarks
        .iter()
        .any(|item| item.realm == "other"));
}

#[test]
fn should_not_restore_dropped_metric_watermarks_from_another_family_projection() {
    // Arrange
    let context = setup_test_context();
    seed_committed_stream_route(&context, "stream://acme/logs/events", 4, b"old");
    context.sink.sync_admin_snapshot();
    let metrics = context.sink.config.durable_metrics.clone();
    assert_eq!(
        drop::drop_stream(&context, "stream://acme/*").as_ref(),
        &[0]
    );

    // Act
    context
        .sink
        .inspect_family_for_tests(RouteFamily::new(2), |runtime| {
            runtime.core.sync_admin_snapshot();
        });
    let after = metrics.snapshot();

    // Assert
    assert!(!after
        .realm_watermarks
        .iter()
        .any(|item| item.realm == "acme"));
    assert!(!after
        .area_watermarks
        .iter()
        .any(|item| item.realm == "acme"));
}

#[test]
fn should_preserve_ancestor_metrics_after_a_resource_drop() {
    // Arrange
    let context = setup_test_context();
    let route = "stream://acme/logs/events";
    seed_committed_stream_route(&context, route, 4, b"old");
    context.sink.sync_admin_snapshot();
    let metrics = context.sink.config.durable_metrics.clone();
    let before = metrics.snapshot();

    // Act
    assert_eq!(drop::drop_stream(&context, route).as_ref(), &[0]);
    context.sink.run_maintenance_slice_for_tests(context.family);
    context.sink.refresh_admin_snapshot_if_dirty();
    let after = metrics.snapshot();

    // Assert
    assert_eq!(after.realm_watermarks, before.realm_watermarks);
    assert_eq!(after.area_watermarks, before.area_watermarks);
}

#[test]
fn should_forget_watermark_metrics_when_a_late_drop_is_observed_by_maintenance() {
    // Arrange
    let context = setup_test_context();
    seed_committed_stream_route(&context, "stream://acme/logs/events", 4, b"old");
    context.sink.sync_admin_snapshot();
    let metrics = context.sink.config.durable_metrics.clone();
    let scope =
        crate::domains::stream::StreamDropScope::parse(context.family, "stream://acme/*").unwrap();
    context
        .sink
        .config
        .stream_store
        .drop_scope(
            &scope,
            crate::domains::stream::protocol::StreamWriteMode::Sync,
        )
        .unwrap();

    // Act
    context.sink.run_maintenance_slice_for_tests(context.family);
    context.sink.refresh_admin_snapshot_if_dirty();
    let after = metrics.snapshot();

    // Assert
    assert!(!after
        .realm_watermarks
        .iter()
        .any(|item| item.realm == "acme"));
    assert!(!after
        .area_watermarks
        .iter()
        .any(|item| item.realm == "acme"));
    assert!(context.admin_read_model.streams(None).is_empty());
}

#[test]
fn should_keep_cached_inventory_refresh_nonblocking_between_cleanup_slices() {
    // Arrange
    let context = setup_test_context();
    let events = vec![
        crate::domains::stream::store::EventPayload {
            body: Bytes::from_static(b"old"),
            metadata: None,
            discriminator: None,
        };
        130
    ];
    context
        .sink
        .config
        .stream_store
        .commit_records(crate::domains::stream::store::CommitRecordsParams {
            family: context.family.as_u64(),
            realm: "acme",
            area: "logs",
            resource: "events",
            expected_resource_next_offset: 0,
            events: &events,
            ingest_metadata: None,
            mode: crate::domains::stream::protocol::StreamWriteMode::Sync,
        })
        .unwrap();
    for _ in 0..256 {
        if !context
            .sink
            .config
            .stream_store
            .has_pending_maintenance(context.family.as_u64())
        {
            break;
        }
        context
            .sink
            .config
            .stream_store
            .run_maintenance(context.family.as_u64())
            .unwrap();
    }
    assert!(!context
        .sink
        .config
        .stream_store
        .has_pending_maintenance(context.family.as_u64()));
    assert_eq!(
        drop::drop_stream(&context, "stream://acme/*").as_ref(),
        &[0]
    );
    context.sink.run_maintenance_slice_for_tests(context.family);
    context.sink.refresh_admin_snapshot_if_dirty();
    context.sink.run_maintenance_slice_for_tests(context.family);
    let (entered_tx, entered_rx) = crossbeam_channel::bounded(1);
    let (release_tx, release_rx) = crossbeam_channel::bounded(1);
    context
        .sink
        .block_family_actor_for_tests(context.family, entered_tx, release_rx);
    entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let sink = context.sink.clone();
    let (done_tx, done_rx) = crossbeam_channel::bounded(1);

    // Act
    let refresh = std::thread::spawn(move || {
        sink.refresh_admin_snapshot_if_dirty();
        done_tx.send(()).unwrap();
    });
    let completed_while_blocked = done_rx.recv_timeout(Duration::from_millis(500)).is_ok();
    release_tx.send(()).unwrap();
    refresh.join().unwrap();

    // Assert
    assert!(
        completed_while_blocked,
        "unchanged cleanup invalidated the cached inventory"
    );
}

#[test]
fn should_observe_a_late_empty_area_drop_without_payload_reclamation() {
    // Arrange
    let context = setup_test_context();
    let route = "stream://acme/logs/events";
    seed_committed_stream_route(&context, route, 4, b"old");
    assert_eq!(drop::drop_stream(&context, route).as_ref(), &[0]);
    for _ in 0..32 {
        if !context
            .sink
            .config
            .stream_store
            .has_pending_maintenance(context.family.as_u64())
        {
            break;
        }
        context.sink.run_maintenance_slice_for_tests(context.family);
    }
    context.sink.sync_admin_snapshot();
    let metrics = context.sink.config.durable_metrics.clone();
    assert!(metrics
        .snapshot()
        .area_watermarks
        .iter()
        .any(|item| item.area == "logs"));
    let scope =
        crate::domains::stream::StreamDropScope::parse(context.family, "stream://acme/logs/*")
            .unwrap();
    context
        .sink
        .config
        .stream_store
        .drop_scope(
            &scope,
            crate::domains::stream::protocol::StreamWriteMode::Sync,
        )
        .unwrap();
    assert!(!context
        .sink
        .config
        .stream_store
        .has_pending_maintenance(context.family.as_u64()));

    // Act
    context.sink.run_maintenance_slice();
    context
        .sink
        .inspect_family_for_tests(context.family, |_| ());
    context.sink.refresh_admin_snapshot_if_dirty();
    let after = metrics.snapshot();

    // Assert
    assert!(!after
        .area_watermarks
        .iter()
        .any(|item| item.realm == "acme" && item.area == "logs"));
    assert_eq!(
        after
            .realm_watermarks
            .iter()
            .find(|item| item.realm == "acme")
            .unwrap()
            .watermark,
        3
    );
}
