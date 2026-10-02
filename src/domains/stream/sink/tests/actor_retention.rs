use super::*;

#[test]
fn should_not_retain_idle_actors_after_high_cardinality_reads() {
    // Arrange
    let context = setup_test_context();
    let family = context.family;

    // Act
    let retained = context
        .sink
        .inspect_family_for_tests(family, move |runtime| {
            for index in 0..256 {
                let route = Route::new(format!("stream://acme/jobs/resource-{index}"));
                runtime
                    .core
                    .encode_last_response_data(family, &route)
                    .expect("last");
                runtime
                    .core
                    .encode_metadata_response_data(family, &route)
                    .expect("metadata");
                runtime
                    .core
                    .encode_read_response_data(
                        crate::domains::stream::sink::model::StreamReadExecution {
                            family_id: family,
                            route: &route,
                            from_offset: 0,
                            limit: 10,
                            max_bytes: None,
                            filter: None,
                            cursor_fingerprint: None,
                            captured_watermark: None,
                        },
                    )
                    .expect("read");
            }
            runtime.core.actors.len()
        });

    // Assert
    assert_eq!(retained, 0);
}

#[test]
fn should_preserve_active_append_session_during_read_pressure() {
    // Arrange
    let context = setup_test_context();
    let route = "stream://acme/jobs/active";
    let session = begin_stream(&context, route);
    let frame = build_stream_append(session, 0, b"staged");
    let (kind, payload) = extract_single_tlv_field(&frame);
    let _ = request(&context, route, kind, payload);
    let family = context.family;

    // Act
    let active = context
        .sink
        .inspect_family_for_tests(family, move |runtime| {
            for index in 0..256 {
                runtime
                    .core
                    .encode_last_response_data(
                        family,
                        &Route::new(format!("stream://acme/jobs/pressure-{index}")),
                    )
                    .expect("read");
            }
            runtime
                .core
                .actors
                .values()
                .filter(|actor| actor.has_active_session())
                .count()
        });
    let frame = build_stream_commit(session, 1);
    let (kind, payload) = extract_single_tlv_field(&frame);
    let committed = request(&context, route, kind, payload);
    let read = stream_read_response(&context, route, 0, 10);

    // Assert
    assert_eq!(active, 1);
    assert_eq!(committed[0], 0);
    assert_eq!(read.records.len(), 1);
    assert_eq!(read.records[0].body.as_ref(), b"staged");
}

#[test]
fn should_reconstruct_committed_offsets_after_idle_actor_release() {
    // Arrange
    let context = setup_test_context();
    let route = "stream://acme/jobs/recreated";
    seed_committed_stream_route(&context, route, 1, b"first");
    let family = context.family;
    let retained = context
        .sink
        .inspect_family_for_tests(family, |runtime| runtime.core.actors.len());
    let session = begin_stream(&context, route);
    let frame = build_stream_append(session, 1, b"second");
    let (kind, payload) = extract_single_tlv_field(&frame);

    // Act
    let appended = request(&context, route, kind, payload);
    let frame = build_stream_commit(session, 1);
    let (kind, payload) = extract_single_tlv_field(&frame);
    let committed = request(&context, route, kind, payload);
    let read = stream_read_response(&context, route, 0, 10);

    // Assert
    assert_eq!(retained, 0);
    assert_eq!(appended[0], 0);
    assert_eq!(committed[0], 0);
    assert_eq!(read.records.len(), 2);
    assert_eq!(read.last_resource_offset, 1);
    assert_eq!(read.records[0].resource_offset, 0);
    assert_eq!(read.records[1].resource_offset, 1);
}
