use super::*;
use crate::domains::stream::protocol::StreamMessage;
use crate::domains::stream::store::StreamStore;
use crate::domains::stream::{StreamClientFrame, StreamClientRequest};

fn begin_with_failing_fence(
    runtime: &mut super::super::model::StreamFamilyRuntime,
    family: RouteFamily,
    source: RouteAddress,
) -> Envelope {
    let route = Route::new("stream://acme/logs/failed-begin");
    let key = super::super::model::StreamResourceScope {
        family,
        realm: "acme".into(),
        area: "logs".into(),
        resource: "failed-begin".into(),
    };
    // Reproduce the request boundary after its constructor succeeded but
    // before BEGIN performs its fallible deletion-fence lookup.
    runtime.core.get_or_create_actor(&key).unwrap();
    StreamStore::fail_next_drop_cutoff_read_for_tests();
    let meta = crate::runtime::ClientFrameMeta::new(
        TEST_CLIENT_SESSION_ID,
        crate::runtime::ClientChannel::Pub,
        600,
        family,
    );
    let request = StreamClientRequest::new(
        meta,
        Ok(StreamClientFrame::Op(StreamMessage::Begin {
            family_id: family,
            route: route.clone(),
            ingest_metadata: None,
        })),
    );
    Envelope::from_route(source, RouteAddress::new(family, route), request)
}

#[test]
fn should_release_an_idle_actor_when_begin_fails_reading_its_drop_fence() {
    // Arrange
    let context = setup_test_context();
    let family = context.family;
    let source = context.source.clone();
    let retained = context
        .sink
        .inspect_family_for_tests(family, move |runtime| {
            let envelope = begin_with_failing_fence(runtime, family, source);

            // Act
            runtime.deliver_envelope(&envelope).unwrap();

            // Assert
            assert_eq!(runtime.core.session_owners.len(), 0);
            runtime.core.actors.len()
        });
    assert_eq!(retained, 0, "a rejected BEGIN must retain no idle actor");
}

#[test]
fn should_retain_no_failed_begin_actor_after_session_disconnect() {
    // Arrange
    let context = setup_test_context();
    let family = context.family;
    let source = context.source.clone();
    context
        .sink
        .inspect_family_for_tests(family, move |runtime| {
            let envelope = begin_with_failing_fence(runtime, family, source);
            runtime.deliver_envelope(&envelope).unwrap();
        });

    // Act
    context
        .sink
        .deliver(Envelope::new(
            RouteAddress::new(family, Route::new("stream://acme/logs/failed-begin")),
            crate::runtime::SessionCleanup {
                session_id: TEST_CLIENT_SESSION_ID,
            },
        ))
        .unwrap();

    // Assert
    let retained = context
        .sink
        .inspect_family_for_tests(family, |runtime| runtime.core.actors.len());
    assert_eq!(retained, 0);
    assert_eq!(context.sink.append_session_count(), 0);
}

#[test]
fn should_preserve_the_original_writer_when_another_begin_is_rejected() {
    // Arrange
    let context = setup_test_context();
    let route = "stream://acme/logs/active-writer";
    let original_session = begin_stream(&context, route);
    let (kind, payload) =
        extract_single_tlv_field(&build_stream_append(original_session, 0, b"staged"));
    assert_eq!(request(&context, route, kind, payload)[0], 0);
    let (other_source, other_inbox) =
        register_session_queue_sink(&context.router, context.family, 2);
    let (kind, payload) = extract_single_tlv_field(&build_stream_begin(route));

    // Act
    let rejected = request_from_session(
        &context,
        &other_source,
        &other_inbox,
        2,
        route,
        kind,
        payload,
    );

    // Assert
    assert_eq!(
        decode_stream_error_message(&rejected).unwrap(),
        "session already active"
    );
    assert_eq!(context.sink.append_session_count(), 1);
    let (kind, payload) = extract_single_tlv_field(&build_stream_commit(original_session, 1));
    assert_eq!(request(&context, route, kind, payload)[0], 0);
    let (kind, payload) = extract_single_tlv_field(&build_stream_read(route, 0));
    let response = request(&context, route, kind, payload);
    let page = decode_stream_read_payload(&decode_stream_success_data(&response));
    assert_eq!(page.records.len(), 1);
    assert_eq!(page.records[0].body.as_ref(), b"staged");
}
