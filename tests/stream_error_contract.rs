use fitz::domains::stream::StreamClientResponseBody;
use fitz::protocol::error_codes::{decode_wire_error, stream};
use fitz::protocol::stream_codec::encode_response;
use fitz::protocol::wire2::envelope::{Effect, ErrorClass};

#[test]
fn should_encode_append_conflict_with_the_domain_code() {
    // Arrange
    let response = StreamClientResponseBody::Error("concurrency conflict".into());

    // Act
    let payload = encode_response(601, &response);

    // Assert
    let error = decode_wire_error(&payload).expect("error envelope");
    assert_eq!(error.code, stream::ERR_CONCURRENCY_CONFLICT);
    assert_eq!(error.class, ErrorClass::Domain);
    assert_eq!(error.effect, Effect::NotApplied);
    assert_eq!(error.message, "concurrency conflict");
}

#[test]
fn should_encode_commit_conflict_with_the_domain_code() {
    // Arrange
    let response = StreamClientResponseBody::Error("ERR_CONCURRENCY_CONFLICT".into());

    // Act
    let payload = encode_response(602, &response);

    // Assert
    assert_eq!(
        decode_wire_error(&payload).expect("error envelope").code,
        stream::ERR_CONCURRENCY_CONFLICT
    );
}

#[test]
fn should_not_classify_infrastructure_failure_by_incidental_conflict_wording() {
    // Arrange
    let response = StreamClientResponseBody::Error(
        "backend concurrency conflict diagnostic unavailable".into(),
    );

    // Act
    let payload = encode_response(602, &response);

    // Assert
    let error = decode_wire_error(&payload).expect("error envelope");
    assert_eq!(error.code, stream::ERR_BACKEND_ERROR);
    assert_eq!(error.class, ErrorClass::Backend);
    assert_eq!(error.effect, Effect::Indeterminate);
}

#[test]
fn should_encode_real_store_commit_conflict_with_domain_code() {
    // Arrange
    use fitz::domains::stream::protocol::StreamWriteMode;
    use fitz::runtime::routing::RouteFamily;
    use fitz::testkit::domain_internals::stream::StreamActor;
    use std::sync::Arc;
    let store = Arc::new(fitz::testkit::create_test_store());
    let make_actor = || {
        StreamActor::new(
            RouteFamily::new(1),
            "test".into(),
            "conflict".into(),
            "commit".into(),
            store.clone(),
        )
        .unwrap()
    };
    // The broker serializes live writers per resource. Two actors over the same
    // real store exercise its commit-time OCC guard without relaxing that rule.
    let mut first = make_actor();
    let mut second = make_actor();
    for actor in [&mut first, &mut second] {
        actor.begin_append_session(1, 1, None).unwrap();
        actor
            .append_to_session_with_discriminator_for_owner(
                1,
                1,
                0,
                bytes::Bytes::from_static(b"event"),
                None,
                None,
            )
            .unwrap();
    }
    first
        .commit_session_for_owner(1, 1, StreamWriteMode::Buffered)
        .unwrap();

    // Act
    let error = second
        .commit_session_for_owner(1, 1, StreamWriteMode::Buffered)
        .unwrap_err();
    let payload = encode_response(602, &StreamClientResponseBody::Error(error.into()));

    // Assert
    assert_eq!(
        decode_wire_error(&payload).expect("error envelope").code,
        stream::ERR_CONCURRENCY_CONFLICT
    );
    assert!(second.has_active_session());
}

#[test]
fn should_use_the_common_envelope_for_every_stream_operation() {
    // Arrange
    use fitz::protocol::payload_codec::PayloadEncoder;
    use fitz::protocol::stream_codec::encode_error_response_into;
    let cases = [
        (stream::ERR_CONCURRENCY_CONFLICT, "unrelated wording"),
        (stream::ERR_SESSION_ALREADY_ACTIVE, "concurrency conflict"),
        (stream::ERR_BACKEND_ERROR, "backend unavailable"),
    ];

    // Act
    for operation in 600..=608 {
        for (code, message) in cases {
            let payload =
                encode_error_response_into(&mut PayloadEncoder::new(), operation, code, message);

            // Assert
            assert_eq!(payload[0], 1, "status byte for operation {operation}");
            let error = decode_wire_error(&payload).expect("error envelope");
            assert_eq!(error.code, code);
            assert_eq!(error.message, message);
        }
    }
}
