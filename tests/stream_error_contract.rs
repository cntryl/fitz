use fitz::domains::stream::StreamClientResponseBody;
use fitz::protocol::{payload_codec::PayloadDecoder, stream_codec::encode_response};

#[test]
fn should_encode_append_conflict_with_versioned_domain_code() {
    // Arrange
    let response = StreamClientResponseBody::Error("concurrency conflict".into());

    // Act
    let payload = encode_response(601, &response);

    // Assert
    let mut decoder = PayloadDecoder::new(&payload);
    assert_eq!(decoder.get_u8().unwrap(), 2);
    assert_eq!(decoder.get_u32().unwrap(), 2001);
    assert_eq!(decoder.get_string().unwrap(), "concurrency conflict");
    assert!(decoder.is_complete());
}

#[test]
fn should_encode_commit_conflict_with_versioned_domain_code() {
    // Arrange
    let response = StreamClientResponseBody::Error("ERR_CONCURRENCY_CONFLICT".into());

    // Act
    let payload = encode_response(602, &response);

    // Assert
    let mut decoder = PayloadDecoder::new(&payload);
    assert_eq!(decoder.get_u8().unwrap(), 2);
    assert_eq!(decoder.get_u32().unwrap(), 2001);
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
    let mut decoder = PayloadDecoder::new(&payload);
    assert_eq!(decoder.get_u8().unwrap(), 2);
    assert_eq!(decoder.get_u32().unwrap(), 2012);
}

#[test]
fn should_encode_real_store_commit_conflict_with_domain_code() {
    // Arrange
    use fitz::domains::stream::protocol::StreamWriteMode;
    use fitz::domains::stream::StreamActor;
    use fitz::runtime::routing::RouteFamily;
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
    let payload = encode_response(602, &StreamClientResponseBody::Error(error));

    // Assert
    let mut decoder = PayloadDecoder::new(&payload);
    assert_eq!(decoder.get_u8().unwrap(), 2);
    assert_eq!(decoder.get_u32().unwrap(), 2001);
    assert!(second.has_active_session());
}

#[test]
fn should_preserve_explicit_codes_for_every_stream_operation() {
    // Arrange
    use fitz::protocol::payload_codec::PayloadEncoder;
    use fitz::protocol::stream_codec::encode_error_response_into;
    let cases = [
        (2001, "unrelated wording"),
        (2002, "concurrency conflict"),
        (2012, "backend unavailable"),
    ];

    // Act
    for operation in 600..=608 {
        for (code, message) in cases {
            let payload =
                encode_error_response_into(&mut PayloadEncoder::new(), operation, code, message);

            // Assert
            let mut decoder = PayloadDecoder::new(&payload);
            assert_eq!(
                decoder.get_u8().unwrap(),
                if operation == 604 { 1 } else { 2 }
            );
            assert_eq!(decoder.get_u32().unwrap(), u32::from(code));
            assert_eq!(decoder.get_string().unwrap(), message);
            assert!(decoder.is_complete());
        }
    }
}
