use super::*;
use crate::domains::stream::protocol::StreamWriteMode;
use crate::protocol::frame::ChannelId;
use crate::protocol::message::MessageType;

fn payload(route: &str, tail: &[u8]) -> Vec<u8> {
    let mut encoder = PayloadEncoder::new();
    encoder.put_string(route);
    let mut bytes = encoder.finish();
    bytes.extend_from_slice(tail);
    bytes
}

fn parse_drop(bytes: &[u8]) -> Result<StreamClientFrame, String> {
    let family = RouteFamily::new(41);
    let context = FrameContext::new(
        7,
        ChannelId::Pub,
        MessageType::new(610),
        bytes::Bytes::copy_from_slice(bytes),
        family,
    );
    parse_request(
        &context,
        bytes,
        family,
        SessionId(7),
        RouteAddress::new(family, Route::new("inbox://session/7")),
    )
}

#[test]
fn should_parse_stream_drop_with_explicit_persistence() {
    // Arrange
    let scopes = [
        (
            "stream://acme/*",
            crate::domains::stream::StreamDropTarget::Realm,
        ),
        (
            "acme/app/*",
            crate::domains::stream::StreamDropTarget::Area { area: "app".into() },
        ),
        (
            "stream://acme/app/orders",
            crate::domains::stream::StreamDropTarget::Resource {
                area: "app".into(),
                resource: "orders".into(),
            },
        ),
    ];

    // Act
    let results = scopes.map(|(route, target)| {
        (
            [0, 1].map(|mode| (mode, parse_drop(&payload(route, &[mode])))),
            target,
        )
    });

    // Assert
    for (choices, target) in results {
        for (mode, parsed) in choices {
            let StreamClientFrame::Drop(request) = parsed.unwrap() else {
                panic!("expected standalone DROP");
            };
            assert_eq!(request.scope.route_family, RouteFamily::new(41));
            assert_eq!(request.scope.realm, "acme");
            assert_eq!(request.scope.target, target);
            assert_eq!(request.mode, StreamWriteMode::decode(mode).unwrap());
        }
    }
}

#[test]
fn should_expand_stream_realm_drop_for_authorization() {
    // Arrange
    let bytes = payload("stream://acme/*", &[1]);

    // Act
    let route = extract_auth_route(610, &bytes);

    // Assert
    assert_eq!(route.unwrap().unwrap(), "stream://acme/*/*");
}

#[test]
fn should_encode_stream_drop_as_an_empty_ack() {
    // Arrange
    let response = StreamClientResponseBody::Ok {
        session_id: None,
        data: Vec::new(),
    };

    // Act
    let bytes = encode_response(610, &response);

    // Assert
    assert_eq!(bytes, [0]);
}

#[test]
fn should_reject_unsupported_stream_drop_selectors_before_authorization_or_dispatch() {
    // Arrange
    let routes = [
        "stream://*/*",
        "stream://acme/**",
        "stream://acme/*/*",
        "stream://acme/app/**",
        "stream://acme/*/orders",
        "stream://acme/app/ord*",
        "stream://acme/app/",
        "stream://acme//orders",
        "stream://acme/app/orders/extra",
        "queue://acme/app/orders",
        "stream://acme/a\0b/orders",
        "stream://acme/app",
        "stream://acme",
        "stream://acme/__realm__/*",
        "stream://acme/app/__area__",
    ];

    // Act
    let results = routes.map(|route| {
        let bytes = payload(route, &[1]);
        (
            route,
            parse_drop(&bytes),
            extract_auth_route(610, &bytes).map(|_| ()),
        )
    });

    // Assert
    for (route, parsed, extracted) in results {
        assert!(parsed.is_err(), "parser accepted {route:?}");
        assert!(extracted.is_err(), "authorization accepted {route:?}");
    }
}

#[test]
fn should_reject_malformed_stream_drop_before_authorization_or_dispatch() {
    // Arrange
    let frames = [
        payload("stream://acme/*", &[]),
        payload("stream://acme/*", &[2]),
        payload("stream://acme/*", &[1, 0]),
        vec![0, 0, 0, 10, b'a'],
    ];

    // Act
    let results = frames.map(|bytes| {
        (
            parse_drop(&bytes),
            extract_auth_route(610, &bytes).map(|_| ()),
        )
    });

    // Assert
    for (parsed, extracted) in results {
        assert!(parsed.is_err());
        assert!(extracted.is_err());
    }
}

#[test]
fn should_register_stream_drop_as_an_authorized_client_write() {
    // Arrange
    let message = MessageType::new(610);

    // Act
    let entry = crate::protocol::manifest::client_entry(message).unwrap();

    // Assert
    assert_eq!(entry.domain, "stream");
    assert_eq!(entry.route_scheme, Some("stream"));
    assert_eq!(
        entry.decoder,
        crate::protocol::manifest::ManifestDecoder::Stream
    );
    assert_eq!(
        entry.authorization,
        crate::protocol::manifest::ManifestAuthorization::RouteWrite
    );
    assert_eq!(
        entry.correlation,
        crate::protocol::manifest::CorrelationPolicy::Required
    );
}
