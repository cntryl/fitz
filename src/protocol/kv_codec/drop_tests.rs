use super::*;
use crate::dispatch::wire::kv::{KvDropTarget, KvMessage, KvResponse};
use crate::runtime::routing::RouteFamily;

fn payload(route: &str, tail: &[u8]) -> Vec<u8> {
    let mut encoder = crate::protocol::payload_codec::PayloadEncoder::new();
    encoder.put_string(route);
    let mut bytes = encoder.finish();
    bytes.extend_from_slice(tail);
    bytes
}

#[test]
fn should_parse_all_drop_scopes_with_explicit_persistence() {
    // Arrange
    let cases = [
        ("kv://acme/*", KvDropTarget::Realm),
        ("acme/app/*", KvDropTarget::Area { area: "app".into() }),
        (
            "kv://acme/app/orders",
            KvDropTarget::Resource {
                area: "app".into(),
                resource: "orders".into(),
            },
        ),
    ];

    // Act
    let messages = cases.map(|(route, expected)| {
        let choices = [0, 1].map(|choice| {
            parse_request(
                msg_type::DROP,
                RouteFamily::new(41),
                &payload(route, &[choice]),
            )
        });
        (choices, expected)
    });

    // Assert
    for (choices, expected) in messages {
        for message in choices {
            let KvMessage::Drop { scope, .. } = message.unwrap() else {
                panic!("expected DROP")
            };
            assert_eq!(scope.route_family, RouteFamily::new(41));
            assert_eq!(scope.realm, "acme");
            assert_eq!(scope.target, expected);
        }
    }
}

#[test]
fn should_encode_drop_success_as_an_empty_ack() {
    // Arrange
    let response = KvResponse::DropOk;
    // Act
    let bytes = encode_response(&response);
    // Assert
    assert_eq!(bytes, [0]);
}

#[test]
fn should_reject_drop_with_unsupported_selector_shapes() {
    // Arrange
    let routes = [
        "kv://*/*",
        "kv://acme/**",
        "kv://acme/*/*",
        "kv://acme/app/**",
        "kv://acme/app/ord*",
        "kv://acme/app/",
        "kv://acme//orders",
        "kv://acme/app/orders/extra",
        "queue://acme/app/orders",
        "kv://acme/a\0b/orders",
        "kv://acme/app",
        "kv://acme",
        "kv://acme/*/orders",
    ];

    // Act
    let results = routes.map(|route| {
        (
            route,
            parse_request(msg_type::DROP, RouteFamily::new(1), &payload(route, &[1])),
        )
    });

    // Assert
    for (route, result) in results {
        assert!(result.is_err(), "accepted {route:?}");
    }
}

#[test]
fn should_reject_malformed_drop_before_authorization_or_dispatch() {
    // Arrange
    let missing = payload("kv://acme/*", &[]);
    let invalid = payload("kv://acme/*", &[2]);
    let trailing = payload("kv://acme/*", &[1, 0]);
    let truncated = vec![0, 0, 0, 10, b'a'];

    // Act
    let results = [missing, invalid, trailing, truncated].map(|bytes| {
        (
            parse_request(msg_type::DROP, RouteFamily::new(1), &bytes),
            extract_auth_route(msg_type::DROP, &bytes).map(|_| ()),
        )
    });

    // Assert
    for (domain, ingress) in results {
        assert!(domain.is_err());
        assert!(ingress.is_err());
    }
}
