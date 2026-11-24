use crate::protocol::{
    kv_codec, lease_codec, notice_codec, queue_codec, rpc_codec, schedule_codec, stream_codec,
    PayloadEncoder,
};
use crate::utils::route_shape::{MAX_ROUTE_BYTES, MAX_ROUTE_SEGMENTS};
use std::borrow::Cow;

type Extractor = for<'a> fn(u16, &'a [u8]) -> Result<Option<Cow<'a, str>>, String>;

const CODECS: [(&str, u16, Extractor); 7] = [
    ("kv", 109, kv_codec::extract_auth_route),
    ("queue", 207, queue_codec::extract_auth_route),
    ("notice", 501, notice_codec::extract_auth_route),
    ("stream", 607, stream_codec::extract_auth_route),
    ("rpc", 301, rpc_codec::extract_auth_route),
    ("lease", 407, lease_codec::extract_auth_route),
    ("schedule", 703, schedule_codec::extract_auth_route),
];

fn payload(route: &str) -> Vec<u8> {
    let mut encoder = PayloadEncoder::new();
    encoder.put_string(route);
    encoder.finish()
}

#[test]
fn should_return_canonical_authorization_routes_from_every_codec() {
    // Arrange
    let bytes = payload("acme/app/orders");

    // Act
    let routes = CODECS
        .map(|(scheme, message, extract)| (scheme, extract(message, &bytes).unwrap().unwrap()));

    // Assert
    for (scheme, route) in routes {
        assert_eq!(route, format!("{scheme}://acme/app/orders"));
    }
}

#[test]
fn should_reject_foreign_schemes_before_codec_canonicalization() {
    // Arrange
    let bytes = payload("foreign://acme/app/orders");

    // Act
    let results = CODECS.map(|(scheme, message, extract)| (scheme, extract(message, &bytes)));

    // Assert
    for (scheme, result) in results {
        assert!(result.is_err(), "{scheme} accepted a foreign scheme");
    }
}

#[test]
fn should_reject_invalid_raw_route_shapes_before_codec_canonicalization() {
    // Arrange
    let routes = [
        "acme/app/orders/\0ignored".to_string(),
        format!("acme/app/orders/{}", "x".repeat(MAX_ROUTE_BYTES)),
        format!(
            "acme/app/orders/{}",
            vec!["x"; MAX_ROUTE_SEGMENTS].join("/")
        ),
    ];

    // Act
    let results = routes.map(|route| {
        let bytes = payload(&route);
        CODECS.map(|(scheme, message, extract)| (scheme, extract(message, &bytes).is_err()))
    });

    // Assert
    for cases in results {
        for (scheme, rejected) in cases {
            assert!(rejected, "{scheme} accepted an invalid raw route shape");
        }
    }
}

#[test]
fn should_reject_routes_that_exceed_bounds_after_codec_canonicalization() {
    // Arrange
    let path = format!("r/a/{}", "x".repeat(MAX_ROUTE_BYTES - 4));
    let bytes = payload(&path);

    // Act
    let results = CODECS.map(|(scheme, message, extract)| (scheme, extract(message, &bytes)));

    // Assert
    for (scheme, result) in results {
        assert!(
            result.is_err(),
            "{scheme} accepted an oversized canonical route"
        );
    }
}
