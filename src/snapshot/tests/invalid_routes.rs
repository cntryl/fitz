use crate::runtime::routing::RouteFamily;
use crate::snapshot::{
    SnapshotArtifact, SnapshotDomain, SnapshotKvResource, SnapshotSelector, SnapshotStreamResource,
};

fn kv_artifact() -> SnapshotArtifact {
    let selector = SnapshotSelector::new(SnapshotDomain::Kv, RouteFamily::new(7), "kv://acme/**")
        .expect("selector");
    SnapshotArtifact::from_kv_resources(
        &selector,
        vec![SnapshotKvResource {
            route: "kv://acme/jobs/orders".to_string(),
            entries: Vec::new(),
        }],
    )
    .expect("valid artifact")
}

fn stream_artifact() -> SnapshotArtifact {
    let selector = SnapshotSelector::new(
        SnapshotDomain::Stream,
        RouteFamily::new(7),
        "stream://acme/**",
    )
    .expect("selector");
    SnapshotArtifact::from_stream_resources(
        &selector,
        vec![SnapshotStreamResource {
            route: "stream://acme/jobs/orders".to_string(),
            captured_watermark: 0,
            records: Vec::new(),
        }],
    )
    .expect("valid artifact")
}

#[test]
fn should_reject_non_concrete_kv_resource_routes_on_construction() {
    // Arrange
    let selector = SnapshotSelector::new(SnapshotDomain::Kv, RouteFamily::new(7), "kv://**")
        .expect("selector");
    let routes = [
        "kv://*/jobs/orders",
        "kv://acme/*/orders",
        "kv://acme/jobs/*",
        "kv://acme/jobs/**",
        "kv://acme/jobs/order*",
        "kv://acme/jobs/ord\0ers",
        "kv://acme/jobs",
        "kv://acme/jobs/orders/extra",
    ];

    // Act
    let results = routes.map(|route| {
        SnapshotArtifact::from_kv_resources(
            &selector,
            vec![SnapshotKvResource {
                route: route.to_string(),
                entries: Vec::new(),
            }],
        )
    });

    // Assert
    for (route, result) in routes.into_iter().zip(results) {
        assert!(result.is_err(), "non-concrete route accepted: {route:?}");
    }
}

#[test]
fn should_reject_non_concrete_stream_resource_routes_on_construction() {
    // Arrange
    let selector =
        SnapshotSelector::new(SnapshotDomain::Stream, RouteFamily::new(7), "stream://**")
            .expect("selector");
    let routes = [
        "stream://*/jobs/orders",
        "stream://acme/*/orders",
        "stream://acme/jobs/*",
        "stream://acme/jobs/**",
        "stream://acme/jobs/order*",
        "stream://acme/jobs/ord\0ers",
        "stream://acme/jobs",
        "stream://acme/jobs/orders/extra",
    ];

    // Act
    let results = routes.map(|route| {
        SnapshotArtifact::from_stream_resources(
            &selector,
            vec![SnapshotStreamResource {
                route: route.to_string(),
                captured_watermark: 0,
                records: Vec::new(),
            }],
        )
    });

    // Assert
    for (route, result) in routes.into_iter().zip(results) {
        assert!(result.is_err(), "non-concrete route accepted: {route:?}");
    }
}

#[test]
fn should_reject_checksum_valid_kv_artifact_with_wildcard_resource_route() {
    // Arrange
    let bytes =
        crate::snapshot::test_support::with_resource_route(&kv_artifact(), 0, "kv://acme/jobs/*");

    // Act
    let decoded = SnapshotArtifact::from_bytes(&bytes);

    // Assert
    assert!(
        decoded.is_err(),
        "checksum-valid resource route must be concrete"
    );
}

#[test]
fn should_reject_checksum_valid_stream_artifact_with_wildcard_resource_route() {
    // Arrange
    let bytes = crate::snapshot::test_support::with_resource_route(
        &stream_artifact(),
        0,
        "stream://acme/jobs/*",
    );

    // Act
    let decoded = SnapshotArtifact::from_bytes(&bytes);

    // Assert
    assert!(
        decoded.is_err(),
        "checksum-valid resource route must be concrete"
    );
}
