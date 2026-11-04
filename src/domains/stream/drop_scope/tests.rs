use super::*;

#[test]
fn should_match_drop_scopes_only_within_their_route_family() {
    // Arrange
    let scope = StreamDropScope::parse(RouteFamily::new(41), "stream://acme/app/*").unwrap();

    // Act
    let matching = scope.matches_parts(RouteFamily::new(41), "acme", "app", "orders");
    let other_family = scope.matches_parts(RouteFamily::new(42), "acme", "app", "orders");
    let other_realm = scope.matches_parts(RouteFamily::new(41), "other", "app", "orders");
    let other_area = scope.matches_parts(RouteFamily::new(41), "acme", "other", "orders");

    // Assert
    assert!(matching);
    assert!(!other_family);
    assert!(!other_realm);
    assert!(!other_area);
}

#[test]
fn should_reject_invalid_components_in_typed_stream_drop_scopes() {
    // Arrange
    let targets = [
        StreamDropTarget::Area { area: "*".into() },
        StreamDropTarget::Area {
            area: String::new(),
        },
        StreamDropTarget::Area {
            area: "app\u{85}".into(),
        },
        StreamDropTarget::Area {
            area: super::super::INTERNAL_REALM_SEGMENT.into(),
        },
        StreamDropTarget::Resource {
            area: "app".into(),
            resource: "one/two".into(),
        },
        StreamDropTarget::Resource {
            area: "app".into(),
            resource: "\0".into(),
        },
        StreamDropTarget::Resource {
            area: "app".into(),
            resource: super::super::INTERNAL_AREA_SEGMENT.into(),
        },
    ];

    // Act
    let results = targets.map(|target| {
        StreamDropScope {
            route_family: RouteFamily::new(41),
            realm: "acme".into(),
            target,
        }
        .validate()
    });

    // Assert
    assert!(results.iter().all(Result::is_err));
}

#[test]
fn should_bound_stream_drop_by_its_expanded_authorization_route() {
    // Arrange
    let maximum = crate::utils::route_shape::MAX_ROUTE_BYTES;
    let realm = "r".repeat(maximum - "stream:///*/*".len());
    let valid = format!("{realm}/*");
    let oversized = format!("{realm}r/*");

    // Act
    let accepted = StreamDropScope::parse(RouteFamily::new(41), &valid);
    let rejected = StreamDropScope::parse(RouteFamily::new(41), &oversized);

    // Assert
    assert_eq!(accepted.unwrap().authorization_route().len(), maximum);
    assert!(rejected.is_err());
}
