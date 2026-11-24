use super::*;

#[test]
fn should_parse_concrete_schedule_route_given_valid_route() {
    // Arrange
    let input = "schedule://acme/billing/invoice/send";

    // Act
    let route = parse_concrete_schedule_route(input).unwrap();

    // Assert
    assert_eq!(route.realm, "acme");
    assert_eq!(route.area, "billing");
    assert_eq!(route.resource, "invoice");
    assert_eq!(route.operation, "send");
}

#[test]
fn should_preserve_whitespace_only_schedule_route_segments_for_wire_compatibility() {
    // Arrange
    let input = "schedule://acme/billing/ /send";

    // Act
    let route = parse_concrete_schedule_route(input);

    // Assert
    assert_eq!(
        route
            .expect("wire route acceptance remains unchanged")
            .resource,
        " "
    );
}

#[test]
fn should_reject_concrete_schedule_route_given_missing_operation() {
    // Arrange
    let input = "schedule://acme/billing/invoice";

    // Act
    let err = parse_concrete_schedule_route(input).unwrap_err();

    // Assert
    assert_eq!(
        err,
        "schedule route must be schedule://{realm}/{area}/{resource}/{operation}"
    );
}

#[test]
fn should_reject_concrete_schedule_route_given_wildcard() {
    let err = parse_concrete_schedule_route("schedule://acme/billing/*/send").unwrap_err();

    assert_eq!(err, "schedule route must not contain wildcards");
}
