use super::*;

#[test]
fn should_preserve_independent_family_and_realm_in_domain_prompt_scope() {
    // Arrange
    let arguments = serde_json::json!({
        "route_family": "41",
        "realm": "operations",
        "area": "jobs",
        "resource": "dispatch",
        "limit": "12"
    })
    .as_object()
    .unwrap()
    .clone();

    // Act
    let scope = parse_resource_prompt_arguments(&arguments, PromptKind::Domain("queue"))
        .expect("valid Queue diagnostic arguments");

    // Assert
    assert_eq!(scope.route_family, Some(41));
    assert_eq!(scope.resource.scheme, "queue");
    assert_eq!(scope.resource.realm, "operations");
    assert_eq!(scope.resource.limit, Some(12));
}

#[test]
fn should_reject_prompt_scope_with_route_family_outside_supported_range() {
    // Arrange
    let arguments = serde_json::json!({
        "route_family": "4294967296",
        "realm": "operations",
        "area": "jobs",
        "resource": "dispatch"
    })
    .as_object()
    .unwrap()
    .clone();

    // Act
    let result = parse_resource_prompt_arguments(&arguments, PromptKind::Domain("queue"));

    // Assert
    assert!(result.is_err());
}

#[test]
fn should_reject_unknown_arguments_in_domain_prompt_scope() {
    // Arrange
    let arguments = serde_json::json!({
        "route_family": "41",
        "realm": "operations",
        "area": "jobs",
        "resource": "dispatch",
        "scheme": "queue"
    })
    .as_object()
    .unwrap()
    .clone();

    // Act
    let result = parse_resource_prompt_arguments(&arguments, PromptKind::Domain("queue"));

    // Assert
    assert!(result.is_err());
}
