use super::*;

fn payload(route: &str) -> Vec<u8> {
    let mut encoder = crate::protocol::payload_codec::PayloadEncoder::new();
    encoder.put_string(route);
    encoder.put_u8(1);
    encoder.finish()
}

#[test]
fn should_require_one_write_grant_covering_the_entire_stream_drop_scope() {
    // Arrange
    let cases = [
        ("stream://acme/*", "stream://acme/*/*#write", true),
        ("stream://acme/*", "stream://acme/**#write", true),
        ("stream://acme/*", "stream://acme/*#write", false),
        ("stream://acme/*", "stream://acme/app/*#write", false),
        ("stream://acme/*", "stream://other/**#write", false),
        ("stream://acme/app/*", "stream://acme/app/*#write", true),
        (
            "stream://acme/app/*",
            "stream://acme/app/orders#write",
            false,
        ),
        ("stream://acme/app/*", "stream://acme/app/*#read", false),
        (
            "stream://acme/app/orders",
            "stream://acme/app/orders#write",
            true,
        ),
        (
            "stream://acme/app/orders",
            "stream://acme/app/other#write",
            false,
        ),
    ];

    // Act
    let results = cases.map(|(route, permission, expected)| {
        let bytes = payload(route);
        let (targets, access) = RuntimeIngress::resolve_authorization_targets(
            DispatchDomain::Stream,
            MessageType::new(610),
            &bytes,
            auth_spec(610).policy,
        )
        .expect("valid DROP authorization target");
        assert_eq!(access, Access::Write);
        let actor = authenticated_actor(permission);
        (
            targets.authorize(&actor, access, DispatchDomain::Stream).0,
            expected,
            route,
            permission,
        )
    });

    // Assert
    for (authorized, expected, route, permission) in results {
        assert_eq!(authorized, expected, "{route} with {permission}");
    }
}

#[test]
fn should_keep_stream_drop_shorthand_out_of_permission_route_canonicalization() {
    // Arrange
    let route = "stream://acme/*";
    let bytes = payload(route);

    // Act
    let permission = RuntimeIngress::canonicalize_domain_route_str(DispatchDomain::Stream, route);
    let operation = extract_auth_route_for_domain(DispatchDomain::Stream, 610, &bytes);

    // Assert
    assert!(permission.is_err());
    assert_eq!(operation.unwrap().unwrap(), "stream://acme/*/*");
}
