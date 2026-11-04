use super::*;

fn payload(route: &str) -> Vec<u8> {
    let mut encoder = crate::protocol::payload_codec::PayloadEncoder::new();
    encoder.put_string(route);
    encoder.put_u8(1);
    encoder.finish()
}

#[test]
fn should_require_write_permission_covering_the_entire_kv_drop_scope() {
    // Arrange
    let cases = [
        ("kv://acme/*", "kv://acme/*/*#write", true),
        ("kv://acme/*", "kv://acme/**#write", true),
        ("kv://acme/*", "kv://acme/*#write", false),
        ("kv://acme/*", "kv://acme/app/*#write", false),
        ("kv://acme/*", "kv://other/**#write", false),
        ("kv://acme/app/*", "kv://acme/app/*#write", true),
        ("kv://acme/app/*", "kv://acme/app/orders#write", false),
        ("kv://acme/app/*", "kv://acme/app/*#read", false),
        ("kv://acme/app/orders", "kv://acme/app/orders#write", true),
        ("kv://acme/app/orders", "kv://acme/app/other#write", false),
    ];

    // Act
    let results = cases.map(|(route, permission, expected)| {
        let bytes = payload(route);
        let (targets, access) = RuntimeIngress::resolve_authorization_targets(
            DispatchDomain::Kv,
            MessageType::new(112),
            &bytes,
            auth_spec(112).policy,
        )
        .expect("valid DROP authorization target");
        assert_eq!(access, Access::Write);
        let actor = authenticated_actor(permission);
        (
            targets.authorize(&actor, access, DispatchDomain::Kv).0,
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
fn should_expand_realm_drop_authorization_to_concrete_kv_depth() {
    // Arrange
    let bytes = payload("acme/*");

    // Act
    let route = extract_auth_route_for_domain(DispatchDomain::Kv, 112, &bytes)
        .unwrap()
        .unwrap();

    // Assert
    assert_eq!(route, "kv://acme/*/*");
}

#[test]
fn should_keep_drop_shorthand_out_of_permission_route_canonicalization() {
    // Arrange
    let route = "kv://acme/*";
    let bytes = payload(route);

    // Act
    let permission = RuntimeIngress::canonicalize_domain_route_str(DispatchDomain::Kv, route);
    let operation = extract_auth_route_for_domain(DispatchDomain::Kv, 112, &bytes);

    // Assert
    assert!(permission.is_err());
    assert_eq!(operation.unwrap().unwrap(), "kv://acme/*/*");
}
