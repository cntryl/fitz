use crate::auth::{Access, Permission};

#[test]
fn should_parse_permission_with_access_fragment() {
    // Arrange
    let perm_str = "notice://prod/orders/**#read";

    // Act
    let perm = Permission::parse(perm_str).unwrap();

    // Assert
    assert_eq!(perm.raw, perm_str);
    assert!(matches!(perm.access, Access::Read));
}

#[test]
fn should_parse_permission_without_access_defaults_to_all() {
    // Arrange
    let perm_str = "notice://prod/orders/**";

    // Act
    let perm = Permission::parse(perm_str).unwrap();

    // Assert
    assert_eq!(perm.raw, perm_str);
    assert!(matches!(perm.access, Access::All));
}

#[test]
fn should_reject_invalid_access_level() {
    // Arrange
    let perm_str = "notice://prod/orders/**#invalid";

    // Act
    let result = Permission::parse(perm_str);

    // Assert
    assert!(result.is_err());
}

#[test]
fn should_reject_permission_route_over_segment_limit() {
    // Arrange
    let route = format!(
        "notice://{}#read",
        vec!["a"; crate::utils::route_shape::MAX_ROUTE_SEGMENTS + 1].join("/")
    );

    // Act
    let result = Permission::parse(&route);

    // Assert
    assert!(result.is_err());
}

#[test]
fn should_reject_permission_with_empty_path_segments() {
    // Arrange
    let permissions = [
        "queue://acme//orders/**#write",
        "queue:///orders/**#write",
        "queue://acme/orders/**/#write",
    ];

    // Act
    let results = permissions.map(Permission::parse);

    // Assert
    assert!(results.iter().all(Result::is_err));
}

#[test]
fn should_reject_whole_permission_claim_given_one_empty_segment_grant() {
    // Arrange
    use base64::Engine;
    let payload = serde_json::json!({
        "iss": "https://idp.example/",
        "aud": "fitz-broker",
        "sub": "user:42",
        "exp": 9_999_999_999_u64,
        "tid": "acme-prod",
        "permissions": [
            "notice://prod/orders/**#read",
            "kv://#read",
            "queue://prod/orders/**#write"
        ],
        "scope": "notice.read"
    });
    let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload.to_string());
    let raw = crate::auth::parse_jwt_noverify(&format!("{{}}.{b64}.sig")).expect("parse jwt");

    // Act
    let result = raw.normalize(
        &["https://idp.example/"],
        &["fitz-broker"],
        0,
        &crate::auth::AuthClaimsConfig::default(),
    );

    // Assert
    let error = result.expect_err("one malformed grant must reject the whole claim");
    assert!(error.contains("kv://#read"), "unexpected error: {error}");
}
