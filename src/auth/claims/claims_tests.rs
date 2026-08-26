use crate::auth::parse_jwt_noverify;
use crate::auth::{AuthClaimsConfig, RouteFamilyResolverConfig, DEFAULT_ROLE_CLAIM};
use base64::Engine;

#[test]
fn should_parse_jwt_noverify_extract_permissions() {
    // Arrange
    let payload = serde_json::json!({
        "iss": "https://idp.example/",
        "aud": "fitz-broker",
        "sub": "user:42",
        "exp": 9_999_999_999_u64,
        "tid": "acme-prod",
        "permissions": ["notice://prod/orders/**#read"]
    });
    let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload.to_string());
    let jwt = format!("{}.{}.{}", "{}", b64, "sig");

    // Act
    let claims = parse_jwt_noverify(&jwt).expect("parse jwt");
    let perms = claims
        .normalized_permissions(None, None, DEFAULT_ROLE_CLAIM)
        .expect("perms");

    // Assert
    assert_eq!(claims.iss, "https://idp.example/");
    assert_eq!(perms.len(), 1);
}

#[test]
fn should_normalize_raw_claims_into_immutable_claims() {
    // Arrange
    let payload = serde_json::json!({
        "iss": "https://idp.example/",
        "aud": "fitz-broker",
        "sub": "user:42",
        "exp": 9_999_999_999_u64,
        "tid": "acme-prod",
        "roles": ["admin"],
        "permissions": ["notice://prod/orders/**#read"]
    });
    let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload.to_string());
    let jwt = format!("{}.{}.{}", "{}", b64, "sig");

    // Act
    let raw = parse_jwt_noverify(&jwt).expect("parse jwt");
    let normalized = raw
        .normalize(
            &["https://idp.example/"],
            &["fitz-broker"],
            0,
            &AuthClaimsConfig::default(),
        )
        .expect("normalize");

    // Assert
    assert_eq!(normalized.identity_claim.as_deref(), Some("tid"));
    assert_eq!(normalized.identity_value.as_deref(), Some("acme-prod"));
    assert_eq!(normalized.permissions.len(), 1);
    assert_eq!(
        normalized.permissions[0].raw,
        "notice://prod/orders/**#read"
    );
    assert!(matches!(
        normalized.permissions[0].access,
        crate::auth::Access::Read
    ));
}

#[test]
fn should_validate_expired_token() {
    // Arrange
    let payload = serde_json::json!({
        "iss": "https://idp.example/",
        "aud": "fitz-broker",
        "sub": "user:42",
        "exp": 100u64,  // in the past
        "tid": "acme-prod",
    });
    let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload.to_string());
    let jwt = format!("{}.{}.{}", "{}", b64, "sig");

    // Act
    let raw = parse_jwt_noverify(&jwt).expect("parse jwt");
    let result = raw.normalize(
        &["https://idp.example/"],
        &["fitz-broker"],
        9_999_999_999,
        &AuthClaimsConfig::default(),
    );

    // Assert
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("expired"));
}

#[test]
fn should_reject_issuer_not_in_allowlist() {
    // Arrange
    let payload = serde_json::json!({
        "iss": "https://attacker.example/",
        "aud": "fitz-broker",
        "sub": "user:42",
        "exp": 9_999_999_999_u64,
        "tid": "acme-prod",
    });
    let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload.to_string());
    let jwt = format!("{}.{}.{}", "{}", b64, "sig");

    // Act
    let raw = parse_jwt_noverify(&jwt).expect("parse jwt");
    let result = raw.normalize(
        &["https://idp.example/"],
        &["fitz-broker"],
        0,
        &AuthClaimsConfig::default(),
    );

    // Assert
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("issuer"));
}

#[test]
fn should_reject_legacy_route_family_claim_by_default() {
    // Arrange
    let payload = serde_json::json!({
        "iss": "https://idp.example/",
        "aud": "fitz-broker",
        "sub": "user:42",
        "exp": 9_999_999_999_u64,
        "tid": "acme-prod",
        "fitz": { "route_family": 1 },
        "permissions": ["notice://prod/orders/**#read"]
    });
    let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload.to_string());
    let jwt = format!("{}.{}.{}", "{}", b64, "sig");

    // Act
    let raw = parse_jwt_noverify(&jwt).expect("parse jwt");
    let result = raw.normalize(
        &["https://idp.example/"],
        &["fitz-broker"],
        0,
        &AuthClaimsConfig::default(),
    );

    // Assert
    assert!(result
        .unwrap_err()
        .contains("fitz.route_family claim is not accepted"));
}

#[test]
fn should_reject_legacy_permissions_claim() {
    // Arrange
    let payload = serde_json::json!({
        "iss": "https://idp.example/",
        "aud": "fitz-broker",
        "sub": "user:42",
        "exp": 9_999_999_999_u64,
        "tid": "acme-prod",
        "fitz": { "permissions": ["notice://prod/orders/**#read"] },
        "permissions": ["notice://prod/orders/**#read"]
    });
    let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload.to_string());
    let jwt = format!("{}.{}.{}", "{}", b64, "sig");

    // Act
    let raw = parse_jwt_noverify(&jwt).expect("parse jwt");
    let result = raw.normalize(
        &["https://idp.example/"],
        &["fitz-broker"],
        0,
        &AuthClaimsConfig::default(),
    );

    // Assert
    assert!(result
        .unwrap_err()
        .contains("fitz.permissions claim is not accepted"));
}

#[test]
fn should_keep_identity_context_orthogonal_to_route_family_resolution() {
    // Arrange
    let payload = serde_json::json!({
        "iss": "https://idp.example/",
        "aud": "fitz-broker",
        "sub": "user:42",
        "exp": 9_999_999_999_u64,
        "tid": "realm-a",
        "permissions": ["notice://prod/orders/**#read"]
    });
    let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload.to_string());
    let jwt = format!("{}.{}.{}", "{}", b64, "sig");

    // Act
    let raw = parse_jwt_noverify(&jwt).expect("parse jwt");
    let normalized = raw
        .normalize(
            &["https://idp.example/"],
            &["fitz-broker"],
            0,
            &AuthClaimsConfig::default(),
        )
        .expect("normalize");
    let resolver = RouteFamilyResolverConfig::from_mappings("tid", [("realm-a", 7)]);
    let route_family = resolver.resolve(&raw).expect("resolve route family");

    // Assert
    assert_eq!(normalized.identity_value.as_deref(), Some("realm-a"));
    assert_eq!(route_family, 7);
}

#[test]
fn should_accept_audience_array_when_any_audience_matches() {
    // Arrange
    let payload = serde_json::json!({
        "iss": "https://idp.example/",
        "aud": ["fitz-broker", "https://idp.example/userinfo"],
        "sub": "user:42",
        "exp": 9_999_999_999_u64,
        "tid": "acme-prod",
        "permissions": ["notice://prod/orders/**#read"]
    });
    let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload.to_string());
    let jwt = format!("{}.{}.{}", "{}", b64, "sig");

    // Act
    let raw = parse_jwt_noverify(&jwt).expect("parse jwt");
    let normalized = raw
        .normalize(
            &["https://idp.example/"],
            &["fitz-broker"],
            0,
            &AuthClaimsConfig::default(),
        )
        .expect("normalize");

    // Assert
    assert_eq!(normalized.sub, "user:42");
}

#[test]
fn should_prefer_configured_custom_claim_permissions() {
    // Arrange
    let payload = serde_json::json!({
        "iss": "https://idp.example/",
        "aud": "fitz-broker",
        "sub": "user:42",
        "exp": 9_999_999_999_u64,
        "tid": "acme-prod",
        "permissions": ["notice://prod/orders/**#read"],
        "roles": ["notice.write"],
        "scp": "notice.read",
        "https://fitz.example.com/claims": {
            "permissions": ["notice://prod/orders/**#write"]
        }
    });
    let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload.to_string());
    let jwt = format!("{}.{}.{}", "{}", b64, "sig");

    // Act
    let raw = parse_jwt_noverify(&jwt).expect("parse jwt");
    let permissions = raw
        .normalized_permissions(
            Some("https://fitz.example.com/claims"),
            None,
            DEFAULT_ROLE_CLAIM,
        )
        .expect("permissions");

    // Assert
    assert_eq!(permissions[0].raw, "notice://prod/orders/**#write");
}

#[test]
fn should_prefer_top_level_permissions_over_role_claim() {
    // Arrange
    let payload = serde_json::json!({
        "iss": "https://idp.example/",
        "aud": "fitz-broker",
        "sub": "user:42",
        "exp": 9_999_999_999_u64,
        "tid": "acme-prod",
        "permissions": ["notice://prod/orders/**#read"],
        "roles": ["notice.write"]
    });
    let raw: crate::auth::RawClaims = serde_json::from_value(payload).expect("raw claims");

    // Act
    let permissions = raw
        .normalized_permissions(None, None, DEFAULT_ROLE_CLAIM)
        .expect("permissions");

    // Assert
    assert_eq!(permissions.len(), 1);
    assert_eq!(permissions[0].raw, "notice://prod/orders/**#read");
}

#[test]
fn should_prefer_role_claim_over_scp() {
    // Arrange
    let payload = serde_json::json!({
        "iss": "https://idp.example/",
        "aud": "fitz-broker",
        "sub": "user:42",
        "exp": 9_999_999_999_u64,
        "tid": "acme-prod",
        "roles": ["notice.write"],
        "scp": "notice.read"
    });
    let raw: crate::auth::RawClaims = serde_json::from_value(payload).expect("raw claims");

    // Act
    let permissions = raw
        .normalized_permissions(None, None, DEFAULT_ROLE_CLAIM)
        .expect("permissions");

    // Assert
    assert_eq!(permissions.len(), 1);
    assert_eq!(permissions[0].raw, "notice://**#write");
}

#[test]
fn should_prefer_scp_over_scope() {
    // Arrange
    let payload = serde_json::json!({
        "iss": "https://idp.example/",
        "aud": "fitz-broker",
        "sub": "user:42",
        "exp": 9_999_999_999_u64,
        "tid": "acme-prod",
        "scp": "notice.read",
        "scope": "notice.write"
    });
    let raw: crate::auth::RawClaims = serde_json::from_value(payload).expect("raw claims");

    // Act
    let permissions = raw
        .normalized_permissions(None, None, DEFAULT_ROLE_CLAIM)
        .expect("permissions");

    // Assert
    assert_eq!(permissions.len(), 1);
    assert_eq!(permissions[0].raw, "notice://**#read");
}

#[test]
fn should_support_entra_roles_shape() {
    // Arrange
    let payload = serde_json::json!({
        "iss": "https://login.microsoftonline.com/11111111-1111-1111-1111-111111111111/v2.0",
        "aud": "api://fitz",
        "sub": "service-principal-1",
        "exp": 9_999_999_999_u64,
        "tid": "11111111-1111-1111-1111-111111111111",
        "roles": ["queue.read"]
    });
    let raw: crate::auth::RawClaims = serde_json::from_value(payload).expect("raw claims");
    let resolver = RouteFamilyResolverConfig::from_mappings(
        "tid",
        [("11111111-1111-1111-1111-111111111111", 6)],
    );

    // Act
    let normalized = raw
        .normalize(
            &["https://login.microsoftonline.com/11111111-1111-1111-1111-111111111111/v2.0"],
            &["api://fitz"],
            0,
            &AuthClaimsConfig::default(),
        )
        .expect("normalize");
    let route_family = resolver.resolve(&raw).expect("resolve route family");

    // Assert
    assert_eq!(normalized.permissions[0].raw, "queue://**#read");
    assert_eq!(route_family, 6);
}

#[test]
fn should_support_cognito_resource_server_scope_shape() {
    // Arrange
    let payload = serde_json::json!({
        "iss": "https://cognito-idp.us-east-1.amazonaws.com/us-east-1_Example",
        "aud": "https://fitz.example.com/api",
        "sub": "cognito-user-1",
        "exp": 9_999_999_999_u64,
        "custom:tenant_id": "acme-prod",
        "scope": "fitz/notice.write"
    });
    let raw: crate::auth::RawClaims = serde_json::from_value(payload).expect("raw claims");
    let claims_config = AuthClaimsConfig::new("custom:tenant_id", None, DEFAULT_ROLE_CLAIM);

    // Act
    let normalized = raw
        .normalize(
            &["https://cognito-idp.us-east-1.amazonaws.com/us-east-1_Example"],
            &["https://fitz.example.com/api"],
            0,
            &claims_config,
        )
        .expect("normalize");

    // Assert
    assert_eq!(normalized.permissions[0].raw, "notice://**#write");
}

#[test]
fn should_reject_malformed_role_claim() {
    // Arrange
    let payload = serde_json::json!({
        "iss": "https://idp.example/",
        "aud": "fitz-broker",
        "sub": "user:42",
        "exp": 9_999_999_999_u64,
        "tid": "acme-prod",
        "roles": ["notice.read", 42]
    });
    let raw: crate::auth::RawClaims = serde_json::from_value(payload).expect("raw claims");

    // Act
    let result = raw.normalized_permissions(None, None, DEFAULT_ROLE_CLAIM);

    // Assert
    assert!(result.unwrap_err().contains("role claim 'roles'"));
}

#[test]
fn should_reject_removed_custom_claim_alias() {
    // Arrange
    let config = AuthClaimsConfig::new("tid", Some("fitz".to_string()), DEFAULT_ROLE_CLAIM);

    // Act
    let result = config.validate();

    // Assert
    assert!(result
        .unwrap_err()
        .contains("FITZ_AUTH_CUSTOM_CLAIM=fitz is no longer supported"));
}

#[test]
fn should_reject_overlapping_role_claim_config() {
    // Arrange
    let config = AuthClaimsConfig::new(
        "tid",
        Some("https://fitz.example.com/claims".to_string()),
        "tid",
    );

    // Act
    let result = config.validate();

    // Assert
    assert!(result
        .unwrap_err()
        .contains("FITZ_AUTH_ROLE_CLAIM must not match FITZ_ROUTE_FAMILY_CLAIM"));
}

#[test]
fn should_reject_reserved_role_claim_config() {
    // Arrange
    let config = AuthClaimsConfig::new(
        "tid",
        Some("https://fitz.example.com/claims".to_string()),
        "scp",
    );

    // Act
    let result = config.validate();

    // Assert
    assert!(result
        .unwrap_err()
        .contains("FITZ_AUTH_ROLE_CLAIM must not overlap with top-level permission sources"));
}

#[test]
fn should_prefer_org_claim_override_over_identity_claim() {
    // Arrange
    let payload = serde_json::json!({
        "iss": "https://tenant.auth0.com/",
        "aud": "https://fitz.example.com/api",
        "sub": "auth0|user-1",
        "exp": 9_999_999_999_u64,
        "tid": "tid-realm",
        "fitz://org_id": "org-realm",
        "permissions": ["notice://prod/orders/**#read"]
    });
    let raw: crate::auth::RawClaims = serde_json::from_value(payload).expect("raw claims");
    let claims_config = AuthClaimsConfig::new_with_overrides(
        "tid",
        None,
        DEFAULT_ROLE_CLAIM,
        Some("fitz://org_id".to_string()),
        None,
    );

    // Act
    let normalized = raw
        .normalize(
            &["https://tenant.auth0.com/"],
            &["https://fitz.example.com/api"],
            0,
            &claims_config,
        )
        .expect("normalize");

    // Assert
    assert_eq!(normalized.identity_claim.as_deref(), Some("fitz://org_id"));
    assert_eq!(normalized.identity_value.as_deref(), Some("org-realm"));
}

#[test]
fn should_fallback_to_identity_claim_when_org_claim_override_missing() {
    // Arrange
    let payload = serde_json::json!({
        "iss": "https://tenant.auth0.com/",
        "aud": "https://fitz.example.com/api",
        "sub": "auth0|user-1",
        "exp": 9_999_999_999_u64,
        "tid": "tid-realm",
        "permissions": ["notice://prod/orders/**#read"]
    });
    let raw: crate::auth::RawClaims = serde_json::from_value(payload).expect("raw claims");
    let claims_config = AuthClaimsConfig::new_with_overrides(
        "tid",
        None,
        DEFAULT_ROLE_CLAIM,
        Some("fitz://org_id".to_string()),
        None,
    );

    // Act
    let normalized = raw
        .normalize(
            &["https://tenant.auth0.com/"],
            &["https://fitz.example.com/api"],
            0,
            &claims_config,
        )
        .expect("normalize");

    // Assert
    assert_eq!(normalized.identity_claim.as_deref(), Some("tid"));
    assert_eq!(normalized.identity_value.as_deref(), Some("tid-realm"));
}

#[test]
fn should_use_permissions_claim_override_before_role_claim() {
    // Arrange
    let payload = serde_json::json!({
        "iss": "https://tenant.auth0.com/",
        "aud": "https://fitz.example.com/api",
        "sub": "auth0|user-1",
        "exp": 9_999_999_999_u64,
        "tid": "tid-realm",
        "roles": ["notice.read"],
        "fitz://permissions": ["notice://prod/orders/**#write"]
    });
    let raw: crate::auth::RawClaims = serde_json::from_value(payload).expect("raw claims");
    let claims_config = AuthClaimsConfig::new_with_overrides(
        "tid",
        None,
        DEFAULT_ROLE_CLAIM,
        None,
        Some("fitz://permissions".to_string()),
    );

    // Act
    let normalized = raw
        .normalize(
            &["https://tenant.auth0.com/"],
            &["https://fitz.example.com/api"],
            0,
            &claims_config,
        )
        .expect("normalize");

    // Assert
    assert_eq!(
        normalized.permissions[0].raw,
        "notice://prod/orders/**#write"
    );
}

#[test]
fn should_prefer_top_level_permissions_over_permissions_claim_override() {
    // Arrange
    let payload = serde_json::json!({
        "iss": "https://tenant.auth0.com/",
        "aud": "https://fitz.example.com/api",
        "sub": "auth0|user-1",
        "exp": 9_999_999_999_u64,
        "tid": "tid-realm",
        "permissions": ["notice://prod/orders/**#read"],
        "fitz://permissions": ["notice://prod/orders/**#write"]
    });
    let raw: crate::auth::RawClaims = serde_json::from_value(payload).expect("raw claims");
    let claims_config = AuthClaimsConfig::new_with_overrides(
        "tid",
        None,
        DEFAULT_ROLE_CLAIM,
        None,
        Some("fitz://permissions".to_string()),
    );

    // Act
    let normalized = raw
        .normalize(
            &["https://tenant.auth0.com/"],
            &["https://fitz.example.com/api"],
            0,
            &claims_config,
        )
        .expect("normalize");

    // Assert
    assert_eq!(
        normalized.permissions[0].raw,
        "notice://prod/orders/**#read"
    );
}

#[test]
fn should_reject_permissions_override_matching_role_claim() {
    // Arrange
    let config = AuthClaimsConfig::new_with_overrides(
        "tid",
        None,
        "fitz://permissions",
        None,
        Some("fitz://permissions".to_string()),
    );

    // Act
    let result = config.validate();

    // Assert
    assert!(result
        .unwrap_err()
        .contains("FITZ_AUTH_PERMISSIONS_CLAIM must not match FITZ_AUTH_ROLE_CLAIM"));
}

#[test]
fn should_reject_permissions_override_matching_custom_claim() {
    // Arrange
    let config = AuthClaimsConfig::new_with_overrides(
        "tid",
        Some("fitz://permissions".to_string()),
        DEFAULT_ROLE_CLAIM,
        None,
        Some("fitz://permissions".to_string()),
    );

    // Act
    let result = config.validate();

    // Assert
    assert!(result
        .unwrap_err()
        .contains("FITZ_AUTH_PERMISSIONS_CLAIM must not match FITZ_AUTH_CUSTOM_CLAIM"));
}

#[test]
fn should_reject_org_override_matching_role_claim() {
    // Arrange
    let config = AuthClaimsConfig::new_with_overrides(
        "tid",
        None,
        "fitz://org_id",
        Some("fitz://org_id".to_string()),
        None,
    );

    // Act
    let result = config.validate();

    // Assert
    assert!(result
        .unwrap_err()
        .contains("FITZ_AUTH_ORG_CLAIM must not match FITZ_AUTH_ROLE_CLAIM"));
}

#[test]
fn should_support_auth0_org_id_permissions_shape() {
    // Arrange
    let payload = serde_json::json!({
        "iss": "https://tenant.auth0.com/",
        "aud": ["https://fitz.example.com/api", "https://tenant.auth0.com/userinfo"],
        "sub": "auth0|user-1",
        "exp": 9_999_999_999_u64,
        "org_id": "org_acme",
        "permissions": ["notice://prod/orders/**#read"]
    });
    let raw: crate::auth::RawClaims = serde_json::from_value(payload).expect("raw claims");
    let claims_config = AuthClaimsConfig::new("org_id", None, DEFAULT_ROLE_CLAIM);
    let resolver = RouteFamilyResolverConfig::from_mappings("org_id", [("org_acme", 2)]);

    // Act
    let normalized = raw
        .normalize(
            &["https://tenant.auth0.com/"],
            &["https://fitz.example.com/api"],
            0,
            &claims_config,
        )
        .expect("normalize");
    let route_family = resolver.resolve(&raw).expect("resolve route family");

    // Assert
    assert_eq!(normalized.identity_claim.as_deref(), Some("org_id"));
    assert_eq!(normalized.identity_value.as_deref(), Some("org_acme"));
    assert_eq!(
        normalized.permissions[0].raw,
        "notice://prod/orders/**#read"
    );
    assert_eq!(route_family, 2);
}

#[test]
fn should_support_entra_scp_shape() {
    // Arrange
    let payload = serde_json::json!({
        "iss": "https://login.microsoftonline.com/11111111-1111-1111-1111-111111111111/v2.0",
        "aud": "api://fitz",
        "sub": "user-1",
        "exp": 9_999_999_999_u64,
        "tid": "11111111-1111-1111-1111-111111111111",
        "scp": "notice://prod/orders/**#read kv.write"
    });
    let raw: crate::auth::RawClaims = serde_json::from_value(payload).expect("raw claims");
    let resolver = RouteFamilyResolverConfig::from_mappings(
        "tid",
        [("11111111-1111-1111-1111-111111111111", 3)],
    );

    // Act
    let normalized = raw
        .normalize(
            &["https://login.microsoftonline.com/11111111-1111-1111-1111-111111111111/v2.0"],
            &["api://fitz"],
            0,
            &AuthClaimsConfig::default(),
        )
        .expect("normalize");
    let route_family = resolver.resolve(&raw).expect("resolve route family");

    // Assert
    assert_eq!(
        normalized.identity_value.as_deref(),
        Some("11111111-1111-1111-1111-111111111111")
    );
    assert_eq!(
        normalized.permissions[0].raw,
        "notice://prod/orders/**#read"
    );
    assert_eq!(normalized.permissions[1].raw, "kv://**#write");
    assert_eq!(route_family, 3);
}

#[test]
fn should_support_cognito_scope_shape() {
    // Arrange
    let payload = serde_json::json!({
        "iss": "https://cognito-idp.us-east-1.amazonaws.com/us-east-1_Example",
        "aud": "https://fitz.example.com/api",
        "sub": "cognito-user-1",
        "exp": 9_999_999_999_u64,
        "custom:tenant_id": "acme-prod",
        "scope": "notice.write"
    });
    let raw: crate::auth::RawClaims = serde_json::from_value(payload).expect("raw claims");
    let claims_config = AuthClaimsConfig::new("custom:tenant_id", None, DEFAULT_ROLE_CLAIM);
    let resolver = RouteFamilyResolverConfig::from_mappings("custom:tenant_id", [("acme-prod", 4)]);

    // Act
    let normalized = raw
        .normalize(
            &["https://cognito-idp.us-east-1.amazonaws.com/us-east-1_Example"],
            &["https://fitz.example.com/api"],
            0,
            &claims_config,
        )
        .expect("normalize");
    let route_family = resolver.resolve(&raw).expect("resolve route family");

    // Assert
    assert_eq!(
        normalized.identity_claim.as_deref(),
        Some("custom:tenant_id")
    );
    assert_eq!(normalized.identity_value.as_deref(), Some("acme-prod"));
    assert_eq!(normalized.permissions[0].raw, "notice://**#write");
    assert_eq!(route_family, 4);
}

#[test]
fn should_support_okta_custom_permissions_shape() {
    // Arrange
    let payload = serde_json::json!({
        "iss": "https://dev-123456.okta.com/oauth2/default",
        "aud": "api://fitz",
        "sub": "okta-user-1",
        "exp": 9_999_999_999_u64,
        "https://fitz.example.com/identity": "okta-acme",
        "scope": "notice.read",
        "https://fitz.example.com/claims": {
            "permissions": ["notice://prod/orders/**#write"]
        }
    });
    let raw: crate::auth::RawClaims = serde_json::from_value(payload).expect("raw claims");
    let claims_config = AuthClaimsConfig::new(
        "https://fitz.example.com/identity",
        Some("https://fitz.example.com/claims".to_string()),
        DEFAULT_ROLE_CLAIM,
    );
    let resolver = RouteFamilyResolverConfig::from_mappings(
        "https://fitz.example.com/identity",
        [("okta-acme", 5)],
    );

    // Act
    let normalized = raw
        .normalize(
            &["https://dev-123456.okta.com/oauth2/default"],
            &["api://fitz"],
            0,
            &claims_config,
        )
        .expect("normalize");
    let route_family = resolver.resolve(&raw).expect("resolve route family");

    // Assert
    assert_eq!(
        normalized.identity_claim.as_deref(),
        Some("https://fitz.example.com/identity")
    );
    assert_eq!(normalized.identity_value.as_deref(), Some("okta-acme"));
    assert_eq!(
        normalized.permissions[0].raw,
        "notice://prod/orders/**#write"
    );
    assert_eq!(route_family, 5);
}

fn claims_from(payload: &serde_json::Value) -> crate::auth::RawClaims {
    let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload.to_string());
    parse_jwt_noverify(&format!("{}.{}.{}", "{}", b64, "sig")).expect("parse jwt")
}

#[test]
fn should_cascade_past_an_empty_permissions_claim_to_the_configured_claim() {
    // Arrange
    // Auth0 emits `permissions: []` whenever RBAC is enabled but no permissions
    // are assigned for that API, even when the real grants live in a configured
    // custom claim. Treating the empty array as a found source strands the
    // token with zero rights and never consults the configured claim.
    let claims = claims_from(&serde_json::json!({
        "iss": "https://idp.example/",
        "aud": "fitz-broker",
        "sub": "user:42",
        "exp": 9_999_999_999_u64,
        "tid": "acme-prod",
        "permissions": [],
        "fitz://permissions": ["queue://prod/jobs/**#write"]
    }));

    // Act
    let perms = claims
        .normalized_permissions(None, Some("fitz://permissions"), DEFAULT_ROLE_CLAIM)
        .expect("configured claim should be used");

    // Assert
    assert_eq!(perms.len(), 1);
    assert_eq!(perms[0].raw, "queue://prod/jobs/**#write");
}

#[test]
fn should_cascade_past_an_empty_custom_claim_to_top_level_permissions() {
    // Arrange
    // The custom claim sits above `permissions`, so an empty one short-circuits
    // the whole cascade including sources that do carry grants.
    let claims = claims_from(&serde_json::json!({
        "iss": "https://idp.example/",
        "aud": "fitz-broker",
        "sub": "user:42",
        "exp": 9_999_999_999_u64,
        "tid": "acme-prod",
        "https://acme.example/fitz": { "permissions": [] },
        "permissions": ["stream://prod/events/**#read"]
    }));

    // Act
    let perms = claims
        .normalized_permissions(Some("https://acme.example/fitz"), None, DEFAULT_ROLE_CLAIM)
        .expect("top-level permissions should be used");

    // Assert
    assert_eq!(perms.len(), 1);
    assert_eq!(perms[0].raw, "stream://prod/events/**#read");
}

#[test]
fn should_reject_a_token_whose_every_permission_source_is_empty() {
    // Arrange
    // Cascading past empties must not end in a session that authenticated but
    // can do nothing; with no source carrying grants the CONNECT is refused.
    let claims = claims_from(&serde_json::json!({
        "iss": "https://idp.example/",
        "aud": "fitz-broker",
        "sub": "user:42",
        "exp": 9_999_999_999_u64,
        "tid": "acme-prod",
        "permissions": [],
        "roles": [],
        "scope": ""
    }));

    // Act
    let result = claims.normalized_permissions(None, None, DEFAULT_ROLE_CLAIM);

    // Assert
    let error = result.expect_err("an all-empty token must not authenticate");
    assert!(
        error.contains("no permission source found"),
        "unexpected error: {error}"
    );
}
