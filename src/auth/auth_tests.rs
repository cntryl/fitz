use super::*;
use base64::Engine;
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use serde_json::json;
use serial_test::serial;

#[test]
fn should_reject_http_jwks_url() {
    // Arrange
    let config = AuthConfig::jwks(
        vec!["fitz-broker".to_string()],
        vec![JwksIssuerConfig {
            issuer: "https://idp.example/".to_string(),
            jwks_url: "http://idp.example/.well-known/jwks.json".to_string(),
        }],
    );

    // Act
    let result = config.validate(true);

    // Assert
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("must use https"));
}

#[test]
fn should_reject_inline_jwks_url_in_validated_auth_config() {
    // Arrange
    let config = AuthConfig::jwks(
        vec!["fitz-broker".to_string()],
        vec![JwksIssuerConfig {
            issuer: "https://idp.example/".to_string(),
            jwks_url: "inline://local".to_string(),
        }],
    );

    // Act
    let result = config.validate(true);

    // Assert
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("must use https"));
}

#[test]
fn should_reject_jwks_url_with_credentials() {
    // Arrange
    let config = AuthConfig::jwks(
        vec!["fitz-broker".to_string()],
        vec![JwksIssuerConfig {
            issuer: "https://idp.example/".to_string(),
            jwks_url: "https://user:pass@idp.example/.well-known/jwks.json".to_string(),
        }],
    );

    // Act
    let result = config.validate(true);

    // Assert
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("must not include credentials"));
}

#[test]
fn should_reject_jwks_url_with_fragment() {
    // Arrange
    let config = AuthConfig::jwks(
        vec!["fitz-broker".to_string()],
        vec![JwksIssuerConfig {
            issuer: "https://idp.example/".to_string(),
            jwks_url: "https://idp.example/.well-known/jwks.json#keys".to_string(),
        }],
    );

    // Act
    let result = config.validate(true);

    // Assert
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("must not include a fragment"));
}

#[test]
fn should_allow_https_jwks_url() {
    // Arrange
    let config = AuthConfig::jwks(
        vec!["fitz-broker".to_string()],
        vec![JwksIssuerConfig {
            issuer: "https://idp.example/".to_string(),
            jwks_url: "https://idp.example/.well-known/jwks.json".to_string(),
        }],
    );

    // Act
    let result = config.validate(true);

    // Assert
    assert!(result.is_ok());
}

#[test]
#[serial]
fn should_allow_http_jwks_url_when_insecure_http_env_is_enabled() {
    // Arrange
    let previous_allow_insecure = std::env::var("FITZ_JWT_ALLOW_INSECURE_HTTP").ok();
    let previous_jwks_map = std::env::var("FITZ_JWT_JWKS_MAP").ok();

    unsafe {
        std::env::set_var("FITZ_JWT_ALLOW_INSECURE_HTTP", "true");
        std::env::set_var(
            "FITZ_JWT_JWKS_MAP",
            "https://idp.example/http=http://idp.example/.well-known/jwks.json",
        );
    }

    let config = AuthConfig::from_env(true);

    unsafe {
        match previous_allow_insecure {
            Some(value) => std::env::set_var("FITZ_JWT_ALLOW_INSECURE_HTTP", value),
            None => std::env::remove_var("FITZ_JWT_ALLOW_INSECURE_HTTP"),
        }
        match previous_jwks_map {
            Some(value) => std::env::set_var("FITZ_JWT_JWKS_MAP", value),
            None => std::env::remove_var("FITZ_JWT_JWKS_MAP"),
        }
    }

    assert!(matches!(config, AuthConfig::Jwks(_)));
    if let AuthConfig::Jwks(config) = config {
        assert_eq!(
            config.issuers[0].jwks_url,
            "http://idp.example/.well-known/jwks.json"
        );
    }
}

#[test]
#[serial]
fn should_build_auth_config_from_fallback_hmac_secret() {
    let previous_hmac_secret = std::env::var("FITZ_JWT_HMAC_SECRET").ok();
    let previous_jwks_map = std::env::var("FITZ_JWT_JWKS_MAP").ok();

    unsafe {
        std::env::set_var("FITZ_JWT_HMAC_SECRET", "test-secret-key");
        std::env::remove_var("FITZ_JWT_JWKS_MAP");
    }

    let config = AuthConfig::from_env(true);

    unsafe {
        match previous_hmac_secret {
            Some(value) => std::env::set_var("FITZ_JWT_HMAC_SECRET", value),
            None => std::env::remove_var("FITZ_JWT_HMAC_SECRET"),
        }
        match previous_jwks_map {
            Some(value) => std::env::set_var("FITZ_JWT_JWKS_MAP", value),
            None => std::env::remove_var("FITZ_JWT_JWKS_MAP"),
        }
    }

    assert!(matches!(config, AuthConfig::Hmac(_)));
    if let AuthConfig::Hmac(config) = config {
        assert_eq!(config.secret, "test-secret-key");
        assert_eq!(config.audiences, vec!["fitz", "fitz-broker"]);
    }
}

#[test]
#[serial]
fn should_reject_partially_invalid_jwks_map_from_environment() {
    // Arrange
    struct EnvGuard {
        previous_jwks_map: Option<String>,
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            unsafe {
                match self.previous_jwks_map.take() {
                    Some(value) => std::env::set_var("FITZ_JWT_JWKS_MAP", value),
                    None => std::env::remove_var("FITZ_JWT_JWKS_MAP"),
                }
            }
        }
    }

    let _guard = EnvGuard {
        previous_jwks_map: std::env::var("FITZ_JWT_JWKS_MAP").ok(),
    };
    unsafe {
        std::env::set_var(
            "FITZ_JWT_JWKS_MAP",
            "https://idp.example/=https://idp.example/.well-known/jwks.json,bad-entry",
        );
    }

    // Act
    let config = AuthConfig::from_env(true);

    // Assert
    assert!(matches!(config, AuthConfig::Disabled));
}

#[test]
#[serial]
fn should_build_auth_config_from_valid_jwks_map_environment() {
    let previous_jwks_map = std::env::var("FITZ_JWT_JWKS_MAP").ok();
    unsafe {
        std::env::set_var(
            "FITZ_JWT_JWKS_MAP",
            "https://idp.example/=https://idp.example/.well-known/jwks.json",
        );
    }

    let config = AuthConfig::from_env(true);

    unsafe {
        match previous_jwks_map {
            Some(value) => std::env::set_var("FITZ_JWT_JWKS_MAP", value),
            None => std::env::remove_var("FITZ_JWT_JWKS_MAP"),
        }
    }

    assert!(matches!(config, AuthConfig::Jwks(_)));
    if let AuthConfig::Jwks(config) = config {
        assert_eq!(config.issuers.len(), 1);
        assert_eq!(config.issuers[0].issuer, "https://idp.example/");
    }
}

#[tokio::test]
async fn should_verify_permissions_using_inline_jwks_for_hmac_token() {
    // Arrange
    let secret = b"test_secret";
    let k_b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(secret);
    let jwks_json = json!({
        "keys": [{ "kty": "oct", "kid": "", "k": k_b64 }]
    })
    .to_string();

    cache_jwks_from_json_with_ttl("inline://local", &jwks_json, 3600).unwrap();

    let claims = json!({
        "iss": "https://idp.example/",
        "aud": "fitz-broker",
        "sub": "user:1",
        "exp": 9999999999u64,
        "tid": "realm1",
        "permissions": ["stream://realm1/area1/orders/*#write"]
    });

    let header = Header::new(Algorithm::HS256);
    let token = jsonwebtoken::encode(&header, &claims, &EncodingKey::from_secret(secret)).unwrap();

    // Act
    let issuer = JwksIssuerConfig {
        issuer: "https://idp.example/".to_string(),
        jwks_url: "inline://local".to_string(),
    };
    let (perms, _claims) =
        permissions_from_jwt_using_jwks(&token, &issuer, &["fitz-broker".to_string()])
            .await
            .unwrap();

    // Assert
    let route = crate::runtime::routing::Route::new("stream://realm1/area1/orders/1");
    assert!(perms.allows(&route, Access::Write));
}

#[tokio::test]
async fn should_reject_unallowlisted_issuer_even_with_signed_token() {
    let secret = b"test_secret";
    let claims = json!({
        "iss": "https://attacker.example/",
        "aud": "fitz-broker",
        "sub": "user:1",
        "exp": 9_999_999_999u64,
        "tid": "realm1",
        "permissions": ["stream://realm1/area1/orders/*#write"]
    });

    let token = jsonwebtoken::encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(secret),
    )
    .unwrap();

    let config = AuthConfig::jwks(
        vec!["fitz-broker".to_string()],
        vec![JwksIssuerConfig {
            issuer: "https://idp.example/".to_string(),
            jwks_url: "inline://local".to_string(),
        }],
    );

    let result = permissions_from_verified_jwt(&token, &config).await;

    assert!(result.is_err());
    assert!(result.unwrap_err().contains("issuer not allowed"));
}

#[tokio::test]
async fn should_reject_wrong_audience_for_hmac_tokens() {
    let claims = json!({
        "iss": "",
        "aud": "wrong-audience",
        "sub": "user:1",
        "exp": 9_999_999_999u64,
        "tid": "realm1",
        "permissions": ["stream://realm1/area1/orders/*#write"]
    });
    let token = jsonwebtoken::encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(b"test-secret-key"),
    )
    .unwrap();

    let config = AuthConfig::hmac("test-secret-key", "fitz-broker");
    let result = permissions_from_verified_jwt(&token, &config).await;

    assert!(result.is_err());
    assert!(result.unwrap_err().contains("audience mismatch"));
}

#[tokio::test]
async fn should_reject_tokens_that_are_not_yet_valid() {
    let now = now_epoch_secs();
    let claims = json!({
        "iss": "",
        "aud": "fitz-broker",
        "sub": "user:1",
        "exp": now + 300,
        "nbf": now + 120,
        "tid": "realm1",
        "permissions": ["stream://realm1/area1/orders/*#write"]
    });
    let token = jsonwebtoken::encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(b"test-secret-key"),
    )
    .unwrap();

    let config = AuthConfig::hmac("test-secret-key", "fitz-broker");
    let result = permissions_from_verified_jwt(&token, &config).await;

    assert!(result.is_err());
    assert!(result.unwrap_err().contains("not yet valid"));
}

#[tokio::test]
async fn should_keep_configured_identity_claim_given_other_identity_claims() {
    let claims = json!({
        "iss": "",
        "aud": "fitz-broker",
        "sub": "user:1",
        "exp": 9_999_999_999u64,
        "tid": "realm1",
        "tenant_id": "realm2",
        "permissions": ["stream://realm1/area1/orders/*#write"]
    });
    let token = jsonwebtoken::encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(b"test-secret-key"),
    )
    .unwrap();

    let config = AuthConfig::hmac("test-secret-key", "fitz-broker");
    let result = permissions_from_verified_jwt(&token, &config).await;

    assert!(result.is_ok());
    assert_eq!(result.unwrap().1.identity_value.as_deref(), Some("realm1"));
}

#[test]
fn should_resolve_org_id_given_configured_identity_claim() {
    // Arrange
    let claims = json!({
        "iss": "",
        "aud": "fitz-broker",
        "sub": "user:1",
        "exp": 9_999_999_999u64,
        "org_id": "realm-from-org",
        "permissions": ["stream://realm-from-org/area1/orders/*#write"]
    });
    let raw_claims: RawClaims = serde_json::from_value(claims).unwrap();

    // Act
    let normalized_claims = raw_claims
        .normalize(
            &[],
            &["fitz-broker"],
            0,
            &AuthClaimsConfig::new("org_id", None, DEFAULT_ROLE_CLAIM),
        )
        .unwrap();

    // Assert
    assert_eq!(
        normalized_claims.identity_value.as_deref(),
        Some("realm-from-org")
    );
}

#[tokio::test]
async fn should_use_empty_identity_for_hmac_jwt_given_no_identity_claims() {
    // Arrange
    let claims = json!({
        "iss": "",
        "aud": "fitz-broker",
        "sub": "user:1",
        "exp": 9_999_999_999u64,
        "permissions": ["stream://realm1/area1/orders/*#write"]
    });
    let token = jsonwebtoken::encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(b"test-secret-key"),
    )
    .unwrap();

    // Act
    let verified = verified_jwt_with_claims_config(
        &token,
        &AuthConfig::hmac("test-secret-key", "fitz-broker"),
        &AuthClaimsConfig::default(),
    )
    .await
    .unwrap();

    // Assert
    assert_eq!(verified.claims.identity_value, None);
}

#[tokio::test]
async fn should_verify_auth0_org_id_permissions_given_hmac_token() {
    // Arrange
    let claims = json!({
        "iss": "",
        "aud": "fitz-broker",
        "sub": "auth0|user-1",
        "exp": 9_999_999_999u64,
        "org_id": "org_acme",
        "permissions": ["notice://prod/orders/**#read"]
    });
    let token = jsonwebtoken::encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(b"test-secret-key"),
    )
    .unwrap();

    // Act
    let verified = verified_jwt_with_claims_config(
        &token,
        &AuthConfig::hmac("test-secret-key", "fitz-broker"),
        &AuthClaimsConfig::new("org_id", None, DEFAULT_ROLE_CLAIM),
    )
    .await
    .unwrap();

    // Assert
    assert_eq!(verified.claims.identity_value.as_deref(), Some("org_acme"));
    let route = crate::runtime::routing::Route::new("notice://prod/orders/1");
    assert!(verified.permissions.allows(&route, Access::Read));
}

#[tokio::test]
async fn should_verify_entra_delegated_scp_given_hmac_token() {
    // Arrange
    let claims = json!({
        "iss": "",
        "aud": "fitz-broker",
        "sub": "entra-user-1",
        "exp": 9_999_999_999u64,
        "tid": "entra-tenant-1",
        "scp": "notice.read kv.write"
    });
    let token = jsonwebtoken::encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(b"test-secret-key"),
    )
    .unwrap();

    // Act
    let verified = verified_jwt_with_claims_config(
        &token,
        &AuthConfig::hmac("test-secret-key", "fitz-broker"),
        &AuthClaimsConfig::default(),
    )
    .await
    .unwrap();

    // Assert
    let notice_route = crate::runtime::routing::Route::new("notice://prod/orders/1");
    let kv_route = crate::runtime::routing::Route::new("kv://prod/orders/1");
    assert!(verified.permissions.allows(&notice_route, Access::Read));
    assert!(verified.permissions.allows(&kv_route, Access::Write));
}

#[tokio::test]
async fn should_verify_entra_app_only_roles_given_hmac_token() {
    // Arrange
    let claims = json!({
        "iss": "",
        "aud": "fitz-broker",
        "sub": "service-principal-1",
        "exp": 9_999_999_999u64,
        "tid": "realm1",
        "roles": ["notice.write"]
    });
    let token = jsonwebtoken::encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(b"test-secret-key"),
    )
    .unwrap();

    // Act
    let verified = verified_jwt_with_claims_config(
        &token,
        &AuthConfig::hmac("test-secret-key", "fitz-broker"),
        &AuthClaimsConfig::default(),
    )
    .await
    .unwrap();

    // Assert
    let route = crate::runtime::routing::Route::new("notice://prod/orders/1");
    assert!(verified.permissions.allows(&route, Access::Write));
}

#[tokio::test]
async fn should_verify_cognito_resource_server_scope_given_hmac_token() {
    // Arrange
    let claims = json!({
        "iss": "",
        "aud": "fitz-broker",
        "sub": "cognito-user-1",
        "exp": 9_999_999_999u64,
        "custom:tenant_id": "realm1",
        "scope": "fitz/kv.read"
    });
    let token = jsonwebtoken::encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(b"test-secret-key"),
    )
    .unwrap();

    // Act
    let verified = verified_jwt_with_claims_config(
        &token,
        &AuthConfig::hmac("test-secret-key", "fitz-broker"),
        &AuthClaimsConfig::new("custom:tenant_id", None, DEFAULT_ROLE_CLAIM),
    )
    .await
    .unwrap();

    // Assert
    let route = crate::runtime::routing::Route::new("kv://prod/orders/1");
    assert!(verified.permissions.allows(&route, Access::Read));
}

#[tokio::test]
async fn should_verify_okta_custom_permissions_given_hmac_token() {
    // Arrange
    let claims = json!({
        "iss": "",
        "aud": "fitz-broker",
        "sub": "okta-user-1",
        "exp": 9_999_999_999u64,
        "https://fitz.example.com/identity": "okta-acme",
        "https://fitz.example.com/claims": {
            "permissions": ["queue://prod/orders/**#read"]
        }
    });
    let token = jsonwebtoken::encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(b"test-secret-key"),
    )
    .unwrap();

    // Act
    let verified = verified_jwt_with_claims_config(
        &token,
        &AuthConfig::hmac("test-secret-key", "fitz-broker"),
        &AuthClaimsConfig::new(
            "https://fitz.example.com/identity",
            Some("https://fitz.example.com/claims".to_string()),
            DEFAULT_ROLE_CLAIM,
        ),
    )
    .await
    .unwrap();

    // Assert
    assert_eq!(verified.claims.identity_value.as_deref(), Some("okta-acme"));
    let route = crate::runtime::routing::Route::new("queue://prod/orders/1");
    assert!(verified.permissions.allows(&route, Access::Read));
}
