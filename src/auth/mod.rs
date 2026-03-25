//! Public auth surface for Fitz.
//!
//! **Strict responsibility boundaries:**
//!
//! This module ONLY does:
//! - Token verification (signature checks)
//! - Claims normalization (identity extraction)
//! - JWKS management (key caching, no fetch/HTTP logic)
//!
//! It does NOT do:
//! - Route matching or authorization decisions (session layer)
//! - Domain-specific validation (domain layer)
//! - HTTP/network I/O (transport layer)
//!
//! **Auth answers:** "Who are you and what do you claim?"
//! **Domains answer:** "Are you allowed to do this?"

mod claims;
mod errors;
mod jwks;
mod realm;
mod token;

pub use claims::{parse_jwt_noverify, Claims, RawClaims};
pub use errors::AuthError;
pub use jwks::{
    cache_jwks_from_json, cache_jwks_from_json_with_ttl, derive_jwks_url_from_issuer,
    ensure_jwks_cached, fetch_and_cache_jwks, get_decoding_key_from_cache, is_jwks_stale,
};
pub use realm::{realm_matches, validate_realm_format, RealmError};
pub use token::{verify_jwt_with_hmac_secret, verify_jwt_with_rsa_pem};

use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

/// Access level attached to a permission fragment.
/// Used in permission strings like "notice://realm/area#read"
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Access {
    Read,
    Write,
    All,
}

impl FromStr for Access {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "read" => Ok(Access::Read),
            "write" => Ok(Access::Write),
            "*" => Ok(Access::All),
            _ => Err(format!("unknown access: {}", s)),
        }
    }
}

/// Thin permission representation emitted by auth.
///
/// This is deliberately runtime-agnostic: it stores the original route-shaped
/// string and the access qualifier but does not attempt to interpret or match routes.
/// Route matching is performed in the session layer when a session snapshot is created.
///
/// **Immutant after auth time.** Once issued, permissions are never reinterpreted.
#[derive(Debug, Clone)]
pub struct Permission {
    /// Original permission string, e.g. "notice://prod/orders/**#read" or "notice://**#write"
    pub raw: String,
    pub access: Access,
}

impl Permission {
    /// Parse from '<route>#<access>' where '#<access>' is optional and defaults to '*'
    pub fn parse(s: &str) -> Result<Self, String> {
        let raw = s.to_string();
        let (_route_part, access_part) = if let Some(idx) = s.rfind('#') {
            (&s[..idx], &s[idx + 1..])
        } else {
            (s, "*")
        };

        let access = Access::from_str(access_part)?;

        Ok(Self { raw, access })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthConfig {
    Disabled,
    Hmac(HmacAuthConfig),
    Jwks(JwksAuthConfig),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HmacAuthConfig {
    pub secret: String,
    pub audiences: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JwksAuthConfig {
    pub audiences: Vec<String>,
    pub issuers: Vec<JwksIssuerConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JwksIssuerConfig {
    pub issuer: String,
    pub jwks_url: String,
}

impl AuthConfig {
    pub fn disabled() -> Self {
        Self::Disabled
    }

    pub fn hmac(secret: impl Into<String>, audience: impl Into<String>) -> Self {
        Self::hmac_with_audiences(secret, vec![audience.into()])
    }

    pub fn hmac_with_audiences(secret: impl Into<String>, audiences: Vec<String>) -> Self {
        Self::Hmac(HmacAuthConfig {
            secret: secret.into(),
            audiences,
        })
    }

    pub fn jwks(audiences: Vec<String>, issuers: Vec<JwksIssuerConfig>) -> Self {
        Self::Jwks(JwksAuthConfig { audiences, issuers })
    }

    pub fn from_env(auth_required: bool) -> Self {
        if !auth_required {
            return Self::Disabled;
        }

        if let Ok(secret) = std::env::var("FITZ_JWT_HMAC_SECRET") {
            return Self::hmac_with_audiences(secret, audiences_from_env());
        }

        if let Ok(raw_map) = std::env::var("FITZ_JWT_JWKS_MAP") {
            let issuers = raw_map
                .split(',')
                .filter_map(|entry| {
                    let (issuer, jwks_url) = entry.split_once('=')?;
                    Some(JwksIssuerConfig {
                        issuer: issuer.trim().to_string(),
                        jwks_url: jwks_url.trim().to_string(),
                    })
                })
                .collect::<Vec<_>>();
            if !issuers.is_empty() {
                return Self::jwks(audiences_from_env(), issuers);
            }
        }

        Self::Disabled
    }

    pub fn validate(&self, auth_required: bool) -> Result<(), String> {
        match self {
            AuthConfig::Disabled => {
                if auth_required {
                    Err(
                        "authentication is required but no valid AuthConfig was provided"
                            .to_string(),
                    )
                } else {
                    Ok(())
                }
            }
            AuthConfig::Hmac(config) => {
                if config.secret.trim().is_empty() {
                    return Err("HMAC auth requires a non-empty secret".to_string());
                }
                if config.audiences.is_empty() {
                    return Err("HMAC auth requires at least one audience".to_string());
                }
                Ok(())
            }
            AuthConfig::Jwks(config) => {
                if config.audiences.is_empty() {
                    return Err("JWKS auth requires at least one audience".to_string());
                }
                if config.issuers.is_empty() {
                    return Err("JWKS auth requires at least one configured issuer".to_string());
                }
                for issuer in &config.issuers {
                    if issuer.issuer.trim().is_empty() {
                        return Err(
                            "JWKS auth issuer allowlist entries must not be empty".to_string()
                        );
                    }
                    url::Url::parse(&issuer.jwks_url).map_err(|e| {
                        format!("invalid JWKS URL for issuer {}: {}", issuer.issuer, e)
                    })?;
                }
                Ok(())
            }
        }
    }

    fn find_issuer(&self, issuer: &str) -> Option<&JwksIssuerConfig> {
        match self {
            AuthConfig::Jwks(config) => config.issuers.iter().find(|entry| entry.issuer == issuer),
            _ => None,
        }
    }

    fn audiences(&self) -> &[String] {
        match self {
            AuthConfig::Disabled => &[],
            AuthConfig::Hmac(config) => &config.audiences,
            AuthConfig::Jwks(config) => &config.audiences,
        }
    }
}

fn audiences_from_env() -> Vec<String> {
    let raw = std::env::var("FITZ_JWT_AUDIENCES")
        .or_else(|_| std::env::var("FITZ_JWT_AUDIENCE"))
        .unwrap_or_else(|_| "fitz,fitz-broker".to_string());
    raw.split(',')
        .map(str::trim)
        .filter(|aud| !aud.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn now_epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn normalized_session_claims(
    raw_claims: RawClaims,
    allowlist: &[&str],
    audiences: &[String],
) -> Result<
    (
        crate::session::permissions::SessionPermissions,
        crate::auth::Claims,
    ),
    String,
> {
    let audience_refs = audiences.iter().map(String::as_str).collect::<Vec<_>>();
    let claims = raw_claims.normalize(allowlist, &audience_refs, now_epoch_secs())?;
    let session_perms = crate::session::permissions::SessionPermissions::from_permissions(
        claims.permissions.clone(),
    );
    Ok((session_perms, claims))
}

/// Map coarse scope strings like `notice.read` into Fitz permission strings.
/// This is a compatibility helper for OAuth2-style scope claims.
pub fn map_coarse_scope(s: &str) -> Option<&'static str> {
    match s {
        "notice.read" => Some("notice://**#read"),
        "notice.write" => Some("notice://**#write"),
        "rpc.read" => Some("rpc://**#read"),
        "rpc.write" => Some("rpc://**#write"),
        "stream.read" => Some("stream://**#read"),
        "stream.write" => Some("stream://**#write"),
        "queue.read" => Some("queue://**#read"),
        "queue.write" => Some("queue://**#write"),
        "lease.read" => Some("lease://**#read"),
        "lease.write" => Some("lease://**#write"),
        "schedule.read" => Some("schedule://**#read"),
        "schedule.write" => Some("schedule://**#write"),
        _ => None,
    }
}

/// Backwards-compatible helper that returns a `SessionPermissions` snapshot and Claims.
/// Returns a tuple (SessionPermissions, Claims) for use by the session manager.
/// The Claims object includes expiration time for security checks.
pub fn permissions_from_compact_jwt(
    compact: &str,
) -> Result<
    (
        crate::session::permissions::SessionPermissions,
        crate::auth::Claims,
    ),
    String,
> {
    let raw_claims = claims::parse_jwt_noverify(compact)?;

    // Resolve tenant (prefer tid > tenant_id > org_id, fallback to empty for no-verify path)
    let tenant = if let Some(t) = &raw_claims.tid {
        t.clone()
    } else if let Some(t) = &raw_claims.tenant_id {
        t.clone()
    } else if let Some(t) = &raw_claims.org_id {
        t.clone()
    } else {
        // For no-verify path without tenant field, use empty string
        String::new()
    };

    // Get normalized permissions
    let perms = raw_claims.normalized_permissions()?;

    // Build Claims object with expiration time
    let claims = crate::auth::Claims {
        sub: raw_claims.sub.clone(),
        tenant,
        roles: raw_claims.roles.clone().unwrap_or_default(),
        permissions: perms.clone(),
        exp: raw_claims.exp,
    };

    let session_perms = crate::session::permissions::SessionPermissions::from_permissions(perms);

    Ok((session_perms, claims))
}

pub fn permissions_from_signed_jwt(
    compact: &str,
    public_pem: &[u8],
) -> Result<
    (
        crate::session::permissions::SessionPermissions,
        crate::auth::Claims,
    ),
    String,
> {
    let claims_value = token::verify_jwt_with_rsa_pem(compact, public_pem)?;
    let raw_claims: RawClaims =
        serde_json::from_value(claims_value).map_err(|e| format!("json parse error: {}", e))?;

    // Resolve tenant (prefer tid > tenant_id > org_id, fallback to empty)
    let tenant = if let Some(t) = &raw_claims.tid {
        t.clone()
    } else if let Some(t) = &raw_claims.tenant_id {
        t.clone()
    } else if let Some(t) = &raw_claims.org_id {
        t.clone()
    } else {
        String::new()
    };

    let perms = raw_claims.normalized_permissions()?;

    // Build Claims object with expiration time
    let claims = crate::auth::Claims {
        sub: raw_claims.sub.clone(),
        tenant,
        roles: raw_claims.roles.clone().unwrap_or_default(),
        permissions: perms.clone(),
        exp: raw_claims.exp,
    };

    Ok((
        crate::session::permissions::SessionPermissions::from_permissions(perms),
        claims,
    ))
}

pub fn permissions_from_hmac_jwt(
    compact: &str,
    secret: &[u8],
) -> Result<
    (
        crate::session::permissions::SessionPermissions,
        crate::auth::Claims,
    ),
    String,
> {
    let claims_value = token::verify_jwt_with_hmac_secret(compact, secret)?;
    let raw_claims: RawClaims =
        serde_json::from_value(claims_value).map_err(|e| format!("json parse error: {}", e))?;

    let tenant = if let Some(t) = &raw_claims.tid {
        t.clone()
    } else if let Some(t) = &raw_claims.tenant_id {
        t.clone()
    } else if let Some(t) = &raw_claims.org_id {
        t.clone()
    } else {
        String::new()
    };

    let perms = raw_claims.normalized_permissions()?;

    let claims = crate::auth::Claims {
        sub: raw_claims.sub.clone(),
        tenant,
        roles: raw_claims.roles.clone().unwrap_or_default(),
        permissions: perms.clone(),
        exp: raw_claims.exp,
    };

    Ok((
        crate::session::permissions::SessionPermissions::from_permissions(perms),
        claims,
    ))
}

pub async fn permissions_from_jwt_using_jwks(
    compact: &str,
    issuer: &JwksIssuerConfig,
    audiences: &[String],
) -> Result<
    (
        crate::session::permissions::SessionPermissions,
        crate::auth::Claims,
    ),
    String,
> {
    // Ensure jwks present or fetched
    crate::auth::jwks::ensure_jwks_cached(&issuer.jwks_url)
        .await
        .map_err(|e| format!("failed to ensure jwks: {}", e))?;
    // Parse header to get kid and alg
    let header =
        jsonwebtoken::decode_header(compact).map_err(|e| format!("invalid jwt header: {}", e))?;
    let kid = header.kid.as_deref().unwrap_or("");

    // Try to get decoding key from cache; if missing, fetch and cache, then retry
    if jwks::get_decoding_key_from_cache(&issuer.jwks_url, kid).is_none() {
        // Attempt network fetch & cache
        jwks::fetch_and_cache_jwks(&issuer.jwks_url)
            .await
            .map_err(|e| format!("failed to fetch jwks: {}", e))?;
    }

    let dk = jwks::get_decoding_key_from_cache(&issuer.jwks_url, kid)
        .ok_or_else(|| "no matching key in jwks".to_string())?;

    // Determine alg for validation
    let alg = header.alg;
    let validation = jsonwebtoken::Validation::new(alg);

    // Verify and extract claims as serde_json::Value
    let token_data = jsonwebtoken::decode::<serde_json::Value>(compact, &dk, &validation)
        .map_err(|e| format!("signature verification failed: {}", e))?;

    let raw_claims: RawClaims = serde_json::from_value(token_data.claims)
        .map_err(|e| format!("json parse error: {}", e))?;

    normalized_session_claims(raw_claims, &[issuer.issuer.as_str()], audiences)
}

/// Verify a JWT using the configured verification path.
///
/// Rules:
/// - Tokens with `iss` must verify against issuer-derived JWKS.
/// - Tokens without `iss` must verify with `FITZ_JWT_HMAC_SECRET`.
/// - There is no permissive no-verify fallback.
pub async fn permissions_from_verified_jwt(
    compact: &str,
    auth_config: &AuthConfig,
) -> Result<
    (
        crate::session::permissions::SessionPermissions,
        crate::auth::Claims,
    ),
    String,
> {
    let raw_claims = parse_jwt_noverify(compact)?;

    match auth_config {
        AuthConfig::Disabled => Err("authentication is disabled".to_string()),
        AuthConfig::Hmac(config) => {
            if !raw_claims.iss.trim().is_empty() {
                return Err("issuer-based tokens are not allowed in HMAC mode".to_string());
            }

            let claims_value =
                token::verify_jwt_with_hmac_secret(compact, config.secret.as_bytes())?;
            let verified_raw: RawClaims = serde_json::from_value(claims_value)
                .map_err(|e| format!("json parse error: {}", e))?;
            normalized_session_claims(verified_raw, &[], auth_config.audiences())
        }
        AuthConfig::Jwks(_) => {
            let issuer = auth_config
                .find_issuer(&raw_claims.iss)
                .ok_or_else(|| "issuer not allowed".to_string())?;
            permissions_from_jwt_using_jwks(compact, issuer, auth_config.audiences()).await
        }
    }
}

/// Create default anonymous permissions with full access across all domains.
/// Used when FITZ_AUTH_REQUIRED=false for development/testing.
pub fn default_anonymous_permissions() -> crate::session::permissions::SessionPermissions {
    let perms = vec![
        Permission {
            raw: "kv://**#*".to_string(),
            access: Access::All,
        },
        Permission {
            raw: "stream://**#*".to_string(),
            access: Access::All,
        },
        Permission {
            raw: "queue://**#*".to_string(),
            access: Access::All,
        },
        Permission {
            raw: "notice://**#*".to_string(),
            access: Access::All,
        },
        Permission {
            raw: "rpc://**#*".to_string(),
            access: Access::All,
        },
        Permission {
            raw: "lease://**#*".to_string(),
            access: Access::All,
        },
        Permission {
            raw: "schedule://**#*".to_string(),
            access: Access::All,
        },
    ];
    crate::session::permissions::SessionPermissions::from_permissions(perms)
}

#[cfg(test)]
mod auth_tests {
    use super::*;
    use base64::Engine;
    use jsonwebtoken::{Algorithm, EncodingKey, Header};
    use serde_json::json;

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
            "fitz": { "permissions": ["stream://realm1/area1/orders/*#write"] }
        });

        let header = Header::new(Algorithm::HS256);
        let token =
            jsonwebtoken::encode(&header, &claims, &EncodingKey::from_secret(secret)).unwrap();

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
            "fitz": { "permissions": ["stream://realm1/area1/orders/*#write"] }
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
            "fitz": { "permissions": ["stream://realm1/area1/orders/*#write"] }
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
            "fitz": { "permissions": ["stream://realm1/area1/orders/*#write"] }
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
    async fn should_reject_tokens_with_ambiguous_tenant_claims() {
        let claims = json!({
            "iss": "",
            "aud": "fitz-broker",
            "sub": "user:1",
            "exp": 9_999_999_999u64,
            "tid": "realm1",
            "tenant_id": "realm2",
            "fitz": { "permissions": ["stream://realm1/area1/orders/*#write"] }
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
        assert!(result.unwrap_err().contains("exactly one tenant id"));
    }
}
