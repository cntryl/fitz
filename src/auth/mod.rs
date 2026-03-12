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
    jwks_url: &str,
) -> Result<
    (
        crate::session::permissions::SessionPermissions,
        crate::auth::Claims,
    ),
    String,
> {
    // Ensure jwks present or fetched
    crate::auth::jwks::ensure_jwks_cached(jwks_url)
        .await
        .map_err(|e| format!("failed to ensure jwks: {}", e))?;
    // Parse header to get kid and alg
    let header =
        jsonwebtoken::decode_header(compact).map_err(|e| format!("invalid jwt header: {}", e))?;
    let kid = header.kid.as_deref().unwrap_or("");

    // Try to get decoding key from cache; if missing, fetch and cache, then retry
    if jwks::get_decoding_key_from_cache(jwks_url, kid).is_none() {
        // Attempt network fetch & cache
        jwks::fetch_and_cache_jwks(jwks_url)
            .await
            .map_err(|e| format!("failed to fetch jwks: {}", e))?;
    }

    let dk = jwks::get_decoding_key_from_cache(jwks_url, kid)
        .ok_or_else(|| "no matching key in jwks".to_string())?;

    // Determine alg for validation
    let alg = header.alg;
    let validation = jsonwebtoken::Validation::new(alg);

    // Verify and extract claims as serde_json::Value
    let token_data = jsonwebtoken::decode::<serde_json::Value>(compact, &dk, &validation)
        .map_err(|e| format!("signature verification failed: {}", e))?;

    // Deserialize into RawClaims to extract all needed fields
    let raw_claims: RawClaims = serde_json::from_value(token_data.claims)
        .map_err(|e| format!("json parse error: {}", e))?;

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

    // Extract permissions directly from the claim value
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

/// Verify a JWT using the configured verification path.
///
/// Rules:
/// - Tokens with `iss` must verify against issuer-derived JWKS.
/// - Tokens without `iss` must verify with `FITZ_JWT_HMAC_SECRET`.
/// - There is no permissive no-verify fallback.
pub async fn permissions_from_verified_jwt(
    compact: &str,
) -> Result<
    (
        crate::session::permissions::SessionPermissions,
        crate::auth::Claims,
    ),
    String,
> {
    let raw_claims = parse_jwt_noverify(compact)?;

    if !raw_claims.iss.is_empty() {
        let jwks_url = derive_jwks_url_from_issuer(&raw_claims.iss)?;
        return permissions_from_jwt_using_jwks(compact, &jwks_url).await;
    }

    let secret = std::env::var("FITZ_JWT_HMAC_SECRET")
        .map_err(|_| "missing FITZ_JWT_HMAC_SECRET for issuer-less JWT".to_string())?;
    permissions_from_hmac_jwt(compact, secret.as_bytes())
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
        let (perms, _claims) = permissions_from_jwt_using_jwks(&token, "inline://local")
            .await
            .unwrap();

        // Assert
        let route =
            crate::runtime::routing::Route::new("stream://realm1/area1/orders/1".to_string());
        assert!(perms.allows(&route, Access::Write));
    }
}
