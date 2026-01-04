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

mod token;
mod jwks;
mod claims;
mod errors;

pub use token::{verify_jwt_with_rsa_pem, verify_jwt_with_hmac_secret};
pub use jwks::{
    cache_jwks_from_json,
    cache_jwks_from_json_with_ttl,
    fetch_and_cache_jwks,
    get_decoding_key_from_cache,
    ensure_jwks_cached,
    derive_jwks_url_from_issuer,
    is_jwks_stale,
};
pub use claims::{parse_jwt_noverify, RawClaims, Claims};
pub use errors::AuthError;

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
        _ => None,
    }
}

/// Backwards-compatible helper that returns a `SessionPermissions` snapshot.
/// These remain convenience wrappers for the transport/session-layer integration.
pub fn permissions_from_compact_jwt(compact: &str) -> Result<crate::session::permissions::SessionPermissions, String> {
    let claims = claims::parse_jwt_noverify(compact)?;
    let perms = claims.normalized_permissions()?;
    Ok(crate::session::permissions::SessionPermissions::from_permissions(perms))
}

pub fn permissions_from_signed_jwt(compact: &str, public_pem: &[u8]) -> Result<crate::session::permissions::SessionPermissions, String> {
    let claims_value = token::verify_jwt_with_rsa_pem(compact, public_pem)?;
    let claims: RawClaims = serde_json::from_value(claims_value).map_err(|e| format!("json parse error: {}", e))?;
    let perms = claims.normalized_permissions()?;
    Ok(crate::session::permissions::SessionPermissions::from_permissions(perms))
}

pub async fn permissions_from_jwt_using_jwks(compact: &str, jwks_url: &str) -> Result<crate::session::permissions::SessionPermissions, String> {
    // Ensure jwks present or fetched
    crate::auth::jwks::ensure_jwks_cached(jwks_url)
        .await
        .map_err(|e| format!("failed to ensure jwks: {}", e))?;
    // Parse header to get kid and alg
    let header = jsonwebtoken::decode_header(compact).map_err(|e| format!("invalid jwt header: {}", e))?;
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

    // Extract permissions directly from the claim value. Signed tokens may omit other
    // standard claims when used only for service permissions; return an empty
    // snapshot if no permission source is present.
    let perms = claims::normalized_permissions_from_value(&token_data.claims)?;
    Ok(crate::session::permissions::SessionPermissions::from_permissions(perms))
}


