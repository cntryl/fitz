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
mod diagnostics;
mod errors;
mod jwks;
mod realm;
mod token;

pub use claims::{
    parse_jwt_noverify, AuthClaimsConfig, Claims, RawClaims, RouteFamilyResolverConfig,
    DEFAULT_ROLE_CLAIM, DEFAULT_ROUTE_FAMILY_CLAIM, ENV_AUTH_CUSTOM_CLAIM, ENV_AUTH_ROLE_CLAIM,
    ENV_ROUTE_FAMILY_CLAIM, ENV_ROUTE_FAMILY_MAP,
};
pub use diagnostics::{jwt_failure_diagnostics, JwtClaimDiagnostics, JwtFailureDiagnostics};
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
/// Used in permission strings like `<notice://realm/area#read>`.
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
            _ => Err(format!("unknown access: {s}")),
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
    /// Original permission string, e.g. `<notice://prod/orders/**#read>` or
    /// `<notice://**#write>`.
    pub raw: String,
    pub access: Access,
}

impl Permission {
    /// Parse from '<route>#<access>' where '#<access>' is optional and defaults to '*'
    ///
    /// # Errors
    ///
    /// Returns an error when the access suffix is present but not one of the
    /// supported access levels.
    pub fn parse(s: &str) -> Result<Self, String> {
        let raw = s.to_string();
        let (route_part, access_part) = if let Some(idx) = s.rfind('#') {
            (&s[..idx], &s[idx + 1..])
        } else {
            (s, "*")
        };

        crate::utils::route_shape::validate_route_shape(route_part)
            .map_err(|error| format!("invalid permission route: {error}"))?;
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

const ENV_ALLOW_INSECURE_JWKS_HTTP: &str = "FITZ_JWT_ALLOW_INSECURE_HTTP";
const ENV_JWT_HMAC_SECRET: &str = "FITZ_JWT_HMAC_SECRET";

impl AuthConfig {
    #[must_use]
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

    #[must_use]
    pub fn jwks(audiences: Vec<String>, issuers: Vec<JwksIssuerConfig>) -> Self {
        Self::Jwks(JwksAuthConfig { audiences, issuers })
    }

    pub fn from_env(auth_required: bool) -> Self {
        if !auth_required {
            return Self::Disabled;
        }

        let audiences = audiences_from_env();

        if let Ok(raw_map) = std::env::var("FITZ_JWT_JWKS_MAP") {
            let allow_insecure_http = allow_insecure_jwks_http();
            let trimmed_map = raw_map.trim();
            if !trimmed_map.is_empty() {
                match parse_jwks_issuers_from_env(trimmed_map, allow_insecure_http) {
                    Ok(issuers) if !issuers.is_empty() => {
                        return Self::jwks(audiences.clone(), issuers);
                    }
                    Ok(_) => {}
                    Err(error) => {
                        tracing::warn!(error = %error, "Ignoring invalid FITZ_JWT_JWKS_MAP");
                        return Self::Disabled;
                    }
                }
            }
        }

        if let Ok(secret) = std::env::var(ENV_JWT_HMAC_SECRET) {
            let secret = secret.trim();
            if !secret.is_empty() {
                tracing::warn!(
                    secret = %secret.len(),
                    "Using FITZ_JWT_HMAC_SECRET for runtime auth. This is insecure and intended only for testing/prototyping."
                );
                return Self::hmac_with_audiences(secret.to_string(), audiences);
            }
        }

        Self::Disabled
    }

    /// Validate auth configuration against the current runtime requirements.
    ///
    /// # Errors
    ///
    /// Returns an error when authentication is required but disabled, when an
    /// HMAC secret or audience set is invalid, or when JWKS issuer or URL
    /// configuration is incomplete or malformed.
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
                let allow_insecure_http = allow_insecure_jwks_http();
                for issuer in &config.issuers {
                    if issuer.issuer.trim().is_empty() {
                        return Err(
                            "JWKS auth issuer allowlist entries must not be empty".to_string()
                        );
                    }
                    validate_jwks_url(&issuer.jwks_url, allow_insecure_http).map_err(|error| {
                        format!("invalid JWKS URL for issuer {}: {}", issuer.issuer, error)
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

fn validate_jwks_url(raw: &str, allow_insecure_http: bool) -> Result<(), String> {
    let url = url::Url::parse(raw).map_err(|error| error.to_string())?;
    if !allow_insecure_http && url.scheme() != "https" {
        return Err("must use https".to_string());
    }
    if allow_insecure_http && url.scheme() != "https" && url.scheme() != "http" {
        return Err("must use https".to_string());
    }
    if url.host_str().is_none() {
        return Err("must include a host".to_string());
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err("must not include credentials".to_string());
    }
    if url.fragment().is_some() {
        return Err("must not include a fragment".to_string());
    }
    Ok(())
}

fn parse_jwks_issuers_from_env(
    raw_map: &str,
    allow_insecure_http: bool,
) -> Result<Vec<JwksIssuerConfig>, String> {
    let mut issuers = Vec::new();

    for entry in raw_map.split(',') {
        let trimmed_entry = entry.trim();
        if trimmed_entry.is_empty() {
            return Err("JWKS auth map must not contain empty entries".to_string());
        }

        let Some((issuer, jwks_url)) = trimmed_entry.split_once('=') else {
            return Err(format!(
                "JWKS auth map entry '{trimmed_entry}' must use issuer=jwks_url"
            ));
        };

        let issuer = issuer.trim();
        if issuer.is_empty() {
            return Err("JWKS auth map entries must not use empty issuers".to_string());
        }

        let jwks_url = jwks_url.trim();
        if jwks_url.is_empty() {
            return Err(format!(
                "JWKS auth map entry for issuer {issuer} must not use an empty JWKS URL"
            ));
        }

        validate_jwks_url(jwks_url, allow_insecure_http)
            .map_err(|error| format!("invalid JWKS URL for issuer {issuer}: {error}"))?;

        issuers.push(JwksIssuerConfig {
            issuer: issuer.to_string(),
            jwks_url: jwks_url.to_string(),
        });
    }

    Ok(issuers)
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
        .map_or(0, |d| d.as_secs())
}

fn allow_insecure_jwks_http() -> bool {
    std::env::var(ENV_ALLOW_INSECURE_JWKS_HTTP)
        .ok()
        .and_then(|value| value.parse::<bool>().ok())
        .unwrap_or(false)
}

#[derive(Debug, Clone)]
pub struct VerifiedJwt {
    pub permissions: crate::session::permissions::SessionPermissions,
    pub claims: crate::auth::Claims,
    pub raw_claims: RawClaims,
}

fn verified_session_claims(
    raw_claims: RawClaims,
    allowlist: &[&str],
    audiences: &[String],
    claims_config: &AuthClaimsConfig,
) -> Result<VerifiedJwt, String> {
    let audience_refs = audiences.iter().map(String::as_str).collect::<Vec<_>>();
    let claims =
        raw_claims.normalize(allowlist, &audience_refs, now_epoch_secs(), claims_config)?;
    let session_perms = crate::session::permissions::SessionPermissions::from_permissions(
        claims.permissions.clone(),
    );
    Ok(VerifiedJwt {
        permissions: session_perms,
        claims,
        raw_claims,
    })
}

/// Map coarse scope strings like `notice.read` into Fitz permission strings.
/// This is a compatibility helper for OAuth2-style scope claims.
#[must_use]
pub fn map_coarse_scope(s: &str) -> Option<&'static str> {
    match s {
        "kv.read" => Some("kv://**#read"),
        "kv.write" => Some("kv://**#write"),
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

/// Verify a JWT through JWKS and return session permissions plus normalized
/// claims using the default claims configuration.
///
/// # Errors
///
/// Returns an error when JWKS retrieval, header parsing, signature
/// verification, or claim normalization fails.
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
    let verified = verified_jwt_using_jwks_with_claims_config(
        compact,
        issuer,
        audiences,
        &AuthClaimsConfig::default(),
    )
    .await?;
    Ok((verified.permissions, verified.claims))
}

/// Verify a JWT through JWKS and return session permissions plus normalized
/// claims.
///
/// # Errors
///
/// Returns an error when JWKS retrieval, header parsing, signature
/// verification, or claim normalization fails.
pub async fn verified_jwt_using_jwks_with_claims_config(
    compact: &str,
    issuer: &JwksIssuerConfig,
    audiences: &[String],
    claims_config: &AuthClaimsConfig,
) -> Result<VerifiedJwt, String> {
    // Ensure jwks present or fetched
    crate::auth::jwks::ensure_jwks_cached(&issuer.jwks_url)
        .await
        .map_err(|e| format!("failed to ensure jwks: {e}"))?;
    // Parse header to get kid and alg
    let header =
        jsonwebtoken::decode_header(compact).map_err(|e| format!("invalid jwt header: {e}"))?;
    let kid = header.kid.as_deref().unwrap_or("");

    // Try to get decoding key from cache; if missing, fetch and cache, then retry
    if jwks::get_decoding_key_from_cache(&issuer.jwks_url, kid).is_none() {
        jwks::refresh_jwks_for_missing_kid(&issuer.jwks_url, kid)
            .await
            .map_err(|e| format!("failed to fetch jwks: {e}"))?;
    }

    let dk = jwks::get_decoding_key_from_cache(&issuer.jwks_url, kid)
        .ok_or_else(|| "no matching key in jwks".to_string())?;

    // Determine alg for validation
    let alg = header.alg;
    let mut validation = jsonwebtoken::Validation::new(alg);
    validation.validate_aud = false;
    validation.validate_exp = false;
    validation.validate_nbf = false;

    // Verify and extract claims as serde_json::Value
    let token_data = jsonwebtoken::decode::<serde_json::Value>(compact, &dk, &validation)
        .map_err(|e| format!("signature verification failed: {e}"))?;

    let raw_claims: RawClaims =
        serde_json::from_value(token_data.claims).map_err(|e| format!("json parse error: {e}"))?;

    verified_session_claims(
        raw_claims,
        &[issuer.issuer.as_str()],
        audiences,
        claims_config,
    )
}

/// Verify a JWT using the configured verification path.
///
/// Rules:
/// - Tokens with `iss` must verify against issuer-derived JWKS.
/// - Tokens without `iss` may be accepted only with explicit HMAC auth config.
/// - There is no permissive no-verify fallback.
///
/// # Errors
///
/// Returns an error when the configured verification path fails or when claim
/// normalization rejects the token.
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
    let verified =
        verified_jwt_with_claims_config(compact, auth_config, &AuthClaimsConfig::default()).await?;
    Ok((verified.permissions, verified.claims))
}

/// Verify a JWT using the configured auth path and normalize session claims.
///
/// # Errors
///
/// Returns an error when the token cannot be parsed, the selected auth mode
/// rejects the issuer or algorithm, signature verification fails, or claim
/// normalization fails.
pub async fn verified_jwt_with_claims_config(
    compact: &str,
    auth_config: &AuthConfig,
    claims_config: &AuthClaimsConfig,
) -> Result<VerifiedJwt, String> {
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
                .map_err(|e| format!("json parse error: {e}"))?;
            verified_session_claims(verified_raw, &[], auth_config.audiences(), claims_config)
        }
        AuthConfig::Jwks(_) => {
            let issuer = auth_config
                .find_issuer(&raw_claims.iss)
                .ok_or_else(|| "issuer not allowed".to_string())?;
            verified_jwt_using_jwks_with_claims_config(
                compact,
                issuer,
                auth_config.audiences(),
                claims_config,
            )
            .await
        }
    }
}

/// Create default anonymous permissions with full access across all domains.
/// Used when `FITZ_AUTH_REQUIRED=false` for development/testing.
#[must_use]
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
mod auth_tests;
