use base64::Engine;
use serde::Deserialize;
use std::collections::HashMap;

use crate::auth::Permission;

pub const DEFAULT_ROUTE_FAMILY_CLAIM: &str = "tid";
pub const ENV_ROUTE_FAMILY_CLAIM: &str = "FITZ_ROUTE_FAMILY_CLAIM";
pub const ENV_ROUTE_FAMILY_MAP: &str = "FITZ_ROUTE_FAMILY_MAP";
pub const ENV_AUTH_CUSTOM_CLAIM: &str = "FITZ_AUTH_CUSTOM_CLAIM";
pub const ENV_AUTH_ALLOW_JWT_ROUTE_FAMILY: &str = "FITZ_AUTH_ALLOW_JWT_ROUTE_FAMILY";

/// Token-claim normalization knobs shared by HMAC and JWKS verification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthClaimsConfig {
    pub identity_claim: String,
    pub custom_claim: Option<String>,
    pub allow_legacy_route_family_claim: bool,
    invalid_reason: Option<String>,
}

impl Default for AuthClaimsConfig {
    fn default() -> Self {
        Self::from_parts(DEFAULT_ROUTE_FAMILY_CLAIM.to_string(), None, false, None)
    }
}

impl AuthClaimsConfig {
    pub fn from_env() -> Self {
        let identity_claim = env_non_empty(ENV_ROUTE_FAMILY_CLAIM)
            .unwrap_or_else(|| DEFAULT_ROUTE_FAMILY_CLAIM.to_string());
        let custom_claim = env_non_empty(ENV_AUTH_CUSTOM_CLAIM);
        let (allow_legacy_route_family_claim, invalid_reason) =
            match env_bool(ENV_AUTH_ALLOW_JWT_ROUTE_FAMILY, false) {
                Ok(value) => (value, None),
                Err(error) => (false, Some(error)),
            };

        Self::from_parts(
            identity_claim,
            custom_claim,
            allow_legacy_route_family_claim,
            invalid_reason,
        )
    }

    pub fn new(
        identity_claim: impl Into<String>,
        custom_claim: Option<String>,
        allow_legacy_route_family_claim: bool,
    ) -> Self {
        Self::from_parts(
            identity_claim.into(),
            custom_claim,
            allow_legacy_route_family_claim,
            None,
        )
    }

    fn from_parts(
        identity_claim: String,
        custom_claim: Option<String>,
        allow_legacy_route_family_claim: bool,
        invalid_reason: Option<String>,
    ) -> Self {
        Self {
            identity_claim,
            custom_claim,
            allow_legacy_route_family_claim,
            invalid_reason,
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        if let Some(reason) = &self.invalid_reason {
            return Err(reason.clone());
        }
        if self.identity_claim.trim().is_empty() {
            return Err(format!("{ENV_ROUTE_FAMILY_CLAIM} must not be empty"));
        }
        if self
            .custom_claim
            .as_ref()
            .is_some_and(|claim| claim.trim().is_empty())
        {
            return Err(format!("{ENV_AUTH_CUSTOM_CLAIM} must not be empty"));
        }
        Ok(())
    }
}

/// Broker-local route-family resolver used as the v1 control-plane substitute.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteFamilyResolverConfig {
    pub identity_claim: String,
    pub mappings: HashMap<String, u32>,
    pub allow_legacy_route_family_claim: bool,
    invalid_reason: Option<String>,
}

impl Default for RouteFamilyResolverConfig {
    fn default() -> Self {
        Self::from_parts(
            DEFAULT_ROUTE_FAMILY_CLAIM.to_string(),
            HashMap::new(),
            false,
            None,
        )
    }
}

impl RouteFamilyResolverConfig {
    pub fn from_env() -> Self {
        let identity_claim = env_non_empty(ENV_ROUTE_FAMILY_CLAIM)
            .unwrap_or_else(|| DEFAULT_ROUTE_FAMILY_CLAIM.to_string());
        let (allow_legacy_route_family_claim, bool_error) =
            match env_bool(ENV_AUTH_ALLOW_JWT_ROUTE_FAMILY, false) {
                Ok(value) => (value, None),
                Err(error) => (false, Some(error)),
            };
        let (mappings, map_error) = parse_route_family_map_env();

        Self::from_parts(
            identity_claim,
            mappings,
            allow_legacy_route_family_claim,
            bool_error.or(map_error),
        )
    }

    pub fn from_mappings(
        identity_claim: impl Into<String>,
        mappings: impl IntoIterator<Item = (impl Into<String>, u32)>,
    ) -> Self {
        Self::from_parts(
            identity_claim.into(),
            mappings
                .into_iter()
                .map(|(identity, family)| (identity.into(), family))
                .collect(),
            false,
            None,
        )
    }

    fn from_parts(
        identity_claim: String,
        mappings: HashMap<String, u32>,
        allow_legacy_route_family_claim: bool,
        invalid_reason: Option<String>,
    ) -> Self {
        Self {
            identity_claim,
            mappings,
            allow_legacy_route_family_claim,
            invalid_reason,
        }
    }

    pub fn validate(
        &self,
        provisioned_families: &[u32],
        auth_required: bool,
    ) -> Result<(), String> {
        if let Some(reason) = &self.invalid_reason {
            return Err(reason.clone());
        }
        if self.identity_claim.trim().is_empty() {
            return Err(format!("{ENV_ROUTE_FAMILY_CLAIM} must not be empty"));
        }
        if auth_required && !self.allow_legacy_route_family_claim && self.mappings.is_empty() {
            return Err(format!(
                "{ENV_ROUTE_FAMILY_MAP} must contain at least one mapping when authentication is required"
            ));
        }

        let provisioned = provisioned_families
            .iter()
            .copied()
            .collect::<std::collections::HashSet<_>>();
        for (identity, family) in &self.mappings {
            if identity.trim().is_empty() {
                return Err(format!(
                    "{ENV_ROUTE_FAMILY_MAP} identity keys must not be empty"
                ));
            }
            if *family == 0 {
                return Err(format!(
                    "{ENV_ROUTE_FAMILY_MAP} must not map identities to route family 0"
                ));
            }
            if !provisioned.contains(family) {
                return Err(format!(
                    "{ENV_ROUTE_FAMILY_MAP} maps identity '{}' to unprovisioned route family {}",
                    identity, family
                ));
            }
        }

        Ok(())
    }

    pub fn resolve(&self, raw_claims: &RawClaims) -> Result<u32, String> {
        if let Some(identity_value) = raw_claims.identity_claim_value(&self.identity_claim)? {
            if let Some(family) = self.mappings.get(&identity_value) {
                return Ok(*family);
            }
            if !self.allow_legacy_route_family_claim {
                return Err(format!(
                    "route family identity claim {}={} is not mapped by {}",
                    self.identity_claim, identity_value, ENV_ROUTE_FAMILY_MAP
                ));
            }
        } else if !self.allow_legacy_route_family_claim {
            return Err(format!(
                "route family identity claim '{}' is required",
                self.identity_claim
            ));
        }

        if self.allow_legacy_route_family_claim {
            return raw_claims
                .legacy_route_family()
                .transpose()?
                .ok_or_else(|| {
                    format!(
                        "route family identity claim '{}' is required or legacy fitz.route_family must be present",
                        self.identity_claim
                    )
                });
        }

        Err(format!(
            "route family identity claim '{}' is required",
            self.identity_claim
        ))
    }
}

fn env_non_empty(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn env_bool(key: &str, default: bool) -> Result<bool, String> {
    match env_non_empty(key) {
        Some(value) => value
            .parse::<bool>()
            .map_err(|_| format!("{key} must be true or false")),
        None => Ok(default),
    }
}

fn parse_route_family_map_env() -> (HashMap<String, u32>, Option<String>) {
    let Some(raw) = env_non_empty(ENV_ROUTE_FAMILY_MAP) else {
        return (HashMap::new(), None);
    };

    let mut mappings = HashMap::new();
    for entry in raw
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
    {
        let Some((identity, family)) = entry.split_once('=') else {
            return (
                mappings,
                Some(format!(
                    "{ENV_ROUTE_FAMILY_MAP} entries must use identity=family format"
                )),
            );
        };
        let identity = identity.trim();
        if identity.is_empty() {
            return (
                mappings,
                Some(format!(
                    "{ENV_ROUTE_FAMILY_MAP} identity keys must not be empty"
                )),
            );
        }
        if mappings.contains_key(identity) {
            return (
                mappings,
                Some(format!(
                    "{ENV_ROUTE_FAMILY_MAP} contains duplicate identity '{}'",
                    identity
                )),
            );
        }
        let Ok(family) = family.trim().parse::<u32>() else {
            return (
                mappings,
                Some(format!(
                    "{ENV_ROUTE_FAMILY_MAP} family for identity '{}' must be an unsigned integer",
                    identity
                )),
            );
        };
        mappings.insert(identity.to_string(), family);
    }

    (mappings, None)
}

/// Claims normalization and validation layer.
///
/// **Strict invariant:** Claims are immutable once produced.
/// They are the "truth layer" - once normalized at auth time, downstream code
/// never reinterprets, reparses, or remaps scope/role strings.
///
/// - `RawClaims`: unparsed claims from JWT payload
/// - `Claims`: fully normalized, immutable claims suitable for authorization checks
///   Portion of the OIDC JWT reserved for Fitz-specific data.
#[derive(Debug, Clone, Deserialize)]
pub struct FitzClaims {
    pub route_family: Option<u32>,
    pub permissions: Option<Vec<String>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CustomFitzClaims {
    pub permissions: Option<Vec<String>>,
}

/// Audience claim can be a single string or an array of strings.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum AudienceClaim {
    String(String),
    Array(Vec<String>),
}

impl AudienceClaim {
    fn matches_any(&self, audiences: &[&str]) -> bool {
        match self {
            Self::String(audience) => audiences.iter().any(|allowed| *allowed == audience),
            Self::Array(values) => values
                .iter()
                .any(|audience| audiences.iter().any(|allowed| *allowed == audience)),
        }
    }
}

/// Scope claim can be a space-delimited string or an array of strings
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum ScopeClaim {
    String(String),
    Array(Vec<String>),
}

/// Raw set of JWT claims as received. This is not the normalized immutable
/// Claims view used by the runtime; call `RawClaims::normalized_permissions()`
/// to obtain a fully normalized permission list.
#[derive(Debug, Clone, Deserialize)]
pub struct RawClaims {
    pub iss: String,
    pub aud: AudienceClaim,
    pub sub: String,
    pub exp: u64,
    pub nbf: Option<u64>,

    // External identity context used by the route-family resolver.
    pub tid: Option<String>,
    pub tenant_id: Option<String>,
    pub org_id: Option<String>,

    #[serde(default)]
    pub fitz: Option<FitzClaims>,

    /// Auth0 RBAC permissions claim.
    #[serde(default)]
    pub permissions: Option<Vec<String>>,

    /// Roles claim (array of strings) - identity/provider metadata only.
    #[serde(default)]
    pub roles: Option<Vec<String>>,

    /// Scopes - could be `scp` (array or string) or `scope` (space-delimited string)
    #[serde(rename = "scp", default)]
    pub scp: Option<ScopeClaim>,

    #[serde(rename = "scope", default)]
    pub scope: Option<String>,

    #[serde(flatten)]
    pub extra: HashMap<String, serde_json::Value>,
}

impl RawClaims {
    pub fn legacy_route_family(&self) -> Option<Result<u32, String>> {
        self.fitz.as_ref().and_then(|fitz| {
            fitz.route_family.map(|family| {
                if family == 0 {
                    Err("fitz.route_family must be non-zero".to_string())
                } else {
                    Ok(family)
                }
            })
        })
    }

    pub fn identity_claim_value(&self, claim: &str) -> Result<Option<String>, String> {
        match claim {
            "tid" => Ok(self.tid.clone()),
            "tenant_id" => Ok(self.tenant_id.clone()),
            "org_id" => Ok(self.org_id.clone()),
            "sub" => Ok(Some(self.sub.clone())),
            other => match self.extra.get(other) {
                Some(value) => value
                    .as_str()
                    .map(|value| Some(value.to_string()))
                    .ok_or_else(|| {
                        format!("route family identity claim '{}' must be a string", other)
                    }),
                None => Ok(None),
            },
        }
    }

    /// Validate standard claims against issuer allowlist and audience, and time
    /// checks. `now` is the current unix epoch seconds.
    pub fn validate(
        &self,
        allowlist: &[&str],
        audiences: &[&str],
        now: u64,
        claims_config: &AuthClaimsConfig,
    ) -> Result<(), String> {
        claims_config.validate()?;

        // Issuer allowlist
        if allowlist.is_empty() {
            if !self.iss.is_empty() {
                return Err("issuer not allowed".to_string());
            }
        } else if !allowlist.iter().any(|&a| a == self.iss) {
            return Err("issuer not allowed".to_string());
        }

        // Audience must match one configured value exactly
        if audiences.is_empty() || !self.aud.matches_any(audiences) {
            return Err("audience mismatch".to_string());
        }

        // Expiration
        if self.exp <= now {
            return Err("token expired".to_string());
        }

        // Not before
        if let Some(nbf) = self.nbf {
            if now < nbf {
                return Err("token not yet valid".to_string());
            }
        }

        if !claims_config.allow_legacy_route_family_claim && self.legacy_route_family().is_some() {
            return Err(format!(
                "fitz.route_family claim is not accepted; configure {} instead",
                ENV_ROUTE_FAMILY_MAP
            ));
        }
        if claims_config.allow_legacy_route_family_claim {
            if let Some(route_family) = self.legacy_route_family() {
                route_family?;
            }
        }

        Ok(())
    }

    /// Normalize permissions from claims using the prioritized sources:
    /// 1) configured namespaced custom claim
    /// 2) top-level permissions array (Auth0 RBAC)
    /// 3) scp / scope (space-delimited or array) - exact or coarse mapping
    ///
    /// Returns Err if the chosen source is malformed or produces no permissions.
    pub fn normalized_permissions(
        &self,
        custom_claim: Option<&str>,
    ) -> Result<Vec<Permission>, String> {
        // Helper to parse a list of candidate permission strings
        fn parse_list(source: &str, cands: Vec<String>) -> Result<Vec<Permission>, String> {
            let mut out = Vec::new();
            for raw in cands.into_iter() {
                let trimmed = raw.trim();
                if trimmed.is_empty() {
                    continue;
                }
                let p = Permission::parse(trimmed)
                    .map_err(|e| format!("malformed permission: {} ({})", trimmed, e))?;
                out.push(p);
            }
            if out.is_empty() {
                return Err(format!("no permissions derived from {}", source));
            }
            Ok(out)
        }

        // 1) configured namespaced custom claim
        if let Some(claim_name) = custom_claim {
            if let Some(perms) = self.custom_claim_permissions(claim_name)? {
                return parse_list(claim_name, perms);
            }
        }

        // 2) Auth0 RBAC permissions
        if let Some(permissions) = &self.permissions {
            return parse_list("permissions", permissions.clone());
        }

        // 3) scopes (scp or scope)
        let mut scope_vals: Vec<String> = Vec::new();
        if let Some(scp) = &self.scp {
            match scp {
                ScopeClaim::String(s) => {
                    for part in s.split_whitespace() {
                        scope_vals.push(part.to_string());
                    }
                }
                ScopeClaim::Array(arr) => {
                    for s in arr.iter() {
                        scope_vals.push(s.clone());
                    }
                }
            }
        } else if let Some(s) = &self.scope {
            for part in s.split_whitespace() {
                scope_vals.push(part.to_string());
            }
        }

        if !scope_vals.is_empty() {
            // For each scope: prefer exact Permission parse; otherwise attempt mapping
            let mut out: Vec<Permission> = Vec::new();
            for sc in scope_vals.into_iter() {
                let trimmed = sc.trim();
                if trimmed.is_empty() {
                    continue;
                }

                // Determine if this is an exact Fitz permission (must contain a scheme)
                if trimmed.contains("://") {
                    let p = Permission::parse(trimmed)
                        .map_err(|e| format!("malformed permission: {} ({})", trimmed, e))?;
                    out.push(p);
                    continue;
                }

                // Try coarse scope mapping for strings like 'notice.read'
                if let Some(mapped) = crate::auth::map_coarse_scope(trimmed) {
                    let p = Permission::parse(mapped)
                        .map_err(|e| format!("malformed mapped permission: {} ({})", mapped, e))?;
                    out.push(p);
                    continue;
                }

                // Unknown scope string -> malformed
                return Err(format!("malformed scope string: {}", trimmed));
            }

            if out.is_empty() {
                return Err("no permissions derived from scopes".to_string());
            }

            return Ok(out);
        }

        Err("no permission source found".to_string())
    }

    fn custom_claim_permissions(&self, claim_name: &str) -> Result<Option<Vec<String>>, String> {
        if claim_name == "fitz" {
            return Ok(self.fitz.as_ref().and_then(|fitz| fitz.permissions.clone()));
        }

        let Some(value) = self.extra.get(claim_name) else {
            return Ok(None);
        };
        let custom_claim: CustomFitzClaims = serde_json::from_value(value.clone())
            .map_err(|e| format!("malformed custom claim {}: {}", claim_name, e))?;
        Ok(custom_claim.permissions)
    }
}

/// Parse a compact JWS/JWT and return the deserialized `RawClaims` WITHOUT verifying signature.
///
/// This function only supports compact serialization (header.payload.signature) and
/// base64-url decoding of the payload. Signature verification is done in a later step.
pub fn parse_jwt_noverify(compact: &str) -> Result<RawClaims, String> {
    let parts: Vec<&str> = compact.split('.').collect();
    if parts.len() != 3 {
        return Err("invalid jwt format".to_string());
    }

    let payload = parts[1];
    // base64 url decode without padding
    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .map_err(|e| format!("base64 decode error: {}", e))?;

    let s = String::from_utf8(decoded).map_err(|e| format!("utf8 error: {}", e))?;
    let claims: RawClaims =
        serde_json::from_str(&s).map_err(|e| format!("json parse error: {}", e))?;
    Ok(claims)
}

/// Normalized, immutable Claims used by the runtime.
///
/// This is produced **once at auth time** from a `RawClaims` and contains:
/// - subject identity
/// - optional identity context used by server-side route-family resolution
/// - roles array (identity/provider metadata, never authorization)
/// - permissions array (fully normalized, never reparsed)
/// - expiration time
///
/// **Critical invariant:** Downstream code never reparses scopes or roles.
/// Permissions are fixed when `Claims` is created.
///
/// This makes Claims the "truth layer" — what you see is what you get,
/// no lazy evaluation, no reinterpretation.
#[derive(Debug, Clone)]
pub struct Claims {
    pub sub: String,
    pub identity_claim: Option<String>,
    pub identity_value: Option<String>,
    pub roles: Vec<String>,
    pub permissions: Vec<crate::auth::Permission>,
    pub exp: u64,
}

impl RawClaims {
    /// Validate and normalize into a `Claims` object. This performs the same
    /// validation as `RawClaims::validate` and resolves Fitz realm semantics
    /// from external claim-source names plus permissions.
    pub fn normalize(
        &self,
        allowlist: &[&str],
        audiences: &[&str],
        now: u64,
        claims_config: &AuthClaimsConfig,
    ) -> Result<Claims, String> {
        // Basic validation (issuer, audience, time checks, legacy route-family policy)
        self.validate(allowlist, audiences, now, claims_config)?;
        let identity_value = self.identity_claim_value(&claims_config.identity_claim)?;
        let identity_claim = identity_value
            .as_ref()
            .map(|_| claims_config.identity_claim.clone());

        // Roles (if absent, empty vec). Clone to avoid partially moving `self`.
        let roles = self.roles.clone().unwrap_or_default();

        // Permissions (normalize using existing helper)
        let permissions = self.normalized_permissions(claims_config.custom_claim.as_deref())?;

        Ok(Claims {
            sub: self.sub.clone(),
            identity_claim,
            identity_value,
            roles,
            permissions,
            exp: self.exp,
        })
    }
}

/// Extract permissions from a `serde_json::Value` representing JWT claims.
/// This is a permissive extractor that returns an empty Vec if no permissions
/// sources are present instead of failing validation.
#[allow(dead_code)]
pub fn normalized_permissions_from_value(
    value: &serde_json::Value,
) -> Result<Vec<Permission>, String> {
    // 1) Auth0 RBAC permissions
    if let Some(permissions_v) = value.get("permissions") {
        if permissions_v.is_array() {
            let mut out = Vec::new();
            for v in permissions_v.as_array().unwrap().iter() {
                if let Some(s) = v.as_str() {
                    let p = Permission::parse(s)
                        .map_err(|e| format!("malformed permission: {} ({})", s, e))?;
                    out.push(p);
                }
            }
            return Ok(out);
        }
    }

    // 3) scopes: scp (array or string) or scope (string space-delimited)
    if let Some(scp_v) = value.get("scp") {
        // scp may be an array of strings
        if scp_v.is_array() {
            let mut out = Vec::new();
            for v in scp_v.as_array().unwrap().iter() {
                if let Some(s) = v.as_str() {
                    // map coarse scope -> permission
                    let mapped = crate::auth::map_coarse_scope(s)
                        .ok_or_else(|| format!("malformed scope mapping: {}", s))?;
                    out.push(
                        Permission::parse(mapped).map_err(|e| {
                            format!("malformed permission from scope: {} ({})", s, e)
                        })?,
                    );
                }
            }
            return Ok(out);
        } else if scp_v.is_string() {
            let s = scp_v.as_str().unwrap();
            let parts: Vec<&str> = s.split_whitespace().collect();
            let mut out = Vec::new();
            for p in parts.into_iter() {
                let mapped = crate::auth::map_coarse_scope(p)
                    .ok_or_else(|| format!("malformed scope mapping: {}", p))?;
                out.push(
                    Permission::parse(mapped)
                        .map_err(|e| format!("malformed permission from scope: {} ({})", p, e))?,
                );
            }
            return Ok(out);
        }
    }

    if let Some(scope_v) = value.get("scope") {
        if scope_v.is_string() {
            let s = scope_v.as_str().unwrap();
            let parts: Vec<&str> = s.split_whitespace().collect();
            let mut out = Vec::new();
            for p in parts.into_iter() {
                let mapped = crate::auth::map_coarse_scope(p)
                    .ok_or_else(|| format!("malformed scope mapping: {}", p))?;
                out.push(
                    Permission::parse(mapped)
                        .map_err(|e| format!("malformed permission from scope: {} ({})", p, e))?,
                );
            }
            return Ok(out);
        }
    }

    Ok(Vec::new())
}

#[cfg(test)]
mod claims_tests {
    use crate::auth::parse_jwt_noverify;
    use crate::auth::{AuthClaimsConfig, RouteFamilyResolverConfig};
    use base64::Engine;

    #[test]
    fn should_parse_jwt_noverify_extract_permissions() {
        // Arrange
        let payload = serde_json::json!({
            "iss": "https://idp.example/",
            "aud": "fitz-broker",
            "sub": "user:42",
            "exp": 9999999999u64,
            "tid": "acme-prod",
            "permissions": ["notice://prod/orders/**#read"]
        });
        let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload.to_string());
        let jwt = format!("{}.{}.{}", "{}", b64, "sig");

        // Act
        let claims = parse_jwt_noverify(&jwt).expect("parse jwt");
        let perms = claims.normalized_permissions(None).expect("perms");

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
            "exp": 9999999999u64,
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
        assert_eq!(normalized.roles.len(), 1);
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
            9999999999,
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
            "exp": 9999999999u64,
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
            "exp": 9999999999u64,
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
    fn should_reject_zero_legacy_route_family_claim_given_compatibility_enabled() {
        // Arrange
        let payload = serde_json::json!({
            "iss": "https://idp.example/",
            "aud": "fitz-broker",
            "sub": "user:42",
            "exp": 9999999999u64,
            "tid": "acme-prod",
            "fitz": { "route_family": 0 },
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
            &AuthClaimsConfig::new("tid", None, true),
        );

        // Assert
        assert_eq!(result.unwrap_err(), "fitz.route_family must be non-zero");
    }

    #[test]
    fn should_keep_identity_context_orthogonal_to_route_family_resolution() {
        // Arrange
        let payload = serde_json::json!({
            "iss": "https://idp.example/",
            "aud": "fitz-broker",
            "sub": "user:42",
            "exp": 9999999999u64,
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
            "exp": 9999999999u64,
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
            "exp": 9999999999u64,
            "tid": "acme-prod",
            "permissions": ["notice://prod/orders/**#read"],
            "https://fitz.example.com/claims": {
                "permissions": ["notice://prod/orders/**#write"]
            }
        });
        let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload.to_string());
        let jwt = format!("{}.{}.{}", "{}", b64, "sig");

        // Act
        let raw = parse_jwt_noverify(&jwt).expect("parse jwt");
        let permissions = raw
            .normalized_permissions(Some("https://fitz.example.com/claims"))
            .expect("permissions");

        // Assert
        assert_eq!(permissions[0].raw, "notice://prod/orders/**#write");
    }

    #[test]
    fn should_support_auth0_org_id_permissions_shape() {
        // Arrange
        let payload = serde_json::json!({
            "iss": "https://tenant.auth0.com/",
            "aud": ["https://fitz.example.com/api", "https://tenant.auth0.com/userinfo"],
            "sub": "auth0|user-1",
            "exp": 9999999999u64,
            "org_id": "org_acme",
            "permissions": ["notice://prod/orders/**#read"]
        });
        let raw: crate::auth::RawClaims = serde_json::from_value(payload).expect("raw claims");
        let claims_config = AuthClaimsConfig::new("org_id", None, false);
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
            "exp": 9999999999u64,
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
            "exp": 9999999999u64,
            "custom:tenant_id": "acme-prod",
            "scope": "notice.write"
        });
        let raw: crate::auth::RawClaims = serde_json::from_value(payload).expect("raw claims");
        let claims_config = AuthClaimsConfig::new("custom:tenant_id", None, false);
        let resolver =
            RouteFamilyResolverConfig::from_mappings("custom:tenant_id", [("acme-prod", 4)]);

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
            "exp": 9999999999u64,
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
            false,
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
}

#[cfg(test)]
mod permission_parsing_tests {
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
}
