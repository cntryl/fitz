use base64::Engine;
use serde::Deserialize;

use crate::auth::Permission;

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
    // Audience may be a single string in our environment; keep it simple for now.
    pub aud: String,
    pub sub: String,
    pub exp: u64,
    pub nbf: Option<u64>,

    // Tenant resolution - prefer one of these
    pub tid: Option<String>,
    pub tenant_id: Option<String>,
    pub org_id: Option<String>,

    #[serde(default)]
    pub fitz: Option<FitzClaims>,

    /// Roles claim (array of strings) - used as verbatim Fitz permission strings
    #[serde(default)]
    pub roles: Option<Vec<String>>,

    /// Scopes - could be `scp` (array or string) or `scope` (space-delimited string)
    #[serde(rename = "scp", default)]
    pub scp: Option<ScopeClaim>,

    #[serde(rename = "scope", default)]
    pub scope: Option<String>,
}

impl RawClaims {
    pub fn route_family(&self) -> Result<u32, String> {
        let family = self
            .fitz
            .as_ref()
            .and_then(|fitz| fitz.route_family)
            .ok_or_else(|| "fitz.route_family claim is required".to_string())?;
        if family == 0 {
            return Err("fitz.route_family must be non-zero".to_string());
        }
        Ok(family)
    }

    /// Validate standard claims against issuer allowlist and audience, and time
    /// checks. `now` is the current unix epoch seconds.
    pub fn validate(&self, allowlist: &[&str], audiences: &[&str], now: u64) -> Result<(), String> {
        // Issuer allowlist
        if allowlist.is_empty() {
            if !self.iss.is_empty() {
                return Err("issuer not allowed".to_string());
            }
        } else if !allowlist.iter().any(|&a| a == self.iss) {
            return Err("issuer not allowed".to_string());
        }

        // Audience must match one configured value exactly
        if audiences.is_empty() || !audiences.iter().any(|aud| *aud == self.aud) {
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

        // Tenant resolution
        let mut resolved: Vec<&str> = Vec::new();
        if let Some(t) = &self.tid {
            resolved.push(t);
        }
        if let Some(t) = &self.tenant_id {
            resolved.push(t);
        }
        if let Some(t) = &self.org_id {
            resolved.push(t);
        }

        if resolved.len() != 1 {
            return Err("must resolve exactly one tenant id".to_string());
        }

        Ok(())
    }

    /// Normalize permissions from claims using the prioritized sources:
    /// 1) fitz.permissions (preferred)
    /// 2) roles (array of strings, treated verbatim)
    /// 3) scp / scope (space-delimited or array) - exact or coarse mapping
    ///
    /// Returns Err if the chosen source is malformed or produces no permissions.
    pub fn normalized_permissions(&self) -> Result<Vec<Permission>, String> {
        // Helper to parse a list of candidate permission strings
        fn parse_list(cands: Vec<String>) -> Result<Vec<Permission>, String> {
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
            Ok(out)
        }

        // 1) fitz.permissions
        if let Some(f) = &self.fitz {
            if let Some(perms) = &f.permissions {
                // If present, use this source exclusively (and reject if malformed)
                return parse_list(perms.clone());
            }
        }

        // 2) roles
        if let Some(roles) = &self.roles {
            // roles must be array of strings; treat each verbatim as a Fitz permission string
            return parse_list(roles.clone());
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
/// - resolved realm (from tid/tenant_id/org_id)
/// - roles array (never re-interpreted)
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
    pub tenant: String,
    pub route_family: u32,
    pub roles: Vec<String>,
    pub permissions: Vec<crate::auth::Permission>,
    pub exp: u64,
}

impl RawClaims {
    /// Validate and normalize into a `Claims` object. This performs the same
    /// validation as `RawClaims::validate` and resolves tenant + permissions.
    pub fn normalize(
        self,
        allowlist: &[&str],
        audiences: &[&str],
        now: u64,
    ) -> Result<Claims, String> {
        // Basic validation (issuer, audience, time checks, tenant resolution)
        self.validate(allowlist, audiences, now)?;

        // Resolve tenant id (we already know validate ensured exactly one present)
        let tenant = if let Some(t) = &self.tid {
            t.clone()
        } else if let Some(t) = &self.tenant_id {
            t.clone()
        } else if let Some(t) = &self.org_id {
            t.clone()
        } else {
            return Err("must resolve exactly one tenant id".to_string());
        };

        // Roles (if absent, empty vec). Clone to avoid partially moving `self`.
        let roles = self.roles.clone().unwrap_or_default();
        let route_family = self.route_family()?;

        // Permissions (normalize using existing helper)
        let permissions = self.normalized_permissions()?;

        Ok(Claims {
            sub: self.sub,
            tenant,
            route_family,
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
    // 1) fitz.permissions
    if let Some(fitz) = value.get("fitz") {
        if let Some(perms_v) = fitz.get("permissions") {
            if perms_v.is_array() {
                let mut out = Vec::new();
                for v in perms_v.as_array().unwrap().iter() {
                    if let Some(s) = v.as_str() {
                        let p = Permission::parse(s)
                            .map_err(|e| format!("malformed permission: {} ({})", s, e))?;
                        out.push(p);
                    }
                }
                return Ok(out);
            }
        }
    }

    // 2) roles
    if let Some(roles_v) = value.get("roles") {
        if roles_v.is_array() {
            let mut out = Vec::new();
            for v in roles_v.as_array().unwrap().iter() {
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
            "fitz": { "route_family": 1, "permissions": ["notice://prod/orders/**#read"] }
        });
        let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload.to_string());
        let jwt = format!("{}.{}.{}", "{}", b64, "sig");

        // Act
        let claims = parse_jwt_noverify(&jwt).expect("parse jwt");
        let perms = claims.normalized_permissions().expect("perms");

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
            "fitz": { "route_family": 1, "permissions": ["notice://prod/orders/**#read"] }
        });
        let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload.to_string());
        let jwt = format!("{}.{}.{}", "{}", b64, "sig");

        // Act
        let raw = parse_jwt_noverify(&jwt).expect("parse jwt");
        let normalized = raw
            .normalize(&["https://idp.example/"], &["fitz-broker"], 0)
            .expect("normalize");

        // Assert
        assert_eq!(normalized.tenant, "acme-prod");
        assert_eq!(normalized.route_family, 1);
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
        let result = raw.normalize(&["https://idp.example/"], &["fitz-broker"], 9999999999);

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
        let result = raw.normalize(&["https://idp.example/"], &["fitz-broker"], 0);

        // Assert
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("issuer"));
    }

    #[test]
    fn should_reject_missing_route_family_claim() {
        // Arrange
        let payload = serde_json::json!({
            "iss": "https://idp.example/",
            "aud": "fitz-broker",
            "sub": "user:42",
            "exp": 9999999999u64,
            "tid": "acme-prod",
            "fitz": { "permissions": [] }
        });
        let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload.to_string());
        let jwt = format!("{}.{}.{}", "{}", b64, "sig");

        // Act
        let raw = parse_jwt_noverify(&jwt).expect("parse jwt");
        let result = raw.normalize(&["https://idp.example/"], &["fitz-broker"], 0);

        // Assert
        assert_eq!(result.unwrap_err(), "fitz.route_family claim is required");
    }

    #[test]
    fn should_reject_zero_route_family_claim() {
        // Arrange
        let payload = serde_json::json!({
            "iss": "https://idp.example/",
            "aud": "fitz-broker",
            "sub": "user:42",
            "exp": 9999999999u64,
            "tid": "acme-prod",
            "fitz": { "route_family": 0, "permissions": [] }
        });
        let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload.to_string());
        let jwt = format!("{}.{}.{}", "{}", b64, "sig");

        // Act
        let raw = parse_jwt_noverify(&jwt).expect("parse jwt");
        let result = raw.normalize(&["https://idp.example/"], &["fitz-broker"], 0);

        // Assert
        assert_eq!(result.unwrap_err(), "fitz.route_family must be non-zero");
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
