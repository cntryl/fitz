//! JWT claims parsing and Fitz authorization
//!
//! This module defines the minimal Claims shape required by Fitz, a simple
//! permission model (permission strings), and helpers to validate and convert
//! claims into runtime permissions.

use crate::runtime::matcher::Pattern;
use crate::runtime::routing::Route;
use serde::Deserialize;
use std::str::FromStr;
use base64::Engine;

#[derive(Debug, Clone, PartialEq, Eq)]
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

#[derive(Debug, Clone)]
pub struct Permission {
    pub pattern: Pattern,
    pub access: Access,
    /// Original string representation
    pub raw: String,
}

impl Permission {
    /// Parse from '<route>#<access>' where '#<access>' is optional and defaults to '*'
    pub fn parse(s: &str) -> Result<Self, String> {
        let raw = s.to_string();
        let (route_part, access_part) = if let Some(idx) = s.rfind('#') {
            (&s[..idx], &s[idx + 1..])
        } else {
            (s, "*")
        };

        let access = Access::from_str(access_part)?;
        let pattern = Pattern::new(route_part);

        Ok(Self {
            pattern,
            access,
            raw,
        })
    }

    pub fn matches(&self, route: &Route, required: &Access) -> bool {
        // First, pattern must match route
        if !self.pattern.matches(route) {
            return false;
        }

        // Access semantics: All matches everything; Read matches Read/All; Write matches Write/All
        match (&self.access, required) {
            (Access::All, _) => true,
            (Access::Read, Access::Read) => true,
            (Access::Write, Access::Write) => true,
            // Allow 'All' on required side or vice versa is handled above
            _ => false,
        }
    }
}

/// Portion of the OIDC JWT reserved for Fitz-specific data.
#[derive(Debug, Clone, Deserialize)]
pub struct FitzClaims {
    pub permissions: Option<Vec<String>>,
}

/// Scope claim can be a space-delimited string or an array of strings
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum ScopeClaim {
    String(String),
    Array(Vec<String>),
}

/// Basic set of JWT claims Fitz requires for validation and tenant resolution.
#[derive(Debug, Clone, Deserialize)]
pub struct Claims {
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

impl Claims {
    /// Validate standard claims against issuer allowlist and audience, and time
    /// checks. `now` is the current unix epoch seconds.
    pub fn validate(
        &self,
        allowlist: &[&str],
        audience: &str,
        now: u64,
    ) -> Result<(), String> {
        // Issuer allowlist
        if !allowlist.iter().any(|&a| a == self.iss) {
            return Err("issuer not allowed".to_string());
        }

        // Audience must match exactly
        if self.aud != audience {
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
        if let Some(t) = &self.tid { resolved.push(t); }
        if let Some(t) = &self.tenant_id { resolved.push(t); }
        if let Some(t) = &self.org_id { resolved.push(t); }

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
                let p = Permission::parse(trimmed).map_err(|e| format!("malformed permission: {} ({})", trimmed, e))?;
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
                    let p = Permission::parse(trimmed).map_err(|e| format!("malformed permission: {} ({})", trimmed, e))?;
                    out.push(p);
                    continue;
                }

                // Try coarse scope mapping for strings like 'notice.read'
                if let Some(mapped) = map_coarse_scope(trimmed) {
                    let p = Permission::parse(mapped).map_err(|e| format!("malformed mapped permission: {} ({})", mapped, e))?;
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

/// Map coarse scope strings like `notice.read` into Fitz permission strings
fn map_coarse_scope(s: &str) -> Option<&'static str> {
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

/// Parse a compact JWS/JWT and return the deserialized `Claims` WITHOUT verifying signature.
///
/// This function only supports compact serialization (header.payload.signature) and
/// base64-url decoding of the payload. Signature verification is done in a later step.
pub fn parse_jwt_noverify(compact: &str) -> Result<Claims, String> {
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
    let claims: Claims = serde_json::from_str(&s).map_err(|e| format!("json parse error: {}", e))?;
    Ok(claims)
}

/// Parse JWT (no signature verification) and return a `SessionPermissions` snapshot via
/// `Claims::normalized_permissions()`.
pub fn permissions_from_compact_jwt(compact: &str) -> Result<crate::session::permissions::SessionPermissions, String> {
    let claims = parse_jwt_noverify(compact)?;
    let perms = claims.normalized_permissions()?;
    Ok(crate::session::permissions::SessionPermissions::from_permissions(perms))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::routing::Route;

    #[test]
    fn parse_permission_with_fragment() {
        let p = Permission::parse("notice://prod/orders/**#read").unwrap();
        assert!(matches!(p.access, Access::Read));
        assert!(p.pattern.matches(&Route::new("notice://prod/orders/1")));
    }

    #[test]
    fn parse_jwt_noverify_and_extract_permissions() {
        // Create a minimal JWT with fitz.permissions claim
        let payload = serde_json::json!({
            "iss": "https://idp.example/",
            "aud": "fitz-broker",
            "sub": "user:42",
            "exp": 9999999999u64,
            "tid": "acme-prod",
            "fitz": { "permissions": ["notice://prod/orders/**#read"] }
        });
        let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload.to_string());
        let jwt = format!("{}.{}.{}", "{}", b64, "sig");

        let claims = parse_jwt_noverify(&jwt).expect("parse jwt");
        assert_eq!(claims.iss, "https://idp.example/");
        let perms_snapshot = permissions_from_compact_jwt(&jwt).expect("perms");
        assert!(perms_snapshot.allows(&Route::new("notice://prod/orders/create"), Access::Read));
    }

    #[test]
    fn parse_permission_without_fragment_defaults_all() {
        let p = Permission::parse("notice://prod/orders/**").unwrap();
        assert!(matches!(p.access, Access::All));
    }

    #[test]
    fn permission_matches_route_and_access() {
        let p = Permission::parse("notice://prod/orders/**#write").unwrap();
        let route = Route::new("notice://prod/orders/create");
        assert!(p.matches(&route, &Access::Write));
        assert!(!p.matches(&route, &Access::Read));
    }

    #[test]
    fn claims_validate_success() {
        let claims = Claims {
            iss: "https://idp.example/".to_string(),
            aud: "fitz-broker".to_string(),
            sub: "user:42".to_string(),
            exp: 9999999999,
            nbf: None,
            tid: Some("acme-prod".to_string()),
            tenant_id: None,
            org_id: None,
            fitz: None,
            roles: None,
            scp: None,
            scope: None,
        };

        assert!(claims
            .validate(&["https://idp.example/"], "fitz-broker", 0)
            .is_ok());
    }

    #[test]
    fn claims_validate_missing_tenant_fails() {
        let claims = Claims {
            iss: "https://idp.example/".to_string(),
            aud: "fitz-broker".to_string(),
            sub: "user:42".to_string(),
            exp: 9999999999,
            nbf: None,
            tid: None,
            tenant_id: None,
            org_id: None,
            fitz: None,
            roles: None,
            scp: None,
            scope: None,
        };

        assert!(claims
            .validate(&["https://idp.example/"], "fitz-broker", 0)
            .is_err());
    }

    #[test]
    fn normalized_permissions_prefers_fitz() {
        let claims = Claims {
            iss: "https://idp.example/".to_string(),
            aud: "fitz-broker".to_string(),
            sub: "user:42".to_string(),
            exp: 9999999999,
            nbf: None,
            tid: Some("acme-prod".to_string()),
            tenant_id: None,
            org_id: None,
            fitz: Some(FitzClaims {
                permissions: Some(vec!["notice://prod/orders/**#read".to_string()]),
            }),
            roles: Some(vec!["notice://prod/orders/**#write".to_string()]),
            scp: None,
            scope: None,
        };

        let p = claims.normalized_permissions().unwrap();
        assert_eq!(p.len(), 1);
        assert!(p[0].pattern.matches(&Route::new("notice://prod/orders/1")));
        assert!(matches!(p[0].access, Access::Read));
    }

    #[test]
    fn normalized_permissions_roles_used_when_no_fitz() {
        let claims = Claims {
            iss: "https://idp.example/".to_string(),
            aud: "fitz-broker".to_string(),
            sub: "user:42".to_string(),
            exp: 9999999999,
            nbf: None,
            tid: Some("acme-prod".to_string()),
            tenant_id: None,
            org_id: None,
            fitz: None,
            roles: Some(vec!["notice://prod/orders/**#write".to_string()]),
            scp: None,
            scope: None,
        };

        let p = claims.normalized_permissions().unwrap();
        assert_eq!(p.len(), 1);
        assert!(p[0].pattern.matches(&Route::new("notice://prod/orders/create")));
        assert!(matches!(p[0].access, Access::Write));
    }

    #[test]
    fn normalized_permissions_scp_array_and_string_and_mapping() {
        // exact permission in scp array
        let claims1 = Claims {
            iss: "https://idp.example/".to_string(),
            aud: "fitz-broker".to_string(),
            sub: "user:42".to_string(),
            exp: 9999999999,
            nbf: None,
            tid: Some("acme-prod".to_string()),
            tenant_id: None,
            org_id: None,
            fitz: None,
            roles: None,
            scp: Some(ScopeClaim::Array(vec!["notice://prod/orders/**#read".to_string(), "rpc.write".to_string()])),
            scope: None,
        };

        let p1 = claims1.normalized_permissions().unwrap();
        // two permissions: exact + mapped
        assert_eq!(p1.len(), 2);

        // space-delimited scp string with mapping
        let claims2 = Claims {
            scp: Some(ScopeClaim::String("notice.write rpc.read".to_string())),
            iss: "https://idp.example/".to_string(),
            aud: "fitz-broker".to_string(),
            sub: "user:42".to_string(),
            exp: 9999999999,
            nbf: None,
            tid: Some("acme-prod".to_string()),
            tenant_id: None,
            org_id: None,
            fitz: None,
            roles: None,
            scope: None,
        };

        let p2 = claims2.normalized_permissions().unwrap();
        assert_eq!(p2.len(), 2);
        assert!(p2.iter().any(|x| matches!(x.access, Access::Write) && x.pattern.route().contains("notice")));
    }

    #[test]
    fn malformed_higher_priority_source_rejected() {
        // fitz present but contains malformed permission
        let claims = Claims {
            iss: "https://idp.example/".to_string(),
            aud: "fitz-broker".to_string(),
            sub: "user:42".to_string(),
            exp: 9999999999,
            nbf: None,
            tid: Some("acme-prod".to_string()),
            tenant_id: None,
            org_id: None,
            fitz: Some(FitzClaims {
                permissions: Some(vec!["badpermission#oops".to_string()]),
            }),
            roles: Some(vec!["notice://prod/orders/**#write".to_string()]),
            scp: None,
            scope: None,
        };

        assert!(claims.normalized_permissions().is_err());
    }

    #[test]
    fn no_permission_source_rejected() {
        let claims = Claims {
            iss: "https://idp.example/".to_string(),
            aud: "fitz-broker".to_string(),
            sub: "user:42".to_string(),
            exp: 9999999999,
            nbf: None,
            tid: Some("acme-prod".to_string()),
            tenant_id: None,
            org_id: None,
            fitz: None,
            roles: None,
            scp: None,
            scope: None,
        };

        assert!(claims.normalized_permissions().is_err());
    }
}
