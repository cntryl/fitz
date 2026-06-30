use serde::Deserialize;
use std::collections::HashMap;

mod config;
mod env;
mod permissions;
mod route_family;
mod token_parser;

pub use config::AuthClaimsConfig;
pub use route_family::RouteFamilyResolverConfig;
pub use token_parser::parse_jwt_noverify;

pub const DEFAULT_ROUTE_FAMILY_CLAIM: &str = "tid";
pub const DEFAULT_ROLE_CLAIM: &str = "roles";
pub const ENV_ROUTE_FAMILY_CLAIM: &str = "FITZ_ROUTE_FAMILY_CLAIM";
pub const ENV_ROUTE_FAMILY_MAP: &str = "FITZ_ROUTE_FAMILY_MAP";
pub const ENV_AUTH_CUSTOM_CLAIM: &str = "FITZ_AUTH_CUSTOM_CLAIM";
pub const ENV_AUTH_ROLE_CLAIM: &str = "FITZ_AUTH_ROLE_CLAIM";
pub const ENV_AUTH_ORG_CLAIM: &str = "FITZ_AUTH_ORG_CLAIM";
pub const ENV_AUTH_PERMISSIONS_CLAIM: &str = "FITZ_AUTH_PERMISSIONS_CLAIM";

const REMOVED_ENV_AUTH_ALLOW_JWT_ROUTE_FAMILY: &str = "FITZ_AUTH_ALLOW_JWT_ROUTE_FAMILY";

#[derive(Debug, Clone, Deserialize)]
pub struct CustomPermissionsClaim {
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

    /// Auth0 RBAC permissions claim.
    #[serde(default)]
    pub permissions: Option<Vec<String>>,

    /// Scopes - could be `scp` (array or string) or `scope` (space-delimited string)
    #[serde(rename = "scp", default)]
    pub scp: Option<ScopeClaim>,

    #[serde(rename = "scope", default)]
    pub scope: Option<String>,

    #[serde(flatten)]
    pub extra: HashMap<String, serde_json::Value>,
}

impl RawClaims {
    /// Read a raw identity claim value as a string.
    ///
    /// # Errors
    ///
    /// Returns an error when the selected claim exists but is not a string.
    pub fn identity_claim_value(&self, claim: &str) -> Result<Option<String>, String> {
        match claim {
            "sub" => Ok(Some(self.sub.clone())),
            other => match self.extra.get(other) {
                Some(value) => value
                    .as_str()
                    .map(|value| Some(value.to_string()))
                    .ok_or_else(|| {
                        format!("route family identity claim '{other}' must be a string")
                    }),
                None => Ok(None),
            },
        }
    }

    /// Validate standard claims against issuer allowlist, audience, and time
    /// checks. `now` is the current unix epoch seconds.
    ///
    /// # Errors
    ///
    /// Returns an error when claim configuration is invalid, the issuer or
    /// audience does not match policy, token time bounds fail, or removed Fitz
    /// claim shapes are present.
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

        self.validate_removed_fitz_claims()?;

        Ok(())
    }

    fn validate_removed_fitz_claims(&self) -> Result<(), String> {
        let Some(fitz) = self.extra.get("fitz") else {
            return Ok(());
        };

        if fitz
            .as_object()
            .is_some_and(|fitz| fitz.contains_key("route_family"))
        {
            return Err(format!(
                "fitz.route_family claim is not accepted; configure {ENV_ROUTE_FAMILY_MAP} instead"
            ));
        }

        if fitz
            .as_object()
            .is_some_and(|fitz| fitz.contains_key("permissions"))
        {
            return Err(
                "fitz.permissions claim is not accepted; emit permissions directly or use a namespaced custom claim"
                    .to_string(),
            );
        }

        Err("fitz claim is not accepted".to_string())
    }
}

/// Normalized, immutable Claims used by the runtime.
///
/// This is produced **once at auth time** from a `RawClaims` and contains:
/// - subject identity
/// - optional identity context used by server-side route-family resolution
/// - permissions array (fully normalized, never reparsed)
/// - expiration time
///
/// **Critical invariant:** Downstream code never reparses scopes.
/// Permissions are fixed when `Claims` is created.
///
/// This makes Claims the "truth layer" — what you see is what you get,
/// no lazy evaluation, no reinterpretation.
#[derive(Debug, Clone)]
pub struct Claims {
    pub sub: String,
    pub identity_claim: Option<String>,
    pub identity_value: Option<String>,
    pub permissions: Vec<crate::auth::Permission>,
    pub exp: u64,
}

impl RawClaims {
    /// Validate and normalize into a `Claims` object. This performs the same
    /// validation as `RawClaims::validate` and resolves Fitz realm semantics
    /// from external claim-source names plus permissions.
    ///
    /// # Errors
    ///
    /// Returns an error when base claim validation fails or when identity or
    /// permission normalization cannot derive a valid `Claims` view.
    pub fn normalize(
        &self,
        allowlist: &[&str],
        audiences: &[&str],
        now: u64,
        claims_config: &AuthClaimsConfig,
    ) -> Result<Claims, String> {
        // Basic validation (issuer, audience, time checks, legacy route-family policy)
        self.validate(allowlist, audiences, now, claims_config)?;
        let identity_value = if let Some(claim_name) = claims_config.org_claim_override.as_deref() {
            if let Some(identity) = self.identity_claim_value(claim_name)? {
                Some(identity)
            } else {
                self.identity_claim_value(&claims_config.identity_claim)?
            }
        } else {
            self.identity_claim_value(&claims_config.identity_claim)?
        };
        let identity_claim = identity_value.as_ref().map(|_| {
            if let Some(claim_name) = claims_config.org_claim_override.as_deref() {
                if self.extra.contains_key(claim_name) {
                    return claim_name.to_string();
                }
            }
            claims_config.identity_claim.clone()
        });

        // Permissions (normalize using existing helper)
        let permissions = self.normalized_permissions(
            claims_config.custom_claim.as_deref(),
            claims_config.permissions_claim_override.as_deref(),
            &claims_config.role_claim,
        )?;

        Ok(Claims {
            sub: self.sub.clone(),
            identity_claim,
            identity_value,
            permissions,
            exp: self.exp,
        })
    }
}

#[cfg(test)]
mod claims_tests;

#[cfg(test)]
mod permission_parsing_tests;
