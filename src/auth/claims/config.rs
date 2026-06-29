use super::env::{env_non_empty, removed_legacy_route_family_env_reason};
use super::{
    DEFAULT_ROLE_CLAIM, DEFAULT_ROUTE_FAMILY_CLAIM, ENV_AUTH_CUSTOM_CLAIM, ENV_AUTH_ORG_CLAIM,
    ENV_AUTH_PERMISSIONS_CLAIM, ENV_AUTH_ROLE_CLAIM, ENV_ROUTE_FAMILY_CLAIM,
};

/// Token-claim normalization knobs shared by HMAC and JWKS verification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthClaimsConfig {
    pub identity_claim: String,
    pub custom_claim: Option<String>,
    pub role_claim: String,
    pub org_claim_override: Option<String>,
    pub permissions_claim_override: Option<String>,
    invalid_reason: Option<String>,
}

impl Default for AuthClaimsConfig {
    fn default() -> Self {
        Self::from_parts(
            DEFAULT_ROUTE_FAMILY_CLAIM.to_string(),
            None,
            DEFAULT_ROLE_CLAIM.to_string(),
            None,
            None,
            None,
        )
    }
}

impl AuthClaimsConfig {
    #[must_use]
    pub fn from_env() -> Self {
        let identity_claim = env_non_empty(ENV_ROUTE_FAMILY_CLAIM)
            .unwrap_or_else(|| DEFAULT_ROUTE_FAMILY_CLAIM.to_string());
        let custom_claim = env_non_empty(ENV_AUTH_CUSTOM_CLAIM);
        let role_claim =
            env_non_empty(ENV_AUTH_ROLE_CLAIM).unwrap_or_else(|| DEFAULT_ROLE_CLAIM.to_string());
        let org_claim_override = env_non_empty(ENV_AUTH_ORG_CLAIM);
        let permissions_claim_override = env_non_empty(ENV_AUTH_PERMISSIONS_CLAIM);

        Self::from_parts(
            identity_claim,
            custom_claim,
            role_claim,
            org_claim_override,
            permissions_claim_override,
            removed_legacy_route_family_env_reason(),
        )
    }

    pub fn new(
        identity_claim: impl Into<String>,
        custom_claim: Option<String>,
        role_claim: impl Into<String>,
    ) -> Self {
        Self::from_parts(
            identity_claim.into(),
            custom_claim,
            role_claim.into(),
            None,
            None,
            None,
        )
    }

    pub fn new_with_overrides(
        identity_claim: impl Into<String>,
        custom_claim: Option<String>,
        role_claim: impl Into<String>,
        org_claim_override: Option<String>,
        permissions_claim_override: Option<String>,
    ) -> Self {
        Self::from_parts(
            identity_claim.into(),
            custom_claim,
            role_claim.into(),
            org_claim_override,
            permissions_claim_override,
            None,
        )
    }

    fn from_parts(
        identity_claim: String,
        custom_claim: Option<String>,
        role_claim: String,
        org_claim_override: Option<String>,
        permissions_claim_override: Option<String>,
        invalid_reason: Option<String>,
    ) -> Self {
        Self {
            identity_claim,
            custom_claim,
            role_claim,
            org_claim_override,
            permissions_claim_override,
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
        if self.custom_claim.as_deref() == Some("fitz") {
            return Err(format!(
                "{ENV_AUTH_CUSTOM_CLAIM}=fitz is no longer supported; emit permissions directly or use a namespaced custom claim"
            ));
        }
        if self
            .org_claim_override
            .as_ref()
            .is_some_and(|claim| claim.trim().is_empty())
        {
            return Err(format!("{ENV_AUTH_ORG_CLAIM} must not be empty"));
        }
        if self
            .permissions_claim_override
            .as_ref()
            .is_some_and(|claim| claim.trim().is_empty())
        {
            return Err(format!("{ENV_AUTH_PERMISSIONS_CLAIM} must not be empty"));
        }
        if self.role_claim.trim().is_empty() {
            return Err(format!("{ENV_AUTH_ROLE_CLAIM} must not be empty"));
        }
        if self.role_claim == self.identity_claim {
            return Err(format!(
                "{ENV_AUTH_ROLE_CLAIM} must not match {ENV_ROUTE_FAMILY_CLAIM}"
            ));
        }
        if self.custom_claim.as_deref() == Some(self.role_claim.as_str()) {
            return Err(format!(
                "{ENV_AUTH_ROLE_CLAIM} must not match {ENV_AUTH_CUSTOM_CLAIM}"
            ));
        }
        if self.org_claim_override.as_deref() == Some(self.role_claim.as_str()) {
            return Err(format!(
                "{ENV_AUTH_ORG_CLAIM} must not match {ENV_AUTH_ROLE_CLAIM}"
            ));
        }
        if self.permissions_claim_override.as_deref() == Some(self.role_claim.as_str()) {
            return Err(format!(
                "{ENV_AUTH_PERMISSIONS_CLAIM} must not match {ENV_AUTH_ROLE_CLAIM}"
            ));
        }
        if self.custom_claim.as_deref() == self.permissions_claim_override.as_deref()
            && self.permissions_claim_override.is_some()
        {
            return Err(format!(
                "{ENV_AUTH_PERMISSIONS_CLAIM} must not match {ENV_AUTH_CUSTOM_CLAIM}"
            ));
        }
        if matches!(self.role_claim.as_str(), "permissions" | "scope" | "scp") {
            return Err(format!(
                "{ENV_AUTH_ROLE_CLAIM} must not overlap with top-level permission sources"
            ));
        }
        Ok(())
    }
}
