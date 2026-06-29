use std::collections::HashMap;

use super::env::{
    env_non_empty, parse_route_family_map_env, removed_legacy_route_family_env_reason,
};
use super::{
    RawClaims, DEFAULT_ROUTE_FAMILY_CLAIM, ENV_AUTH_ORG_CLAIM, ENV_ROUTE_FAMILY_CLAIM,
    ENV_ROUTE_FAMILY_MAP,
};

/// Broker-local route-family resolver used as the v1 control-plane substitute.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteFamilyResolverConfig {
    pub identity_claim: String,
    pub org_claim_override: Option<String>,
    pub mappings: HashMap<String, u32>,
    invalid_reason: Option<String>,
}

impl Default for RouteFamilyResolverConfig {
    fn default() -> Self {
        Self::from_parts(
            DEFAULT_ROUTE_FAMILY_CLAIM.to_string(),
            None,
            HashMap::new(),
            None,
        )
    }
}

impl RouteFamilyResolverConfig {
    #[must_use]
    pub fn from_env() -> Self {
        let identity_claim = env_non_empty(ENV_ROUTE_FAMILY_CLAIM)
            .unwrap_or_else(|| DEFAULT_ROUTE_FAMILY_CLAIM.to_string());
        let org_claim_override = env_non_empty(ENV_AUTH_ORG_CLAIM);
        let (mappings, map_error) = parse_route_family_map_env();

        Self::from_parts(
            identity_claim,
            org_claim_override,
            mappings,
            removed_legacy_route_family_env_reason().or(map_error),
        )
    }

    pub fn from_mappings(
        identity_claim: impl Into<String>,
        mappings: impl IntoIterator<Item = (impl Into<String>, u32)>,
    ) -> Self {
        Self::from_parts(
            identity_claim.into(),
            None,
            mappings
                .into_iter()
                .map(|(identity, family)| (identity.into(), family))
                .collect(),
            None,
        )
    }

    fn from_parts(
        identity_claim: String,
        org_claim_override: Option<String>,
        mappings: HashMap<String, u32>,
        invalid_reason: Option<String>,
    ) -> Self {
        Self {
            identity_claim,
            org_claim_override,
            mappings,
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
        if auth_required && self.mappings.is_empty() {
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
                    "{ENV_ROUTE_FAMILY_MAP} maps identity '{identity}' to unprovisioned route family {family}"
                ));
            }
        }

        Ok(())
    }

    pub fn resolve(&self, raw_claims: &RawClaims) -> Result<u32, String> {
        if let Some(org_claim) = self.org_claim_override.as_deref() {
            if let Some(identity_value) = raw_claims.identity_claim_value(org_claim)? {
                if let Some(family) = self.mappings.get(&identity_value) {
                    return Ok(*family);
                }
                return Err(format!(
                    "route family identity claim {org_claim}={identity_value} is not mapped by {ENV_ROUTE_FAMILY_MAP}"
                ));
            }
        }

        let Some(identity_value) = raw_claims.identity_claim_value(&self.identity_claim)? else {
            return Err(format!(
                "route family identity claim '{}' is required",
                self.identity_claim
            ));
        };

        if let Some(family) = self.mappings.get(&identity_value) {
            return Ok(*family);
        }

        Err(format!(
            "route family identity claim {}={} is not mapped by {}",
            self.identity_claim, identity_value, ENV_ROUTE_FAMILY_MAP
        ))
    }
}
