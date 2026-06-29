use crate::auth::Permission;

use super::{CustomPermissionsClaim, RawClaims, ScopeClaim};

impl RawClaims {
    /// Normalize permissions from claims using the prioritized sources:
    /// 1) configured namespaced custom claim
    /// 2) top-level permissions array (Auth0 RBAC)
    /// 3) configured role claim array
    /// 4) scp (space-delimited or array)
    /// 5) scope (space-delimited string)
    ///
    /// Returns Err if the chosen source is malformed or produces no permissions.
    pub fn normalized_permissions(
        &self,
        custom_claim: Option<&str>,
        permissions_claim_override: Option<&str>,
        role_claim: &str,
    ) -> Result<Vec<Permission>, String> {
        if let Some(claim_name) = custom_claim {
            if let Some(perms) = self.custom_claim_permissions(claim_name)? {
                return parse_permission_values(claim_name, perms, false, "permission");
            }
        }

        if let Some(permissions) = &self.permissions {
            return parse_permission_values(
                "permissions",
                permissions.clone(),
                false,
                "permission",
            );
        }

        if let Some(claim_name) = permissions_claim_override {
            if let Some(perms) = self.string_array_claim(claim_name, "permission")? {
                return parse_permission_values(claim_name, perms, false, "permission");
            }
        }

        if let Some(roles) = self.string_array_claim(role_claim, "role")? {
            return parse_permission_values(role_claim, roles, false, "role");
        }

        if let Some(scp) = &self.scp {
            return parse_permission_values("scp", scope_claim_values(scp), true, "scope string");
        }

        if let Some(scope) = &self.scope {
            return parse_permission_values(
                "scope",
                scope.split_whitespace().map(ToOwned::to_owned).collect(),
                true,
                "scope string",
            );
        }

        Err("no permission source found".to_string())
    }

    fn custom_claim_permissions(&self, claim_name: &str) -> Result<Option<Vec<String>>, String> {
        let Some(value) = self.extra.get(claim_name) else {
            return Ok(None);
        };
        let custom_claim: CustomPermissionsClaim = serde_json::from_value(value.clone())
            .map_err(|e| format!("malformed custom claim {claim_name}: {e}"))?;
        Ok(Some(custom_claim.permissions.unwrap_or_default()))
    }

    fn string_array_claim(
        &self,
        claim_name: &str,
        source_kind: &str,
    ) -> Result<Option<Vec<String>>, String> {
        let Some(value) = self.extra.get(claim_name) else {
            return Ok(None);
        };

        let Some(values) = value.as_array() else {
            return Err(format!(
                "{source_kind} claim '{claim_name}' must be an array of strings"
            ));
        };

        let mut out = Vec::with_capacity(values.len());
        for value in values {
            let Some(value) = value.as_str() else {
                return Err(format!(
                    "{source_kind} claim '{claim_name}' must be an array of strings"
                ));
            };
            out.push(value.to_string());
        }

        Ok(Some(out))
    }
}

fn parse_permission_values(
    source: &str,
    values: Vec<String>,
    allow_resource_prefix: bool,
    error_kind: &str,
) -> Result<Vec<Permission>, String> {
    let mut out = Vec::new();
    for raw in values {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            continue;
        }
        out.push(parse_permission_value(
            trimmed,
            allow_resource_prefix,
            error_kind,
        )?);
    }

    if out.is_empty() {
        return Err(format!("no permissions derived from {source}"));
    }

    Ok(out)
}

fn parse_permission_value(
    value: &str,
    allow_resource_prefix: bool,
    error_kind: &str,
) -> Result<Permission, String> {
    if is_fitz_permission(value) {
        return Permission::parse(value)
            .map_err(|error| format!("malformed {error_kind}: {value} ({error})"));
    }

    if let Some(mapped) = crate::auth::map_coarse_scope(value) {
        return Permission::parse(mapped)
            .map_err(|error| format!("malformed {error_kind}: {value} ({error})"));
    }

    if allow_resource_prefix {
        if let Some((_, suffix)) = value.rsplit_once('/') {
            if is_fitz_permission(suffix) {
                return Permission::parse(suffix)
                    .map_err(|error| format!("malformed {error_kind}: {value} ({error})"));
            }
            if let Some(mapped) = crate::auth::map_coarse_scope(suffix) {
                return Permission::parse(mapped)
                    .map_err(|error| format!("malformed {error_kind}: {value} ({error})"));
            }
        }
    }

    Err(format!("malformed {error_kind}: {value}"))
}

fn is_fitz_permission(value: &str) -> bool {
    [
        "kv://",
        "notice://",
        "rpc://",
        "stream://",
        "queue://",
        "lease://",
        "schedule://",
    ]
    .iter()
    .any(|prefix| value.starts_with(prefix))
}

fn scope_claim_values(scope_claim: &ScopeClaim) -> Vec<String> {
    match scope_claim {
        ScopeClaim::String(value) => value.split_whitespace().map(ToOwned::to_owned).collect(),
        ScopeClaim::Array(values) => values.clone(),
    }
}
