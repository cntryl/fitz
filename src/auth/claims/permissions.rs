use crate::auth::Permission;

use super::{CustomPermissionsClaim, RawClaims, ScopeClaim};

impl RawClaims {
    /// Normalize permissions from claims using the prioritized sources:
    /// 1) configured namespaced custom claim
    /// 2) top-level permissions array (Auth0 RBAC)
    /// 3) configured permissions claim override
    /// 4) configured role claim array
    /// 5) scp (space-delimited or array)
    /// 6) scope (space-delimited string)
    ///
    /// A source that is present but supplies no permission values is skipped
    /// and the cascade continues, so an empty claim cannot mask a populated
    /// one further down.
    ///
    /// # Errors
    ///
    /// Returns an error if the chosen permission source is malformed or if no
    /// non-empty permission values can be derived from the resolved source.
    pub fn normalized_permissions(
        &self,
        custom_claim: Option<&str>,
        permissions_claim_override: Option<&str>,
        role_claim: &str,
    ) -> Result<Vec<Permission>, String> {
        // A source that is present but carries no permission values is not a
        // source. Auth0 emits `permissions: []` whenever RBAC is enabled with
        // no permissions assigned for that API, even when the real grants live
        // in a configured claim - so treating "present" as "found" stranded
        // those tokens and reported the wrong claim as the cause. Each tier
        // below yields `None` when it supplies nothing, and the cascade
        // continues; if every tier is empty the chain still ends in
        // "no permission source found", which refuses the CONNECT.
        if let Some(claim_name) = custom_claim {
            if let Some(perms) = self.custom_claim_permissions(claim_name)? {
                if let Some(parsed) =
                    parse_optional_permission_values(claim_name, perms, false, "permission")?
                {
                    return Ok(parsed);
                }
            }
        }

        if let Some(permissions) = &self.permissions {
            if let Some(parsed) = parse_optional_permission_values(
                "permissions",
                permissions.clone(),
                false,
                "permission",
            )? {
                return Ok(parsed);
            }
        }

        if let Some(claim_name) = permissions_claim_override {
            if let Some(perms) = self.string_array_claim(claim_name, "permission")? {
                if let Some(parsed) =
                    parse_optional_permission_values(claim_name, perms, false, "permission")?
                {
                    return Ok(parsed);
                }
            }
        }

        if let Some(roles) = self.string_array_claim(role_claim, "role")? {
            if let Some(parsed) =
                parse_optional_permission_values(role_claim, roles, false, "role")?
            {
                return Ok(parsed);
            }
        }

        if let Some(scp) = &self.scp {
            if let Some(parsed) = parse_optional_permission_values(
                "scp",
                scope_claim_values(scp),
                true,
                "scope string",
            )? {
                return Ok(parsed);
            }
        }

        if let Some(scope) = &self.scope {
            if let Some(parsed) = parse_optional_permission_values(
                "scope",
                scope.split_whitespace().map(ToOwned::to_owned).collect(),
                true,
                "scope string",
            )? {
                return Ok(parsed);
            }
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

/// Parse one candidate source, returning `None` when it supplies no permission
/// values at all.
///
/// Malformed values still error: skipping those would let a typo silently
/// downgrade a token to whatever the next source happens to grant. Only a
/// source that says nothing is passed over.
fn parse_optional_permission_values(
    source: &str,
    values: Vec<String>,
    allow_resource_prefix: bool,
    error_kind: &str,
) -> Result<Option<Vec<Permission>>, String> {
    if values.iter().all(|value| value.trim().is_empty()) {
        return Ok(None);
    }
    parse_permission_values(source, values, allow_resource_prefix, error_kind).map(Some)
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
