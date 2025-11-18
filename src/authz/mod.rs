//! Authorization module (renamed from auth)

pub mod mock_jwks;
pub mod permissions;
pub mod tokens;

/// Session authentication and authorization state.
/// Created once per connection at WebSocket upgrade time.
#[derive(Debug, Clone)]
pub struct SessionAuth {
    /// Subject (user/client identifier) from JWT
    pub subject: String,
    /// Route family (tenant/realm) from JWT - used for sharding and multi-tenancy
    pub route_family: String,
    /// Scopes/roles from JWT claims
    pub scopes: Vec<String>,
    /// Resolved permission grants from scopes
    pub grants: PermissionGrants,
}

/// Permission grants resolved from JWT claims.
/// Built once at connection time and used for per-frame authorization.
#[derive(Debug, Clone)]
pub struct PermissionGrants {
    /// Internal grant list (may be refactored to use permissions module)
    pub(crate) grants: Vec<permissions::InternalGrant>,
}

impl PermissionGrants {
    /// Create grants from scopes for a given route_family/realm
    pub fn from_scopes(route_family: &str, _scopes: &[String]) -> Self {
        // For now, create wildcard grants for the route_family
        // TODO: Parse scopes and create fine-grained grants
        let grants = permissions::derive_grants_for_realm(route_family);
        Self { grants }
    }

    /// Check if a route is authorized
    pub fn allows(&self, route: &crate::protocol::route::Route) -> bool {
        permissions::check_grants(&self.grants, route)
    }
}

/// Validate a token using the mock JWKS validator. Returns Some(tenant) on success.
pub fn validate_token(token: &str) -> Option<String> {
    // If NO_AUTH is enabled, accept every token as belonging to the dev tenant.
    if crate::config::load().auth.no_auth {
        return Some("dev".to_string());
    }

    // Allow other authenticators to participate in the validation flow in future
    // (OIDC) — for now we continue to accept the local mock token format.
    if let Some(claims) = mock_jwks::validate_mock_token(token) {
        // Use aud as tenant when present, otherwise derive from sub
        Some(claims.aud.unwrap_or(claims.sub))
    } else {
        None
    }
}

/// Initialize authorization subsystem (stub)
pub fn init() {
    // TODO: init authz backends
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_accept_any_token_when_no_auth() {
        // Arrange
        std::env::set_var("FITZ_NO_AUTH", "1");

        // Act
        let val = validate_token("anything");

        // Assert
        assert_eq!(val, Some("dev".to_string()));
    }
}
