//! Tenant → Route Family resolution
//!
//! Route family assignment is managed by the control plane, NOT by Fitz.
//! This module provides the lookup mechanism to resolve JWT → route_family.
//!
//! # Control Plane Integration
//!
//! The control plane receives the full JWT and makes routing decisions based on:
//! - `iss` (issuer): Which identity provider issued the token
//! - `aud` (audience): Which API/service the token is for
//! - Custom claims: `org_id`, `tenant_id`, `env`, `product_tier`, etc.
//! - Signature verification context
//!
//! This allows flexible multi-tenancy models:
//! - **Org-based**: Route by `organization_id` claim
//! - **Environment-based**: Route by `env` claim (prod/staging/dev)
//! - **Hybrid**: Route by `(org, env)` tuple
//! - **Provider-specific**: Different routing for Auth0 vs Okta vs custom
//!
//! # Example Control Plane Logic
//!
//! ```ignore
//! // Control plane receives JWT: "eyJhbGc..."
//! let claims = parse_jwt(jwt)?;
//!
//! match claims.iss.as_str() {
//!     "https://auth0.com" => {
//!         // Auth0: use org_id claim
//!         route_family_for_org(claims.org_id)
//!     }
//!     "https://okta.com" => {
//!         // Okta: use tenant_id claim
//!         route_family_for_tenant(claims.tenant_id)
//!     }
//!     "https://custom.com" => {
//!         // Custom: route by (org, env) tuple
//!         route_family_for_org_env(claims.org_id, claims.env)
//!     }
//!     _ => Err("Unknown issuer")
//! }
//! ```
//!
//! # Stub Implementation
//!
//! For now, we stub the control plane and return RouteFamily(0) for all JWTs.

use crate::runtime::routing::RouteFamily;
use std::sync::Arc;

/// Control plane client for tenant route family lookups
///
/// # Design
///
/// - **Control plane owns assignments**: Route families are allocated by the control plane
/// - **Fitz queries only**: Fitz looks up but never creates route family assignments
/// - **Stub implementation**: Returns RouteFamily(0) until control plane integration is built
///
/// # Future Implementation
///
/// When integrating with the control plane:
/// 1. Add HTTP/gRPC client to query control plane API
/// 2. Implement caching with TTL for performance
/// 3. Handle control plane unavailability gracefully
/// 4. Support tenant onboarding/offboarding events
pub struct ControlPlaneStub {
    // Future: Add HTTP client, cache, etc.
}

impl ControlPlaneStub {
    /// Create a new control plane stub
    pub fn new() -> Self {
        Self {}
    }

    /// Look up route family for a JWT from control plane
    ///
    /// # Parameters
    ///
    /// - `jwt`: The full JWT string (not just tenant_id)
    ///
    /// # Design Rationale
    ///
    /// The control plane needs the full JWT to make routing decisions:
    /// - `iss` (issuer): Which identity provider issued the token
    /// - `aud` (audience): Which API/service the token is for
    /// - Custom claims: Organization, environment, product tier, etc.
    /// - Signature verification: Control plane validates against appropriate keys
    ///
    /// This allows flexible multi-tenancy models:
    /// - Org-based: Route by organization_id claim
    /// - Environment-based: Route by env claim (prod/staging/dev)
    /// - Hybrid: Route by (org, env) tuple
    /// - Provider-specific: Different routing for different issuers
    ///
    /// # Stub Behavior
    ///
    /// Currently returns `RouteFamily(0)` for all JWTs.
    /// This allows single-tenant development mode until control plane is integrated.
    ///
    /// # Future Implementation
    ///
    /// ```ignore
    /// async fn lookup_route_family(&self, jwt: &str) -> Result<RouteFamily, Error> {
    ///     // 1. Parse JWT to extract cache key (could be iss+sub, or org_id, etc.)
    ///     let claims = parse_jwt_claims(jwt)?;
    ///     let cache_key = (claims.iss.clone(), claims.sub.clone());
    ///     
    ///     // 2. Check cache
    ///     if let Some(family) = self.cache.get(&cache_key) {
    ///         return Ok(family);
    ///     }
    ///     
    ///     // 3. Query control plane API with full JWT
    ///     let response = self.client
    ///         .post("/api/v1/route-family/lookup")
    ///         .header("Authorization", format!("Bearer {}", jwt))
    ///         .await?;
    ///     
    ///     // 4. Parse and cache result
    ///     let family = RouteFamily::new(response.route_family_id);
    ///     self.cache.insert(cache_key, family);
    ///     
    ///     Ok(family)
    /// }
    /// ```
    pub fn lookup_route_family(&self, _jwt: &str) -> RouteFamily {
        // Stub: Always return family 0 until control plane is integrated
        // Control plane will parse JWT and make routing decision based on:
        // - iss, aud, custom claims
        // - signature verification
        // - tenant/org/env routing policies
        RouteFamily::new(0)
    }
}

impl Default for ControlPlaneStub {
    fn default() -> Self {
        Self::new()
    }
}

/// Resolve route family for a session based on auth status
///
/// # Rules
///
/// - **No auth**: Returns `RouteFamily(0)` (single-tenant mode)
/// - **Authenticated**: Looks up JWT's route family from control plane
///
/// # Control Plane Integration
///
/// When a session authenticates, we pass the full JWT to the control plane.
/// The control plane parses claims (iss, aud, custom claims) and returns the
/// appropriate route family based on its routing policies.
///
/// # Parameters
///
/// - `jwt`: The full JWT string (not parsed, not just tenant_id)
/// - `control_plane`: Control plane client for lookups
pub fn resolve_route_family(
    jwt: Option<&str>,
    control_plane: Option<&Arc<ControlPlaneStub>>,
) -> RouteFamily {
    match (jwt, control_plane) {
        // Auth enabled: lookup from control plane
        (Some(jwt_str), Some(cp)) => {
            let family = cp.lookup_route_family(jwt_str);
            tracing::debug!(
                route_family = family.id(),
                "Resolved route family from control plane (JWT-based lookup)"
            );
            family
        }

        // No auth or no control plane: use family 0
        _ => RouteFamily::new(0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_return_family_zero_from_stub() {
        // Arrange
        let control_plane = ControlPlaneStub::new();
        let jwt = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c";

        // Act
        let family = control_plane.lookup_route_family(jwt);

        // Assert - Stub always returns 0
        assert_eq!(family.id(), 0);
    }

    #[test]
    fn should_resolve_family_zero_for_no_auth() {
        // Arrange

        // Act
        let family = resolve_route_family(None, None);

        // Assert
        assert_eq!(family.id(), 0);
    }

    #[test]
    fn should_resolve_family_from_control_plane_for_authenticated() {
        // Arrange
        let control_plane = Arc::new(ControlPlaneStub::new());
        let jwt = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c";

        // Act
        let family = resolve_route_family(Some(jwt), Some(&control_plane));

        // Assert - Stub returns 0, but this tests the lookup path
        assert_eq!(family.id(), 0);
    }

    #[test]
    fn should_accept_jwt_with_different_issuers() {
        // Arrange
        let control_plane = ControlPlaneStub::new();

        // Different JWT structures - control plane decides routing based on claims
        let jwt_auth0 = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJodHRwczovL2F1dGgwLmNvbSIsInN1YiI6ImFjbWUiLCJhdWQiOiJmaXR6In0.signature";
        let jwt_okta = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJodHRwczovL29rdGEuY29tIiwic3ViIjoiYWNtZSIsImF1ZCI6ImZpdHoifQ.signature";
        let jwt_custom = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJodHRwczovL2N1c3RvbS5jb20iLCJvcmdfaWQiOiJhY21lIiwiZW52IjoicHJvZCJ9.signature";

        // Act - Control plane receives full JWT and makes routing decision
        let family1 = control_plane.lookup_route_family(jwt_auth0);
        let family2 = control_plane.lookup_route_family(jwt_okta);
        let family3 = control_plane.lookup_route_family(jwt_custom);

        // Assert - Stub returns 0, but in production each could route differently
        // based on iss, custom claims (org_id, env), etc.
        assert_eq!(family1.id(), 0);
        assert_eq!(family2.id(), 0);
        assert_eq!(family3.id(), 0);
    }
}
