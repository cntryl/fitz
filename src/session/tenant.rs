//! Tenant to RouteFamily resolution.
//!
//! Route family assignment is owned by the control plane. The in-process stub
//! used in tests and local development still has to preserve the runtime's
//! isolation contract, so it allocates distinct RouteFamily values per tenant.

use crate::runtime::routing::RouteFamily;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

#[derive(Debug, Clone, Copy)]
pub struct RouteFamilyAssignment {
    pub family: RouteFamily,
    pub created: bool,
}

/// Control plane client for tenant route family lookups.
///
/// The production implementation should delegate this to a real control plane.
/// The development stub allocates stable in-process families per tenant:
/// - unauthenticated / unparsable JWTs use family 1
/// - authenticated tenants get sequential families starting at 2
pub struct ControlPlaneStub {
    assignments: Mutex<HashMap<String, RouteFamily>>,
    next_family: AtomicU32,
}

impl ControlPlaneStub {
    /// Create a new control plane stub.
    pub fn new() -> Self {
        Self {
            assignments: Mutex::new(HashMap::new()),
            next_family: AtomicU32::new(2),
        }
    }

    fn tenant_from_jwt(jwt: &str) -> Option<String> {
        let raw = crate::auth::parse_jwt_noverify(jwt).ok()?;
        raw.tid
            .or(raw.tenant_id)
            .or(raw.org_id)
            .filter(|tenant| !tenant.is_empty())
    }

    fn assignment_for_tenant(&self, tenant: Option<&str>) -> RouteFamilyAssignment {
        let Some(tenant) = tenant.filter(|tenant| !tenant.is_empty()) else {
            return RouteFamilyAssignment {
                family: RouteFamily::new(1),
                created: false,
            };
        };

        let mut assignments = self.assignments.lock();
        if let Some(existing) = assignments.get(tenant).copied() {
            return RouteFamilyAssignment {
                family: existing,
                created: false,
            };
        }

        let family = RouteFamily::from_u32(self.next_family.fetch_add(1, Ordering::SeqCst));
        assignments.insert(tenant.to_string(), family);
        RouteFamilyAssignment {
            family,
            created: true,
        }
    }

    /// Look up route family for a JWT from the control plane.
    pub fn lookup_route_family(&self, jwt: &str) -> RouteFamily {
        self.assign_route_family(jwt).family
    }

    /// Assign a route family for a JWT and report whether it was newly created.
    pub fn assign_route_family(&self, jwt: &str) -> RouteFamilyAssignment {
        self.assignment_for_tenant(Self::tenant_from_jwt(jwt).as_deref())
    }
}

impl Default for ControlPlaneStub {
    fn default() -> Self {
        Self::new()
    }
}

/// Resolve route family for a session based on auth status.
pub fn resolve_route_family(
    jwt: Option<&str>,
    control_plane: Option<&Arc<ControlPlaneStub>>,
) -> RouteFamily {
    match (jwt, control_plane) {
        (Some(jwt_str), Some(cp)) => {
            let family = cp.lookup_route_family(jwt_str);
            tracing::debug!(
                route_family = family.id(),
                "Resolved route family from control plane"
            );
            family
        }
        _ => RouteFamily::new(1),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;

    fn jwt_with_tenant(claim_name: &str, tenant: &str) -> String {
        let payload = serde_json::json!({
            "iss": "",
            "aud": "fitz",
            "sub": "user:test",
            "exp": 9999999999u64,
            claim_name: tenant,
        });
        let header = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode("{}");
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload.to_string());
        format!("{}.{}.sig", header, payload)
    }

    #[test]
    fn should_assign_new_family_for_first_tenant() {
        let control_plane = ControlPlaneStub::new();
        let jwt = jwt_with_tenant("tid", "acme");

        let family = control_plane.lookup_route_family(&jwt);

        assert_eq!(family.id(), 2);
    }

    #[test]
    fn should_resolve_family_one_for_no_auth() {
        let family = resolve_route_family(None, None);

        assert_eq!(family.id(), 1);
    }

    #[test]
    fn should_resolve_family_from_control_plane_for_authenticated() {
        let control_plane = Arc::new(ControlPlaneStub::new());
        let jwt = jwt_with_tenant("tid", "acme");

        let family = resolve_route_family(Some(&jwt), Some(&control_plane));

        assert_eq!(family.id(), 2);
    }

    #[test]
    fn should_allocate_distinct_families_for_distinct_tenants() {
        let control_plane = ControlPlaneStub::new();
        let jwt_a = jwt_with_tenant("tid", "acme");
        let jwt_b = jwt_with_tenant("tid", "beta");

        let family_a = control_plane.lookup_route_family(&jwt_a);
        let family_b = control_plane.lookup_route_family(&jwt_b);

        assert_ne!(family_a, family_b);
        assert_eq!(family_a.id(), 2);
        assert_eq!(family_b.id(), 3);
    }

    #[test]
    fn should_reuse_existing_assignment_for_same_tenant() {
        let control_plane = ControlPlaneStub::new();
        let jwt_a = jwt_with_tenant("tid", "acme");
        let jwt_b = jwt_with_tenant("tenant_id", "acme");

        let family1 = control_plane.lookup_route_family(&jwt_a);
        let family2 = control_plane.lookup_route_family(&jwt_b);

        assert_eq!(family1, family2);
    }
}
