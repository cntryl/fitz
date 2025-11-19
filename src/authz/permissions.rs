//! Permission handling with scope-based grants
//!
//! Grants are packed by scope to minimize comparisons: scheme + realm + optional area/resource,
//! with wildcard matching for descendants. For the baseline, we derive grants from tenant's
//! authorization context to allow all actions within that tenant's default realm.
//! Later, wire JWT claims to build precise grants.
//!
//! Note: tenant (authorization/user context) and realm (route namespace) are separate concepts,
//! though tenants typically have access to a default realm.

/// Internal grant structure (exposed for PermissionGrants)
#[derive(Debug, Clone)]
pub struct InternalGrant {
    intent: Option<String>, // read, write, * for any intent
    scheme: Option<&'static str>,
    realm: Option<String>,
    area: Option<String>,
    resource: Option<String>,
    operation: Option<String>, // specific operation, None means any operation within intent
    wildcard: bool,            // when true, descendants under resource are allowed
}

impl InternalGrant {
    /// Create a new grant with the specified parameters
    pub fn new(
        intent: Option<String>,
        operation: Option<String>,
        scheme: Option<&'static str>,
        realm: Option<String>,
        area: Option<String>,
        resource: Option<String>,
        wildcard: bool,
    ) -> Self {
        Self {
            intent,
            operation,
            scheme,
            realm,
            area,
            resource,
            wildcard,
        }
    }

    /// Create a wildcard grant for all operations within a realm
    pub fn wildcard_for_realm(realm: &str, scheme: Option<&'static str>) -> Self {
        Self::new(
            None,
            None,
            scheme,
            Some(realm.to_string()),
            None,
            None,
            true,
        )
    }

    fn matches(&self, route: &crate::protocol::route::Route) -> bool {
        // Check intent/operation
        if let Some(intent) = &self.intent {
            if let Some(route_op) = &route.operation {
                // Route has an operation, check if it matches the intent
                let allowed = match intent.as_str() {
                    "*" => true,
                    "read" => matches!(route_op.as_str(), "get" | "subscribe" | "consume" | "read"),
                    "write" => matches!(
                        route_op.as_str(),
                        "put" | "publish" | "produce" | "append" | "write"
                    ),
                    _ => false,
                };
                if !allowed {
                    return false;
                }
            }
        }

        // Check operation if specified
        if let Some(grant_op) = &self.operation {
            if let Some(route_op) = &route.operation {
                if grant_op != route_op {
                    return false;
                }
            } else {
                // Grant specifies operation but route doesn't - deny
                return false;
            }
        }

        if let Some(s) = self.scheme {
            if route.scheme.as_str() != s {
                return false;
            }
        }
        // control/inbox are bypassed elsewhere; but if present here, accept
        if route.scheme == crate::protocol::route::Scheme::Control
            || route.scheme == crate::protocol::route::Scheme::Inbox
        {
            return true;
        }
        if let Some(gr) = &self.realm {
            match &route.realm {
                Some(r) if r == gr => {}
                _ => return false,
            }
        }
        if let Some(ga) = &self.area {
            match &route.area {
                Some(a) if a == ga => {}
                _ => return false,
            }
        }
        if let Some(grc) = &self.resource {
            match &route.resource {
                Some(r) if r == grc => {}
                Some(r) if self.wildcard => {
                    // wildcard covers descendants under this resource name; op is ignored here
                    if r != grc {
                        return false;
                    }
                }
                _ => return false,
            }
        }
        true
    }
}

#[derive(Debug, Clone)]
struct Grant {
    intent: Option<String>, // read, write, * for any intent
    scheme: Option<&'static str>,
    realm: Option<String>,
    area: Option<String>,
    resource: Option<String>,
    operation: Option<String>, // specific operation, None means any operation within intent
    wildcard: bool,            // when true, descendants under resource are allowed
}

impl Grant {
    /// Create a new grant with the specified parameters
    fn new(
        intent: Option<String>,
        operation: Option<String>,
        scheme: Option<&'static str>,
        realm: Option<String>,
        area: Option<String>,
        resource: Option<String>,
        wildcard: bool,
    ) -> Self {
        Self {
            intent,
            operation,
            scheme,
            realm,
            area,
            resource,
            wildcard,
        }
    }

    /// Create a wildcard grant for all operations within a realm
    fn wildcard_for_realm(realm: &str, scheme: Option<&'static str>) -> Self {
        Self::new(
            None,
            None,
            scheme,
            Some(realm.to_string()),
            None,
            None,
            true,
        )
    }

    fn matches(&self, route: &crate::protocol::route::Route) -> bool {
        // Check intent/operation
        if let Some(intent) = &self.intent {
            if let Some(route_op) = &route.operation {
                // Route has an operation, check if it matches the intent
                let allowed = match intent.as_str() {
                    "*" => true,
                    "read" => matches!(route_op.as_str(), "get" | "subscribe" | "consume" | "read"),
                    "write" => matches!(
                        route_op.as_str(),
                        "put" | "publish" | "produce" | "append" | "write"
                    ),
                    _ => false,
                };
                if !allowed {
                    return false;
                }
            } else {
                // Route has no operation - for backward compatibility, allow if intent is "*" or if we have operation restriction
                // But since current routes don't have operations, we'll allow for now
                // TODO: When routes have operations, this should be more strict
            }
        }

        // Check operation if specified
        if let Some(grant_op) = &self.operation {
            if let Some(route_op) = &route.operation {
                if grant_op != route_op {
                    return false;
                }
            } else {
                // Grant specifies operation but route doesn't - deny
                return false;
            }
        }

        if let Some(s) = self.scheme {
            if route.scheme.as_str() != s {
                return false;
            }
        }
        // control/inbox are bypassed elsewhere; but if present here, accept
        if route.scheme == crate::protocol::route::Scheme::Control
            || route.scheme == crate::protocol::route::Scheme::Inbox
        {
            return true;
        }
        if let Some(gr) = &self.realm {
            match &route.realm {
                Some(r) if r == gr => {}
                _ => return false,
            }
        }
        if let Some(ga) = &self.area {
            match &route.area {
                Some(a) if a == ga => {}
                _ => return false,
            }
        }
        if let Some(grc) = &self.resource {
            match &route.resource {
                Some(r) if r == grc => {}
                Some(r) if self.wildcard => {
                    // wildcard covers descendants under this resource name; op is ignored here
                    if r != grc {
                        return false;
                    }
                }
                _ => return false,
            }
        }
        true
    }
}

/// Derive baseline grants for a realm (public for SessionAuth)
pub fn derive_grants_for_realm(realm: &str) -> Vec<InternalGrant> {
    // Baseline: allow all actions on any scheme within this realm, wildcard under any area/resource
    // Note: realm is the route namespace for which we're creating baseline permissions
    use crate::protocol::route::Scheme;
    let mut grants = Vec::new();
    let schemes = [Scheme::Notice, Scheme::Stream, Scheme::Queue, Scheme::Rpc];
    for sch in schemes {
        grants.push(InternalGrant::wildcard_for_realm(realm, Some(sch.as_str())));
    }
    grants
}

/// Check if grants allow a route (public for PermissionGrants)
pub fn check_grants(grants: &[InternalGrant], route: &crate::protocol::route::Route) -> bool {
    // Control and inbox routes are always allowed
    if route.scheme == crate::protocol::route::Scheme::Control
        || route.scheme == crate::protocol::route::Scheme::Inbox
    {
        return true;
    }

    grants.iter().any(|g| g.matches(route))
}

fn derive_grants_for_realm_internal(realm: &str) -> Vec<Grant> {
    // Baseline: allow all actions on any scheme within this realm, wildcard under any area/resource
    // Note: realm is the route namespace for which we're creating baseline permissions
    use crate::protocol::route::Scheme;
    let mut grants = Vec::new();
    let schemes = [Scheme::Notice, Scheme::Stream, Scheme::Queue, Scheme::Rpc];
    for sch in schemes {
        grants.push(Grant::wildcard_for_realm(realm, Some(sch.as_str())));
    }
    grants
}

use once_cell::sync::OnceCell;
use std::collections::HashMap;
use tokio::sync::Mutex;

static REGISTRY: OnceCell<Mutex<HashMap<String, Vec<Grant>>>> = OnceCell::new();

fn registry() -> &'static Mutex<HashMap<String, Vec<Grant>>> {
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Result of parsing a route scope expression in policy strings
#[derive(Debug, Clone)]
pub struct RouteScope {
    pub intent: Option<String>,
    pub scheme: Option<&'static str>,
    pub realm: Option<String>,
    pub area: Option<String>,
    pub resource: Option<String>,
    pub wildcard: bool,
}

fn action_from_str(_s: &str) -> bool {
    // With additive, route-shaped permissions we no longer distinguish
    // between different actions in the grant itself.
    true
}

fn parse_route_scope(scope: &str) -> RouteScope {
    // Expect intent::scheme://realm[/area[/resource]] with optional trailing /* wildcard
    // intent can be "read", "write", "*" or omitted (defaults to None for any)
    let mut wildcard = false;
    let mut s = scope.trim();
    if let Some(stripped) = s.strip_suffix("/*") {
        s = stripped;
        wildcard = true;
    }

    let intent = if let Some((intent_part, _rest)) = s.split_once("::") {
        match intent_part {
            "*" => Some("*".to_string()),
            "read" => Some("read".to_string()),
            "write" => Some("write".to_string()),
            _ => None, // invalid intent, treat as any
        }
    } else {
        None // no intent specified, defaults to any
    };

    let route_part = if s.contains("::") {
        s.split_once("::").unwrap().1
    } else {
        s
    };

    if let Some((scheme, rest)) = route_part.split_once("://") {
        let sc = match scheme {
            "*" => None,
            "notice" | "stream" | "queue" | "rpc" | "inbox" | "control" => {
                Some(Box::leak(scheme.to_string().into_boxed_str()) as &'static str)
            }
            _ => None,
        };
        let mut parts = rest.split('/').filter(|p| !p.is_empty());
        let realm = parts.next().map(|x| x.to_string());
        let area = parts.next().map(|x| x.to_string());
        let resource = parts.next().map(|x| x.to_string());
        return RouteScope {
            intent,
            scheme: sc,
            realm,
            area,
            resource,
            wildcard,
        };
    }
    RouteScope {
        intent,
        scheme: None,
        realm: None,
        area: None,
        resource: None,
        wildcard,
    }
}

pub async fn install_claim_grants(tenant: &str, claims: &crate::authz::mock_jwks::Claims) {
    if let Some(perms) = &claims.perms {
        let mut grants: Vec<Grant> = Vec::new();
        for p in perms {
            if let Some((act_s, scope)) = p.split_once(':') {
                if action_from_str(act_s) {
                    let parsed = parse_route_scope(scope);
                    let intent = parsed.intent;
                    let scheme = parsed.scheme;
                    let realm = parsed.realm;
                    let area = parsed.area;
                    let resource = parsed.resource;
                    let wildcard = parsed.wildcard;
                    // If realm not specified in permission scope, use tenant's default realm
                    // Note: tenant (authorization context) and realm (route namespace) are separate concepts
                    let realm = realm.or_else(|| Some(tenant.to_string()));
                    grants.push(Grant::new(
                        intent, None, scheme, realm, area, resource, wildcard,
                    ));
                }
            }
        }
        // Store under tenant key
        let reg = registry();
        let mut g = reg.lock().await;
        g.insert(tenant.to_string(), grants);
    }
}

pub fn has_permission(tenant: &str, route_str: &str) -> bool {
    check_route_authorization(tenant, route_str)
}

/// Simplified authorization check that works directly with route strings.
/// Routes carry the realm information, so we can do basic authorization without
/// complex parsing for the common case.
///
/// Note: tenant (user's authorization context) and realm (route namespace) are separate concepts,
/// but this function assumes tenant-scoped access to matching realms for baseline security.
pub fn check_route_authorization(tenant: &str, route_str: &str) -> bool {
    let cfg = crate::config::load();
    if !cfg.broker.enforce_authz {
        return true; // permissive baseline when enforcement is disabled
    }

    // Allow dev/test bare routes
    if route_str.starts_with("ntc/") || route_str.starts_with("rpc/reply/") {
        return true;
    }

    // Control and inbox routes are always allowed (system routes)
    if route_str.starts_with("control://") || route_str.starts_with("inbox://") {
        return true;
    }

    // For scheme-based routes, check that realm matches tenant's allowed realm
    // This provides baseline tenant isolation - tenants can only access routes in their realm
    if let Some(realm_part) = extract_realm_from_route(route_str) {
        if realm_part != tenant {
            return false;
        }
    } else {
        // Malformed route - deny access
        return false;
    }

    // For advanced permission checking, fall back to grant-based system
    // This handles cases where we need more granular permissions beyond realm matching
    check_grants_if_available(tenant, route_str)
}

/// Extract realm from route string: scheme://realm/... -> realm
/// Note: realm is the route namespace, separate from tenant (authorization context)
fn extract_realm_from_route(route_str: &str) -> Option<&str> {
    route_str
        .split_once("://")?
        .1
        .split('/')
        .next()
        .filter(|s| !s.is_empty())
}

/// Check detailed grants only when needed for advanced permissions
fn check_grants_if_available(tenant: &str, route_str: &str) -> bool {
    // Parse the route for grant matching
    let parsed = match crate::protocol::route::parse_route(route_str) {
        Ok(r) => r,
        Err(_) => return false,
    };

    // Get grants for this tenant
    let grants_vec = {
        let reg = registry();
        let g = reg.try_lock();
        if let Ok(guard) = g {
            guard.get(tenant).cloned()
        } else {
            None
        }
    }
    .unwrap_or_else(|| derive_grants_for_realm_internal(tenant));

    // Check if any grant allows this route
    grants_vec.into_iter().any(|g| g.matches(&parsed))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn r(s: &str) -> crate::protocol::route::Route {
        crate::protocol::route::parse_route(s).expect("valid route")
    }

    fn profile(name: &str) -> Vec<Grant> {
        // Helper: parse multiple permission strings into grants
        fn parse_permission_to_grants(permission: &str) -> Vec<Grant> {
            let parsed = parse_route_scope(permission);
            let intent = parsed.intent;
            let scheme = parsed.scheme;
            let realm = parsed.realm;
            let area = parsed.area;
            let resource = parsed.resource;
            let wildcard = parsed.wildcard;
            vec![Grant::new(
                intent, None, scheme, realm, area, resource, wildcard,
            )]
        }

        fn gvec(list: &[&str]) -> Vec<Grant> {
            list.iter()
                .flat_map(|p| parse_permission_to_grants(p))
                .collect()
        }

        match name {
            //
            // 🔶 Full Realm: all intents on all domains within this realm
            //
            "FullRealm" => gvec(&[
                "*::*://acme/*", // Single wildcard entry for full realm access
            ]),

            //
            // 🔶 Domain-scoped profiles
            //
            "StreamOnly" => gvec(&["*::stream://acme/*"]),

            "QueueOnly" => gvec(&["*::queue://acme/*"]),

            "RpcOnly" => gvec(&["*::rpc://acme/*"]),

            "KvOnly" => gvec(&["*::kv://acme/*"]),

            //
            // 🔶 Area-scoped read/write/both
            //
            "OrdersStreamRW" => gvec(&["*::stream://acme/orders/*"]),

            "OrdersStreamReadOnly" => gvec(&["read::stream://acme/orders/*"]),

            "OrdersStreamWriteOnly" => gvec(&["write::stream://acme/orders/*"]),

            //
            // 🔶 Resource-scoped read/write/both
            //
            "OrdersCheckoutQueueRW" => gvec(&["*::queue://acme/orders/checkout/*"]),

            "OrdersCheckoutQueueProduce" => gvec(&["write::queue://acme/orders/checkout/*"]),

            "OrdersCheckoutQueueConsume" => gvec(&["read::queue://acme/orders/checkout/*"]),

            //
            // 🔶 RPC client & handler roles
            //
            "OrdersRpcClient" => gvec(&[
                "write::rpc://acme/orders/*", // calling RPCs
            ]),

            "OrdersRpcHandler" => gvec(&[
                "read::rpc://acme/orders/*", // responding to RPCs
            ]),

            //
            // 🔶 Notification publisher/subscriber roles
            //
            "OrdersNoticePublisher" => gvec(&["write::notice://acme/orders/*"]),

            "OrdersNoticeSubscriber" => gvec(&["read::notice://acme/orders/*"]),

            //
            // 🔶 KV access profiles
            //
            "OrdersKvFull" => gvec(&["*::kv://acme/orders/*"]),

            "OrdersKvReadOnly" => gvec(&["read::kv://acme/orders/*"]),

            "OrdersKvWriteOnly" => gvec(&["write::kv://acme/orders/*"]),

            //
            // 🔶 Tenant-wide read or write
            //
            "TenantWideReadOnly" => gvec(&[
                "read::stream://acme/*",
                "read::queue://acme/*",
                "read::notice://acme/*",
                "read::rpc://acme/*",
                "read::kv://acme/*",
            ]),

            "TenantWideWriteOnly" => gvec(&[
                "write::stream://acme/*",
                "write::queue://acme/*",
                "write::notice://acme/*",
                "write::rpc://acme/*",
                "write::kv://acme/*",
            ]),

            //
            // 🔶 Billing domain
            //
            "BillingReadOnly" => gvec(&[
                "read::stream://acme/billing/*",
                "read::queue://acme/*",
                "read::notice://acme/billing/*",
                "read::rpc://acme/billing/*",
                "read::kv://acme/billing/*",
            ]),

            "OrdersStreamAccess" => gvec(&["*::stream://acme/orders/*"]),

            "OrdersCheckoutWorker" => gvec(&["*::queue://acme/orders/checkout/*"]),

            "BillingOnly" => gvec(&[
                "*::stream://acme/billing/*",
                "*::queue://acme/billing/*",
                "*::notice://acme/billing/*",
                "*::rpc://acme/billing/*",
                "*::kv://acme/billing/*",
            ]),

            "OrdersQueueWriteOnly" => gvec(&["write::queue://acme/orders/*"]),

            "OrdersQueueReadOnly" => gvec(&["read::queue://acme/orders/*"]),

            _ => panic!("unknown profile"),
        }
    }

    #[test]
    fn should_verify_permission_matrix() {
        // Arrange
        struct Case<'a> {
            profile: &'a str,
            route: &'a str,
            allowed: bool,
        }

        let cases = [
            // FullRealm allows everything in the realm
            Case {
                profile: "FullRealm",
                route: "stream://acme/orders/created",
                allowed: true,
            },
            Case {
                profile: "FullRealm",
                route: "queue://acme/payments/refund",
                allowed: true,
            },
            Case {
                profile: "FullRealm",
                route: "notice://acme/alerts/system",
                allowed: true,
            },
            Case {
                profile: "FullRealm",
                route: "rpc://acme/services/payment",
                allowed: true,
            },
            Case {
                profile: "FullRealm",
                route: "stream://other/orders/created",
                allowed: false,
            }, // wrong realm
            // OrdersStreamAccess allows full access to stream orders
            Case {
                profile: "OrdersStreamAccess",
                route: "stream://acme/orders/created",
                allowed: true,
            },
            Case {
                profile: "OrdersStreamAccess",
                route: "stream://acme/orders/updated",
                allowed: true,
            },
            Case {
                profile: "OrdersStreamAccess",
                route: "stream://acme/billing/invoices",
                allowed: false,
            }, // wrong area
            Case {
                profile: "OrdersStreamAccess",
                route: "queue://acme/orders/checkout",
                allowed: false,
            }, // wrong scheme
            // OrdersCheckoutWorker only allows queue orders checkout
            Case {
                profile: "OrdersCheckoutWorker",
                route: "queue://acme/orders/checkout",
                allowed: true,
            },
            Case {
                profile: "OrdersCheckoutWorker",
                route: "queue://acme/orders/other",
                allowed: false,
            }, // wrong resource
            Case {
                profile: "OrdersCheckoutWorker",
                route: "stream://acme/orders/checkout",
                allowed: false,
            }, // wrong scheme
            // BillingOnly allows any scheme in billing
            Case {
                profile: "BillingOnly",
                route: "stream://acme/billing/invoices",
                allowed: true,
            },
            Case {
                profile: "BillingOnly",
                route: "queue://acme/billing/payments",
                allowed: true,
            },
            Case {
                profile: "BillingOnly",
                route: "stream://acme/orders/created",
                allowed: false,
            }, // wrong area
            // StreamOnly allows only streams
            Case {
                profile: "StreamOnly",
                route: "stream://acme/orders/created",
                allowed: true,
            },
            Case {
                profile: "StreamOnly",
                route: "stream://acme/billing/invoices",
                allowed: true,
            },
            Case {
                profile: "StreamOnly",
                route: "queue://acme/orders/checkout",
                allowed: false,
            }, // wrong scheme
            // Conceptual write-only profiles (currently grant full access, but would ideally restrict to write operations)
            // OrdersStreamWriteOnly: conceptually allows only appending to orders streams
            Case {
                profile: "OrdersStreamWriteOnly",
                route: "stream://acme/orders/created",
                allowed: true,
            }, // currently allows full access
            Case {
                profile: "OrdersStreamWriteOnly",
                route: "stream://acme/billing/invoices",
                allowed: false,
            }, // wrong area
            // OrdersQueueWriteOnly: conceptually allows only enqueueing to orders queues
            Case {
                profile: "OrdersQueueWriteOnly",
                route: "queue://acme/orders/checkout",
                allowed: true,
            }, // currently allows full access
            Case {
                profile: "OrdersQueueWriteOnly",
                route: "queue://acme/billing/payments",
                allowed: false,
            }, // wrong area
            // Conceptual read-only profiles (currently grant full access, but would ideally restrict to read operations)
            // OrdersStreamReadOnly: conceptually allows only reading/subscribing to orders streams
            Case {
                profile: "OrdersStreamReadOnly",
                route: "stream://acme/orders/created",
                allowed: true,
            }, // currently allows full access
            Case {
                profile: "OrdersStreamReadOnly",
                route: "stream://acme/billing/invoices",
                allowed: false,
            }, // wrong area
            // OrdersQueueReadOnly: conceptually allows only dequeuing from orders queues
            Case {
                profile: "OrdersQueueReadOnly",
                route: "queue://acme/orders/checkout",
                allowed: true,
            }, // currently allows full access
            Case {
                profile: "OrdersQueueReadOnly",
                route: "queue://acme/billing/payments",
                allowed: false,
            }, // wrong area
        ];

        // Act
        for c in cases {
            let grants = profile(c.profile);
            let route = r(c.route);
            let pass = grants.iter().any(|g| g.matches(&route));

            // Assert
            assert_eq!(pass, c.allowed, "profile={} route={}", c.profile, c.route);
        }
    }

    #[test]
    fn should_fail_to_parse_malformed_scheme() {
        let result = crate::protocol::route::parse_route("foo://acme/orders/checkout");
        assert!(result.is_err());
    }

    #[test]
    fn should_fail_to_parse_missing_realm() {
        let result = crate::protocol::route::parse_route("stream:///");
        assert!(result.is_err());
    }

    #[test]
    fn should_parse_route_with_trailing_slash() {
        // Arrange
        let route_str = "stream://acme/orders/checkout/";

        // Act
        let route = r(route_str);

        // Assert
        assert_eq!(route.scheme, crate::protocol::route::Scheme::Stream);
        assert_eq!(route.realm, Some("acme".to_string()));
        assert_eq!(route.area, Some("orders".to_string()));
        assert_eq!(route.resource, Some("checkout".to_string()));
    }
}
