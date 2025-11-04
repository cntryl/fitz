//! Permission handling with scope-based grants
//!
//! Grants are packed by scope to minimize comparisons: scheme + realm + optional area/resource,
//! with wildcard matching for descendants. For the baseline, we derive grants from tenant realm
//! (mock tokens) to allow all actions within that realm. Later, wire JWT claims to build precise grants.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    Pub,
    Read,
    Consume,
    Peek,
}

#[derive(Debug, Clone)]
struct Grant {
    action: Action,
    scheme: Option<&'static str>,
    realm: Option<String>,
    area: Option<String>,
    resource: Option<String>,
    wildcard: bool, // when true, descendants under resource are allowed
}

impl Grant {
    fn matches(&self, route: &crate::protocol::route::Route, action: Action) -> bool {
        if self.action != action {
            return false;
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

fn derive_grants_for_tenant(tenant: &str) -> Vec<Grant> {
    // Baseline: allow all actions on any scheme within this tenant (realm-scoped), wildcard under any area/resource
    use crate::protocol::route::Scheme;
    let mut grants = Vec::new();
    let schemes = [Scheme::Notice, Scheme::Stream, Scheme::Queue, Scheme::Rpc];
    let actions = [Action::Pub, Action::Read, Action::Consume, Action::Peek];
    for sch in schemes {
        for act in actions {
            grants.push(Grant {
                action: act,
                scheme: Some(sch.as_str()),
                realm: Some(tenant.to_string()),
                area: None,
                resource: None,
                wildcard: true,
            });
        }
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

fn action_from_str(s: &str) -> Option<Action> {
    match s {
        "pub" => Some(Action::Pub),
        "read" => Some(Action::Read),
        "consume" => Some(Action::Consume),
        "peek" => Some(Action::Peek),
        _ => None,
    }
}

fn parse_route_scope(
    scope: &str,
) -> (
    Option<&'static str>,
    Option<String>,
    Option<String>,
    Option<String>,
    bool,
) {
    // Expect scheme://realm[/area[/resource]] with optional trailing /* wildcard
    let mut wildcard = false;
    let mut s = scope.trim();
    if let Some(stripped) = s.strip_suffix("/*") {
        s = stripped;
        wildcard = true;
    }
    if let Some((scheme, rest)) = s.split_once("://") {
        let sc = match scheme {
            "notice" | "stream" | "queue" | "rpc" | "inbox" | "control" => {
                Some(Box::leak(scheme.to_string().into_boxed_str()) as &'static str)
            }
            _ => None,
        };
        let mut parts = rest.split('/').filter(|p| !p.is_empty());
        let realm = parts.next().map(|x| x.to_string());
        let area = parts.next().map(|x| x.to_string());
        let resource = parts.next().map(|x| x.to_string());
        return (sc, realm, area, resource, wildcard);
    }
    (None, None, None, None, wildcard)
}

pub async fn install_claim_grants(tenant: &str, claims: &crate::authz::mock_jwks::Claims) {
    if let Some(perms) = &claims.perms {
        let mut grants: Vec<Grant> = Vec::new();
        for p in perms {
            if let Some((act_s, scope)) = p.split_once(':') {
                if let Some(act) = action_from_str(act_s) {
                    let (scheme, realm, area, resource, wildcard) = parse_route_scope(scope);
                    // If realm missing, default to tenant
                    let realm = realm.or_else(|| Some(tenant.to_string()));
                    grants.push(Grant {
                        action: act,
                        scheme,
                        realm,
                        area,
                        resource,
                        wildcard,
                    });
                }
            }
        }
        // Store under tenant key
        let reg = registry();
        let mut g = reg.lock().await;
        g.insert(tenant.to_string(), grants);
    }
}

pub fn has_permission(tenant: &str, route_str: &str, action: Action) -> bool {
    let cfg = crate::config::load();
    if !cfg.broker.enforce_authz {
        return true; // permissive baseline when enforcement is disabled
    }
    // Allow dev/test bare routes
    if route_str.starts_with("ntc/") || route_str.starts_with("rpc/reply/") {
        return true;
    }
    // Parse the route; control/inbox shortcuts
    let parsed = match crate::protocol::route::parse_route(route_str) {
        Ok(r) => r,
        Err(_) => {
            return false;
        }
    };
    if parsed.scheme == crate::protocol::route::Scheme::Control
        || parsed.scheme == crate::protocol::route::Scheme::Inbox
    {
        return true;
    }
    // Prefer installed grants (from claims), else fallback to derived realm grants
    let grants_vec = {
        let reg = registry();
        let g = reg.try_lock();
        if let Ok(guard) = g {
            guard.get(tenant).cloned()
        } else {
            None
        }
    }
    .unwrap_or_else(|| derive_grants_for_tenant(tenant));
    grants_vec.into_iter().any(|g| g.matches(&parsed, action))
}
