# Auth Evaluation Domain Specification

**Version:** 1.0  
**Status:** Specification  
**Durability:** Ephemeral (runtime cache)  
**Last Updated:** December 11, 2025  

---

## Overview

The Auth Evaluation domain provides **runtime** permission checking and identity resolution. It loads policies from AuthConfigActor (durable) and caches them in-memory for fast evaluation during request processing.

### Key Features

- **JWT validation**: Parse and validate JWTs using cached JWKS
- **Permission checking**: Evaluate route access against policies
- **Identity caching**: Cache validated identities per session
- **Policy hot-reload**: Update cached policies when config changes
- **Fast evaluation**: <50µs per authorization check

### Ephemeral Characteristics

- **Policy cache**: Loaded from AuthConfigActor on startup
- **Identity cache**: Per-session validated identities
- **Not persisted**: Cache rebuilt on restart
- **Hot reload**: Policies updated without restart

---

## Core Operations

### 1. Validate Token

Validate JWT and extract identity.

**Internal Message:**
```rust
AuthEvalMsg::ValidateToken {
    token: String,
    reply_to: ActorRef<AuthEvalReply>,
}
```

**Flow:**
1. Parse JWT header to get `kid` (key ID)
2. Look up public key from cached JWKS
3. Verify JWT signature
4. Extract claims (sub, realm, roles, exp)
5. Resolve roles → permissions
6. Return Identity

**Response:**
```rust
AuthEvalReply::Identity {
    subject: String,
    route_family: String,
    realm: String,
    roles: Vec<String>,
    permissions: Vec<String>,
    expires_at: Instant,
}
```

---

### 2. Check Permission

Evaluate if identity can access route.

**Internal Message:**
```rust
AuthEvalMsg::CheckPermission {
    identity: Identity,
    route: ParsedRoute,
    operation: String,
    reply_to: ActorRef<AuthEvalReply>,
}
```

**Evaluation Logic:**
```rust
fn check_permission(
    &self,
    identity: &Identity,
    route: &ParsedRoute,
    operation: &str,
) -> bool {
    // Get policy for route family + realm
    let policy_key = (route.route_family.clone(), route.realm.clone());
    let policy = match self.policies.get(&policy_key) {
        Some(p) => p,
        None => return false, // Default deny
    };
    
    // Find matching rule
    for rule in &policy.rules {
        if matches_route_pattern(&rule.path_pattern, route) {
            // Check if identity has required permission
            if rule.permissions.contains(operation) {
                // Check if identity has required role
                for role in &identity.roles {
                    if rule.roles.contains(role) {
                        return true;
                    }
                }
            }
        }
    }
    
    false // Default deny
}
```

---

### 3. Reload Policies

Update cached policies from AuthConfigActor.

**Internal Message:**
```rust
AuthEvalMsg::ReloadPolicies {
    route_family: String,
    reply_to: ActorRef<AuthEvalReply>,
}
```

**Triggered by:**
- Startup
- AuthConfigActor policy update
- Manual refresh request

---

## Actor Implementation

### AuthEvalActor State

```rust
pub struct AuthEvalActor {
    /// Cached JWKS keyed by (route_family, realm)
    jwks_cache: Arc<DashMap<(String, String), JwksDocument>>,
    
    /// Cached policies
    policies: Arc<DashMap<(String, String), RealmPolicy>>,
    
    /// Cached identities (validated tokens)
    identity_cache: Arc<DashMap<String, CachedIdentity>>,
    
    /// Reference to config actor
    auth_config: ActorRef<AuthConfigMsg>,
    
    /// Cache TTL
    identity_cache_ttl: Duration,
}

struct CachedIdentity {
    identity: Identity,
    cached_at: Instant,
    expires_at: Instant,
}

struct RealmPolicy {
    version: u64,
    default_deny: bool,
    rules: Vec<PolicyRule>,
}

struct PolicyRule {
    path_pattern: String,  // "stream://orders/*/append"
    permissions: Vec<String>,
    roles: Vec<String>,
}
```

---

## Performance Characteristics

### Latency

- **Token validation (cached JWKS)**: <1ms
- **Permission check (cached policy)**: <50µs
- **Identity cache hit**: <10µs

### Caching Strategy

- **JWKS**: Cache until reload signal
- **Policies**: Cache until update notification
- **Identities**: TTL-based (e.g., 5 minutes)

---

## Testing Strategy

- JWT validation correctness
- Permission evaluation logic
- Cache hit rates
- Policy reload behavior

---

## References

- [Auth Configuration](../durable/AUTH_CONFIG.md)
- [Sessions Domain](SESSIONS.md)
