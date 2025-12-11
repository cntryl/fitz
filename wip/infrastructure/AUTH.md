# Auth Configuration Domain Specification

**Version:** 1.0  
**Status:** Specification  
**Durability:** Midge-backed (persistent)  
**Last Updated:** December 11, 2025  

---

## Overview

The Auth Configuration domain manages persistent authentication and authorization policies. This includes JWT validation keys (JWKS), realm permission policies, and role definitions that survive system restarts.

### Key Features

- **JWKS management**: Store and retrieve JWT public keys
- **Realm policies**: Per-realm authorization rules
- **Permission definitions**: Define granular permissions
- **Role mappings**: Map roles to permission sets
- **Policy versioning**: Track policy changes over time
- **Hot reload**: Configuration changes apply without restart

### Durability Characteristics

- **Persistent**: All configuration stored in Midge
- **Versioned**: Policy changes tracked with versions
- **Auditable**: All changes logged
- **Replicated**: Configuration can be replicated across nodes (future)

### Use Cases

- JWT public key distribution
- Realm-specific authorization policies
- Role-based access control (RBAC)
- API key management
- Client certificate policies

---

## Route Format

Auth configuration routes:

```
authcfg://{realm}/jwks/{operation}
authcfg://{realm}/policy/{operation}
authcfg://{realm}/roles/{operation}
```

### Examples
- `authcfg://acme/jwks/set` - Store JWKS for realm
- `authcfg://acme/policy/get` - Retrieve realm policy
- `authcfg://acme/roles/admin/define` - Define admin role

---

## Core Operations

### 1. Set JWKS

Store JWT validation keys for a realm.

**Route:** `authcfg://{realm}/jwks/set`

**Request (TLV):**
```
Type: 0x0800 (Auth Config Request)
Tags:
  0x01 (realm)        → "acme"
  0x04 (operation)    → "set_jwks"
  0x10 (jwks)         → JSON JWKS document
  0x11 (version)      → varint(2)  # optional
```

**Response:**
```
Type: 0x0801 (Auth Config Response)
Tags:
  0x01 (status)       → "ok"
  0x10 (version)      → varint(2)
```

**Storage:**
```
Key: authcfg/{realm}/jwks
Value: {
  "keys": [
    {
      "kty": "RSA",
      "kid": "key-1",
      "use": "sig",
      "n": "...",
      "e": "AQAB"
    }
  ],
  "version": 2,
  "updated_at": "2025-12-11T10:00:00Z"
}
```

---

### 2. Get JWKS

Retrieve current JWKS for a realm.

**Route:** `authcfg://{realm}/jwks/get`

**Request:**
```
Type: 0x0800
Tags:
  0x01 (realm)        → "acme"
  0x04 (operation)    → "get_jwks"
```

**Response:**
```
Type: 0x0801
Tags:
  0x01 (status)       → "ok"
  0x10 (jwks)         → JSON JWKS document
  0x11 (version)      → varint(2)
```

---

### 3. Set Realm Policy

Define authorization policy for a realm.

**Route:** `authcfg://{realm}/policy/set`

**Request:**
```
Type: 0x0800
Tags:
  0x01 (realm)        → "acme"
  0x04 (operation)    → "set_policy"
  0x10 (policy)       → JSON policy document
```

**Policy Format:**
```json
{
  "version": 1,
  "default_deny": true,
  "rules": [
    {
      "path": "kv://{realm}/config/*",
      "permissions": ["read", "write"],
      "roles": ["admin", "operator"]
    },
    {
      "path": "stream://{realm}/events/*",
      "permissions": ["read"],
      "roles": ["viewer"]
    }
  ]
}
```

**Response:**
```
Type: 0x0801
Tags:
  0x01 (status)       → "ok"
  0x10 (version)      → varint(1)
```

---

### 4. Define Role

Create or update a role definition.

**Route:** `authcfg://{realm}/roles/{role_name}/define`

**Request:**
```
Type: 0x0800
Tags:
  0x01 (realm)        → "acme"
  0x04 (operation)    → "define_role"
  0x10 (role_name)    → "admin"
  0x11 (permissions)  → ["read", "write", "delete", "admin"]
  0x12 (description)  → "Full administrative access"
```

**Storage:**
```
Key: authcfg/{realm}/roles/{role_name}
Value: {
  "name": "admin",
  "permissions": ["read", "write", "delete", "admin"],
  "description": "Full administrative access",
  "created_at": "2025-12-11T10:00:00Z",
  "version": 1
}
```

---

### 5. List Roles

Retrieve all roles for a realm.

**Route:** `authcfg://{realm}/roles/list`

**Request:**
```
Type: 0x0800
Tags:
  0x01 (realm)        → "acme"
  0x04 (operation)    → "list_roles"
```

**Response:**
```
Type: 0x0801
Tags:
  0x01 (status)       → "ok"
  0x10 (roles)        → ["admin", "operator", "viewer", "guest"]
```

---

## Actor Implementation

### AuthConfigActor State

```rust
pub struct AuthConfigActor {
    /// Storage bridge
    midge: ActorRef<MidgeMsg>,
    
    /// In-memory cache of configuration
    jwks_cache: Arc<DashMap<String, JwksDocument>>,
    policy_cache: Arc<DashMap<String, RealmPolicy>>,
    role_cache: Arc<DashMap<String, RoleDefinition>>,
    
    /// Cache TTL
    cache_ttl: Duration,
}

#[derive(Debug, Clone)]
struct JwksDocument {
    keys: Vec<Jwk>,
    version: u64,
    updated_at: Instant,
}

#[derive(Debug, Clone)]
struct RealmPolicy {
    version: u64,
    default_deny: bool,
    rules: Vec<PolicyRule>,
}

#[derive(Debug, Clone)]
struct PolicyRule {
    path_pattern: String,
    permissions: Vec<String>,
    roles: Vec<String>,
}

#[derive(Debug, Clone)]
struct RoleDefinition {
    name: String,
    permissions: Vec<String>,
    description: String,
    version: u64,
}
```

---

### Message Handler

```rust
impl Actor for AuthConfigActor {
    type Message = AuthConfigMsg;
    
    fn on_message(&mut self, msg: Self::Message, ctx: &ActorContext<Self>) {
        match msg {
            AuthConfigMsg::SetJwks { realm, jwks, reply_to } => {
                // Store in Midge
                let key = format!("authcfg/{}/jwks", realm);
                let value = serde_json::to_vec(&jwks).unwrap();
                
                self.midge.send(MidgeMsg::KvPut {
                    realm: realm.clone(),
                    area: "_system".to_string(),
                    key,
                    value,
                    ttl: None,
                    reply_to: ActorRef::dead(),
                });
                
                // Update cache
                self.jwks_cache.insert(realm, jwks.clone());
                
                reply_to.send(AuthConfigReply::Ok { version: jwks.version });
            }
            
            AuthConfigMsg::GetJwks { realm, reply_to } => {
                // Check cache first
                if let Some(jwks) = self.jwks_cache.get(&realm) {
                    reply_to.send(AuthConfigReply::Jwks(jwks.clone()));
                    return;
                }
                
                // Fetch from Midge
                let key = format!("authcfg/{}/jwks", realm);
                self.midge.send(MidgeMsg::KvGet {
                    realm: realm.clone(),
                    area: "_system".to_string(),
                    key,
                    reply_to: ctx.actor_ref(),
                });
                
                // Store reply_to for async response (needs correlation)
            }
            
            AuthConfigMsg::SetPolicy { realm, policy, reply_to } => {
                // Store in Midge
                let key = format!("authcfg/{}/policy", realm);
                let value = serde_json::to_vec(&policy).unwrap();
                
                self.midge.send(MidgeMsg::KvPut {
                    realm: realm.clone(),
                    area: "_system".to_string(),
                    key,
                    value,
                    ttl: None,
                    reply_to: ActorRef::dead(),
                });
                
                // Update cache
                self.policy_cache.insert(realm, policy.clone());
                
                reply_to.send(AuthConfigReply::Ok { version: policy.version });
            }
            
            AuthConfigMsg::DefineRole { realm, role, reply_to } => {
                // Store in Midge
                let key = format!("authcfg/{}/roles/{}", realm, role.name);
                let value = serde_json::to_vec(&role).unwrap();
                
                self.midge.send(MidgeMsg::KvPut {
                    realm: realm.clone(),
                    area: "_system".to_string(),
                    key,
                    value,
                    ttl: None,
                    reply_to: ActorRef::dead(),
                });
                
                // Update cache
                let cache_key = format!("{}/{}", realm, role.name);
                self.role_cache.insert(cache_key, role);
                
                reply_to.send(AuthConfigReply::Ok { version: 1 });
            }
        }
    }
}
```

---

## Integration with Auth Evaluation

Auth Config (persistent) feeds into Auth Evaluation (ephemeral):

```
AuthConfigActor (Midge) → AuthEvalActor (runtime)
```

On startup:
1. AuthEvalActor requests current policies from AuthConfigActor
2. AuthConfigActor loads from Midge, returns to AuthEvalActor
3. AuthEvalActor caches in-memory for fast evaluation

On policy update:
1. Control plane updates AuthConfigActor
2. AuthConfigActor persists to Midge
3. AuthConfigActor notifies AuthEvalActor of change
4. AuthEvalActor reloads policies

---

## Error Handling

### Error Codes

- `REALM_NOT_FOUND` - Realm doesn't exist
- `INVALID_JWKS` - Malformed JWKS document
- `INVALID_POLICY` - Policy syntax error
- `STORAGE_ERROR` - Midge write failure
- `VERSION_CONFLICT` - Optimistic locking failure

### Recovery

- **Storage failures**: Retry with exponential backoff
- **Invalid data**: Reject and return error
- **Version conflicts**: Return current version to client

---

## Performance Characteristics

### Latency

- **Config write**: <10ms (Midge write)
- **Config read (cached)**: <100µs
- **Config read (cold)**: <5ms (Midge read)
- **Cache refresh**: Async, non-blocking

### Caching Strategy

- **TTL-based**: Cached configs expire after TTL
- **Write-through**: Updates invalidate cache immediately
- **Lazy load**: Fetch on-demand from Midge

---

## Testing Strategy

### Unit Tests

- JWKS storage and retrieval
- Policy validation
- Role definition and listing
- Cache invalidation

### Integration Tests

- End-to-end config update flow
- Cache consistency
- Midge persistence
- Auth eval integration

---

## References

- [Auth Evaluation Domain](../ephemeral/AUTH_EVAL.md)
- [Midge Storage](MIDGE.md)
- [RFC 7517 - JSON Web Key (JWK)](https://tools.ietf.org/html/rfc7517)
