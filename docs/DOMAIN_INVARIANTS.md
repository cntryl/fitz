# Domain Layer Invariants & Boundary Enforcement

## The Domain Contract

A domain is a **pure business logic module** that:

✅ Processes **authorized requests only**
✅ Implements domain-specific behavior (KV, notice, stream, etc.)
✅ Returns response data
✅ **Never checks permissions**
✅ **Never inspects claims**
✅ **Never calls auth or session code**

## Hard Boundaries

### ✅ Allowed Imports in Domains

```rust
// Domain implementation
use crate::domains::kv::{...};  // Other domains only if needed
use crate::protocol::{...};      // Protocol types
use std::{...};                   // Standard library
use parking_lot::{...};           // Concurrency primitives
// etc.
```

### ❌ Forbidden Imports in Domains

```rust
use crate::auth;              // ❌ NO - auth layer
use crate::session;           // ❌ NO - session layer
use crate::auth::Claims;      // ❌ NO
use crate::auth::Permission;  // ❌ NO
use crate::auth::Access;      // ❌ NO
use crate::session::actor;    // ❌ NO
```

## Enforcement Strategy

### 1. Rust Compiler (Static Checks)

Place domain code in modules that don't import auth/session:

```
src/
├── auth/              # Public auth surface
├── session/           # Public session surface
└── domains/
    ├── kv/
    │   ├── mod.rs     # ❌ Must NOT import auth or session
    │   ├── store.rs
    │   └── handler.rs
    ├── notice/
    │   └── ...        # ❌ Must NOT import auth or session
    └── ...
```

### 2. Clippy Lints (Semantic Checks)

Add to `clippy.toml`:

```toml
[lints.clippy]
# Reject specific imports in domain code
forbidden-imports = ["crate::auth", "crate::session"]
```

Then annotate domain modules:

```rust
#![forbid(unsafe_code)]
// Add custom lint to reject auth/session imports

mod store;  // ❌ Will fail clippy if it imports auth/session
```

### 3. Test Infrastructure

**Test Strategy:**

```rust
#[test]
fn should_verify_domain_independence() {
    // Use cargo-tree or syn to parse domain source
    // Assert: No crate::auth:: or crate::session:: imports found
}
```

## Request Flow Validation

### Authorized Path (Correct)

```
Protocol Frame
    ↓
Session Layer:
  1. Parse frame
  2. route = extract_route(frame)
  3. access = extract_access(frame)
  4. actor.authorize(route, access) → ✓ OK
  5. domain.handle(request) ← Request is GUARANTEED authorized
    ↓
Domain Layer:
  - Does NOT call actor.authorize() again
  - Does NOT inspect claims
  - Does NOT check permissions
  - Just processes business logic
    ↓
Response
```

### Rejected Path (Correct)

```
Protocol Frame
    ↓
Session Layer:
  1. Parse frame
  2. route = extract_route(frame)
  3. access = extract_access(frame)
  4. actor.authorize(route, access) → ✗ DENIED
  5. Return error frame immediately
  ↓ (Never reaches domain)
Domain Layer: (NOT CALLED)
  ↓
Error Response
```

## Common Violation Patterns

### Violation 1: Domain Re-checks Permissions

```rust
// ❌ WRONG - Domain checking permissions
pub fn handle_kv_get(key: &str, session: &SessionActor) -> Result<Vec<u8>, String> {
    // Re-authorizing is a sign of wrong boundary
    if !session.authorize(&route, Access::Read) {
        return Err("denied".to_string());
    }
    // ...
}

// ✅ CORRECT - Domain assumes authorization
pub fn handle_kv_get(key: &str) -> Result<Vec<u8>, String> {
    // No authorization check
    // Session layer guaranteed this request was authorized
    let value = self.store.get(key)?;
    Ok(value)
}
```

### Violation 2: Domain Inspects Claims

```rust
// ❌ WRONG - Domain reading Claims
pub fn handle_publish(
    event: &str,
    claims: &Claims,  // ← WRONG
) -> Result<(), String> {
    if claims.tenant != "prod" {
        return Err("wrong tenant".to_string());
    }
    // ...
}

// ✅ CORRECT - Tenant is passed separately if needed
pub fn handle_publish(
    tenant: &str,
    event: &str,
) -> Result<(), String> {
    // Tenant comes from session/protocol layer, not claims
    self.publish_to_tenant(tenant, event)
}
```

### Violation 3: Domain Verifies Tokens

```rust
// ❌ WRONG - Domain calling auth
pub fn handle_rpc(
    request: &Request,
    auth: &AuthService,  // ← WRONG
) -> Result<Response, String> {
    let claims = auth.verify_token(&request.token)?;
    // ...
}

// ✅ CORRECT - Token verified before reaching domain
pub fn handle_rpc(request: &Request) -> Result<Response, String> {
    // Token was already verified by session layer
    // request.data is guaranteed to be from authorized user
}
```

## Testing Domains in Isolation

### Unit Test Pattern

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_get_value_from_store() {
        // Arrange
        let mut store = KvStore::new();
        store.set("key", b"value");

        // Act
        let result = store.get("key");

        // Assert
        assert_eq!(result, Some(b"value"));
    }

    #[test]
    fn should_reject_empty_key() {
        // Arrange
        let store = KvStore::new();

        // Act
        let result = store.get("");

        // Assert
        assert!(result.is_err());
    }
}
```

**Key points:**
- No SessionActor needed
- No Claims needed
- No Permission checking
- Just business logic

### Integration Test Pattern

```rust
#[test]
fn should_complete_authorized_kv_get_request() {
    // Arrange
    let perm = Permission::parse("kv://prod/**#read").unwrap();
    let claims = create_claims_with_permissions(vec![perm]);
    let perms = SessionPermissions::from_permissions(claims.permissions.clone());
    let actor = SessionActor::new(SessionId(1), claims, perms);

    let mut domain = KvDomain::new();
    domain.set("foo", b"bar");

    let route = Route::new("kv://prod/data/foo");

    // Act
    assert!(actor.authorize(&route, Access::Read));  // Session checks auth
    let result = domain.get("foo");                   // Domain does work

    // Assert
    assert_eq!(result, Ok(b"bar"));
}
```

**Key points:**
- Test authorization at session layer
- Then call domain without re-checking
- Domain never sees SessionActor

## Boundary Validation Script

Create `scripts/validate_domain_boundaries.sh`:

```bash
#!/bin/bash
# Verify no domain imports auth or session

for domain_file in src/domains/**/*.rs; do
    if grep -q "use crate::auth\|use crate::session" "$domain_file"; then
        echo "❌ VIOLATION: $domain_file imports auth or session"
        exit 1
    fi
done

echo "✅ All domains respect boundaries"
```

Run in CI:
```yaml
- name: Validate Domain Boundaries
  run: bash scripts/validate_domain_boundaries.sh
```

## Summary

| Responsibility | Layer | Can Check Auth? | Can Inspect Claims? | Can Call Domain? |
|---|---|---|---|---|
| **Verify credentials** | Auth | ✅ N/A | ✅ (itself) | ❌ NO |
| **Check authorization** | Session | ✅ Uses compiled perms | ✅ Reads immutable claims | ✅ YES (after auth) |
| **Process business logic** | Domain | ❌ NO | ❌ NO | ❌ N/A |

**Golden Rule:** If a domain is checking permissions or reading claims, the boundary is wrong and should be fixed by moving the check to the session layer.
