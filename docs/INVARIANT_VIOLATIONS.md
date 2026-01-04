# Invariant Violations & Boundary Enforcement

## Detecting Violations

This document shows how to identify and fix boundary violations in the auth-session-domain architecture.

---

## Violation Category 1: Auth Knows About Domains

### Pattern
Auth layer references domain types, routes, or business logic.

### Example Violation
```rust
// ❌ auth/claims.rs
use crate::domains::kv;  // WRONG

pub fn normalize_permissions(scope: &str) -> Result<Vec<Permission>, String> {
    match scope {
        "kv.read" => {
            let default_perms = kv::Domain::default_read_permissions();  // ❌
            Ok(default_perms)
        }
        _ => Err("unknown scope".to_string()),
    }
}
```

### Why It's Wrong
- Auth should be **domain-agnostic**
- Permission strings should be **generic**: "kv://**#read"
- Domain specifics should never be in auth

### Fix
```rust
// ✅ auth/claims.rs
pub fn map_coarse_scope(scope: &str) -> Option<&'static str> {
    match scope {
        "kv.read" => Some("kv://**#read"),        // Generic route string
        "notice.write" => Some("notice://**#write"),
        _ => None,
    }
}
```

---

## Violation Category 2: Domain Calls Auth/Session

### Pattern A: Domain Verifies Tokens
```rust
// ❌ domains/kv/handler.rs
use crate::auth;

pub fn handle_get(request: &Request, auth: &AuthService) -> Result<Response, String> {
    let claims = auth::verify_jwt(&request.token)?;  // ❌ Domain checking auth
    let user_id = claims.sub;
    
    // Business logic with re-verified claims
}
```

### Why It's Wrong
- Domains should assume requests are **pre-authorized**
- If domain needs to verify, boundary is wrong
- Token verification belongs in session layer

### Fix
```rust
// ✅ domains/kv/handler.rs
pub fn handle_get(request: &Request, user_id: &str) -> Result<Response, String> {
    // user_id comes from session, no verification needed
    let value = self.store.get(&request.key)?;
    Ok(Response { data: value })
}
```

### Pattern B: Domain Inspects Claims
```rust
// ❌ domains/notice/handler.rs
use crate::session;

pub fn handle_subscribe(
    request: &Request,
    actor: &SessionActor,  // ❌ Domain checking permissions
) -> Result<Response, String> {
    if !actor.authorize(&request.route, Access::Read) {
        return Err("denied".to_string());
    }
    // ...
}
```

### Why It's Wrong
- If domain is calling `actor.authorize()`, session isn't doing its job
- Authorization should be **mandatory before domain is called**

### Fix
```rust
// ✅ Session layer (before calling domain)
if !actor.authorize(&route, Access::Read) {
    return Err("access denied".to_string());
}

// Then domain is called only if authorized
domain.handle(request)?;

// ✅ domains/notice/handler.rs (no authorization checks)
pub fn handle_subscribe(request: &Request) -> Result<Response, String> {
    // Assume request is pre-authorized (session guaranteed this)
    self.subscriptions.add(request.client_id, &request.topic);
    Ok(Response::ok())
}
```

---

## Violation Category 3: Session Calls Auth

### Pattern A: Session Re-Verifies Token
```rust
// ❌ session/actor.rs
use crate::auth;

pub fn handle_frame(&mut self, frame: &[u8]) -> Result<Vec<u8>, String> {
    let jwt = extract_jwt_from_frame(frame);
    
    // Re-verifying is WRONG - claims are immutable
    let claims = auth::verify_jwt(jwt)?;  // ❌
    
    // ...
}
```

### Why It's Wrong
- Claims are **set once at auth time, never change**
- Re-verification is unnecessary and breaks immutability invariant
- Token expiration is enough to check

### Fix
```rust
// ✅ session/actor.rs
pub fn handle_frame(&mut self, frame: &[u8]) -> Result<Vec<u8>, String> {
    // Check expiration only (no re-verification)
    self.check_expiration(now)?;
    
    // Parse frame and authorize
    let route = extract_route_from_frame(frame)?;
    self.authorize(&route, Access::Read)?;
    
    // Call domain
    // ...
}
```

### Pattern B: Session Recompiles Permissions
```rust
// ❌ session/manager.rs
pub fn handle_frame(&mut self, session_id: u64, frame: &[u8]) -> Result<Vec<u8>, String> {
    let session = self.sessions.get_mut(&session_id)?;
    
    // Permissions are recompiled every request - WRONG
    let perms = SessionPermissions::from_permissions(
        session.claims().unwrap().permissions.clone()  // ❌
    );
    
    if !perms.allows(&route, access) {
        return Err("denied".to_string());
    }
    // ...
}
```

### Why It's Wrong
- Permissions should be **compiled once at auth time**
- Recompiling every request is wasteful and breaks immutability

### Fix
```rust
// ✅ session/actor.rs (permissions compiled at auth)
pub fn authenticate(&mut self, claims: Claims) {
    let compiled_perms = SessionPermissions::from_permissions(
        claims.permissions.clone()
    );
    self.claims = Some(Arc::new(claims));
    self.compiled_perms = Arc::new(compiled_perms);
}

// ✅ Per-request authorization (use cached compiled perms)
pub fn authorize(&self, route: &Route, access: Access) -> bool {
    self.compiled_perms.allows(route, access)  // Fast, cached
}
```

---

## Violation Category 4: Wrong Layer Doing Work

### Pattern: Authorization Check in Domain

```rust
// ❌ WRONG - Domain checking authorization
pub fn handle_rpc_call(request: &Request) -> Result<Response, String> {
    // This should have been checked by session BEFORE domain was called
    if !has_permission(request.operation) {  // ❌ Wrong layer
        return Err("denied".to_string());
    }
    
    // Business logic
}

// ✅ CORRECT - Session checks before domain
// In session layer:
if !actor.authorize(&route, Access::Execute) {
    return Err("denied".to_string());
}

// Then domain is GUARANTEED authorized
domain.handle(request)?;

// ✅ In domain layer - just business logic
pub fn handle_rpc_call(request: &Request) -> Result<Response, String> {
    // No permission check - session guarantees authorization
    self.execute_operation(request.operation)
}
```

### Pattern: Permission Compilation in Domain

```rust
// ❌ WRONG - Domain compiling permissions
pub fn handle_query(request: &Request, raw_permissions: Vec<String>) -> Result<Response, String> {
    let compiled = compile_permissions(&raw_permissions)?;  // ❌ Wrong layer
    if !compiled.allows(&request.route, Access::Read) {
        return Err("denied".to_string());
    }
    // ...
}

// ✅ CORRECT - Compilation happens in session at auth time
// In session layer (auth phase):
let compiled = SessionPermissions::from_permissions(claims.permissions.clone());

// Stored in actor, used for all requests
pub fn authorize(&self, route: &Route, access: Access) -> bool {
    self.compiled_perms.allows(route, access)
}

// In domain - no compilation, no authorization
pub fn handle_query(request: &Request) -> Result<Response, String> {
    // Just query data
    self.db.query(&request.filter)
}
```

---

## Testing for Violations

### Static Analysis: Import Checks

```bash
#!/bin/bash
# Check no domain imports auth/session

echo "Checking domain independence..."
for domain_file in src/domains/**/*.rs; do
    if grep -q "use crate::auth\|use crate::session" "$domain_file"; then
        echo "❌ ERROR: $domain_file imports forbidden modules"
        grep -n "use crate::auth\|use crate::session" "$domain_file"
        exit 1
    fi
done

echo "✅ All domains are independent"
```

### Runtime Tests: Boundary Validation

```rust
#[test]
fn should_domain_receive_only_authorized_requests() {
    // Arrange
    let claims = Claims {
        permissions: vec![
            Permission::parse("kv://prod/**#read").unwrap(),
        ],
        // ...
    };
    let perms = SessionPermissions::from_permissions(claims.permissions.clone());
    let actor = SessionActor::new(SessionId(1), claims, perms);

    let mut domain = KvDomain::new();
    domain.set("secret", b"data");

    let route_allowed = Route::new("kv://prod/public/key");
    let route_denied = Route::new("kv://prod/secret");

    // Act & Assert
    
    // Authorized read should go through
    assert!(actor.authorize(&route_allowed, Access::Read));
    let result = domain.get("public/key");  // Domain called
    assert!(result.is_ok());

    // Unauthorized read should be rejected BEFORE domain is called
    assert!(!actor.authorize(&route_denied, Access::Read));
    // Domain.get() is NEVER called for unauthorized request
}
```

### Integration Tests: Full Flow

```rust
#[test]
fn should_reject_unauthorized_operation_at_session_layer() {
    // Arrange
    let jwt = create_test_jwt_with_permission("notice://**#read");
    let mut session = create_session();
    session.authenticate_with_jwt(jwt)?;

    let request_frame = encode_frame(Operation::Notice {
        route: "notice://prod/events",
        access: Access::Write,  // Not authorized - only has Read
    });

    // Act: Try to write to notice (unauthorized)
    let result = session.handle_frame(&request_frame);

    // Assert: Rejected at session layer, domain NEVER called
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("access denied"));

    // Verify domain was never called by checking call count
    assert_eq!(domain_call_count, 0);
}
```

---

## Automated Boundary Validation

Add to CI pipeline:

```yaml
name: Validate Boundaries

on: [push, pull_request]

jobs:
  check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      
      - name: Check Domain Independence
        run: |
          #!/bin/bash
          violations=0
          for domain_file in src/domains/**/*.rs; do
            if grep -q "use crate::auth\|use crate::session" "$domain_file"; then
              echo "::error::Domain boundary violation in $domain_file"
              violations=$((violations + 1))
            fi
          done
          exit $violations
      
      - name: Run Tests
        run: cargo test --lib
      
      - name: Check Authorization Gate Coverage
        run: |
          # Verify SessionActor::authorize is called before domain.handle
          # in all request paths
          ./scripts/verify_authorization_coverage.sh
```

---

## Summary of Common Violations

| Violation | Layer | Symptom | Fix |
|---|---|---|---|
| **Auth knows domains** | Auth | Permission mapping to domain types | Use generic route strings only |
| **Domain calls auth** | Domain | Domain imports `crate::auth` | Remove auth calls, assume authorized |
| **Domain checks perms** | Domain | Domain calling `actor.authorize()` | Move to session layer |
| **Session re-verifies** | Session | Calling `auth::verify()` per-request | Check expiration only, not signature |
| **Permissions recompiled** | Session | `from_permissions()` called per-request | Compile once at auth, cache |
| **Domain inspects claims** | Domain | Domain accessing `Claims` fields | Pass needed data separately |

---

## Validation Checklist

- [ ] No `use crate::auth::*` in domain modules
- [ ] No `use crate::session::*` in domain modules  
- [ ] No `actor.authorize()` calls in domain code
- [ ] No `auth::verify_*` calls in session per-request handlers
- [ ] Permissions compiled once at auth time, not per-request
- [ ] Domain function signatures don't reference Claims or Permissions
- [ ] All integration tests verify authorization gate works
- [ ] CI/CD runs boundary validation checks

**Golden Rule:** If a layer is importing from an earlier layer or re-doing work that was already done, the boundary is broken.
