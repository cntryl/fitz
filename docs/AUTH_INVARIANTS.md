# Auth Layer Invariants

Auth is **NOT** a security boundary. It answers one question:
> *"Who are you and what do you claim?"*

Domain layers answer:
> *"Are you allowed to do this?"*

## Five Hard Rules

1. **No routing** — Auth never knows routes, areas, or resources
2. **No domain logic** — Auth never validates what operations are allowed
3. **No HTTP** — Only transport/rotation logic do network I/O
4. **Claims are immutable** — Once normalized at auth time, never reparsed
5. **Permission strings are route-shaped** — But never matched by auth

## Deletion Test

If you deleted `auth/` and replaced it with:
```rust
fn mock_auth(jwt: &str) -> Result<Claims, String> {
    // Return a normalized Claims object
    Ok(Claims { ... })
}
```

...would Fitz still work?

If yes → auth has correct boundaries.
If no → auth is doing too much.

## Module Responsibilities

### `claims.rs`
- Parse `RawClaims` from JWT payload
- Validate issuer/audience/time
- Resolve tenant from tid/tenant_id/org_id
- Normalize permissions (extract from fitz/roles/scp/scope)
- Return immutable `Claims`

### `token.rs`
- Verify JWT signature (RSA, HMAC)
- Return decoded payload as JSON

### `jwks.rs`
- Cache JWK sets in-memory
- TTL and staleness tracking
- Key lookup by kid
- **No HTTP** (that's transport layer)

### `jwk.rs`
- Parse JWK components (n, e, k)
- Crypto operations only

### `errors.rs`
- `AuthError` type
- Token, signature, claims, JWKS errors only

### `mod.rs`
- Public surface
- `Permission` type
- `Access` enum
- Backwards-compat helpers (deprecated)

---

## Permission Format

Permissions are immutable route-shaped strings:
```
notice://realm123/area/resource#read
stream://realm456/orders/checkout#write
queue://**#write
```

Auth emits them. Session layer compiles/matches them.
Auth never interprets them.

---

## What Auth Doesn't Do

❌ Know what a "notice" or "stream" is
❌ Match routes against permissions
❌ Make authorization decisions
❌ Perform domain-specific validation
❌ Create or rotate keys
❌ Fetch JWKS from the network
❌ Handle realm/area resolution (except tenant_id in claims)
❌ Reinterpret permissions after normalization
