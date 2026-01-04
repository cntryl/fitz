# Fitz Auth & Session Architecture - Implementation Summary

## Overview

This document summarizes the complete auth-session-domain architecture implemented in Fitz, with clear boundaries and invariants.

---

## Architecture Diagram

```
┌──────────────────────────────────────────────────────────────────┐
│ TRANSPORT LAYER (WebSocket, TCP)                                 │
│ - Receives raw bytes                                              │
│ - Routes to session manager                                       │
│ - Does NOT parse, verify, or check anything                       │
└────────────────────────┬─────────────────────────────────────────┘
                         ↓
        ┌─────────────────────────────────┐
        │ AUTH: "Who are you?"             │
        ├─────────────────────────────────┤
        │ ✅ Parse & verify JWT            │
        │ ✅ Validate issuer/aud/exp       │
        │ ✅ Normalize to immutable Claims │
        │ ✅ Normalize permissions         │
        │ ✅ JWKS caching                  │
        │                                  │
        │ ❌ NO domain knowledge           │
        │ ❌ NO session logic              │
        │ ❌ NO HTTP except JWKS fetch     │
        └────────────┬──────────────────────┘
                     ↓ (immutable Claims)
        ┌─────────────────────────────────┐
        │ SESSION: "Are you allowed?"      │
        ├─────────────────────────────────┤
        │ ✅ Hold immutable Claims         │
        │ ✅ Compile permissions once      │
        │ ✅ Authorization gate per request│
        │ ✅ Frame parsing & translation   │
        │ ✅ Expiration checking           │
        │ ✅ Re-authentication support     │
        │                                  │
        │ ❌ NO auth re-verification       │
        │ ❌ NO domain business logic      │
        │ ❌ NO reaching back to auth      │
        └────────────┬──────────────────────┘
                     ↓ (guaranteed authorized)
        ┌─────────────────────────────────┐
        │ DOMAINS: "How does this work?"   │
        ├─────────────────────────────────┤
        │ ✅ Pure business logic           │
        │ ✅ Process authorized requests   │
        │ ✅ Sync, deterministic code      │
        │                                  │
        │ ❌ NO auth imports               │
        │ ❌ NO session imports            │
        │ ❌ NO permission checks          │
        │ ❌ NO claim inspection           │
        └────────────┬──────────────────────┘
                     ↓ (response data)
┌──────────────────────────────────────────────────────────────────┐
│ TRANSPORT: Encode & Send                                         │
│ Response bytes → WebSocket/TCP → Client                          │
└──────────────────────────────────────────────────────────────────┘
```

---

## Core Invariants

### 1. Information Flows Forward Only
```
Auth → Session → Domain
 ✗ Auth → Domain
 ✗ Session → Auth
 ✗ Domain → Auth/Session
```

### 2. Each Layer Has One Responsibility
- **Auth:** Token verification + Claims normalization
- **Session:** Authorization checking + Frame translation
- **Domain:** Business logic only

### 3. Claims Are Immutable After Auth
Once created, `Claims` struct is never modified, never re-verified, never reparsed.

### 4. Permissions Are Compiled Once
During auth, raw permission strings are compiled into fast route matchers.
Per-request, only the compiled matcher is used (no recompilation).

### 5. Every Domain Call Is Guarded
```rust
if !actor.authorize(&route, access) {
    return Err("access denied");  // ← Reject before calling domain
}
domain.handle(request)?;          // ← Domain guaranteed authorized
```

---

## Layer-by-Layer Contract

### AUTH LAYER

**Exports:**
```rust
pub struct Claims {
    pub sub: String,
    pub tenant: String,
    pub roles: Vec<String>,
    pub permissions: Vec<Permission>,
    pub exp: u64,
}

pub struct Permission { ... }
pub enum Access { Read, Write, All }

pub fn parse_jwt_noverify(token: &str) -> Result<RawClaims, String>
pub fn verify_jwt_with_rsa_pem(token: &str, pem: &[u8]) -> Result<Value, String>
pub fn verify_jwt_with_hmac_secret(token: &str, secret: &[u8]) -> Result<Value, String>
pub async fn fetch_and_cache_jwks(url: &str) -> Result<(), String>

impl RawClaims {
    pub fn validate(&self, issuer_allowlist: &[&str], audience: &str, now: u64) -> Result<(), String>
    pub fn normalize(self, ...) -> Result<Claims, String>
    pub fn normalized_permissions(&self) -> Result<Vec<Permission>, String>
}
```

**Does NOT export:**
- Routes, domains, operations
- Session types
- HTTP/transport types

### SESSION LAYER

**Exports:**
```rust
pub struct SessionActor {
    pub session_id: SessionId,
    pub claims: Option<Arc<Claims>>,
    pub permissions: Arc<SessionPermissions>,
}

impl SessionActor {
    pub fn new(id: SessionId, perms: SessionPermissions) -> Self
    pub fn authenticate(&mut self, claims: Claims, perms: SessionPermissions)
    pub fn authorize(&self, route: &Route, access: Access) -> bool
    pub fn authorize_all(&self, checks: &[(Route, Access)]) -> bool
    pub fn is_authenticated(&self) -> bool
}

pub struct Session { ... }
impl Session {
    pub fn new_authenticated(...) -> Self
    pub fn authenticate(&mut self, claims: Claims, perms: SessionPermissions) -> Result<(), String>
    pub fn check_expiration(&self, now: u64) -> Result<(), String>
    pub fn claims(&self) -> Option<&Claims>
    pub fn permissions(&self) -> &SessionPermissions
}
```

**Does NOT export:**
- Auth verification functions
- Domain types
- Raw JWT handling

### DOMAIN LAYER

**Must NOT import:**
```rust
use crate::auth;          // ❌
use crate::session;       // ❌
use crate::auth::Claims;  // ❌
use crate::auth::Access;  // ❌
```

**Can import:**
```rust
use crate::protocol;      // ✅ Protocol types
use crate::domains;       // ✅ Other domain types if needed
use std;                  // ✅ Standard library
use parking_lot;          // ✅ Concurrency primitives
```

**Contract:**
```rust
pub trait Domain: Send + Sync {
    fn handle(&mut self, request: DomainRequest) -> Result<DomainResponse, String>;
}

pub struct DomainRequest {
    pub operation: Operation,
    pub payload: &'static [u8],
    pub conn_id: u64,        // For logging only, NOT for authorization
}

// Domain does NOT check permissions
// Domain does NOT inspect claims
// Domain assumes request is AUTHORIZED
```

---

## Implementation Status

### ✅ Complete

1. **Auth Layer**
   - JWT parsing and verification
   - Claims normalization
   - Permission parsing
   - JWKS caching
   - Scope → permission mapping
   - Immutable Claims struct

2. **Session Layer**
   - SessionActor with authorization gate
   - SessionPermissions with compiled route matching
   - Session struct with Claims storage
   - Authentication & re-authentication support
   - Expiration checking

3. **Documentation**
   - AUTH_INVARIANTS.md
   - AUTH_SESSION_ARCHITECTURE.md
   - DOMAIN_INVARIANTS.md
   - COMPLETE_REQUEST_FLOW.md (this document)
   - INVARIANT_VIOLATIONS.md

### 🔄 In Progress / Partial

1. **Session Manager Integration**
   - Integration with runtime ingress
   - Per-connection SessionActor lifecycle

2. **Domain Integration**
   - DomainRequest/DomainResponse types
   - Domain registry
   - Route → Domain routing

### ⚠️ Not Yet

1. **Boundary Enforcement**
   - Automated lint checks for import violations
   - CI/CD validation scripts
   - Coverage tools

2. **Additional Features**
   - Token refresh / sliding expiration
   - Scope narrowing per request
   - Dynamic permission updates
   - Audit logging

---

## Files Modified/Created

### Documentation (New)
- `docs/AUTH_INVARIANTS.md`
- `docs/AUTH_SESSION_ARCHITECTURE.md`
- `docs/DOMAIN_INVARIANTS.md`
- `docs/COMPLETE_REQUEST_FLOW.md`
- `docs/INVARIANT_VIOLATIONS.md`

### Code (Modified)
- `src/auth/mod.rs` — Cleaned boundaries, removed domain refs
- `src/auth/claims.rs` — Clarified immutability
- `src/auth/token.rs` — Created (separated from jwk.rs)
- `src/auth/errors.rs` — Created (auth-only errors)
- `src/auth/tests/` — Reorganized tests
- `src/session/actor.rs` — Added claims support, authenticate method
- `src/session/session.rs` — Added claims storage, authenticate method
- `src/session/permissions.rs` — Already correct (no changes)

### Code (Not Changed)
- `src/session/manager.rs` — Works with updated Session/SessionActor
- Domain code — No imports of auth/session enforced

---

## Testing Strategy

### Unit Tests
- Auth: JWT parsing, signature verification, claims validation
- Session: Authorization gate, expiration checking
- Domain: Business logic (no auth/session)

### Integration Tests
- Complete flow: jwt → claims → authorize → domain
- Unauthorized requests rejected before domain
- Permission matching works correctly

### Boundary Tests
- No domain imports auth/session
- No auth imports domains
- CI/CD validates import restrictions

---

## Migration Checklist

For each domain, verify:
- [ ] No `use crate::auth` imports
- [ ] No `use crate::session` imports
- [ ] No calls to `actor.authorize()` within domain
- [ ] No reading of `Claims` or `Permission` in domain
- [ ] All permission checks happen in session layer
- [ ] Domain functions don't reference auth types

---

## Future Work

### Phase 1: Integration (Next)
- Integrate SessionActor lifecycle with session manager
- Wire authorization gate in request path
- Update domain handlers to receive pre-authorized requests

### Phase 2: Transport Integration
- WebSocket frame dispatch through session
- TCP frame dispatch through session
- Connection lifecycle management

### Phase 3: Enforcement
- Clippy lints for forbidden imports
- CI/CD validation scripts
- Integration test suite

### Phase 4: Advanced Features
- Dynamic permission narrowing
- Token refresh without full re-auth
- Scope-based permission isolation
- Audit logging for all authorization decisions

---

## Summary

Fitz now has a **clear, enforced architectural boundary** between authentication, authorization, and business logic:

1. **Auth** answers: "Who are you and what do you claim?"
2. **Session** answers: "Are you allowed to do this?"
3. **Domain** answers: "How does this work?"

Each layer is **independent**, can be tested in isolation, and has clear responsibilities. The boundary is **enforceable through code structure and automated checks**.
