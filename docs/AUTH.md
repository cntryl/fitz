# Authentication (AUTH)

This document consolidates the auth design and flow for Fitz: the *who/claims* responsibilities, the session authorization gate, the precise invariants, common boundary violations and how to fix them.

## Quick overview 🔧

```
TCP/WS frame
   ↓
[AUTH] — Who are you? (verify token, produce immutable Claims)
   ↓
[SESSION ACTOR] — Are you allowed? (compile perms once, authorize per-op)
   ↓
[DOMAIN] — Business logic (assume pre-authorized request)
   ↓
session writes response → TCP/WS
```

- Auth's job: **answer "who are you and what do you claim?"** — produce normalized, immutable `Claims`.
- Session's job: **enforce authorization per connection** using compiled permissions derived from `Claims`.
- Domain's job: **perform business logic only** and **assume requests are authorized**.

---

## Core Auth invariants ✅

- **Auth is NOT a security boundary.** It validates tokens and normalizes claims, but does not decide domain-level authorization.
- **No routing, no domain logic in auth.** Auth emits route-shaped permission strings but never matches or interprets them.
- **Claims are immutable** once normalized at auth time; session holds the claims for the connection lifecycle.
- **Permissions are route-shaped strings** (immutable) and are compiled into a matcher once by the session actor.
- **Auth performs crypto and claim validation only** (signature verification, timestamps, issuer/audience, scope→permission mapping).

---

## What Auth does (concise)

- Parse and validate JWTs (verify signature, issuer, audience, exp/nbf).
- Resolve/normalize claims into an immutable `Claims` struct:
  - `sub`, `realm` (tenant id), `roles`, `permissions` (route-shaped), `exp`.
- Map coarse scopes/roles to generic route-shaped permissions (e.g. `kv.read` → `kv://**#read`).
- Provide JWKS parsing/caching helpers.
  - **Exception:** `fetch_and_cache_jwks()` performs async HTTP fetch (implemented in `auth/jwks.rs` for convenience).
  - Transport layer may use this helper or provide JWKS via other means (file, config, etc.).
  - Auth logic itself treats JWKS as input after caching.
- Export `Permission`, `Access` types and helpers.

What Auth does NOT do:
- Match routes or make authorization decisions
- Know about domains, routes semantics, or session lifecycle
- Re-verify tokens per request (session checks expiration only)

---

## Public types & API (summary)

- Claims (immutable): { sub, realm, roles, permissions, exp }
- Permission: raw route-shaped string + access (Read/Write/All)
- Access enum: Read | Write | All

Common functions:
- `parse_jwt_noverify(compact: &str) -> RawClaims`
- `verify_jwt_with_*` (RSA/HMAC/JWKS) -> payload JSON
- `RawClaims::validate(...)` and `RawClaims::normalize(...)` → `Claims`

Refer to `src/auth/*` for authoritative implementations.

---

## Session actor responsibilities

- One `SessionActor` per connection; it stores immutable `Claims` and the `CompiledPermissions` matcher built once from `Claims.permissions`.
- **Gate every operation** through `authorize(route, access)` before calling domains.
  - **Token expiration is checked automatically** in `authorize()` — expired tokens are rejected.
  - No signature re-verification per request (only expiration timestamp check).
- Support full reauth via `reauth(new_claims, new_permissions)` method.
  - Atomically replaces claims and recompiles permissions.
  - Use for token refresh/rotation without session teardown.
- Helper methods:
  - `is_token_expired()` — check expiration without authorization
  - `token_expiration()` — get expiration timestamp
  - `is_authenticated()` — check if session has claims

Files: `session/actor.rs`, `session/permissions.rs`, `session/manager.rs`.

---

## Transport → Auth handshake flow

**Current implementation:**

1. **Pre-auth session creation:**
   - Transport creates `SessionInfo` with `claims: None`, `authenticated: false`
   - Calls `ingress.on_open(session_info)` → session ID assigned
   - Session enters unauthenticated state

2. **Authentication (future/planned):**
   - Transport extracts JWT from:
     - WebSocket: First frame or HTTP upgrade headers
     - HTTP: Authorization header or query param
   - Transport calls `auth::verify_jwt_with_*()` or uses JWKS helpers
   - On success, transport calls `session_actor.authenticate(claims, permissions)`
   - Session becomes authenticated

3. **Re-authentication (token refresh):**
   - Client sends new token via control frame or dedicated AUTH operation
   - Transport verifies new token
   - Calls `session_actor.reauth(new_claims, new_permissions)`
   - Existing subscriptions/state preserved

**Note:** Initial handshake protocol is under development. Current tests use direct `authenticate()` calls.

---

## Security module status

The `src/security/` module exists as a placeholder for future authorization features:
- `security/claims/` — Reserved for claims-based authorization (future)
- `security/identity/` — Reserved for identity management (future)
- `security/policy/` — Reserved for policy evaluation (future)

**Current auth implementation lives in `src/auth/`** and is production-ready.
The security module is not yet used and can be ignored.

---

## Domain layer contract (short)

- Domains implement synchronous business logic only (no async in domain internals).
- Domains MUST assume requests they receive are pre-authorized by session and MUST NOT call auth or session code for permission checks.
- Domain functions accept already-authorized requests and produce responses.

Example trait:
```rust
pub trait Domain { fn handle(&mut self, req: DomainRequest) -> Result<DomainResponse, String>; }
```

**Exception:** Domain test helpers in `domains/*/session.rs` files may import auth/session types for test fixture purposes only. These are not production code and are excluded from CI isolation checks.

---

## Common violations & fixes (cheat sheet) ⚠️

1. Auth knows domains
   - Symptom: `auth` references `crate::domains` or domain code.
   - Fix: mapping must return generic route-strings (e.g. `kv://**#read`) not domain structs.

2. Domain calls auth/session
   - Symptom: Domain re-verifies tokens or calls `actor.authorize()`.
   - Fix: Move authorization into session; domain should receive only pre-authorized calls.

3. Session re-verifies or recompiles permissions per-request
   - Symptom: `auth::verify_*` called inside `handle_frame` or permissions recompiled on every request.
   - Fix: Set claims at authenticate time, compile permissions once, only check expiration per-request.

---

## Testing & CI checks

**Implemented:**
- ✅ Unit tests: signature verification, claims validation, permission normalization, scope→permission mapping
- ✅ Session tests: compile-once behavior, authorize gate basics
- ✅ Integration tests: basic auth → session → domain flow (`tests/notification_auth.rs`)
- ✅ Static checks: `scripts/verify_domain_auth_imports.sh` enforces domain isolation

**Needed (gaps identified):**
- ⚠️ Token expiration handling tests (now that `authorize()` checks expiration)
- ⚠️ `reauth()` flow tests (new feature)
- ⚠️ JWKS cache invalidation tests
- ⚠️ Multi-domain authorization scenarios
- ⚠️ Audience/issuer validation edge cases
- ⚠️ Expired token rejection in `authorize()` gate

---

## Where to look in the codebase

- Auth implementation: `src/auth/{mod.rs, claims.rs, token.rs, jwk.rs, jwks.rs, errors.rs}`
- Session code: `src/session/actor.rs`, `src/session/permissions.rs`, `src/session/manager.rs`
- Domains: `src/domains/*` (must not import `auth`/`session`)
- Tests: `tests/notification_auth.rs`, other `tests/*_auth.rs`

---

## Migration note 📌

This file consolidates the content from `docs/AUTH_INVARIANTS.md`, `docs/AUTH_SESSION_ARCHITECTURE.md` and the auth sections of `docs/INVARIANT_VIOLATIONS.md`. Keep those older files for historical reference for now and point readers here for the canonical design.

---

If you'd like, I can:
- Add a short doc header badge and update the TOC/README to point to `docs/AUTH.md` ✅
- Add a CI job snippet that fails the build on domain→auth/session imports ✅

