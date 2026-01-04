# Fitz Authentication & Session Architecture

## Core Invariant

```
ws/tcp frame
    ↓
[AUTH: "who are you?"]
    ↓ (immutable Claims)
[SESSION ACTOR: "are you allowed?"]
    ↓ (authorized request)
[DOMAIN: "how does this work?"]
    ↓ (response)
session actor writes frame → ws/tcp
```

Each layer is **100% independent** and **never reaches backward** to earlier layers.

---

## AUTH Layer

**Single Responsibility:** Token verification + Claims normalization.

### What Auth Does

✅ Parse JWT payload (no verification needed yet)
✅ Verify JWT signature (RSA, HMAC, JWKS)
✅ Validate standard claims:
   - Issuer (allowlist)
   - Audience
   - Expiration (nbf, exp)
   - Tenant resolution (tid/tenant_id/org_id)
✅ Normalize ALL claims to immutable `Claims` struct:
   - Sub (subject ID)
   - Tenant ID (realm)
   - Roles (array of strings, never reinterpreted)
   - **Permissions** (fully resolved, route-shaped strings)
   - Exp (timestamp)
✅ Scope → Permission mapping (coarse OAuth2 scopes to Fitz routes)
✅ JWKS caching (in-memory, TTL-based)

### What Auth Does NOT Do

❌ Know what a domain is
❌ Know what a route is or how to match it
❌ Know about sessions, connections, or transports
❌ Make authorization decisions ("can you do X")
❌ Parse permission strings for matching
❌ Hold state per connection
❌ Handle reauth or token refresh
❌ Perform any I/O except JWKS fetch (in transport layer)

### Auth API

```rust
pub struct Claims {
    pub sub: String,              // user identifier
    pub tenant: String,           // realm isolation
    pub roles: Vec<String>,       // immutable, never reparsed
    pub permissions: Vec<Permission>, // fully normalized
    pub exp: u64,                 // unix timestamp
}

pub struct Permission {
    pub raw: String,              // "notice://realm/area/**#read"
    pub access: Access,           // Read | Write | All
}

pub enum Access {
    Read,
    Write,
    All,
}

// Auth entry points
pub fn parse_jwt_noverify(compact: &str) -> Result<RawClaims, String>
pub fn verify_jwt_with_rsa_pem(token: &str, pem: &[u8]) -> Result<Value, String>
pub fn verify_jwt_with_hmac_secret(token: &str, secret: &[u8]) -> Result<Value, String>
pub async fn permissions_from_jwt_using_jwks(token: &str, url: &str) -> Result<SessionPermissions, String>

impl RawClaims {
    pub fn validate(
        &self,
        issuer_allowlist: &[&str],
        audience: &str,
        now: u64,
    ) -> Result<(), String>

    pub fn normalize(
        self,
        issuer_allowlist: &[&str],
        audience: &str,
        now: u64,
    ) -> Result<Claims, String>

    pub fn normalized_permissions(&self) -> Result<Vec<Permission>, String>
}
```

### Auth Files

- `auth/mod.rs` — Public surface, Permission/Access types
- `auth/claims.rs` — Claims parsing and normalization
- `auth/token.rs` — JWT signature verification
- `auth/jwk.rs` — Single JWK crypto operations
- `auth/jwks.rs` — JWKS set caching (no HTTP)
- `auth/errors.rs` — AuthError only

---

## SESSION ACTOR Layer

**Single Responsibility:** Connection lifecycle + authorization + frame translation.

One session actor per connection. Holds the connection's immutable `Claims` and enforces authorization on every operation before calling domains.

### Session Actor Design

```rust
pub struct SessionActor {
    conn_id: u64,
    claims: Claims,               // immutable
    compiled_perms: CompiledPermissions, // route matcher
    // connection state
}

impl SessionActor {
    pub fn new(conn_id: u64, claims: Claims) -> Result<Self, String> {
        // Compile permissions from Claims.permissions into route matcher
        let compiled = CompiledPermissions::from(claims.permissions.clone())?;
        Ok(SessionActor {
            conn_id,
            claims,
            compiled_perms: compiled,
        })
    }

    /// Core gate: every operation passes through here
    pub fn authorize(&self, route: &Route, access: Access) -> Result<(), String> {
        // Check if compiled_perms allows (route, access)
        // Return Err immediately if not allowed
    }

    pub fn handle_frame(&mut self, frame: &[u8]) -> Result<Vec<u8>, String> {
        // 1. Parse frame into operation
        let op = self.parse_operation(frame)?;

        // 2. Check authorization
        self.authorize(&op.route, op.access)?;

        // 3. Call domain (now guaranteed authorized)
        let result = self.call_domain(&op)?;

        // 4. Encode result into response frame
        Ok(self.encode_response(result)?)
    }

    pub fn check_expiration(&self, now: u64) -> Result<(), String> {
        if now >= self.claims.exp {
            return Err("token expired".to_string());
        }
        Ok(())
    }

    pub fn reauth(&mut self, new_claims: Claims) -> Result<(), String> {
        // Replace claims (permissions already compiled)
        self.claims = new_claims.clone();
        self.compiled_perms = CompiledPermissions::from(new_claims.permissions)?;
        Ok(())
    }
}
```

### Session Actor Invariants

1. **Claims are immutable after construction** — No mutation except full reauth
2. **Permissions are compiled once** — Never reparsed per-request
3. **Authorization is mandatory** — Every domain call is guarded
4. **No auth code is called** — Claims are static
5. **No domain code is called without authorization**

### Session Actor Files

- `session/actor.rs` — SessionActor struct and lifetime
- `session/permissions.rs` — CompiledPermissions (route matcher)
- `session/manager.rs` — SessionActor pool by conn_id

---

## DOMAIN Layer

**Single Responsibility:** Business logic only. Assume all requests are authorized.

### Domain Contract

```rust
pub trait Domain: Send + Sync {
    fn handle(&mut self, req: DomainRequest) -> Result<DomainResponse, String>;
}

pub struct DomainRequest {
    pub operation: Operation,    // e.g. KvOp::Get
    pub payload: &'static [u8],  // request data
    pub conn_id: u64,            // for logging only
}

pub struct DomainResponse {
    pub data: Vec<u8>,           // response frame bytes
}
```

### Domain Invariants

✅ Receive pre-authorized requests only
✅ Never call auth code
✅ Never call session code
✅ Never inspect Claims or permissions
✅ Never verify tokens
✅ Never make authorization decisions
✅ Synchronous (no async, no tokio in domain logic)

### Domain Files

- `domains/kv/` — Key-value domain
- `domains/notice/` — Pub/sub domain
- `domains/rpc/` — RPC domain
- etc.

**Hard rule:** A domain NEVER imports `auth::` or `session::`.

---

## Data Flow: Complete Example

### Request Path

```
TCP frame arrives (48 bytes):
  [header: op=kv.get] [body: key="foo"]

→ Session Manager finds SessionActor(conn_id=42)

→ SessionActor::handle_frame()
  1. Parse frame → Operation { route: "kv://prod/data/foo", access: Read }
  2. self.authorize(&route, Read) → OK (claims allow)
  3. self.call_domain(op) → KvDomain::get("foo") → Ok(value)
  4. Encode into response frame

← Return 24 bytes [header: ok] [body: value="bar"]

TCP sends to client
```

### Authorization Check (Inside SessionActor)

```rust
fn authorize(&self, route: &Route, access: Access) -> Result<(), String> {
    // self.compiled_perms was built from Claims::permissions
    // Example perms: ["kv://prod/data/**#read", "notice://**#write"]
    
    if self.compiled_perms.allows(route, access) {
        Ok(())
    } else {
        Err(format!("access denied: {} {}", route, access))
    }
}
```

### Domain Call (Inside SessionActor)

```rust
fn call_domain(&mut self, op: &Operation) -> Result<DomainResponse, String> {
    // Domain is GUARANTEED to be called with authorized requests only
    // Domain does not check permissions again
    
    let req = DomainRequest {
        operation: op.clone(),
        payload: &op.payload,
        conn_id: self.conn_id,
    };
    
    // Call domain synchronously
    // Domain returns response, no auth checks
    self.domain.handle(req)
}
```

### Domain Implementation Example

```rust
pub struct KvDomain {
    store: HashMap<String, Vec<u8>>,
}

impl Domain for KvDomain {
    fn handle(&mut self, req: DomainRequest) -> Result<DomainResponse, String> {
        // Domain receives pre-authorized request
        // Does NOT:
        //   - Check permissions
        //   - Parse Claims
        //   - Verify routes
        // Just does business logic
        
        match req.operation {
            KvOp::Get(key) => {
                let value = self.store.get(key).cloned();
                Ok(DomainResponse { data: encode(value) })
            }
            KvOp::Set(key, value) => {
                self.store.insert(key, value);
                Ok(DomainResponse { data: encode(()) })
            }
        }
    }
}
```

---

## Invariant Enforcement

### Allowed Dependencies

```
Transport → Auth → Session → Domain
   ✗ Auth → Session/Domain
   ✗ Session → Auth
   ✗ Domain → Auth/Session
```

### Example Violations and Fixes

**Violation 1: Domain calls session to check permissions**

```rust
// ❌ WRONG
pub fn handle_request(session: &Session, op: Op) -> Result<(), String> {
    if !session.allows(&op.route) {  // ← WRONG
        return Err("denied".to_string());
    }
    // ...
}

// ✅ CORRECT
pub fn handle_request(op: Op) -> Result<(), String> {
    // Assume caller (session) already checked authorization
    // Just do business logic
}
```

**Violation 2: Session calls auth to re-verify token**

```rust
// ❌ WRONG
pub fn handle_frame(&mut self, frame: &[u8]) -> Result<Vec<u8>, String> {
    let jwt = parse_jwt_from_frame(frame);
    let claims = auth::verify_jwt(jwt)?;  // ← WRONG, re-auth inside session
    // ...
}

// ✅ CORRECT
pub fn handle_frame(&mut self, frame: &[u8]) -> Result<Vec<u8>, String> {
    // Claims were set at session creation, immutable now
    // Only check expiration if needed
    self.check_expiration(now)?;
    // Use self.claims, no re-verification
}
```

**Violation 3: Auth references domains**

```rust
// ❌ WRONG
pub fn normalize_claims(raw: &RawClaims) -> Result<Claims, String> {
    let permissions = match raw.scope {
        "kv.read" => kv_domain::get_default_perms(),  // ← WRONG
        // ...
    }
}

// ✅ CORRECT
pub fn normalize_claims(raw: &RawClaims) -> Result<Claims, String> {
    let permissions = match raw.scope {
        "kv.read" => Some("kv://**#read"),  // ← Generic route string
        // Domain never consulted
    }
}
```

---

## Testing Strategy

### Auth Tests (Unit)

- Signature verification (RSA, HMAC)
- Claims validation (issuer, audience, time)
- Permission normalization (fitz/roles/scp/scope)
- Scope → permission mapping

**Constraint:** No session or domain knowledge.

### Session Tests (Unit + Integration)

- Authorization gate (route matching)
- Frame parsing and translation
- Expiration checks
- Reauth flow

**Constraint:** Domain logic is mocked/stubbed.

### Domain Tests (Unit)

- Business logic only
- Assume authorized requests
- No permission checks

**Constraint:** No auth/session code imported.

### End-to-End Tests (Integration)

- Full request path (ws → auth → session → domain)
- Authorized operations succeed
- Unauthorized operations are rejected at session layer
- Domain is never called with unauthorized requests

---

## Implementation Phases

### Phase 1: Auth ✅
- Claims parsing and validation
- Token verification
- Permission normalization

### Phase 2: Session Actor 🔄
- SessionActor struct
- CompiledPermissions (route matcher)
- Frame parsing
- Authorization gate

### Phase 3: Domain Integration
- Register domains with session
- Handle frame routing
- Response encoding

### Phase 4: Transport Integration
- WebSocket frame dispatch
- TCP frame dispatch
- Connection lifecycle

### Phase 5: Boundary Enforcement
- Lint checks to prevent auth/domain imports
- Testing strategy validation
- Documentation

---

## Summary

| Layer | Responsibility | Input | Output | Dependencies |
|-------|---|---|---|---|
| **Auth** | Token → Claims | JWT string | Claims{sub, tenant, roles, permissions, exp} | JWT library, JWKS cache |
| **Session** | Claims → Authorization | Frame + Claims | Authorized operation | Nothing backward |
| **Domain** | Authorization → Response | Authorized operation | Response data | Domain state only |

**The Golden Rule:** If a layer reaches backward to an earlier layer, the boundary is wrong.
