# Complete Fitz Request Flow: WebSocket → Auth → Session → Domain

This document walks through a complete request from a WebSocket client through all layers.

## Example: KV Put Operation

**Client Goal:** Store a value in the KV domain

```
Client sends: [kv-put] [tenant=prod] [key=orders/123] [value={"status":"pending"}]
```

---

## Phase 1: Transport Reception

### WebSocket Handler

```rust
async fn handle_ws_frame(
    ws: &mut WebSocket,
    session_manager: &SessionManager,
) -> Result<(), String> {
    let frame = ws.recv().await?;  // Binary frame from client
    let bytes = frame.as_bytes();   // [op=kv-put][tenant=...][data...]

    // Find or create session for this connection
    let session_id = get_or_create_session_id();
    
    // Forward raw bytes to session handler
    session_manager.handle_frame(session_id, bytes)?;
    
    Ok(())
}
```

**What transport does:**
- Receive raw bytes
- Route to session manager
- Does NOT parse, does NOT check auth, does NOT touch domains

---

## Phase 2: Session Layer (Authorization Gate)

### Session Manager

```rust
pub struct SessionManager {
    sessions: HashMap<u64, Session>,
    domains: Arc<DomainRegistry>,
}

impl SessionManager {
    pub fn handle_frame(
        &mut self,
        session_id: u64,
        frame: &[u8],
    ) -> Result<Vec<u8>, String> {
        // Get the session (must be authenticated)
        let session = self.sessions.get_mut(&session_id)
            .ok_or("session not found")?;

        // Check expiration
        let now = std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        session.check_expiration(now)?;

        // Parse protocol frame
        let (op, route, access, payload) = self.parse_operation(frame)?;

        // **CRITICAL GATE: Authorize before calling domain**
        let actor = session.actor();  // Gets SessionActor with claims + permissions
        if !actor.authorize(&route, access) {
            return Err(format!("access denied: {} {}", route, access));
        }

        // Now safe to call domain - guaranteed authorized
        let response = self.domains.handle(op, payload)?;

        // Encode response
        Ok(response)
    }
}
```

**What session does:**
1. ✅ Retrieves immutable Claims
2. ✅ Compiles & checks permissions
3. ✅ Parses protocol frame into route + operation
4. ✅ **Calls authorize() for every operation**
5. ✅ Only calls domain if authorized
6. ✅ Never lets unauthorized requests through

**What session does NOT do:**
- ❌ Does not re-verify tokens
- ❌ Does not inspect raw claims
- ❌ Does not make business decisions

---

## Phase 3: Authorization Check (SessionActor)

### Session Authentication (Initial)

```rust
// When client authenticates (CONNECT phase):
pub fn authenticate_session(
    session: &mut Session,
    jwt: &str,
) -> Result<(), String> {
    // Parse JWT without verification (will be verified by auth layer)
    let raw_claims = auth::parse_jwt_noverify(jwt)?;

    // Verify signature (auth responsibility)
    let claims = auth::verify_and_normalize(
        jwt,
        &signing_key,
        &issuer_allowlist,
        &audience,
        now,
    )?;
    // claims is now immutable Claims { sub, tenant, roles, permissions, exp }

    // Compile permissions (done once)
    let permissions = SessionPermissions::from_permissions(claims.permissions.clone());

    // Store in session
    session.authenticate(claims, permissions)?;
    
    Ok(())
}
```

### Per-Request Authorization

```rust
impl SessionActor {
    pub fn authorize(&self, route: &Route, access: Access) -> bool {
        // Check compiled permissions
        // Example: claims.permissions = ["kv://prod/**#write", "notice://**#read"]
        // Compiled matcher checks:
        //   - Route "kv://prod/orders/123" matches pattern "kv://prod/**"
        //   - Access "write" matches "write" level
        self.permissions.allows(route, access)
    }
}
```

**Per-request flow:**

```
Frame received: [op=kv-put] [route=kv://prod/orders/123] [data=...]

actor.authorize(&Route::new("kv://prod/orders/123"), Access::Write)
  ↓
permissions.allows(route, access)
  ↓
Check compiled patterns:
  - Pattern "kv://prod/**" matches "kv://prod/orders/123" ✓
  - Access Write matches Write ✓
  ↓
Return: true (AUTHORIZED)
```

**What authorization does:**
- ✅ Fast route matching (compiled once at auth time)
- ✅ Access level check (Read/Write/All)
- ✅ Returns boolean only (no side effects)

---

## Phase 4: Domain Call (Guaranteed Authorized)

### KV Domain Handler

```rust
pub struct KvDomain {
    store: HashMap<String, Vec<u8>>,
}

impl Domain for KvDomain {
    fn handle(&mut self, request: DomainRequest) -> Result<DomainResponse, String> {
        // ⚠️ CRITICAL: Domain receives PRE-AUTHORIZED request
        // Session layer GUARANTEES this request was authorized
        // Domain does NOT re-check

        match request.operation {
            KvOperation::Put { key, value } => {
                // Just do the work
                self.store.insert(key, value);
                Ok(DomainResponse {
                    status: ResponseStatus::Ok,
                    data: vec![],
                })
            }
            KvOperation::Get { key } => {
                let value = self.store.get(&key)
                    .cloned()
                    .unwrap_or_default();
                Ok(DomainResponse {
                    status: ResponseStatus::Ok,
                    data: value,
                })
            }
        }
    }
}
```

**What domain does:**
- ✅ Processes business logic
- ✅ Reads/writes to domain-specific state
- ✅ Returns response data

**What domain does NOT do:**
- ❌ Check permissions (would be WRONG)
- ❌ Inspect claims (would be WRONG)
- ❌ Call auth/session code (would be WRONG)

---

## Complete Request Lifecycle

```
┌─────────────────────────────────────────────────────────────┐
│ 1. TRANSPORT: WebSocket Frame Arrives                       │
│    [op=kv.put][route=kv://prod/orders/123][data=...]       │
└────────────────────┬────────────────────────────────────────┘
                     ↓
┌─────────────────────────────────────────────────────────────┐
│ 2. SESSION: Receive in SessionManager                        │
│    - Find session (must exist and be authenticated)          │
│    - Check expiration                                        │
│    - Parse operation from frame                              │
└────────────────────┬────────────────────────────────────────┘
                     ↓
┌─────────────────────────────────────────────────────────────┐
│ 3. SESSION: Authorization Gate                              │
│    actor.authorize(route, access) ?                         │
│    - No: Return error frame immediately                     │
│    - Yes: Continue to domain ↓                              │
└────────────────────┬────────────────────────────────────────┘
                     ↓
┌─────────────────────────────────────────────────────────────┐
│ 4. DOMAIN: KvDomain::handle()                               │
│    - Guaranteed to be called only for AUTHORIZED request    │
│    - No permission checks needed                            │
│    - Just business logic: put value in store                │
└────────────────────┬────────────────────────────────────────┘
                     ↓
┌─────────────────────────────────────────────────────────────┐
│ 5. SESSION: Encode Response                                 │
│    [status=ok][data=...]                                    │
└────────────────────┬────────────────────────────────────────┘
                     ↓
┌─────────────────────────────────────────────────────────────┐
│ 6. TRANSPORT: Send to Client                                │
│    WebSocket sends response frame                           │
└─────────────────────────────────────────────────────────────┘
```

---

## Failure Case: Unauthorized Request

```
┌──────────────────────────────────────────────────────────────┐
│ 1. TRANSPORT: WebSocket Frame                                │
│    [op=notice.publish][route=notice://prod/events][data=...] │
└────────────────┬─────────────────────────────────────────────┘
                 ↓
┌──────────────────────────────────────────────────────────────┐
│ 2. SESSION: Parse Operation                                  │
│    route = "notice://prod/events"                            │
│    access = Access::Write                                    │
└────────────────┬─────────────────────────────────────────────┘
                 ↓
┌──────────────────────────────────────────────────────────────┐
│ 3. SESSION: Authorization Gate                               │
│    actor.authorize(&route, access) ?                         │
│    Claims permissions: ["kv://prod/**#read"]                │
│    Does "notice://prod/events" match "kv://prod/**" ? NO   │
│    Does ANY pattern match with Write access ? NO             │
│    → Return false                                            │
└────────────────┬─────────────────────────────────────────────┘
                 ↓
┌──────────────────────────────────────────────────────────────┐
│ 4. SESSION: Reject (Domain NEVER Called)                     │
│    Return error: "access denied: notice://prod/events write" │
└────────────────┬─────────────────────────────────────────────┘
                 ↓
┌──────────────────────────────────────────────────────────────┐
│ 5. TRANSPORT: Send Error to Client                           │
│    [status=denied][message=access denied]                    │
└──────────────────────────────────────────────────────────────┘
```

**Key point:** Domain is NEVER called for unauthorized requests.

---

## Initial Authentication (CONNECT Phase)

```
┌──────────────────────────────────────────────────────┐
│ 1. Client: CONNECT Frame with JWT                    │
│    [op=CONNECT][jwt=eyJ0eXAi...]                     │
└────────────┬───────────────────────────────────────┘
             ↓
┌──────────────────────────────────────────────────────┐
│ 2. SESSION: Create Unauthenticated Session           │
│    (session created with empty permissions)          │
└────────────┬───────────────────────────────────────┘
             ↓
┌──────────────────────────────────────────────────────┐
│ 3. AUTH: Verify JWT                                  │
│    - Parse JWT structure                             │
│    - Verify signature (RSA/HMAC/JWKS)                │
│    - Validate issuer, audience, expiration           │
│    - Resolve tenant (tid/tenant_id/org_id)           │
│    - Normalize permissions from scopes/roles         │
│    → Returns immutable Claims                        │
└────────────┬───────────────────────────────────────┘
             ↓
┌──────────────────────────────────────────────────────┐
│ 4. SESSION: Authenticate                             │
│    session.authenticate(claims, compiled_perms)      │
│    - Store immutable claims in session               │
│    - Compile permissions (done once, cached)         │
│    - Mark session as authenticated                   │
└────────────┬───────────────────────────────────────┘
             ↓
┌──────────────────────────────────────────────────────┐
│ 5. TRANSPORT: Send CONNECT_OK                        │
│    [op=CONNECT_OK][session_id=42]                    │
└──────────────────────────────────────────────────────┘
```

After CONNECT_OK, all subsequent operations use the authenticated session with:
- Immutable `Claims { sub, tenant, roles, permissions, exp }`
- Compiled `SessionPermissions` for fast route matching

---

## Token Expiration Check

During normal operation:

```rust
pub fn handle_frame(&mut self, session_id: u64, frame: &[u8]) -> Result<Vec<u8>, String> {
    let session = self.sessions.get_mut(&session_id)?;
    
    // Check expiration before ANY operation
    let now = current_unix_timestamp();
    session.check_expiration(now)?;
    
    // If we get here, token is still valid
    // Continue with authorization + domain call
    // ...
}
```

If expired:
```
session.check_expiration(now) → Err("token expired")
  ↓
Return error frame immediately
  ↓
Domain is NEVER called
  ↓
Transport sends error to client
```

---

## Re-authentication

Client can provide new JWT during session:

```rust
pub fn handle_reauth(
    session: &mut Session,
    new_jwt: &str,
) -> Result<(), String> {
    // Verify new JWT (auth responsibility)
    let new_claims = auth::verify_and_normalize(new_jwt, ...)?;
    
    // Compile new permissions
    let new_permissions = SessionPermissions::from_permissions(
        new_claims.permissions.clone()
    );
    
    // Replace claims atomically
    session.authenticate(new_claims, new_permissions)?;
    
    Ok(())
}
```

After re-auth, session has fresh claims + permissions, both immutable until next re-auth.

---

## Key Invariants Maintained

1. **Auth does NOT know domains** — Auth just normalizes claims
2. **Session does NOT call auth** — Claims are immutable, no re-verification
3. **Domain does NOT check auth** — Authorization is guaranteed by session
4. **No layer reaches backward** — Info flows forward only: auth → session → domain

---

## Summary

| Layer | Input | Decision | Output | Next |
|-------|-------|----------|--------|------|
| **Transport** | Raw bytes | Route to session | None | Session handler |
| **Session** | Frame | Authorize via compiled perms | Error OR forward | Domain (if OK) |
| **Domain** | Authorized operation | Business logic | Response data | Session encoder |
| **Transport** | Response data | Encode frame | Bytes | Client |

**The critical moment:** At the authorization gate in Session. Every domain call is guarded, every unauthorized request is rejected before reaching business logic.
