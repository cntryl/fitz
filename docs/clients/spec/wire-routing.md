## Wire Protocol

### TLV Record Encoding

Frame payloads consist of one or more **TLV records** concatenated back-to-back.
Each record is:

- **Type** (variable, 1 or 3 bytes):
  - If `type <= 0xFE`: encoded as a single byte
  - If `type > 0xFE`: escape byte `0xFF` followed by 2-byte big-endian u16

**IMPORTANT:** The wire examples in this document show MessageTypes as 2-byte big-endian for readability. Conformant implementations MUST use the variable-length encoding: types 0–254 are 1 byte, types 255+ use the `0xFF` escape followed by `u16 BE`. See **Type Encoding Rules** at the end of the Constants section for decoder/encoder pseudocode.
- **Length** (u16, big-endian): byte count of value (0..=65535)
- **Value**: exactly `length` bytes

### Message Framing (How Domain Operations Map to TLV)

**CRITICAL: Every Fitz message is a single TLV record where:**
- **Type** = MessageType (verb wire code: 100-111 for KV, 500-504 for Notice, etc.)
- **Length** = Total byte count of domain payload (all fields concatenated)
- **Value** = Domain-specific fields (as documented per domain)

**TLV is NOT nested** - the entire domain payload is the TLV Value, pre-encoded.

#### Message Structure

```
[MessageType (u16 BE)][Length (u16 BE)][Payload (Length bytes)]
│                     │                 │
│                     │                 └─ Domain fields (concatenated)
│                     └─ Total payload size
└─ Verb wire code
```

#### Complete Message Examples

**Example 1: KV PUT (MessageType=104)**

Wire format specification:
```
[u64 BE]   tx_id
[u32 BE]   route_len
[bytes]    route
[u32 BE]   key_len
[bytes]    key
[u32 BE]   value_len
[bytes]    value
```

Actual bytes on wire:
```
[0x00 0x68]                              (MessageType=104, KV PUT)
[0x00 0x39]                              (Length=57 bytes)
  [0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x01]  (tx_id=1, u64 BE)
  [0x00 0x00 0x00 0x15]                  (route_len=21)
  [6b 76 3a 2f 2f 70 72 6f 64 2f 61 70 70 2f 75 73 65 72 73]  (route="kv://prod/app/users", 21 bytes)
  [0x00 0x00 0x00 0x03]                  (key_len=3)
  [62 6f 62]                             (key="bob", 3 bytes)
  [0x00 0x00 0x00 0x05]                  (value_len=5)
  [61 6c 69 63 65]                       (value="alice", 5 bytes)

Total frame size: 2 (type) + 2 (length) + 57 (payload) = 61 bytes
```

**Example 2: Notice SUBSCRIBE (MessageType=501)**

Wire format specification:
```
[u32 BE]   route_pattern_len
[bytes]    route_pattern
```

Actual bytes on wire:
```
[0x01 0xF5]                              (MessageType=501, Notice SUBSCRIBE)
[0x00 0x18]                              (Length=24 bytes)
  [0x00 0x00 0x00 0x14]                  (route_pattern_len=20)
  [6e 6f 74 69 63 65 3a 2f 2f 70 72 6f 64 2f 61 70 70 2f 2a]  (pattern="notice://prod/app/*", 20 bytes)

Total frame size: 2 (type) + 2 (length) + 24 (payload) = 28 bytes
```

**Example 3: KV BEGIN (MessageType=100)**

Wire format specification:
```
[u32 BE]  route_len
[bytes]   route
[u8]      mode (0=ReadOnly, 1=ReadWrite)
[u8]      durability (0=Buffered, 1=Sync; other values invalid)
```

Actual bytes on wire:
```
[0x00 0x64]                              (MessageType=100, KV BEGIN)
[0x00 0x1F]                              (Length=31 bytes)
  [0x00 0x00 0x00 0x15]                  (route_len=21)
  [6b 76 3a 2f 2f 70 72 6f 64 2f 61 70 70 2f 75 73 65 72 73]  (route="kv://prod/app/users", 21 bytes)
  [0x01]                                 (mode=1, ReadWrite)
  [0x01]                                 (durability=1, Sync)

Total frame size: 2 (type) + 2 (length) + 31 (payload) = 35 bytes
```

**Example 4: RPC REQUEST (MessageType=302)**

Wire format specification:
```
[16 bytes] correlation_id (UUID)
[u32 BE]   route_len
[bytes]    route
[u32 BE]   body_len
[bytes]    body
```

Actual bytes on wire:
```
[0xFF 0x01 0x2E]                         (MessageType=302, RPC REQUEST)
[0x00 0x30]                              (Length=48 bytes)
  [12 34 56 78 9a bc de f0 12 34 56 78 9a bc de f0]  (correlation_id, 16 bytes UUID)
  [0x00 0x00 0x00 0x15]                  (route_len=21)
  [72 70 63 3a 2f 2f 70 72 6f 64 2f 61 70 70 2f 77 6f 72 6b 65 72]  (route="rpc://prod/app/worker", 21 bytes)
  [0x00 0x00 0x00 0x03]                  (body_len=3)
  [66 6f 6f]                             (body="foo", 3 bytes)

Total frame size: 3 (type) + 2 (length) + 48 (payload) = 53 bytes
```

#### Transport Layer Framing

**WebSocket:**
```
Binary WebSocket frame contains raw TLV message:
[MessageType][Length][Payload]

Example: Send KV PUT
WebSocket binary frame body = 51 bytes (from Example 1 above)
```

**TCP (with length-prefixed framing):**
```
[Frame Length (u32 BE)][MessageType][Length][Payload]
│                       │
│                       └─ TLV message
└─ Total message size (including MessageType + Length + Payload)

Example: Send KV PUT (51 bytes TLV)
[0x00 0x00 0x00 0x33]  (frame_length=51)
[0x00 0x68]            (MessageType=104)
[0x00 0x2F]            (Length=47)
[...47 bytes payload...]
```

#### Reference Decoder Pseudocode

```python
def decode_frame(frame_bytes):
    """Decode a single TLV frame into a domain message."""
    # Parse TLV header
    message_type = read_u16_be(frame_bytes[0:2])
    length = read_u16_be(frame_bytes[2:4])
    payload = frame_bytes[4:4+length]
    
    # Verify payload matches declared length
    if len(payload) != length:
        raise ProtocolError("Payload length mismatch")
    
    # Route to domain decoder based on MessageType
    if 100 <= message_type <= 199:
        return decode_kv_message(message_type, payload)
    elif 200 <= message_type <= 299:
        return decode_queue_message(message_type, payload)
    elif 300 <= message_type <= 399:
        return decode_rpc_message(message_type, payload)
    elif 400 <= message_type <= 499:
        return decode_lease_message(message_type, payload)
    elif 500 <= message_type <= 599:
        return decode_notice_message(message_type, payload)
    elif 600 <= message_type <= 699:
        return decode_stream_message(message_type, payload)
    elif 700 <= message_type <= 799:
        return decode_schedule_message(message_type, payload)
    else:
        raise UnknownMessageType(message_type)

def decode_kv_message(message_type, payload):
    """Decode KV domain message based on MessageType."""
    offset = 0
    
    if message_type == 100:  # BEGIN
        route_len = read_u32_be(payload[offset:offset+4])
        offset += 4
        route = payload[offset:offset+route_len].decode('utf-8')
        offset += route_len
        mode = payload[offset]
        offset += 1
        durability = payload[offset]
        offset += 1
        if mode not in (0, 1):
            raise ProtocolError("Invalid transaction mode")
        if durability not in (0, 1):
            raise ProtocolError("Invalid durability mode")
        
        return KvBegin(route=route, mode=mode, durability=durability)
    
    elif message_type == 104:  # PUT
        tx_id = read_u64_be(payload[offset:offset+8])
        offset += 8
        
        route_len = read_u32_be(payload[offset:offset+4])
        offset += 4
        route = payload[offset:offset+route_len].decode('utf-8')
        offset += route_len
        
        key_len = read_u32_be(payload[offset:offset+4])
        offset += 4
        key = payload[offset:offset+key_len]
        offset += key_len
        
        value_len = read_u32_be(payload[offset:offset+4])
        offset += 4
        value = payload[offset:offset+value_len]
        offset += value_len
        
        # Verify all payload consumed
        if offset != len(payload):
            raise ProtocolError("Trailing data in PUT payload")
        
        return KvPut(tx_id=tx_id, route=route, key=key, value=value)
    
    # ... other KV verbs

def decode_notice_message(message_type, payload):
    """Decode Notice domain message based on MessageType."""
    offset = 0
    
    if message_type == 501:  # SUBSCRIBE
        pattern_len = read_u32_be(payload[offset:offset+4])
        offset += 4
        pattern = payload[offset:offset+pattern_len].decode('utf-8')
        offset += pattern_len
        
        if offset != len(payload):
            raise ProtocolError("Trailing data in SUBSCRIBE payload")
        
        return NoticeSubscribe(pattern=pattern)
    
    # ... other Notice verbs
```

**Key Insights for Implementers:**

1. **Single TLV Level:** Each message is ONE TLV record, not nested TLVs
2. **MessageType = Verb:** The TLV Type field IS the verb wire code (100-799)
3. **Payload = Concatenated Fields:** Just concatenate domain fields in order (no internal TLV structure)
4. **Length Validation:** Always verify `offset == len(payload)` after decoding (detects trailing data)
5. **Transport Agnostic:** Same TLV message format for WebSocket and TCP (TCP adds outer length prefix)

### Primitive Encodings

All fields use **big-endian** byte order.
| Type | Encoding |
| ------------- | ------------------------------- |
| `u8` | single byte |
| `u16` | 2 bytes, big-endian |
| `u32` | 4 bytes, big-endian |
| `u64` | 8 bytes, big-endian |
| `String` | `[u32 BE len][UTF-8 bytes]` |
| `Bytes` | `[u32 BE len][raw bytes]` |
| `Optional<T>` | `[u8 present]` + T if present=1 |
| `UUID` | 16 raw bytes (no hyphens) |

### Encoding Invariants

Clients MUST:

1. Encode all integers in big-endian byte order
2. Consume all bytes in request payloads; error if trailing data remains
3. Encode responses with exact length prefixes
4. Handle both single-byte and escape-byte MessageTypes identically
5. **Duplicate TLV tags are NOT permitted within a single frame.** If a TLV tag appears more than once in a frame the frame **MUST** be treated as malformed and the receiver **MUST** close the connection with a TLV parse error. **Rationale:** Fitz TLV disallows duplicate tags to keep decoding deterministic and to simplify client implementations and conformance testing.
6. **A single TLV value MUST NOT exceed 65535 bytes (≈64 KiB).** Large payloads MUST be split across multiple frames or multiple operations — never by repeating the same TLV tag within a single frame (which would violate rule 5).

### Response Format Convention

**All domain responses follow this standard structure:**

1. **Status byte** (u8): 0=success, 1=error
2. **If success (status=0):** Domain-specific success payload
3. **If error (status=1):** Error message
   ```
   [u32 BE] error_len
   [bytes]  error_msg (UTF-8, human-readable)
   ```

**Clients MUST check status byte first before parsing payload.**

**Exception: RPC Domain**
RPC responses include a `correlation_id` field (16-byte UUID) to match responses to requests across multiple in-flight operations. See [RPC Domain](queue-rpc-kv.md) for details on how RPC enables per-request correlation.

**Example (KV GET success):**
```
[0x00]                    (status=0, success)
[0x00 0x00 0x00 0x05]     (value_len=5)
[0x61 0x6c 0x69 0x63 0x65] ("alice")
```

**Example (KV GET error):**
```
[0x01]                    (status=1, error)
[0x00 0x00 0x00 0x0d]     (error_len=13)
[0x4b 0x65 0x79...]       ("Key not found")
```

**Rationale:** Standardized error format across all domains simplifies client error handling and ensures consistent debugging experience. Multiplexing is channel-based for different domains; RPC is the only domain with explicit per-request correlation IDs for true request/response matching.

### Frame Size Limits

**Default maximum frame size: 1 MB (1,048,576 bytes)**

**Rules:**
- Client MUST NOT send frames exceeding broker's limit
- Broker MUST close connection if frame exceeds limit (transport error)
- No negotiation protocol (clients assume 1 MB default)
- Deployments with custom limits MUST document them
- Clients SHOULD make `max_frame_size` configurable

**Handling large payloads:**
- Split across multiple operations (e.g., batch ENQUEUE)
- Use streaming (e.g., Stream APPEND multiple records)
- Application-level chunking (not protocol-level)
- For Queue/Notice/RPC: Keep payload under limit or use external blob storage with reference

**Discovery:**
- No runtime discovery mechanism
- Clients assume 1 MB by default
- Server documentation MUST specify if non-default

## Connection Lifecycle

### 1. Open Transport

- **WebSocket:** `wss://broker:port/` (TLS recommended)
- **TCP:** `tcp://broker:port` (TLS recommended)
- Broker address and credentials must be configured before opening

### Client State Machine

Clients SHOULD implement a simple connection state machine to keep behavior predictable and testable.
States:

- DISCONNECTED → CONNECTING → AUTHENTICATED → CLOSED
  Transitions:
- DISCONNECTED: initial state
- CONNECTING: transport open; send CONNECT
- AUTHENTICATED: CONNECT accepted (no close); ready for domain requests
- CLOSED: transport closed or unrecoverable error
  ASCII diagram:

```
DISCONNECTED --(open transport)--> CONNECTING --(CONNECT & accepted)--> AUTHENTICATED
     ^                                            |
     |                                            v
     +---------(close / unrecoverable)----------- CLOSED
```

Notes:

- Clients MUST handle transport failures and implement exponential backoff on reconnect.
- **Multiplexing Support**: Clients MAY send multiple in-flight requests **on different channels** (domains). For example, a client can send a KV PUT while also sending a Notice PUBLISH—these go to different logical channels and are processed independently. However, within a single domain, clients SHOULD follow request/response sequencing unless the domain supports explicit correlation IDs (currently only RPC).

### 2. Send CONNECT Record (FIRST MESSAGE)

Clients MUST send a **CONNECT** TLV record as the first message:

```
MessageType: 1 (CONNECT)
Value: compact JWT string bytes (UTF-8), NO length prefix
Length: JWT byte length
```

**Example (Authenticated Mode):**

```
[0x01]                    (MessageType=1)
[0x00 0x63]               (Length=99, u16)
[99 bytes of JWT...]
```

**Example (Anonymous Mode - Empty JWT):**

```
[0x01]                    (MessageType=1)
[0x00 0x00]               (Length=0, u16)
(no JWT bytes)
```

**Constraints:**

- CONNECT MUST be first frame sent
- **Authenticated mode (`FITZ_AUTH_REQUIRED=true`):** JWT required, invalid JWT causes connection close
- **Anonymous mode (`FITZ_AUTH_REQUIRED=false`):** JWT optional, empty or placeholder accepted
- JWT payload MUST be valid UTF-8 (if present)
- Clients SHOULD implement CONNECT timeout (5–10 seconds)

### 3. Await Broker Confirmation

**Session Confirmation Protocol:**
Broker behavior:

- **Valid CONNECT:** No explicit ACK message. Broker remains silent and is ready for requests.
- **Invalid CONNECT:** Broker closes connection within 1 second (no response frame sent)
- **No CONNECT within 10 seconds:** Broker closes connection with graceful shutdown

Clients MUST:

- Send CONNECT as the first frame after transport is established
- **Immediately proceed to send domain requests** after sending CONNECT (do not wait for an ACK)
- If the broker rejects the JWT, it closes the connection — the client discovers this when the transport drops or when the first domain response fails
- If the connection closes within 1 second of CONNECT, treat as authentication failure
- Do NOT retry with the same JWT after auth failure
- Implement a CONNECT timeout of 5–10 seconds — if no domain response AND no connection close within this window, close and retry with backoff

**Recommended client pattern:**

```
1. Open transport (WebSocket/TCP)
2. Send CONNECT frame with JWT
3. Immediately send first domain request (e.g., KV BEGIN, Notice SUBSCRIBE)
4. If domain response arrives → connection is authenticated and working
5. If connection closes → auth failure, do NOT retry same JWT
6. If neither within timeout → close, retry with backoff
```
  **Session State After Successful CONNECT:**
  On successful CONNECT, broker creates session and MUST:
- Assign unique session ID (internal use only)
- Extract JWT identity context and resolve route family through server configuration
- Establish normalized route-shaped permissions for all subsequent requests
- Track active subscriptions, transactions, and resources
  **Session Cleanup On Disconnect:**
  When client disconnects:
- All active subscriptions are dropped
- All active transactions (KV) are rolled back
- All active stream sessions are aborted
- All held leases are released
- All RPC worker registrations are cleared
- All pending RPC requests are discarded
- Queued notifications are discarded
  **State NOT Restored On Reconnect:**
  On reconnect with new CONNECT:
- New session ID issued (previous session ID is invalid)
- Previous subscriptions, transactions, and worker registrations are NOT recovered
- Previous pending RPC requests are NOT durably recovered or replayed
- Client MUST explicitly re-subscribe, re-begin, or re-register if needed

### 4. Send Domain Requests

After successful CONNECT, client may send domain-specific requests.

**Channel-Based Multiplexing:**

- **Clients MAY send multiple in-flight requests on different channels (domains).** Each domain (KV, RPC, Notice, etc.) is routed to its own logical channel by the broker. This allows concurrent operations across different domains on the same connection.
- **Within a single domain**: Follow request/response sequencing unless the domain explicitly supports per-request correlation IDs (currently only RPC). Sending multiple requests of the same type without waiting for responses is undefined behavior.
- **RPC domain is special**: RPC REQUEST includes an explicit 16-byte UUID `correlation_id` that clients generate. This allows true request/response matching for multiple in-flight RPC requests.
- **RPC registrations are session-scoped**: A worker reconnecting after disconnect or broker restart MUST send `Subscribe` again before it will receive new requests.
- **Out-of-band messages**: Asynchronous deliveries (e.g., Notice NOTIFY, RPC RESPONSE streaming) arrive without correlation IDs to requests; clients MUST handle them separately.
- **Order guarantees**: Responses are delivered in the order requests were sent (per domain/channel).

### 5. Receive Responses

Each request receives one response frame. Response format is domain-specific (see domain specs).

### 6. Close Connection

Clients SHOULD:

- Send WebSocket close frame or TCP FIN gracefully
- Clean up resources
- Discard pending requests on abrupt close
  Clients MUST:
- Assume connection is closed if transport layer signals close
- Reconnect and explicitly rebuild any required session-scoped state

## Authentication & Security

### Authentication Modes

Fitz brokers support two authentication modes controlled by server configuration:
**1. Authenticated Mode** (`FITZ_AUTH_REQUIRED=true`):

- JWT authentication is **required** for all connections
- CONNECT frame MUST include valid JWT
- Broker validates JWT signature and claims
- Missing or invalid JWT, including missing or unmapped route-family identity context, causes immediate connection close
  **2. Anonymous Mode** (`FITZ_AUTH_REQUIRED=false`):
- JWT authentication is **optional**
- CONNECT frame MAY include empty JWT or placeholder value
- Broker assigns default permissions (typically full access to all realms/areas)
- Broker always uses internal route family `1`
- Useful for development, testing, or trusted internal networks

### JWT (Authentication Mechanism)

**When authentication is required,** clients MUST:

1. Obtain a JWT from an external authentication service
2. Pass the compact JWT string in the CONNECT record
3. Treat JWT as opaque (do not parse or validate server-side)
4. Resend JWT on reconnect
   **When authentication is optional (anonymous mode),** clients MAY:

- Send empty JWT (zero-length payload)
- Send placeholder JWT (e.g., "anonymous")
- Omit JWT field (broker accepts connection without authentication)
  Clients MUST NOT:
- Generate or sign JWTs
- Validate JWT signatures
- Cache or reuse JWTs across sessions
- Attempt to decode JWT claims

### Authorization

Authorization is **always server-side**:

- **Authenticated mode:** Broker validates JWT claims against route permissions
- **Anonymous mode:** Broker uses default permission set (no JWT validation)
- If client sends unauthorized request, broker returns error
- Clients MUST NOT attempt local permission checking

### TLS on Untrusted Networks

When traffic crosses an untrusted network, clients MUST:
Clients MUST:

- Use `wss://` for WebSocket (never plain `ws://`)
- Use TLS for TCP (never plain TCP on untrusted networks)
- Validate server certificate chain against system CA roots
- Perform hostname verification (certificate CN or SAN must match broker hostname)
- Reject expired certificates
- Reject revoked certificates (if OCSP stapling available)
- Reject self-signed certificates (unless explicitly in trust store via deployment config)

Browser WebSocket deployments must also send a normal HTTP `Origin` header
during the upgrade. Fitz accepts only exact configured browser origins, such as
`https://app.example.com`, when an origin allowlist is configured. Configured
origins must not include paths, query strings, fragments, wildcards, or trailing
slashes.

  **Development/Testing (MAY Skip with Explicit Flag):**
  Clients MAY accept self-signed or invalid certificates ONLY if:
- Explicitly enabled via configuration flag (e.g., `insecure_skip_verify=true`)
- User acknowledges security risk in documentation
- Never default to insecure; require explicit opt-in
  Clients MUST NOT:
- Skip certificate validation to "work around" deployment issues
- Accept expired or revoked certificates without explicit flag
- Disable hostname verification
- Accept any certificate on an untrusted network

## Flow Control & Backpressure

Clients MUST implement queueing, backoff, and bounded concurrency:

- Implement configurable write queue with maximum size.
- Enforce a configurable per-connection maximum in-flight work limit.
- When the queue or concurrency limit is reached, surface a retryable backpressure error or wait for capacity before admitting more work. Do NOT silently drop or spawn unbounded work.
- Implement exponential backoff for retries.

**Server backpressure signaling:**

Brokers signal backpressure through **domain error responses**. There is no separate backpressure frame. When a domain's internal queue is full, the broker returns a domain error in the standard response format:

- RPC: `6003 = ERR_RPC_BACKPRESSURE`
- Queue: `4005 = ERR_QUEUE_FULL`
- Other domains: connection close if internal buffers overflow

**Client behavior on backpressure errors:**

1. Pause sending to the affected domain
2. Apply exponential backoff (starting at 100ms, max 30s)
3. Retry the failed operation after backoff
4. If backpressure persists, surface error to caller

**Notice domain exception:** Since PUBLISH is fire-and-forget with no response, the broker silently drops notifications under backpressure. Clients have no visibility into dropped notices — this is by design (best-effort semantics).

## Routing

Routes are **opaque URI-like strings** that address domain resources. The
message type identifies the operation; some domains accept operation suffixes
for live protocol ergonomics and canonicalize them to the resource identity for
authorization.

### Route Format

```
{scheme}://{realm}/{area}/{resource}
```

**Components:**
| Component | Type | Example | Rules |
| ----------- | ------ | ------------------------- | ----------------------------------------------------- |
| `scheme` | string | `kv`, `queue`, `notice` | Identifies domain; MUST match known domain list |
| `realm` | string | `prod`, `tenant-123` | Opaque to client; case-sensitive |
| `area` | string | `app`, `system` | Opaque to client; case-sensitive |
| `resource` | string | `users`, `orders` | Opaque to client; may be omitted for admin operations |
**Route Examples:**

```
kv://prod/app/users              # KV resource
queue://prod/app/orders/send     # Queue enqueue route, authorized as queue://prod/app/orders
notice://prod/app/events         # Live fanout route
```

## HTTP-Like Design Principle

Fitz follows an **HTTP-like model** where every operation is explicitly
addressed on the wire, while domains may still maintain live session-scoped
state when their contract requires it:

### Core Analogy

**HTTP:**

```
POST /api/users HTTP/1.1
Host: example.com
Content-Type: application/json

{"name": "alice"}
```

**Fitz:**

```
Verb: PUT (MessageType=104)
Route: kv://prod/app/users
Payload: [tx_id][key][value]
```

### Key Principles

1. **Every operation is explicitly scoped**
   - Route-scoped example: KV PUT = `[tx_id][route][key][value]`
   - Route-scoped example: Queue ENQUEUE = `[route][body][delay]`
   - Session-scoped example: Stream APPEND = `[session_id][body]` after a prior BEGIN bound the session to one resource

2. **Operations are explicitly addressable**
   - Most operations carry a route on the wire
   - Session-based operations may carry an opaque handle instead of repeating the route
   - Connection loss does not require reconstructing hidden broker topology, though session-scoped domain state may still be cleaned up

3. **Verbs determine action** (like HTTP GET/POST/PUT/DELETE)
   - MessageType selects operation
   - Wire codes are stable ABI
   - Domain+Verb fully specifies behavior

4. **TLV is the wire format** (like HTTP has headers+body)
   - Type-Length-Value encoding
   - Binary efficient, not text-based
   - Extensible without version negotiation

### Why This Matters

**For implementers:**

- Simple mental model: "It's like HTTP but binary and over WebSocket/TCP"
- Familiar patterns: routes, verbs, self-contained requests
- Easy to reason about: no hidden state machines

**For operations:**

- Debuggable: every message carries explicit addressing and can be inspected without hidden route context
- Explicitly addressed: the wire format does not depend on per-connection route defaults
- Session-scoped domains still require re-establishing live state after reconnect when the domain contract says that state is ephemeral

### Comparison

| Aspect         | HTTP                            | Fitz                                   |
| -------------- | ------------------------------- | -------------------------------------- |
| **Addressing** | URL path                        | Route (kv://realm/area/resource)       |
| **Verb**       | GET, POST, PUT, DELETE          | MessageType (100=BEGIN, 104=PUT, etc.) |
| **Transport**  | TCP + TLS                       | WebSocket or TCP + TLS                 |
| **Format**     | Text (headers + body)           | Binary (TLV)                           |
| **State**      | Stateless (cookies for session) | Explicit routing; some domains keep live session state |
| **Operations** | Self-contained requests         | Self-contained requests                |

## Route Acceptance Criteria (Authoritative)

A request is valid **only if**:

1. The route shape is valid for the domain
2. Wildcards appear only in allowed positions (per domain)
3. The method permits those wildcards
4. The route depth matches the method's plane
   **Violations are protocol errors.** Broker MUST reject; clients **MAY** perform local route shape validation for ergonomics, but the broker is authoritative. Clients **MUST** accept broker rejection as the source of truth and MUST NOT rely on local validation as a substitute for server-side checks.

## Global Route Rules (Normative)

- Routes are opaque strings with a fixed, domain-defined shape
- `{realm}` may be a whole-segment wildcard for registration operations that support patterns
- `*` MAY appear only in positions explicitly allowed by the domain
- Extra path segments are **forbidden**
- Route shape validation occurs **before** permission or dispatch checks

### Wildcard Support by Domain

**Domains supporting wildcards (`*` and `**` patterns):**
- **KV:** SUBSCRIBE and UNSUBSCRIBE accept patterns capable of matching a three-segment route; mutations remain concrete
- **Stream:** READ patterns and SUBSCRIBE/UNSUBSCRIBE registration patterns are supported; writes remain concrete
- **Queue:** RESERVE, SUBSCRIBE, and UNSUBSCRIBE accept patterns capable of
  matching a three-segment route; other Queue operations remain concrete
- **Notice:** Full wildcard support in SUBSCRIBE patterns (`notice://realm/area/*`, `notice://realm/**`)
- **RPC:** Worker registrations accept `*` and `**`; calls remain concrete
- **Schedule:** SUBSCRIBE and UNSUBSCRIBE accept patterns capable of matching a four-segment route; CREATE and CANCEL remain concrete

**Domains requiring concrete routes only (no wildcards):**
- **Lease:** All operations use concrete routes only (`lease://realm/area/resource`)
- **KV mutations:** use concrete routes only (`kv://realm/area/resource`)
- **Queue mutations:** ENQUEUE, EXTEND, and COMPLETE use concrete routes only
  (`queue://realm/area/resource`)
- **Stream writes:** use concrete routes only (`stream://realm/area/resource`)
- **RPC calls:** use concrete routes only (`rpc://realm/area/resource/operation`)
- **Schedule definitions:** `CREATE` and `CANCEL` use concrete routes only (`schedule://realm/area/resource/operation`)

**Pattern matching semantics:**
- `*` matches exactly one path segment
- `**` matches zero or more path segments (greedy)
- Concrete routes (no wildcards) match exactly
- KV, Queue, Notice, Stream, RPC, and Schedule each permit at most 128 wildcard
  registrations per session. Exact registrations do not count, and duplicate
  `(session, original registration string)` requests are checked before the limit
- Wildcards never cross `RouteFamily` or permission boundaries, and overlapping registrations have no exact-pattern precedence
- Notifications carry the matching `subscription_id` and the exact concrete
  route, never the registration pattern

## Route Shapes by Domain

### KV Domain

**Valid Route Shapes:**

- `kv://{realm}/{area}`
- `kv://{realm}/{area}/{resource}`
- `kv://{realm}/{area}/*`
- `kv://{realm}/*/*`
- `kv://{realm}/**`
- `kv://*/{area}/{resource}`
  **Method Acceptance:**
  | Method | Accepted Route Shapes |
  | ---------------- | ----------------------------------------------- |
  | `LIST` | `{realm}/{area}`, `{realm}/*/*` |
  | `CREATE` | `{realm}/{area}` |
  | `DELETE` (admin) | `{realm}/{area}` |
  | `BEGIN` | `{realm}/{area}/{resource}` |
  | `GET` | `{realm}/{area}/{resource}` |
  | `PUT` | `{realm}/{area}/{resource}` |
  | `INSERT` | `{realm}/{area}/{resource}` |
  | `DELETE` | `{realm}/{area}/{resource}` |
  | `DELETE_RANGE` | `{realm}/{area}/{resource}` |
  | `SCAN` | `{realm}/{area}/{resource}`, `{realm}/{area}/*` |
  | `SUBSCRIBE` | `{realm}/{area}/{resource}`, `{realm}/{area}/*`, `{realm}/*/*` |
  | `UNSUBSCRIBE` | same as `SUBSCRIBE` |
  | `COMMIT` | `{realm}/{area}/{resource}` |
  | `ROLLBACK` | `{realm}/{area}/{resource}` |
  **Note:** `LIST`, `CREATE`, and `DELETE` (admin) operations are broker-internal management operations not currently exposed in the client wire protocol. Clients should focus on data operations: BEGIN, GET, PUT, INSERT, DELETE, DELETE_RANGE, SCAN, SUBSCRIBE, UNSUBSCRIBE, COMMIT, ROLLBACK.

### Stream Domain

**Valid Route Shapes:**

- `stream://{realm}/{area}/{resource}`
- `stream://{realm}/{area}/*`
- `stream://{realm}/*/*`
- `stream://{realm}/**`
- `stream://*/{area}/{resource}`
  **Method Acceptance:**
  | Method | Accepted Route Shapes |
  | ---------------- | -------------------------------------------------------------- |
  | `LIST` | `{realm}/{area}`, `{realm}/*/*` |
  | `CREATE` | `{realm}/{area}` |
  | `DELETE` (admin) | `{realm}/{area}` |
  | `BEGIN` | `{realm}/{area}/{resource}` |
  | `APPEND` | session_id established by `BEGIN({realm}/{area}/{resource})` |
  | `READ` | `{realm}/{area}/{resource}`, `{realm}/{area}/*`, `{realm}/*/*` |
  | `SUBSCRIBE` | `{realm}/{area}/{resource}`, `{realm}/{area}/*`, `{realm}/*/*` |
  | `UNSUBSCRIBE` | same as `SUBSCRIBE` |
  | `COMMIT` | session_id established by `BEGIN({realm}/{area}/{resource})` |
  | `ROLLBACK` | session_id established by `BEGIN({realm}/{area}/{resource})` |
  **Note:** `LIST`, `CREATE`, and `DELETE` (admin) operations are broker-internal management operations not currently exposed in the client wire protocol. Clients should focus on stream operations: BEGIN, APPEND, READ, SUBSCRIBE, UNSUBSCRIBE, COMMIT, ROLLBACK.

### Queue Domain

**Valid Route Shapes:**

- `queue://{realm}/{area}/{resource}`
- `queue://{realm}/{area}/*`
- `queue://{realm}/*/*`
- `queue://{realm}/**`
- `queue://*/{area}/{resource}`

**Route format:** For per-resource isolation, use the 3-segment form `queue://{realm}/{area}/{resource}`. Each distinct resource has its own queue and lease state.

RESERVE response items are selector-dependent without adding a negotiation
field: an exact request returns the established route-less item shape, while a
request containing a whole-segment wildcard returns the matched concrete route
before each item. The client always knows which decoder to use from the request
it sent.

**Lease expiry:** Servers process lease expiry lazily (e.g. when the next RESERVE or other operation runs). A reserved message whose lease has expired is returned to the ready queue on the next operation that touches that queue. Clients that rely on lease expiry (e.g. to re-reserve) should allow for this delay (e.g. wait a few seconds after lease TTL before re-reserving).

  **Method Acceptance:**
  | Method | Accepted Route Shapes |
  | ---------- | ----------------------------------------------- |
  | `LIST` | `{realm}/{area}`, `{realm}/*/*` |
  | `ENQUEUE` | `{realm}/{area}/{resource}` |
  | `RESERVE` | exact route or whole-segment pattern capable of matching three segments |
  | `COMPLETE` | `{realm}/{area}/{resource}` |
  | `EXTEND` | `{realm}/{area}/{resource}` |
  | `SUBSCRIBE` | exact route or whole-segment pattern capable of matching three segments |
  | `UNSUBSCRIBE` | same as `SUBSCRIBE` |

  **Note:** `LIST` is a broker-internal management operation not currently exposed in the client wire protocol. Clients should use: ENQUEUE, RESERVE, COMPLETE, EXTEND as documented in the wire format section.

### Schedule Domain

**Valid Route Shapes:**

- concrete route: `schedule://{realm}/{area}/{resource}/{operation}`
  **Method Acceptance:**
  | Method | Accepted Route Shapes |
  | -------- | ----------------------------------------------- |
  | `CREATE` | `{realm}/{area}/{resource}/{operation}` |
  | `CANCEL` | `{realm}/{area}/{resource}/{operation}` |
  | `LIST` | no route payload; optional `[offset][limit]` pagination fields only |
  | `SUBSCRIBE` | exact route or whole-segment pattern capable of matching four segments |
  | `UNSUBSCRIBE` | same as `SUBSCRIBE` |

  **Note:** `DELETE` (admin) and `TRIGGER` operations are broker-internal. Clients should use: CREATE, CANCEL, LIST, SUBSCRIBE, UNSUBSCRIBE as documented in the wire format section. LIST returns a single response payload containing `total_count` plus zero or more schedule entries.

### Lease Domain

**Valid Route Shapes:**

- `lease://{realm}/{area}/{resource}`
  **Method Acceptance:**
  | Method | Accepted Route Shapes |
  | --------- | --------------------------- |
  | `ACQUIRE` | `{realm}/{area}/{resource}` |
  | `RENEW` | `{realm}/{area}/{resource}` |
  | `RELEASE` | `{realm}/{area}/{resource}` |
  | `QUERY` | `{realm}/{area}/{resource}` |
  | `SUBSCRIBE` | `{realm}/{area}/{resource}` |
  | `UNSUBSCRIBE` | same as `SUBSCRIBE` |

Lease subscription routes are exact. Any wildcard, wrong scheme, empty segment,
or route with fewer or more than three segments is rejected with 5010.

### Notice Domain

**Valid Route Shapes:**

- `notice://{realm}/{area}/{resource}`
- `notice://{realm}/{area}/*`
- `notice://{realm}/*/*`
  **Method Acceptance:**
  | Method | Accepted Route Shapes |
  | ------------- | -------------------------------------------------------------- |
  | `SUBSCRIBE` | `{realm}/{area}/{resource}`, `{realm}/{area}/*`, `{realm}/*/*` |
  | `UNSUBSCRIBE` | same as `SUBSCRIBE` |
  | `PUBLISH` | `{realm}/{area}/{resource}` |

### RPC Domain

**Route Shape Guidance:**

- Calls use concrete route strings. Worker registrations accept strict
  whole-segment `*` and `**` patterns, including wildcard realm.
- Every registration owns independent concurrency credit across all concrete
  routes it matches. Exact and wildcard overlaps are equal candidates.
- Ready concrete routes rotate fairly within one `RouteFamily`; matching never
  crosses a family boundary.
- A session may retain at most 128 wildcard RPC registrations. Duplicate
  `(session, pattern)` registration is idempotent and retains its original credit.
- The common operation-style form is `rpc://{realm}/{area}/{resource}/{operation}`.
  **Method Acceptance:**
  | Method | Accepted Route Shapes |
  | ------------- | ------------------------------------------------------------ |
  | `CALL` | exact route (commonly `{realm}/{area}/{resource}/{operation}`) |
  | `SUBSCRIBE` | exact route or whole-segment wildcard pattern |
  | `UNSUBSCRIBE` | same as `SUBSCRIBE` |

## Lock-In Rule

**If a route shape is not explicitly listed for a method, it is invalid.**
This specification is the **single source of truth** for:

- Broker validation
- SDK conformance testing
- Permission enforcement
- Long-term protocol stability

## Verbs

Verbs are the **primary behavior selector**. They determine what action a request performs.

### Verb Requirements

Clients MUST:

1. **Expose verbs as constants or enums** in the client's native language
   - Python: `class KvVerb: GET = "GET"; PUT = "PUT"`
   - Rust: `enum KvVerb { Get, Put, ... }`
   - JavaScript: `const KvVerb = { Get: "get", Put: "put" }`
2. **Never expose wire codes** in public API
3. **Map verbs to i16 wire codes internally**
4. **Treat wire codes as ABI-stable** (never reused, append-only)

### Verb Set (All Domains)

| Domain   | Verb               | Wire Code | Plane         | Notes                   |
| -------- | ------------------ | --------: | ------------- | ----------------------- |
| KV       | BEGIN              |       100 | Data          | Start transaction       |
| KV       | COMMIT             |       101 | Data          | Finalize transaction    |
| KV       | ROLLBACK           |       102 | Data          | Abort transaction       |
| KV       | GET                |       103 | Data          | Read key                |
| KV       | PUT                |       104 | Data          | Write key               |
| KV       | INSERT             |       105 | Data          | Insert (fail if exists) |
| KV       | DELETE             |       106 | Data          | Delete key              |
| KV       | DELETE_RANGE       |       107 | Data          | Delete key range        |
| KV       | SCAN               |       108 | Data          | Scan keys in range      |
| KV       | SUBSCRIBE          |       109 | Data          | Watch committed changes |
| KV       | UNSUBSCRIBE        |       110 | Data          | Stop watching changes   |
| KV       | NOTIFY             |       111 | Notification  | Committed change event  |
| Queue    | ENQUEUE            |       200 | Data          | Add message             |
| Queue    | ENQUEUE_BATCH      |       201 | Reserved      | Batch add (future)      |
| Queue    | RESERVE            |       202 | Data          | Lease message(s)        |
| Queue    | EXTEND             |       203 | Data          | Extend lease            |
| Queue    | COMPLETE           |       204 | Data          | Mark complete           |
| Queue    | SUBSCRIBE          |       207 | Data          | Subscribe to pattern    |
| Queue    | UNSUBSCRIBE        |       208 | Data          | Unsubscribe pattern     |
| Queue    | NOTIFY             |       209 | Notification  | Availability event      |
| RPC      | SUBSCRIBE_WORKER   |       300 | Data          | Register worker         |
| RPC      | UNSUBSCRIBE_WORKER |       301 | Data          | Unregister worker       |
| RPC      | REQUEST            |       302 | Data          | Send request            |
| RPC      | RESPONSE           |       303 | Data          | Send response           |
| Lease    | ACQUIRE            |       400 | Data          | Acquire lease           |
| Lease    | RENEW              |       401 | Data          | Extend lease            |
| Lease    | RELEASE            |       402 | Data          | Release lease           |
| Lease    | QUERY              |       403 | Data          | Query lease status      |
| Lease    | SUBSCRIBE          |       407 | Data          | Subscribe to changes    |
| Lease    | UNSUBSCRIBE        |       408 | Data          | Unsubscribe             |
| Lease    | NOTIFY             |       409 | Server→Client | Lease change event      |
| Notice   | PUBLISH            |       500 | Data          | Publish message         |
| Notice   | SUBSCRIBE          |       501 | Data          | Subscribe to pattern    |
| Notice   | UNSUBSCRIBE        |       502 | Data          | Unsubscribe             |
| Notice   | UNSUBSCRIBE_ALL    |       503 | Data          | Clear all subscriptions |
| Notice   | NOTIFY             |       504 | Server→Client | Delivery                |
| Stream   | BEGIN              |       600 | Data          | Start session           |
| Stream   | APPEND             |       601 | Data          | Append record           |
| Stream   | COMMIT             |       602 | Data          | Finalize session        |
| Stream   | ROLLBACK           |       603 | Data          | Abort session           |
| Stream   | READ               |       604 | Data          | Read range              |
| Stream   | LAST               |       605 | Data          | Get last record         |
| Stream   | GET_METADATA       |       606 | Data          | Get metadata            |
| Stream   | SUBSCRIBE          |       607 | Data          | Subscribe to changes    |
| Stream   | UNSUBSCRIBE        |       608 | Data          | Unsubscribe             |
| Stream   | NOTIFY             |       609 | Server→Client | Change notification     |
| Schedule | CREATE             |       700 | Data          | Create schedule         |
| Schedule | CANCEL             |       701 | Data          | Cancel schedule         |
| Schedule | LIST               |       702 | Data          | List schedules          |
| Schedule | SUBSCRIBE          |       703 | Data          | Subscribe to fires      |
| Schedule | UNSUBSCRIBE        |       704 | Data          | Unsubscribe             |
| Schedule | NOTIFY             |       705 | Server→Client | Fire notification       |

### MessageType Ranges Are Non-Overlapping

Each domain occupies an exclusive 100-code block. The broker's mux layer routes by numeric range — **no overlap, no disambiguation needed**.
**Clients MUST use the wire codes from the Constants & TLV Registry section.**
