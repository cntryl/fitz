## Constants & TLV Registry

### MessageType Ranges

**Control (0–99):**
| Value | Name |
|---:|---|
| 1 | CONNECT |
**KV Domain (100–111):**
| Value | Name |
|---:|---|
| 100 | BEGIN |
| 101 | COMMIT |
| 102 | ROLLBACK |
| 103 | GET |
| 104 | PUT |
| 105 | INSERT |
| 106 | DELETE |
| 107 | DELETE_RANGE |
| 108 | SCAN |
| 109 | SUBSCRIBE |
| 110 | UNSUBSCRIBE |
| 111 | NOTIFY |
**Queue Domain (200–209):**
| Value | Name |
|---:|---|
| 200 | ENQUEUE |
| 201 | ENQUEUE_BATCH (reserved; servers may reject with unknown message type until defined) |
| 202 | RESERVE |
| 203 | EXTEND |
| 204 | COMPLETE |
| 207 | SUBSCRIBE |
| 208 | UNSUBSCRIBE |
| 209 | NOTIFY |
**RPC Domain (300–303):**
| Value | Name |
|---:|---|
| 300 | SUBSCRIBE_WORKER |
| 301 | UNSUBSCRIBE_WORKER |
| 302 | REQUEST |
| 303 | RESPONSE |
**Lease Domain (400–403):**
| Value | Name |
|---:|---|
| 400 | ACQUIRE |
| 401 | RENEW |
| 402 | RELEASE |
| 403 | QUERY |
**Notice Domain (500–504):**
| Value | Name |
|---:|---|
| 500 | PUBLISH |
| 501 | SUBSCRIBE |
| 502 | UNSUBSCRIBE |
| 503 | UNSUBSCRIBE_ALL |
| 504 | NOTIFY |
**Stream Domain (600–609):**
| Value | Name |
|---:|---|
| 600 | BEGIN |
| 601 | APPEND |
| 602 | COMMIT |
| 603 | ROLLBACK |
| 604 | READ |
| 605 | LAST |
| 606 | GET_METADATA |
| 607 | SUBSCRIBE |
| 608 | UNSUBSCRIBE |
| 609 | NOTIFY |
**Schedule Domain (700–705):**
| Value | Name |
|---:|---|
| 700 | CREATE |
| 701 | CANCEL |
| 702 | LIST |
| 703 | SUBSCRIBE |
| 704 | UNSUBSCRIBE |
| 705 | NOTIFY |

### MessageType Routing

Each domain occupies an exclusive 100-code block. The broker's mux layer routes by numeric range — **no overlap, no disambiguation needed**.
**Future compatibility:** If a domain exhausts its range, extend to a new 100-block (e.g., 1100–1199 for KV expansion)

### Error Code Allocation (Authoritative)

Error codes are allocated by domain in 100-block ranges:
| Range | Domain | Capacity | Notes |
| --------- | -------- | --------- | ----------------------------------- |
| 1000–1099 | KV | 100 codes | Transactions, isolation, durability |
| 2000–2099 | Stream | 100 codes | Concurrency, watermarks, ordering |
| 3000–3099 | Notice | 100 codes | Routing, patterns, delivery |
| 4000–4099 | Queue | 100 codes | Leasing, visibility, delivery |
| 5000–5099 | Lease | 100 codes | Mutual exclusion, fencing, TTL |
| 6000–6099 | RPC | 100 codes | Routing, backpressure, correlation |
| 7000–7099 | Schedule | 100 codes | Scheduling, persistence, execution |
**Expansion Strategy:**
If domain exhausts range (>99 error codes allocated):

- First expansion block: {base}100–{base}199 (e.g., 1100–1199 for KV)
- Second expansion: {base}200–{base}299 (e.g., 1200–1299 for KV)
- Continue as needed
  **Cross-Domain Error Codes:**
  These error codes are standardized across ALL domains:
- `*001` = ERR_UNAUTHORIZED (permission denied, see Permissions section)
- `*002` = ERR_INVALID_SCOPE (scope mismatch)
- `*003` = ERR_REALM_MISMATCH (requested route realm is not authorized by the compiled permission set)
  All other error codes are domain-specific and MUST NOT be reused across domains.

### Channel IDs (Broker-Internal Reference)

Clients do NOT encode these; listed for reference:
| ChannelId | Value | Purpose |
| --------- | ----: | ---------------------- |
| Control | 0 | Control/handshake |
| Pub | 1 | Publishing/notice |
| Sub | 2 | Subscriptions/delivery |
| Rpc | 3 | RPC request/response |
| Lease | 4 | Lease domain |

### Type Encoding Rules

- `type 0x00..0xFE`: single byte on wire
- `type 0xFF`: escape marker — followed by `u16 BE` for actual type value (for types > 0xFE)

**Decoder pseudocode:**

```python
def read_message_type(stream):
    """Read MessageType from wire. Returns u16."""
    first_byte = stream.read_u8()
    if first_byte == 0xFF:
        # Escape: next 2 bytes are the actual type
        return stream.read_u16_be()
    else:
        # Single byte type (0x00–0xFE)
        return first_byte
```

**Encoder pseudocode:**

```python
def write_message_type(stream, msg_type):
    """Write MessageType to wire."""
    if msg_type <= 0xFE:
        stream.write_u8(msg_type)
    else:
        stream.write_u8(0xFF)
        stream.write_u16_be(msg_type)
```

**Current implications:**

- CONNECT (type=1): encodes as 1 byte `[0x01]`
- KV BEGIN (type=100): encodes as 1 byte `[0x64]`
- Notice PUBLISH (type=500): encodes as 3 bytes `[0xFF][0x01][0xF4]`

**IMPORTANT:** The wire examples elsewhere in this document show all MessageTypes as 2-byte `[u16 BE]` for readability. Conformant implementations MUST use the variable-length encoding described above.

## Acceptance Criteria

Client implementations MUST pass the following test suite against a reference broker:

### Transport-Level Tests

1. **WebSocket connect** - Establish WebSocket, send CONNECT, verify session opens
2. **TCP connect** - Establish TCP, send length-prefixed CONNECT, verify session opens
3. **Frame size enforcement** - Send frame > `max_frame_size`, broker closes connection
4. **Reconnect** - Drop connection, reconnect, re-send CONNECT, verify session re-established

### Domain-Level Tests (per domain)

**Notice:**

- Subscribe to pattern, receive matching publications
- Multiple subscriptions on same pattern both receive
- Publish with no subscribers returns ok
- Unsubscribe stops delivery
- Wildcard patterns match correctly
  **Stream:**
- Begin/append/commit cycle succeeds
- Read returns records in offset order
- Read beyond watermark returns an empty success
- Append with mismatched expected_offset fails
- Rollback discards uncommitted appends
  **Queue:**
- Enqueue/reserve/complete cycle succeeds
- Lease expiry returns message to ready queue
- Extend lease delays expiry
- Complete with wrong token fails
- Batch reserve returns up to specified count
  **RPC:**
- Single request/response cycle succeeds
- Streaming response reassembled in order
- Request timeout returns error
- Multiple workers on same route handle requests
  **KV:**
- Begin/put/commit cycle succeeds
- Begin/get on non-existent key handled correctly
- ReadOnly mode rejects write operations
- Two transactions on same resource conflict
- Scan returns lexicographically ordered pairs
  **Lease:**
- Acquire succeeds when free, fails when held
- Renew with valid token extends TTL
- Release with valid token releases lease
- Expired lease acquirable by new owner
  **Schedule:**
- Create schedule and verify execution
- Cancel prevents future runs
- List returns created schedules

### Interoperability Tests

Client implementations MUST pass these cross-cutting tests:
**Multi-Realm Isolation:**

- Create two clients with permissions scoped to different route realms
- One client publishes to realm A, other subscribes in realm B
- Verify no cross-realm delivery (subscriber receives nothing)
  **Permission Enforcement:**
- Client with `kv:read` scope sends PUT request
- Broker returns ERR_UNAUTHORIZED (1001 domain error)
- Verify client surfaces error correctly to caller
  **Multiplexing Across Domains (Channel-Based):**
- Client sends KV PUT (KV channel)
- While KV is in-flight, client sends Notice PUBLISH (Notice channel)
- Both proceed concurrently (independent channels)
- Verify both responses received correctly
  **Reconnect State:**
- Client subscribes to pattern, closes connection
- Reconnects with same JWT, old subscription is lost
- Verify client must re-subscribe explicitly (no auto-recovery)
  **Fanout Scale:**
- Single PUBLISH to 1000 SUBSCRIBE clients
- All clients receive NOTIFY within 100ms (broker-dependent)
- Verify no message loss
  **Within-Domain Pipelining (NOT Supported):**
- Client sends KV REQUEST 1 without waiting for response
- Client sends KV REQUEST 2 on same channel while REQUEST 1 pending
- Broker MAY close connection or serialize; behavior undefined
- Clients MUST NOT pipeline multiple requests within a single domain/channel
  **RPC Multiplexing (Exception: Correlation ID Based):**
- Client sends RPC REQUEST with correlation_id_1
- Client sends another RPC REQUEST with correlation_id_2 (both in-flight)
- Broker matches responses by correlation_id
- Clients MAY truly multiplex RPC requests via correlation IDs

## Known Broker-Specific Behaviors

### Implementation Notes

These items are **not standardized** and may require broker-specific implementation notes.

#### Session IDs and State Tracking

**When broker tracks session state:**

- Notice subscriptions: Broker maintains per-session subscription list
- Stream sessions: Broker maintains per-session stream offset and metadata
- RPC workers: Broker maintains per-session worker registration
  **Session ID lifetime:**
- Issued on CONNECT, unique per connection
- Lost on disconnect (previous session ID becomes invalid)
- NOT returned to client in standard response (internal only, except where specified per domain)

#### Wire Protocol Philosophy (Explicit Routing)

**Fitz operations are always explicitly scoped, but not always by repeating the route:**
- KV: Every operation includes `[tx_id][route_len][route]` (not just BEGIN)
- Stream: `BEGIN` carries the route; `APPEND` / `COMMIT` / `ROLLBACK` carry the `session_id` bound by BEGIN
- Queue: Every operation includes `[route_len][route]`
- Notice: PUBLISH/SUBSCRIBE include full route/pattern
- RPC: REQUEST includes full route
- Lease: ACQUIRE/RENEW/RELEASE include full route
- Schedule: CREATE uses `[route][cron][mode][payload]`, CREATE_BATCH repeats that
  entry shape, CANCEL includes the full route, and LIST uses optional
  offset/limit pagination while returning mode in every entry. Mode is required:
  `0` is broadcast and `1` is single.

**Why this design:**
- Explicit scoping: Each message is addressable either by route or by a previously issued opaque handle
- No hidden route defaults: Clients and brokers do not rely on per-connection realm/area/resource state
- Domain state still exists where required: KV and Stream keep live transaction/session state, and reconnect requires re-establishing that state
- Debuggable: Every message can be inspected without hidden addressing context

**Client convenience wrappers:**
- Client implementations MAY provide ergonomic wrapper objects (Transaction, Session, Subscription)
- These wrappers store route/session_id internally to hide repetition from users
- Example: `tx.put(key, value)` internally sends `[tx_id][route][key][value]` on wire
- But the wire protocol always remains fully explicit

**Session-scoped behavior:**
- KV transactions and Stream append sessions: breaking connection aborts live handles
- Notice, Queue, Lease, Stream, and Schedule subscriptions: breaking connection drops live subscriptions
- RPC workers and pending RPC calls: breaking connection unregisters workers and interrupts pending calls
- Queue item handles: breaking connection invalidates live inflight tokens from the client's perspective; reserve again
- Leases: in-memory only; lost on disconnect or broker restart unless reacquired

#### Serialization Formats (Domain-Specific)

- **Stream data:** Binary-safe; format broker-defined (client treats as opaque payload)
- **RPC response:** Binary-safe; serialization app-dependent
- **Lease tokens:** Opaque binary; do not parse or modify

#### Version Negotiation (Future)

No version negotiation in current protocol. If new verbs are added:

1. New verb codes use next available in range (e.g., 109 for KV)
2. Old clients reject unknown verbs with ERR_UNKNOWN_VERB (domain error)
3. Clients MUST gracefully handle unknown verbs (close connection or error)
   Recommended: Brokers should document supported verbs and wire codes in deployment docs.

### Broker-Specific Behaviors Summary

1. **Session ID exposure**: Notice/Stream payloads include session IDs, but no standard server-to-client notification mechanism yet
2. **KV routing**: KV payloads include route on every operation alongside `tx_id`; the broker still treats `tx_id` as a live session-scoped handle that becomes invalid after disconnect or restart
3. **Stream response data**: Response data is opaque; serialization format is broker-defined
4. **Verb code extensions**: New verbs added after current broker release use new wire codes in existing ranges
   Clients SHOULD consult broker documentation for domain-specific behavior.

## References

- Fitz repository: https://github.com/cntryl/fitz
- Domain specifications: See [Canonical operation reference](operations.md#domain-operations-reference-canonical-standard)
- Codec implementations: See Fitz `src/protocol/` directory
- Integration tests: See Fitz `tests/` directory
