### Notice Domain (Fire-and-Forget Live Fanout)

**Purpose:** Low-latency session-scoped notifications with wildcard pattern matching.

#### Message Types

| Type | Name            | Direction                  |
| ---: | --------------- | -------------------------- |
|  500 | PUBLISH         | Client → Server            |
|  501 | SUBSCRIBE       | Client → Server            |
|  502 | UNSUBSCRIBE     | Client → Server            |
|  503 | UNSUBSCRIBE_ALL | Client → Server            |
|  504 | NOTIFY          | Server → Client (delivery) |

#### PUBLISH Request

```
[u32 BE]  route_len
[bytes]   route (UTF-8, e.g., "notice://realm/area/events")
[u32 BE]  payload_len
[bytes]   payload
```

**No response frame.** PUBLISH is fire-and-forget. The broker accepts the frame and fans out to matching subscribers. The client MUST NOT wait for a response after sending PUBLISH.

**Design Notes:**

- No delivery confirmation (best-effort)
- No error returned for invalid routes or missing subscribers
- Client-side errors (e.g., connection closed, frame too large) are transport-level only
- This matches the Notice domain's non-durable, best-effort semantics

#### SUBSCRIBE Request

```
[u32 BE]  route_pattern_len
[bytes]   route_pattern (supports * and ** wildcards)
Response (status=0):
  [u8]     0                    // status: success
  [u8]     1                    // has_subscription_id flag (always 1 for success)
  [u64 BE] subscription_id
Response (status=1):
  [u8]     1                    // status: error
  [u32 BE] error_len
  [bytes]  error_msg
```

**Wire Format Note:**
The success response uses the "optional u64" encoding pattern: a 1-byte flag followed by the value if present. For SUBSCRIBE success responses, the flag is always `1` (has value), followed by the `subscription_id` as a big-endian u64.

**Idempotency:**
- If client re-subscribes to same pattern, server returns the SAME `subscription_id`
- No duplicate server-side subscription created
- Client is responsible for local multiplexing (tracking multiple handlers per subscription_id)

**Design Notes:**

- Server tracks subscriptions by `(session_id, route_pattern)` tuple
- `session_id` is implicit from connection (not sent by client)
- Duplicate SUBSCRIBE to same pattern is idempotent (returns same `subscription_id`)
- Client handles local multiplexing (multiple handlers per `subscription_id`)

#### UNSUBSCRIBE Request

```
[u64 BE]  subscription_id
Response (status=0):
  [u8]     0
Response (status=1):
  [u8]     1
  [u32 BE] error_len
  [bytes]  error_msg
```

**Design Notes:**

- Client sends `subscription_id` returned from SUBSCRIBE
- Server removes subscription for calling session
- Idempotent: unsubscribing non-existent subscription_id returns success

#### UNSUBSCRIBE_ALL Request

```
(no payload - session_id implicit from connection)
Response (status=0):
  [u8]     0
Response (status=1):
  [u8]     1
  [u32 BE] error_len
  [bytes]  error_msg
```

**Design Notes:**

- Removes all subscriptions for calling session
- Idempotent: safe to call even if no subscriptions exist

#### NOTIFY (Server Delivery)

```
[u64 BE]  subscription_id
[u32 BE]  route_len
[bytes]   route (exact published route, not subscription pattern)
[u32 BE]  payload_len
[bytes]   payload
```

**Design Notes:**

- `subscription_id` tells client which subscription(s) matched
- Client demultiplexes to local handlers registered for that `subscription_id`
- Single NOTIFY delivered per `(session_id, pattern)` tuple (server deduplicates)
- If client has multiple handlers for same pattern, client calls all handlers locally

**Example Flow (Client-Side Routing):**

1. Client subscribes: `"notice://prod/app/*"` → server returns `subscription_id=42`
2. Server receives PUBLISH to: `"notice://prod/app/orders/created"`
3. Server sends NOTIFY: `[subscription_id=42][route="notice://prod/app/orders/created"][payload]`
4. Client looks up `subscription_id=42` → finds `[handler1, handler2]`
5. Client calls:
   - `handler1("notice://prod/app/orders/created", payload)`
   - `handler2("notice://prod/app/orders/created", payload)`

#### Pattern Matching

- `*` matches one segment (e.g., `notice://realm/*/events` matches `notice://realm/orders/events`)
- `**` matches zero or more segments (e.g., `notice://realm/**` matches all routes in realm)
- Exact routes (no wildcards) also supported
- Wildcard realms are valid (for example, `notice://*/orders/events`)
- The scheme must be `notice://`, segments must be non-empty, and wildcards
  must occupy whole segments; invalid patterns return 3002
- A session may retain at most 128 wildcard registrations. Exact registrations
  do not count, and duplicate `(session, original registration string)` requests
  are idempotent and checked before the limit; overflow returns 3003
- Matching is isolated by `RouteFamily`. Overlapping registrations remain
  independent and exact registrations have no precedence

#### Client-Side Multiplexing

**Key Design Principle:** One subscription per `(session, pattern)` on server, multiple handlers per subscription on client.

**What Happens When Multiple Handlers Subscribe to Same Pattern:**

```python
# User creates two handlers for same pattern
sub1 = client.notice_subscribe("notice://prod/app/*", handler1)
sub2 = client.notice_subscribe("notice://prod/app/*", handler2)

# Behind the scenes:
# - First subscribe: Client sends SUBSCRIBE to server, gets subscription_id=42
# - Second subscribe: Client reuses subscription_id=42, adds handler2 to local list
# - Server has ONE subscription (session_id, "notice://prod/app/*") → subscription_id=42
# - Client tracks: subscription_id=42 → [handler1, handler2]

# When message published to "notice://prod/app/orders":
# - Server sends ONE NOTIFY with subscription_id=42
# - Client demuxes locally: calls handler1(route, payload) then handler2(route, payload)
```

**Reference Counting for Unsubscribe:**

```python
sub1.unsubscribe()  # Client removes handler1 from local list
# subscription_id=42 still has handler2 registered
# NO UNSUBSCRIBE sent to server

sub2.unsubscribe()  # Client removes handler2 from local list
# subscription_id=42 now has zero handlers
# NOW client sends UNSUBSCRIBE(subscription_id=42) to server
```

**Benefits:**

- **Wire efficiency**: One NOTIFY per pattern match (not N duplicates)
- **Server simplicity**: No duplicate subscription tracking
- **Familiar pattern**: Same as local listener registration APIs

**Client Implementation Responsibilities:**

- Track map: `subscription_id → [handler1, handler2, ...]`
- Track map: `route_pattern → subscription_id` (for dedup)
- Reference count handlers per subscription
- Only send UNSUBSCRIBE when last handler removed

#### Usage Example

**Recommended User-Facing API (Subscription Object with Multiplexing):**

```python
# Client connects
client = FitzClient.connect_tcp("127.0.0.1:4091", jwt_token)

# Multiple handlers can subscribe to same pattern
sub1 = client.notice_subscribe(
    pattern="notice://prod/app/orders/*",
    handler=lambda route, payload: print(f"Handler1: {route}")
)

sub2 = client.notice_subscribe(
    pattern="notice://prod/app/orders/*",
    handler=lambda route, payload: print(f"Handler2: {route}")
)

# Behind scenes: Both share subscription_id=42 from server
# Client tracks: sub_id=42 → [handler1, handler2]

# When notification arrives, client calls both handlers
# (Server sent ONE NOTIFY, client demuxes locally)

# Unsubscribe individual handler
sub1.unsubscribe()  # Removes handler1, keeps handler2
# No server UNSUBSCRIBE sent yet (handler2 still active)

sub2.unsubscribe()  # Removes handler2 (last handler)
# NOW sends UNSUBSCRIBE(subscription_id=42) to server

# Publisher (stateless)
client.notice_publish(
    route="notice://prod/app/orders/created",
    payload=b'{"order_id": 123}'
)
```

**NoticeSubscription Object Implementation:**

```python
class NoticeClient:
    def __init__(self):
        self._subscriptions = {}  # subscription_id → [handlers]
        self._patterns = {}       # route_pattern → subscription_id
        self._handler_refs = {}   # handler → (subscription_id, pattern)

    def subscribe(self, pattern, handler):
        """Subscribe handler to pattern (may reuse existing subscription)"""
        # Check if already subscribed to this pattern
        if pattern in self._patterns:
            sub_id = self._patterns[pattern]
            self._subscriptions[sub_id].append(handler)
            self._handler_refs[id(handler)] = (sub_id, pattern)
            return NoticeSubscription(self, pattern, sub_id, handler)

        # New pattern - send SUBSCRIBE to server
        sub_id = self._send_subscribe_wire(pattern)  # Wire: [pattern_len][pattern]
        self._patterns[pattern] = sub_id
        self._subscriptions[sub_id] = [handler]
        self._handler_refs[id(handler)] = (sub_id, pattern)
        return NoticeSubscription(self, pattern, sub_id, handler)

    def _handle_notify(self, subscription_id, route, payload):
        """Called when NOTIFY frame arrives from server"""
        # Fan out to all local handlers for this subscription_id
        handlers = self._subscriptions.get(subscription_id, [])
        for handler in handlers:
            handler(route, payload)

    def _unsubscribe_handler(self, subscription_id, pattern, handler):
        """Remove specific handler (may unsubscribe from server)"""
        handlers = self._subscriptions.get(subscription_id, [])
        if handler in handlers:
            handlers.remove(handler)
            del self._handler_refs[id(handler)]

        # If no handlers left, unsubscribe from server
        if not handlers:
            self._send_unsubscribe_wire(subscription_id)  # Wire: [subscription_id]
            del self._subscriptions[subscription_id]
            del self._patterns[pattern]

class NoticeSubscription:
    def __init__(self, client, pattern, subscription_id, handler):
        self._client = client
        self._pattern = pattern              # Stored internally
        self._subscription_id = subscription_id  # From server
        self._handler = handler              # Specific handler

    def unsubscribe(self):
        """Remove this handler (may send UNSUBSCRIBE if last handler)"""
        self._client._unsubscribe_handler(
            self._subscription_id,
            self._pattern,
            self._handler
        )
```

**Wire Protocol (what actually goes on the wire):**

```
CLIENT → SERVER (first subscribe to pattern):
  SUBSCRIBE: [pattern_len]["notice://prod/app/*"]

SERVER → CLIENT:
  Response: [status=0][subscription_id=42]

CLIENT → SERVER (second subscribe to SAME pattern):
  (nothing sent - client reuses subscription_id=42 locally)

SERVER → CLIENT (when message published):
  NOTIFY: [subscription_id=42][route_len]["notice://prod/app/orders/created"][payload_len][payload]

CLIENT PROCESSING:
  - Looks up subscription_id=42 → finds [handler1, handler2]
  - Calls handler1(route, payload)
  - Calls handler2(route, payload)

CLIENT → SERVER (first unsubscribe):
  (nothing sent - handler2 still active locally)

CLIENT → SERVER (second unsubscribe, last handler removed):
  UNSUBSCRIBE: [subscription_id=42]
```

**Key Points:**

- User calls `client.subscribe(pattern, handler)` - simple, handler-focused API
- First subscribe to pattern sends SUBSCRIBE to server, gets `subscription_id`
- Subsequent subscribes to same pattern reuse server subscription, track handler locally
- NOTIFY includes `subscription_id` for client demux (NOT route pattern)
- Unsubscribe only sends to server when last handler removed (reference counting)
- Pattern: Familiar to local listener registration APIs

#### Semantics

- **Fire-and-Forget PUBLISH**: PUBLISH sends a frame with no response. No delivery confirmation, no error response. Transport errors (connection closed) are the only failure mode.
- **Client-Side Multiplexing**: Server tracks one subscription per `(session, pattern)`. Client tracks multiple handlers per `subscription_id`. Server sends one NOTIFY per pattern match; client demuxes to all local handlers.
- **Idempotent SUBSCRIBE**: Duplicate SUBSCRIBE to same pattern returns same `subscription_id` (no duplicate server subscription created)
- **Delivery**: Best-effort; under backpressure, notifications may be dropped
- **Ordering**: Delivered in publish order per subscription
- **Fanout**: Single publish reaches all matching subscriptions
- **Session-Scoped**: Subscriptions tied to connection; lost on disconnect
- **Acknowledgements & Retries**: `NOTIFY` frames are never acknowledged by clients and are never retried by the broker. Clients MUST NOT send acknowledgements for `NOTIFY` frames and MUST NOT expect guaranteed replay.
- **Toleration:** Clients **MUST** tolerate missed notifications across reconnects and transient backpressure periods.
- **Usage Guidance:** `NOTICE` is a **best-effort, non-durable** mechanism. **Clients MUST NOT use Notices for workflows that require acknowledgement, durability, or guaranteed delivery. Use Queue for work delivery that needs reservation, redelivery, and configurable durability. Use RPC only for low-latency request/response when callers and workers can tolerate disconnect or broker-restart loss and retry explicitly at the application layer.**

##### Pattern Matching & Precedence

**Multiple pattern matches:**

- Single PUBLISH may match multiple subscriptions
- Each matching subscription receives NOTIFY
- No precedence or filtering (all matches deliver)

**Example:**

- Client subscribes to: `notice://prod/app/*`
- Client subscribes to: `notice://prod/**`
- Publish to: `notice://prod/app/orders`
- Result: Client receives 2 NOTIFY frames (one per subscription)

**Deduplication:**

- Server does NOT deduplicate across different subscriptions
- If client subscribes twice to same pattern: both reference same `subscription_id` (client-side multiplexing)
- Client SHOULD use reference counting to avoid premature unsubscribe

#### Error Codes (3xxx range)

- 3001 = ERR_INVALID_ROUTE
- 3002 = ERR_INVALID_PATTERN
- 3003 = ERR_SUBSCRIPTION_LIMIT
- 3004 = ERR_TRANSPORT_CLOSED
- 3005 = ERR_BACKEND_ERROR
- 3009 = ERR_UNAUTHORIZED

#### Acceptance Tests

- subscribe to pattern, receive matching publications
- **client-side multiplexing**: two handlers subscribe to same pattern, both receive NOTIFY (client tracks, server sends one)
- **reference counting**: unsubscribe first handler doesn't send UNSUBSCRIBE to server; unsubscribe second handler (last) sends UNSUBSCRIBE
- **idempotent subscribe**: second SUBSCRIBE to same pattern returns same `subscription_id`
- publish with no subscribers returns ok
- unsubscribe stops delivery
- wildcard patterns match correctly (`*` single segment, `**` multi-segment)
- exact routes work without wildcards

### Stream Domain (Durable Append-Only Logs)

**Purpose:** Durable append-only records with optimistic concurrency at BEGIN and commit-time sequencing for resource, area, and realm order. Clients MAY attach an optional immutable discriminator to each append for server-side filtered replay, and replay responses MAY include synthetic delivery markers for skipped committed offsets.

#### Workflow Profiles

Stream supports two client workflow profiles through the same wire operations.

**Event-sourced aggregate streams:** Clients MAY model one stream resource as one aggregate history, using `resource_offset` as the aggregate revision. A command that produces multiple domain events should `BEGIN`, `APPEND` each event with consecutive `expected_offset` values, and `COMMIT` the batch atomically. A stale `expected_offset` is a concurrency conflict: clients should `READ` the exact resource, optionally inspect `LAST` or `GET_METADATA`, rebuild their aggregate state, and retry with the current next offset. Event metadata remains opaque client data; the optional discriminator is a replay sidecar for server-side filtering, not a server-owned event schema.

**General append/replay streams:** Clients MAY use resources as durable logs or feeds and replay exact resource, area wildcard, or realm wildcard histories from client-owned offsets. `READ` returns cursor data for the response, but Fitz does not own consumer checkpoints. Clients that need projection progress, catch-up state, or subscription recovery must persist that state themselves. Live `SUBSCRIBE` delivers notifications only for active subscriptions and is not a replay cursor.

Stream is not a queue, a broker-managed consumer group system, an exactly-once command processor, or a duplicate-suppression layer. Clients that need idempotency should include their own command/event identifiers in body or metadata and enforce that policy in their application state.

#### Message Types

| Type | Name         | Direction                  |
| ---: | ------------ | -------------------------- |
|  600 | BEGIN        | Client → Server            |
|  601 | APPEND       | Client → Server            |
|  602 | COMMIT       | Client → Server            |
|  603 | ROLLBACK     | Client → Server            |
|  604 | READ         | Client → Server            |
|  605 | LAST         | Client → Server            |
|  606 | GET_METADATA | Client → Server            |
|  607 | SUBSCRIBE    | Client → Server            |
|  608 | UNSUBSCRIBE  | Client → Server            |
|  609 | NOTIFY       | Server → Client (delivery) |

#### BEGIN Request

```
[u32 BE]  route_len
[bytes]   route
[u8]      has_ingest_metadata (0 or 1)
[u32 BE]  ingest_metadata_len (if has_ingest_metadata=1)
[bytes]   ingest_metadata
Response (status=0):
  [u8]     0
  [u64 BE] session_id
  [u32 BE] data_len
  [bytes]  data (broker-defined opaque bytes; clients MUST treat as opaque and MAY ignore)
Response (status=1):
  [u8]     1
  [u32 BE] error_len
  [bytes]  error_msg
```

BEGIN creates a session only. Optimistic concurrency is enforced when appending records.

#### APPEND Request

```
[u64 BE]  session_id
[u64 BE]  expected_offset
[u32 BE]  body_len
[bytes]   body
[u8]      has_metadata
[u32 BE]  metadata_len (if has_metadata=1)
[bytes]   metadata
[u8]      has_discriminator
[u32 BE]  discriminator_len (if has_discriminator=1)
[bytes]   discriminator
Response (status=0):
  [u8]     0
  [u32 BE] data_len
  [bytes]  data
Response (status=1):
  [u8]     1
  [u32 BE] error_len
  [bytes]  error_msg
```

**expected_offset (OCC):** Clients MUST send `expected_offset` on every APPEND. It is the client's view of the stream's next write offset for that route (0 for a new stream). Servers MUST enforce it: if `expected_offset` does not match the server's next offset for that route, the server MUST reject the append with status=1 and an error message (e.g. containing "conflict"). This provides optimistic concurrency control; clients that receive a conflict should re-read the stream and retry with the correct offset.

**Optional discriminator:** Clients MAY include an immutable discriminator string on APPEND. The broker stores it as a replay sidecar and uses it only for filtered reads. Clients that do not need filtered replay SHOULD omit it.

**Design Note:** The `data` field in Stream responses carries broker-defined metadata (e.g., current watermark, stream info). Clients MUST parse past it (read `data_len` bytes) but SHOULD NOT interpret its contents unless broker documentation specifies a schema.

**Design Note:** `session_id` is `u64` (not string), returned from BEGIN response.

#### COMMIT Request

```
[u64 BE]  session_id
[u8]      mode (0=Buffered, 1=Sync)
Response (status=0):
  [u8]     0
  [u32 BE] data_len
  [bytes]  data
Response (status=1):
  [u8]     1
  [u32 BE] error_len
  [bytes]  error_msg
```

**Commit Modes:**

- **Sync (mode=1)**: COMMIT uses the broker's sync write policy before success is returned. Higher latency, stronger crash durability.
- **Buffered (mode=0)**: COMMIT uses the broker's buffered write policy. Lower latency, best-effort durability until the buffered write is persisted.

#### ROLLBACK Request

```
[u64 BE]  session_id
Response (status=0):
  [u8]     0
Response (status=1):
  [u8]     1
  [u32 BE] error_len
  [bytes]  error_msg
```

#### READ Request

```
[u32 BE]  route_len
[bytes]   route
[u64 BE]  from_offset
[u64 BE]  limit
[u8]      has_max_bytes
[u64 BE]  max_bytes (if present)
[u8]      has_filter
[u32 BE]  filter_len (if has_filter=1)
[bytes]   filter (StreamFilterSet codec)
[u8]      has_cursor_fingerprint (optional trailing field)
[u64 BE]  cursor_fingerprint (if has_cursor_fingerprint=1)
[u8]      has_captured_watermark (optional trailing field)
[u64 BE]  captured_watermark (if has_captured_watermark=1)
Response (status=0):
  [u8]     0
  [u8]     has_session_id = 0
  [u32 BE] data_len
  [bytes]  read_page_data
Response (status=1):
  [u8]     1
  [u32 BE] error_code
  [u32 BE] error_len
  [bytes]  error_msg
```

**Optional filter:** Clients MAY include a `StreamFilterSet` to request server-side replay filtering. The filter is carried as an optional bytes field after `max_bytes`: a 1-byte presence flag, a u32 BE length when present, and the raw filter payload. The filter payload uses Fitz's custom fixed-field stream filter codec and is conjunctive: all clauses must match the record discriminator.

**StreamFilterSet codec:**

```
[u8]      marker_0 = 0
[u8]      marker_1 = 0xF1
[u32 BE]  clause_count
repeat clause_count times:
  [u8]      clause_tag
  clause payload
```

Clause payloads:

- `0` = Equals: `[u32 BE string_len][bytes utf8]`
- `1` = NotEquals: `[u32 BE string_len][bytes utf8]`
- `2` = StartsWith: `[u32 BE string_len][bytes utf8]`
- `3` = AnyOf: `[u32 BE value_count][repeat value_count times: u32 BE string_len + bytes utf8]`

Missing discriminators are treated as empty strings for matching. Unsupported marker/version and malformed payloads return typed stream errors instead of closing the connection.

**Read page:** On success, the `data` payload is a count-prefixed sequence of
route-prefixed tagged delivery items followed by the read cursor:

```
[u32 BE] item_count
repeat item_count times:
  [u32 BE] concrete_route_len
  [bytes]  concrete_route
  [u8]     item_tag
  if item_tag = 0 (event):
    [u64 BE] resource_offset
    [u8]     has_area_offset
    [u64 BE] area_offset (if has_area_offset=1)
    [u8]     has_realm_offset
    [u64 BE] realm_offset (if has_realm_offset=1)
    [u32 BE] body_len
    [bytes]  body
    [u8]     has_metadata
    [u32 BE] metadata_len (if has_metadata=1)
    [bytes]  metadata (if has_metadata=1)
    [u64 BE] created_at
  if item_tag = 1 (filtered offset):
    [u64 BE] offset
    [u8]     reason
  if item_tag = 2 (filtered range):
    [u64 BE] from_offset
    [u64 BE] to_offset
    [u8]     reason
[u64 BE] last_resource_offset
[u8]     has_last_area_offset
[u64 BE] last_area_offset (if has_last_area_offset=1)
[u8]     has_last_realm_offset
[u64 BE] last_realm_offset (if has_last_realm_offset=1)
[u8]     has_more (0=false, 1=true)
```

That is the compatibility layout for resource-, area-, and realm-scoped READ.
For a global-scope selector, the response uses the extended layout:

- event records add `[u8 has_global_offset][u64 BE global_offset]` immediately
  after the optional realm offset;
- the cursor adds `[u8 has_last_global_offset][u64 BE last_global_offset]`
  immediately before `has_more`; and
- the cursor adds optional `cursor_fingerprint` and `captured_watermark` u64
  fields immediately after `has_more`.

The global fields are always decoded according to selector scope, not by
probing the payload. LAST and resource-, area-, and realm-scoped READ never
contain them. Existing READ requests may omit the two trailing continuation
fields. A client continuing a global read echoes both values from the prior
page.

The concrete route is always present, including for exact resource reads. Tag
`0` is an event record, tag `1` is a filtered offset, and tag `2` is a filtered
range. Filter reason `0` means unspecified, `1` means server filter, `2` means
permission, and `3` means projection. Optional flags are exactly `0` or `1`.
Clients MAY expose the raw page or flatten event-only results, but every public
item and record MUST retain its concrete route and cursor progress. All fields
above are inside the single length-prefixed `read_page_data` value; one READ
request produces one response page.

READ and SUBSCRIBE accept the same finite selector matrix:
`stream://{realm}/{area}/{resource}`, `stream://{realm}/{area}/*`,
`stream://{realm}/*/{resource}`, `stream://{realm}/*/*` (or
`stream://{realm}/**`), `stream://*/{area}/{resource}`,
`stream://*/{area}/*`, `stream://*/*/{resource}`, and `stream://*/*/*` (or
`stream://**`). SUBSCRIBE NOTIFY payloads carry the matched concrete route.

**Cursor:** `ReadCursor` is response metadata that advances with every committed offset the broker considers during replay, including filtered markers. It is not a durable broker-side resume token.

#### Client replay and follow contract

The versioned machine-readable client contract is
[`stream-read-conformance.json`](./stream-read-conformance.json). Clients MUST
validate selector classification and replay progress against that fixture.

A high-level read API requires an explicit starting offset and mode. `replay`
issues READ until the first `has_more=false` response, yields that terminal
batch, and completes. `follow` establishes SUBSCRIBE before its initial READ,
drains through `has_more=false`, then waits for a commit or reconnect wake.
Subscribing first prevents a lost commit between catch-up and subscription.

Every successful page is observable as one batch, including filtered-only
pages and empty pages whose cursor advances. A batch contains all tagged items,
the event-only record view, the requested offset, the next durable client
offset derived from the selector's cursor axis, and `caughtUp`. A client MUST
NOT fabricate an offset when the applicable cursor axis is absent; it retains
the requested offset.

Continuation fingerprint and captured watermark fields are protocol-private.
Clients carry them automatically between global READ pages and do not expose
them as application resume tokens. Two consecutive `has_more=true` responses
with the same applicable cursor position are a non-retryable stalled read even
if private continuation values rotate. This permits one empty continuation page
while bounding a malformed broker loop.

Cancellation, iterator return, reconnect failure, decode failure, and other
terminal failures release a live subscription. Explicit unsubscribe operations
surface broker errors; automatic iterator disposal is best effort.

#### LAST Request

```
[u32 BE]  route_len
[bytes]   route
Response: status byte + data
```

#### GET_METADATA Request

```
[u32 BE]  route_len
[bytes]   route
Response: status byte + data
```

#### Usage Example

**Recommended User-Facing API (StreamSession Object):**

```python
# Client connects
client = FitzClient.connect_tcp("127.0.0.1:4091", jwt_token)

# BEGIN returns a StreamSession object
session = client.stream_begin(
    route="stream://prod/app/events"
)

# Session methods are slim - no route or session_id needed in API
session.append(0, b"event_data_1", discriminator="proj.alpha")
session.append(1, b"event_data_2")
session.commit(mode=CommitMode.Sync)

# Or rollback
session.rollback()

# Read from stream (stateless - no session needed)
records = client.stream_read(
    route="stream://prod/app/events",
    from_offset=100,
    limit=10,
    filter=StreamFilterSet(
        clauses=[StreamFilterClause.Equals("proj.alpha")]
    )
)
```

**StreamSession Object Implementation:**

```python
class StreamSession:
    def __init__(self, client, route, session_id):
        self._client = client
        self._route = route         # Stored internally
        self._session_id = session_id  # Stored internally

    def append(self, expected_offset, body, metadata=None, discriminator=None):
        """Slim API - route/session_id hidden from user"""
        # Wire protocol: packs session_id + expected_offset + body + optional metadata + optional discriminator
        return self._client._send_stream_append(
            self._session_id,
            expected_offset,
            body,
            metadata,
            discriminator,
        )

    def commit(self, mode=CommitMode.Sync):
        """Commit session"""
        # Wire: [session_id][mode]
        return self._client._send_stream_commit(
            self._session_id,
            mode
        )

    def rollback(self):
        """Rollback session"""
        # Wire: [session_id]
        return self._client._send_stream_rollback(
            self._session_id
        )
```

**Wire Protocol (what actually happens):**

- `BEGIN`: `[route_len][route][...] → returns session_id`
- `APPEND`: `[session_id][expected_offset][body_len][body][optional metadata][optional discriminator]`
- `COMMIT`: `[session_id][mode]`
- `ROLLBACK`: `[session_id]`
- `READ`: `[route_len][route][from_offset][limit][optional max_bytes][optional filter]` (stateless)

**Key Points:**

- User calls `session.append(expected_offset, data)` - simple, focused on data and sequencing
- BEGIN binds `session_id` to exactly one stream resource on the broker
- APPEND/COMMIT/ROLLBACK are session-scoped, not stateless route-scoped operations
- Connection loss aborts the append session; clients must begin a new session after reconnect

#### Semantics

- **Atomicity**: Appends are atomic; partial writes never visible
- **Ordering**: Records strictly ordered by offset within resource
- **Commit-Time Global Order**: Area and realm offsets are assigned durably at COMMIT time and remain monotonic across restart
- **Watermarks**: Reads stop at the current committed watermark; reading past it returns an empty success
- **Optimistic Concurrency**: `expected_offset` on APPEND prevents lost updates
- **Filtered Replay**: `StreamFilterSet` clauses are conjunctive and operate on the optional append discriminator sidecar
- **Durability**: All committed data survives broker restart
- **Isolation**: Only one active append session exists per resource at a time; subscriptions are separate live session-scoped state
- **Resume Model**: Clients track resume offsets locally; `ReadCursor` is response metadata, not a durable broker cursor

#### Error Codes (2xxx)

- 2001 = ERR_CONCURRENCY_CONFLICT (expected_offset mismatch)
- 2002 = ERR_SESSION_ALREADY_ACTIVE
- 2003 = ERR_SESSION_NOT_FOUND
- 2004 = ERR_INVALID_READ_BOUND
- 2005 = ERR_RESOURCE_NOT_FOUND
- 2006 = ERR_STREAM_FILTER_UNSUPPORTED_VERSION (read filter marker/version is not supported by this broker)
- 2007 = ERR_STREAM_FILTER_INVALID_PAYLOAD (read filter payload malformed)
- 2009 = ERR_UNAUTHORIZED
- 2010 = ERR_INVALID_SUBSCRIPTION_PATTERN
- 2011 = ERR_SUBSCRIPTION_LIMIT
- 2012 = ERR_BACKEND_ERROR
- 2013 = ERR_READ_RESPONSE_TOO_LARGE (a single record's wire-encoded size alone exceeds the maximum broker response frame size and can never be returned by any READ call at that offset; this is distinct from `max_bytes` pagination, which stops a page early instead of failing)

#### Acceptance Tests

- begin/append/commit cycle
- read returns records in offset order
- read beyond watermark returns an empty success
- append with mismatched expected_offset fails
- rollback discards uncommitted appends
- disconnect cleanup aborts abandoned append sessions
- only one active append session per resource is allowed at a time

#### Stream SUBSCRIBE (607)

Subscribe to stream change notifications for a route pattern.

```
[u32 BE]  route_pattern_len
[bytes]   route_pattern (supports * and ** wildcards)
Response (status=0):
  [u8]     0                    // status: success
  [u8]     1                    // has_subscription_id flag (always 1 for success)
  [u64 BE] subscription_id
Response (status=1):
  [u8]     1                    // status: error
  [u32 BE] error_len
  [bytes]  error_msg
```

**Wire Format Note:**
The success response uses the "optional u64" encoding pattern: a 1-byte flag followed by the value if present. For SUBSCRIBE success responses, the flag is always `1` (has value), followed by the `subscription_id` as a big-endian u64.

**Pattern Examples:**
- `stream://realm/area/resource` — specific resource changes
- `stream://realm/area/*` — area-level (all resources in area)
- `stream://realm/**` — realm-level (all areas and resources in realm)
- `stream://*/area/resource` — the same concrete area and resource in any realm

**Semantics:**
- Subscriptions are **session-scoped** — all subscriptions are lost on disconnect
- Idempotent: re-subscribing to the same pattern returns the same `subscription_id`
- Server tracks subscriptions by `(session_id, route_pattern)` tuple
- `*` matches exactly one segment and `**` matches zero or more complete segments
- The scheme must be `stream://`, segments must be non-empty, wildcards must be
  whole segments, and the pattern must be capable of matching a concrete
  three-segment Stream route; invalid SUBSCRIBE or UNSUBSCRIBE input returns 2010
- A session may retain at most 128 wildcard registrations. Exact registrations
  do not count, and a duplicate is checked before the limit; overflow returns 2011
- Matching is isolated by `RouteFamily`. Overlapping registrations remain
  independent and exact registrations have no precedence

#### Stream UNSUBSCRIBE (608)

Unsubscribe from stream change notifications.

```
[u32 BE]  route_pattern_len
[bytes]   route_pattern
Response (status=0):
  [u8]     0
Response (status=1):
  [u8]     1
  [u32 BE] error_len
  [bytes]  error_msg
```

**Design Notes:**
- Client sends the original pattern string used in SUBSCRIBE
- Idempotent: unsubscribing a non-existent pattern returns success

#### Stream NOTIFY (609) — Server to Client

Server pushes a stream change notification to a subscriber.

```
[u64 BE]  subscription_id
[u32 BE]  route_len
[bytes]   route (exact resource route, not subscription pattern)
[u32 BE]  payload_len
[bytes]   payload (JSON)
```

**Payload Schemas:**

Commit notification (one or more records committed):
```json
{
  "event": "committed",
  "first_resource_offset": <u64>,
  "last_resource_offset": <u64>,
  "first_area_offset": <u64>,
  "last_area_offset": <u64>,
  "first_realm_offset": <u64>,
  "last_realm_offset": <u64>,
  "batch_size": <u32>
}
```

Watermark advance notification:
```json
{
  "event": "watermark_advanced",
  "previous": <u64>,
  "watermark": <u64>
}
```

**Delivery Semantics:**
- **Best-effort**: notifications may be dropped under backpressure
- **Debounced**: commit notifications are batched per 25ms window — multiple rapid commits to the same resource produce a single notification covering the full offset range
- **Session-scoped**: all subscriptions lost on disconnect; clients must re-subscribe after reconnecting
- `subscription_id` tells the client which subscription matched; client demultiplexes to local handlers
