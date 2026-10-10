# KV Domain Spec

Split from the former `queue-rpc-kv.md`; the Queue and RPC domains are in [queue-rpc.md](queue-rpc.md).

### KV Domain (Durable Key-Value)

**Purpose:** Transaction-based CRUD and range operations with isolation.
**IMPORTANT:** All KV operations occur within transactions (Begin/Commit/Rollback).

#### Message Types

| Type | Name         |
| ---: | ------------ |
|  100 | BEGIN        |
|  101 | COMMIT       |
|  102 | ROLLBACK     |
|  103 | GET          |
|  104 | PUT          |
|  105 | INSERT       |
|  106 | DELETE       |
|  107 | DELETE_RANGE |
|  108 | SCAN         |
|  109 | SUBSCRIBE    |
|  110 | UNSUBSCRIBE  |
|  111 | NOTIFY       |

#### BEGIN Request

```
[u32 BE]  route_len
[bytes]   route (UTF-8, e.g., "kv://realm/area/resource")
[u8]      mode (0=ReadOnly, 1=ReadWrite)
[u8]      durability (0=Buffered, 1=Sync; other values invalid)
Response (success):
  [u8]     0 (status: success)
  [u64 BE] tx_id
Response (error):
  [u8]     1 (status: error)
  [u32 BE] error_len
  [bytes]  error_msg
```

The broker authorizes BEGIN by its mode, so it decodes the whole payload before
dispatch. A BEGIN that cannot be decoded (truncated fields, an invalid route,
mode, or durability, or bytes after `durability`) closes the connection with an
`authorization parse failed` reason instead of receiving an error response.
Clients MUST NOT retry the same malformed frame.

#### PUT Request

```
[u64 BE]  tx_id
[u32 BE]  route_len
[bytes]   route
[u32 BE]  key_len
[bytes]   key
[u32 BE]  value_len
[bytes]   value
Response (success):
  [u8]     0 (status: success)
Response (error):
  [u8]     1 (status: error)
  [u32 BE] error_len
  [bytes]  error_msg
```

#### GET Request

```
[u64 BE]  tx_id
[u32 BE]  route_len
[bytes]   route
[u32 BE]  key_len
[bytes]   key
Response (success):
  [u8]     0 (status: success)
  [u8]     found (0=not_found, 1=found)
  [u32 BE] value_len (present only if found=1)
  [bytes]  value (present only if found=1)
Response (error):
  [u8]     1 (status: error)
  [u32 BE] error_len
  [bytes]  error_msg
```

#### INSERT Request

```
[u64 BE]  tx_id
[u32 BE]  route_len
[bytes]   route
[u32 BE]  key_len
[bytes]   key
[u32 BE]  value_len
[bytes]   value
Response (success):
  [u8]     0 (status: success)
Response (error):
  [u8]     1 (status: error)
  [u32 BE] error_len
  [bytes]  error_msg
```

#### DELETE Request

```
[u64 BE]  tx_id
[u32 BE]  route_len
[bytes]   route
[u32 BE]  key_len
[bytes]   key
Response (success):
  [u8]     0 (status: success)
Response (error):
  [u8]     1 (status: error)
  [u32 BE] error_len
  [bytes]  error_msg
```

#### DELETE_RANGE Request

```
[u64 BE]  tx_id
[u32 BE]  route_len
[bytes]   route
[u32 BE]  start_key_len
[bytes]   start_key
[u32 BE]  end_key_len
[bytes]   end_key
Response (success):
  [u8]     0 (status: success)
Response (error):
  [u8]     1 (status: error)
  [u32 BE] error_len
  [bytes]  error_msg
```

#### SCAN Request

```
[u64 BE]  tx_id
[u32 BE]  route_len
[bytes]   route
[u8]      has_start (0 or 1)
[u32 BE]  start_key_len (if present)
[bytes]   start_key
[u8]      has_end (0 or 1)
[u32 BE]  end_key_len (if present)
[bytes]   end_key
[u8]      has_limit (0 or 1)
[u32 BE]  limit (if present)
[u8]      reverse (0 or 1)
[u8]      start_exclusive (0 or 1, optional; absent means 0)
Response (success):
  [u8]     0 (status: success)
  [u32 BE] item_count
  [repeat]
    [u32 BE] key_len
    [bytes]  key
    [u32 BE] value_len
    [bytes]  value
  [u8]     has_more (0 or 1)
Response (error):
  [u8]     1 (status: error)
  [u32 BE] error_len
  [bytes]  error_msg
```

#### COMMIT Request

```
[u64 BE]  tx_id
[u32 BE]  route_len
[bytes]   route
Response (success):
  [u8]     0 (status: success)
Response (error):
  [u8]     1 (status: error)
  [u32 BE] error_len
  [bytes]  error_msg
```

#### SUBSCRIBE Request

```
[u32 BE]  route_pattern_len
[bytes]   route_pattern
Response (success):
  [u8]     0 (status: success)
  [u64 BE] subscription_id
Response (error):
  [u8]     1 (status: error)
  [u32 BE] error_len
  [bytes]  error_msg
```

`route_pattern` MAY be an exact resource route like
`kv://realm/area/resource` or a wildcard pattern like `kv://realm/area/*`,
`kv://realm/**`, or `kv://*/area/resource`. The scheme must be `kv://`,
segments must be non-empty, wildcards must be whole `*` or `**` segments, and
the pattern must be capable of matching a concrete three-segment KV route.
Invalid SUBSCRIBE or UNSUBSCRIBE input returns 1012.

Registrations are session-scoped and isolated by `RouteFamily`. Overlapping
registrations remain independent and exact registrations have no precedence. A
session may retain at most 1,024 registrations total and 128 wildcard
registrations. Duplicate `(session, original registration string)` requests are
idempotent and checked before either limit. Overflow returns 1013.

#### UNSUBSCRIBE Request

```
[u32 BE]  route_pattern_len
[bytes]   route_pattern
Response (success):
  [u8]     0 (status: success)
Response (error):
  [u8]     1 (status: error)
  [u32 BE] error_len
  [bytes]  error_msg
```

#### NOTIFY Delivery

```
[u64 BE]  subscription_id
[u32 BE]  route_len
[bytes]   route
[u64 BE]  mutation_count
```

`NOTIFY` is server-to-client only. The broker emits it after a successful
`COMMIT` when the committed transaction changed at least one key in a watched
resource. `route` is always the exact concrete resource route, even when the
registration was a wildcard pattern.

#### ROLLBACK Request

```
[u64 BE]  tx_id
[u32 BE]  route_len
[bytes]   route
Response (success):
  [u8]     0 (status: success)
Response (error):
  [u8]     1 (status: error)
  [u32 BE] error_len
  [bytes]  error_msg
```

#### Semantics

- **Self-Contained Operations**: Every request includes the full route; no implicit state beyond tx_id
- **Persistence**: All committed data survives broker restart
- **Client Convenience**: Clients MAY track route internally per tx_id for ergonomics, but MUST send route with every operation on the wire

##### Isolation Levels

**ReadOnly (mode=0):**

- Multiple ReadOnly transactions MAY run concurrently on same resource
- Reads see committed state at BEGIN time (snapshot isolation)
- Cannot see uncommitted writes from active ReadWrite transactions
- Cannot perform writes (PUT/INSERT/DELETE fail with ERR_WRITE_IN_READONLY)

**ReadWrite (mode=1):**

- Exclusive lock on resource (realm+area+resource tuple)
- Only one ReadWrite transaction active per resource at a time
- Other transactions (ReadOnly or ReadWrite) block until COMMIT/ROLLBACK
- Serializable isolation (all-or-nothing commit)

**Conflict Resolution:**

- If ReadWrite transaction begins while another active: ERR_ISOLATION_CONFLICT
- If ReadOnly transaction begins during ReadWrite: blocks until commit/rollback

##### Durability Modes

Only `0` and `1` are valid durability values. Other values are rejected.

**Sync (durability=1):**

- Commits are flushed to durable storage (WAL fsync) before returning
- Survives broker crash/restart
- Higher latency, stronger crash durability at the configured storage layer

**Buffered (durability=0):**

- Commits written to memory buffer, background flush to storage
- Lower latency, best-effort durability
- May lose recent commits on broker crash (up to flush interval)
- Use for caching or when throughput > durability

##### SCAN Semantics

**`start_exclusive` flag:**

- `start_exclusive=0` (default, and the value assumed when the byte is absent):
  `start_key` is inclusive, as described below
- `start_exclusive=1`: the scan begins strictly after `start_key` in the
  selected direction. Required for continuation; see the pagination rules below

**`reverse` flag:**

- `reverse=0` (forward): Scan keys in ascending lexicographic order
  - Includes keys where `start_key <= key < end_key`
  - An omitted `start_key` or `end_key` leaves that side unbounded
- `reverse=1` (backward): Scan keys in descending lexicographic order
  - Includes keys where `end_key < key <= start_key`
  - An omitted `start_key` starts at the last key; an omitted `end_key` leaves
    the lower side unbounded
- Equal bounds, or bounds inverted for the selected direction, return an empty
  successful result
- `limit` applies regardless of direction and bounds a page from above. The
  broker bounds every page by two further limits: at most 1024 items, and at
  most what fits in one response frame (a scan response is carried as a single
  TLV value with a `u16` length, so a page of large values reaches the byte
  bound well before the item cap). A page ends at whichever limit is reached
  first.
- **An omitted `limit` therefore does not mean unlimited.** It means "as much as
  fits in one page". A scan of 300 keys with 1 KiB values returns a partial page
  with `has_more=1` even though no limit was supplied.
- `has_more=1` means at least one additional matching key exists beyond the
  returned page. Clients MUST honour it whether or not they supplied a `limit`;
  treating an omitted limit as complete silently leaves data unread.
- To continue, re-issue the scan with `start_key` set to the last key returned
  by the previous page, `start_exclusive=1`, and the same `reverse` value and
  opposite bound. Repeat until `has_more=0`.
- `start_exclusive` is a trailing optional byte on the SCAN request, defaulting
  to `0` when absent. Resuming with an inclusive `start_key` does not
  terminate: a page bounded by the byte budget can hold a single pair, and the
  next request then returns that same pair forever, so later keys are never
  reached.

**Mixed-version behaviour.** The byte is additive and older brokers reject
trailing data, so clients MUST NOT send it unless the broker advertises support.
Clients that cannot yet encode it can still paginate **forward** with no wire
change: re-issue with `start_key` set to the last returned key followed by a
single `0x00` byte, which is that key's immediate successor and therefore an
exclusive resume. **Reverse** continuation has no such equivalent - the
symmetric operation is a byte-string predecessor, which is not expressible - so
a client that cannot send `start_exclusive` must not paginate reverse scans
across a byte-bounded page. Until a client ships the byte, keep reverse scans
within a single page by supplying a `limit` small enough that the page is not
byte-bounded.

#### Usage Example

**Recommended User-Facing API (see [Recommended Client API Design](overview.md)):**

```python
# Connect with JWT
client = FitzClient.connect_tcp("127.0.0.1:4091", jwt_token)

# Begin transaction - returns Transaction object
# Route is full URI: kv://realm/area/resource
tx = client.kv_begin("kv://prod/app/users", TxMode.ReadWrite, Durability.Sync)

# Transaction methods focus on data, hide route repetition
tx.put(b"user:123", b"alice")
value = tx.get(b"user:123")
tx.commit()

# Context manager pattern (Python)
with client.kv_begin("kv://prod/app/users", TxMode.ReadWrite, Durability.Sync) as tx:
    tx.put(b"key", b"value")
    tx.commit()  # Or auto-commit on __exit__
```

**Wire Protocol (what actually happens under the hood):**

Every transaction operation sends **both tx_id AND route** on the wire:

- `PUT`: `[tx_id][route_len][route][key_len][key][value_len][value]`
- `GET`: `[tx_id][route_len][route][key_len][key]`
- `COMMIT`: `[tx_id][route_len][route]`

The Transaction object stores the route internally and includes it in every wire
message, making each request fully addressable on the wire while the broker
still maintains live session-scoped transaction state keyed by `tx_id`.

#### Error Codes (1xxx)

- 1001 = ERR_TRANSACTION_NOT_FOUND
- 1002 = ERR_INVALID_MODE
- 1003 = ERR_KEY_NOT_FOUND
- 1004 = ERR_ISOLATION_CONFLICT
- 1005 = ERR_WRITE_IN_READONLY
- 1006 = ERR_KEY_EXISTS (INSERT on existing key)
- 1007 = ERR_INVALID_ROUTE
- 1008 = ERR_REALM_MISMATCH
- 1009 = ERR_BACKEND_ERROR
- 1010 = ERR_TRANSACTION_ABORTED
- 1011 = ERR_UNAUTHORIZED
- 1012 = ERR_INVALID_SUBSCRIPTION_PATTERN
- 1013 = ERR_SUBSCRIPTION_LIMIT
- 1014 = ERR_BUSY (retryable)

`ERR_BUSY` means the domain mailbox was full and the request was never accepted. Nothing applied, so re-sending after a backoff is safe; this is distinct from `ERR_BACKEND_ERROR`, which is fatal and says nothing about whether the request took effect.

#### Acceptance Tests

- begin/put/commit cycle
- begin/get on non-existent key
- ReadOnly mode rejects put
- two transactions on same resource conflict
- rollback discards all changes
- scan returns lexicographically ordered pairs
