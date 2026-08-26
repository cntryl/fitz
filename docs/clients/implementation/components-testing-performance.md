## Core Components

### 1. Connection Manager

**Responsibilities:**
- WebSocket lifecycle (connect, reconnect, close)
- JWT authentication (CONNECT frame)
- Frame send/receive
- Correlation ID tracking (for RPC)
- Subscription routing (Notice/RPC)

**Key Features:**
```
- Automatic reconnection with exponential backoff
- Pending request tracking (Map<correlationId, Promise/Future>)
- Subscription routing (Map<subscriptionId, handler>)
- Frame batching (optional optimization)
```

#### Reconnect State Rebuild

Reconnect always creates a new Fitz broker session. Client libraries MAY hide the reconnect loop, but they MUST NOT hide the fact that broker session state was lost. Rebuild only from client-owned configuration:

| Client-owned state | Reconnect behavior |
|--------------------|--------------------|
| Notice subscriptions | Re-send SUBSCRIBE and bind the new subscription id before reporting the subscription active. Missed notices are not replayed. |
| Queue availability subscriptions | Re-send SUBSCRIBE. Queue items reserved before disconnect are stale; callers must reserve again. |
| RPC workers | Re-send worker registration before reporting the worker active. Pending calls fail with a connection/interruption error. |
| Lease change subscriptions | Re-send SUBSCRIBE. Acquired lease handles are stale; workflows that still need ownership must acquire again. |
| Stream commit subscriptions | Re-send SUBSCRIBE as a live wake signal. Open append sessions are stale, and replay resumes only from client-owned offsets via READ. |
| Schedule fire subscriptions | Re-send SUBSCRIBE. Durable schedule definitions remain broker state, but live notification subscriptions do not. |
| KV transactions | Fail/close open transaction handles and require a fresh BEGIN. |

### 2. TLV Codec

**Encoding:**
```
[MessageType u16 BE][Length u16 BE][Payload bytes]
```

**Decoder pattern** (pseudocode):
```
decode_frame(bytes):
    message_type = read_u16_be(bytes[0:2])
    length = read_u16_be(bytes[2:4])
    payload = bytes[4:4+length]
    
    if 100 <= message_type <= 199:
        return decode_kv_message(message_type, payload)
    elif 500 <= message_type <= 599:
        return decode_notice_message(message_type, payload)
    # ... other domains
```

### 3. Domain Facades

Each domain should provide:
- **High-level API** (hides wire protocol)
- **Object wrappers** (Transaction, Subscription, Session)
- **Error translation** (wire codes → domain exceptions)

**Example: KV facade structure**
```
KvClient
├── begin(route) → Transaction
│   ├── get(key) → GetResult
│   ├── put(key, value)
│   ├── delete(key)
│   ├── scan(start, end, limit) → Iterator
│   ├── commit()
│   └── rollback()
```

### 4. Error Handling

**Three-level error hierarchy:**

```
1. Transport errors (connection lost, timeout)
   └─ Retryable: Yes (with backoff)

2. Protocol errors (malformed TLV, unknown message type)
   └─ Retryable: No (fatal)

3. Domain errors (key not found, unauthorized)
   └─ Retryable: Depends on error code
```

**Domain error code mapping:**
```
1000-1999: KV
  1001: Transaction not found → Retryable: No
  1006: Key exists (Insert) → Retryable: No
  1011: Unauthorized → Retryable: No

2000-2999: Stream
  2001: Concurrency conflict → Retryable: No
    2006: Unsupported filter version → Retryable: No
    2007: Invalid filter payload → Retryable: No
  2009: Unauthorized → Retryable: No

3000-3999: Notice
  3009: Unauthorized → Retryable: No

4000-4999: Queue
  4001: Invalid token → Retryable: No
  4005: Queue full (backpressure) → Retryable: Yes (with backoff)
  4009: Unauthorized → Retryable: No

5000-5999: Lease
  5001: Lease held → Retryable: Yes (with backoff)
  5005: Invalid token → Retryable: No
  5009: Unauthorized → Retryable: No

6000-6999: RPC
  6001: Timeout → Retryable: Yes (with backoff)
  6004: Route not registered → Retryable: Yes (with backoff)
  6009: Unauthorized → Retryable: No

7000-7999: Schedule
  7002: Invalid cron → Retryable: No
  7009: Unauthorized → Retryable: No
  7010: Backend unavailable or saturated → Retryable: Yes when replay-safe

See the client-acceptance-criteria.md appendix for the complete error code reference.
```

---

## Error Handling Strategies

### Strategy 1: Explicit Result Types (Recommended)

**Rust:**
```rust
pub enum Result<T, E> {
    Ok(T),
    Err(E),
}
```

**TypeScript:**
```typescript
type Result<T, E> = { ok: true; value: T } | { ok: false; error: E };
```

**Pros:**
- Forces error handling at call site
- No hidden control flow
- Type-safe

### Strategy 2: Exceptions (C#, Python, Go-style)

**C#:**
```csharp
try {
    await tx.InsertAsync("key", "value");
} catch (KvException ex) when (ex.Code == 1006) {
    // Handle key exists (ERR_KEY_EXISTS)
}
```

**Python:**
```python
try:
    await tx.get("key")
except KvKeyExistsError:
    pass
```

**Pros:**
- Familiar to developers
- Can skip intermediate error handling

### Strategy 3: Error Codes + Nulls (Not Recommended)

**Bad:**
```go
value, err := tx.Get("key")
if err == ErrNotFound {
    // Was it "not found" or a network error?
}
```

**Why:** Conflates missing data with errors.

### Recommended: Hybrid Approach

**Go:**
```go
result, err := tx.Get(ctx, "key")
if err != nil {
    return fmt.Errorf("get: %w", err)
}

switch result := result.(type) {
case *GetResultFound:
    // Use result.Value
case *GetResultNotFound:
    // Handle missing key
}
```

**Python:**
```python
result = await tx.get("key")
match result:
    case GetResult.Found(value):
        print(value)
    case GetResult.NotFound():
        print("not found")
```

---

## Testing Your Client

### Unit Tests (Protocol Layer)

Test TLV encoding/decoding in isolation:

```python
def test_encode_put_request():
    req = PutRequest(
        tx_id=1,
        route="kv://prod/users",
        key=b"user:123",
        value=b"alice"
    )
    
    encoded = encode_message(MessageType.PUT, req)
    
    # Verify bytes match expected format
    assert encoded[0:2] == b'\x00\x68'  # MessageType 104
    assert encoded[2:4] == b'\x00\x2F'  # Length 47
```

### Integration Tests (Against Real Broker)

Use client-acceptance-criteria.md as the test template:

```python
async def test_kv_transaction_lifecycle():
    """AC-KV-001: Transaction Lifecycle (Begin → Commit)"""
    async with FitzClient("ws://localhost:4090") as client:
        await client.connect(jwt=TEST_JWT)
        
        # Begin transaction
        tx = await client.kv.begin("kv://test/app/users", durability="Sync")
        assert tx.tx_id > 0
        
        # Put data
        await tx.put("user:123", b"alice")
        
        # Commit
        await tx.commit()
        
        # Verify persistence
        tx2 = await client.kv.begin("kv://test/app/users", durability="Sync")
        result = await tx2.get("user:123")
        assert isinstance(result, GetResult.Found)
        assert result.value == b"alice"
```

### Property-Based Tests

Test invariants:

```rust
#[quickcheck]
fn prop_tlv_roundtrip(message_type: u16, payload: Vec<u8>) -> bool {
    let encoded = encode_frame(message_type, &payload);
    let (decoded_type, decoded_payload) = decode_frame(&encoded).unwrap();
    
    message_type == decoded_type && payload == decoded_payload
}
```

### Load Tests

Measure throughput:

```go
func BenchmarkNoticePublish(b *testing.B) {
    client := connectClient()
    defer client.Close()
    
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        client.Notice.Publish(ctx, "notice://test/bench/events", []byte("event"))
    }
}
```

---

## Performance Optimization

### 1. Connection Pooling (Not Needed in Fitz)

**Fitz uses multiplexing over single WebSocket** - no need for connection pools.

### 2. Request Batching

Batch multiple operations:

```go
batch := client.KV.NewBatch()
batch.Put("key1", value1)
batch.Put("key2", value2)
batch.Put("key3", value3)
results, err := batch.Commit(ctx)
```

### 3. Zero-Copy Decoding

**Avoid:**
```rust
// Allocates string
let route = String::from_utf8(payload[offset..offset+route_len].to_vec())?;
```

**Prefer:**
```rust
// Zero-copy string slice
let route = std::str::from_utf8(&payload[offset..offset+route_len])?;
```

### 4. Subscription Buffering

Use bounded buffers so notice delivery cannot block the receive loop or grow
without limit. When the buffer fills, terminate only that local consumer with
a typed backpressure error; sibling consumers remain active:

```python
class NoticeSubscription:
    def __init__(self):
        self._queue = asyncio.Queue(maxsize=1000)  # Buffer 1000 notices
```

### 5. Compression (Optional)

For large payloads, compress at application level:

```python
import zlib

compressed = zlib.compress(large_payload)
await tx.put("key", compressed)
```

---

## Common Pitfalls

### ❌ Pitfall 1: Not Handling Reconnection

**Problem:**
```typescript
// Connection lost → entire app crashes
await client.notice.publish("notice://prod/events", payload);
```

**Solution:**
```typescript
// Implement retry with backoff
async function publishWithRetry(route: string, payload: Uint8Array, maxRetries = 3) {
    for (let i = 0; i < maxRetries; i++) {
        try {
            await client.notice.publish(route, payload);
            return;
        } catch (err) {
            if (i === maxRetries - 1) throw err;
            await sleep(Math.pow(2, i) * 1000);
        }
    }
}
```

### ❌ Pitfall 2: Treating Reconnect as Session Recovery

**Problem:**
```python
# Subscriptions lost on reconnect
sub = await client.notice.subscribe("notice://prod/orders/*")
# ... connection drops ...
# No more notifications unless the client rebuilds the subscription on the new session.
```

**Solution:**
```python
class ResilientSubscription:
    async def _reconnect_loop(self):
        while not self._closed:
            try:
                await self._client.connect(self._jwt)
                self._sub_id = await self._resubscribe()
            except Exception as e:
                await asyncio.sleep(self._backoff())
```

Apply the same rule to every session-scoped handle: re-register RPC workers, re-subscribe Queue/Lease/Stream/Schedule listeners, fail open KV transactions and Stream append sessions, invalidate QueueItem and Lease handles, and resume Stream history only from offsets owned by the application or client.

### ❌ Pitfall 3: Exposing tx_id to Users

**Problem:**
```go
// User has to manage tx_id manually
txID, _ := client.KV.Begin(ctx, "kv://prod/users", fitz.KVDurabilitySync)
client.KV.Put(ctx, txID, "key", value)  // Easy to use wrong tx_id
client.KV.Commit(ctx, txID)
```

**Solution:**
```go
// Transaction object encapsulates state
tx, _ := client.KV.Begin(ctx, "kv://prod/users", fitz.KVDurabilitySync)
tx.Put(ctx, "key", value)  // tx_id is internal
tx.Commit(ctx)
```

### ❌ Pitfall 4: Not Validating Frame Size

**Problem:**
```rust
// Send 10 MB payload → broker rejects
tx.put("key", huge_value)?;  // Frame too large error
```

**Solution:**
```rust
const MAX_FRAME_SIZE: usize = 1_048_576; // 1 MB

pub fn put(&self, key: &str, value: &[u8]) -> Result<()> {
    if value.len() > MAX_FRAME_SIZE {
        return Err(KvError::PayloadTooLarge {
            size: value.len(),
            max: MAX_FRAME_SIZE,
        });
    }
    // ... send request
}
```

### ❌ Pitfall 5: Blocking on Subscription Handlers

**Problem:**
```python
# Slow handler blocks entire subscription
async for notice in sub:
    await slow_database_write(notice)  # Blocks next notification
```

**Solution:**
```python
# Spawn tasks for handlers
async for notice in sub:
    asyncio.create_task(handle_notice(notice))  # Non-blocking
```

---

## Summary

### Quick Checklist for Implementers

- [ ] **Connection:**
  - [ ] WebSocket connect/reconnect with exponential backoff
  - [ ] JWT authentication (CONNECT frame)
  - [ ] Subscription state tracking
- [ ] **Protocol:**
  - [ ] TLV encoder/decoder
  - [ ] MessageType routing (100-799 ranges)
  - [ ] Frame size validation (1 MB limit)
- [ ] **Domains:**
  - [ ] Object-based APIs (Transaction, Subscription, Session)
  - [ ] Idiomatic error handling (Result types or exceptions)
  - [ ] Automatic cleanup (destructors/finalizers)
- [ ] **Testing:**
  - [ ] Protocol layer unit tests (TLV roundtrip)
  - [ ] Integration tests (against real broker)
  - [ ] All AC criteria passing
- [ ] **Performance:**
  - [ ] Subscription buffering (avoid blocking)
  - [ ] Zero-copy where possible
  - [ ] Connection reuse (WebSocket multiplexing)

### Recommended Implementation Order

1. **Week 1:** Connection + TLV codec + basic KV
2. **Week 2:** Complete KV domain + error handling
3. **Week 3:** Notice + RPC domains (subscriptions)
4. **Week 4:** Queue + Lease + Stream domains
5. **Week 5:** Schedule domain + polish + docs

---

## Next Steps

1. **Read client-spec.md** for wire protocol details
2. **Read client-acceptance-criteria.md** for test requirements
3. **Choose your language** and start with Connection + TLV
4. **Implement KV domain first** (most complex, good learning)
5. **Write tests** as you go (one AC at a time)
6. **Join Fitz community** for help (Discord, GitHub Discussions)

---

**Last Updated:** February 8, 2026  
**Contributing:** See CONTRIBUTING.md for how to improve this guide
