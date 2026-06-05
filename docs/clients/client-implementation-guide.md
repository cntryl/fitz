# Fitz Client Implementation Guide

**Version:** 1.0  
**Date:** February 8, 2026  
**Purpose:** Practical guide for building idiomatic Fitz clients in any language

---

## Table of Contents

- [Overview](#overview)
- [Design Principles](#design-principles)
- [Architecture Patterns](#architecture-patterns)
- [Language-Specific Guidance](#language-specific-guidance)
  - [Go](#go-implementation)
  - [Python](#python-implementation)
  - [Rust](#rust-implementation)
  - [TypeScript](#typescript-implementation)
  - [C#](#c-implementation)
- [Core Components](#core-components)
- [Error Handling Strategies](#error-handling-strategies)
- [Testing Your Client](#testing-your-client)
- [Performance Optimization](#performance-optimization)
- [Common Pitfalls](#common-pitfalls)

## Cross-Language Conformance Suite

Use these companion artifacts when implementing or reviewing multiple SDKs:

- `cross-language-conformance-suite.yaml`: canonical scenario definitions and pass/fail policy
- `cross-language-conformance-runner.md`: harness input/output contract and CI matrix guidance

---

## Overview

This guide complements **client-spec.md** (wire protocol) and **client-acceptance-criteria.md** (test requirements) by showing you how to build production-ready clients that feel natural in your target language.

### Document Relationship

```
client-spec.md
└─ Defines: Wire format, TLV encoding, domain operations
   │
    ├─> client-acceptance-criteria.md
   │   └─ Defines: Testable requirements (Given-When-Then)
   │
    └─> client-implementation-guide.md (this doc)
       └─ Shows: Idiomatic patterns, best practices, examples
```

### Who Should Read This

- **Client library authors** building multi-language support
- **Application developers** integrating Fitz directly
- **Architects** designing Fitz-based systems

---

## Design Principles

### 1. Hide Wire Complexity from Users

**Bad API:**
```python
# ❌ User shouldn't see TLV encoding
client.send_frame([0x00, 0x68, 0x00, 0x2F, ...])
```

**Good API:**
```python
# ✅ Clean, type-safe interface
tx.put("user:123", b"alice")
```

### 2. Use Language-Native Types

**Bad API:**
```go
// ❌ Exposing broker internals
func (c *Client) CurrentShard() uint64
```

**Good API:**
```go
// ✅ Broker routing stays internal
func (c *Client) Connect(jwt string) error
```

### 3. Object-Based State Management

**Bad API:**
```typescript
// ❌ User manages tx_id manually
await client.kvPut(txId, route, key, value);
await client.kvCommit(txId);
```

**Good API:**
```typescript
// ✅ Transaction object encapsulates state
const tx = await client.kv.begin("kv://prod/users", {
  durability: "Sync",
});
await tx.put("user:123", "alice");
await tx.commit();
```

### 4. Make Durability Explicit

**Bad API:**
```rust
// ❌ Hidden durability choice
let tx = client.begin("kv://prod/users");
```

**Good API:**
```rust
// ✅ Caller chooses durability
let tx = client.begin("kv://prod/users", Durability::Sync);
```

### 5. Fail Fast, Fail Clear

**Bad API:**
```csharp
// ❌ Silent failure
var value = await tx.GetAsync("key"); // Returns null for both "missing" and "error"
```

**Good API:**
```csharp
// ✅ Explicit error types
var result = await tx.GetAsync("key");
if (result is GetResult.Found(var value)) { ... }
else if (result is GetResult.NotFound) { ... }
```

### 6. Keep Stream Metadata Explicit

Stream clients SHOULD expose an optional discriminator on append and an optional filter object on read. Treat both as ordinary request fields, not client-global state, and default them to omitted/null so older call sites stay compatible.

If your language offers builders, wrap the shared `StreamFilterSet` and `StreamFilterClause` shapes directly instead of inventing a separate filter DSL per SDK.

The wire encoding for stream reads is fixed: `route`, `from_offset`, `limit`, optional `max_bytes`, then an optional raw `StreamFilterSet` blob. Clients SHOULD encode the filter blob with the server's versioned marker and big-endian length fields, and MUST surface `ERR_STREAM_FILTER_UNSUPPORTED_VERSION` and `ERR_STREAM_FILTER_INVALID_PAYLOAD` as typed request errors rather than transport failures.

---

## Architecture Patterns

### Recommended Client Structure

```
fitz-client/
├── connection.{ext}        # WebSocket management, reconnect logic
├── protocol/
│   ├── tlv.{ext}          # TLV encoder/decoder
│   └── messages.{ext}      # Message type constants, wire structs
├── domains/
│   ├── kv.{ext}           # KV domain client
│   ├── notice.{ext}       # Notice domain client
│   ├── rpc.{ext}          # RPC domain client
│   ├── queue.{ext}        # Queue domain client
│   ├── lease.{ext}        # Lease domain client
│   ├── stream.{ext}       # Stream domain client
│   └── schedule.{ext}     # Schedule domain client
├── errors.{ext}           # Domain-specific error types
└── client.{ext}           # Main client facade
```

### Layered Design

```
┌─────────────────────────────────────┐
│   User Application Code             │
├─────────────────────────────────────┤
│   Domain Facades (KV, RPC, Notice)  │  ← High-level, idiomatic APIs
├─────────────────────────────────────┤
│   Connection Manager                 │  ← WebSocket, auth, reconnect
├─────────────────────────────────────┤
│   Protocol Layer (TLV codec)        │  ← Wire format encoding/decoding
├─────────────────────────────────────┤
│   Transport (WebSocket/TCP)         │  ← Low-level I/O
└─────────────────────────────────────┘
```

---

## Language-Specific Guidance

### Go Implementation

#### Project Structure

```
github.com/yourorg/fitz-go/
├── client.go              # Main client
├── kv.go                  # KV facade
├── notice.go              # Notice facade
├── connection.go          # WebSocket manager
├── protocol/
│   ├── tlv.go            # TLV encoding
│   └── messages.go        # Wire types
├── errors.go             # Error types
└── examples/
    └── kv_example.go
```

#### Route Validation Notes

- Most domains use `scheme://realm/area/resource`.
- Schedule routes include an operation segment: `schedule://realm/area/resource/operation`.
- For schedule create/cancel, pass the full 4-segment route (for example, `schedule://prod/jobs/cleanup/run`).

#### Idiomatic Patterns

**1. Use context.Context for cancellation**

```go
func (c *Client) Connect(ctx context.Context, jwt string) error {
    ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
    defer cancel()
    
    conn, err := websocket.Dial(ctx, c.url, nil)
    if err != nil {
        return fmt.Errorf("websocket dial: %w", err)
    }
    
    // Send CONNECT frame
    if err := c.sendConnect(ctx, jwt); err != nil {
        return fmt.Errorf("auth: %w", err)
    }
    
    return nil
}
```

**2. Return structured errors**

```go
type KvError struct {
    Code    int
    Op      string
    Message string
}

func (e *KvError) Error() string {
    return fmt.Sprintf("kv %s: %s (code %d)", e.Op, e.Message, e.Code)
}

// Usage
if err := tx.Insert("key", value); err != nil {
    var kvErr *KvError
    if errors.As(err, &kvErr) && kvErr.Code == 1006 {
        // Handle "key exists" (ERR_KEY_EXISTS) specifically
    }
}
```

**3. Object-based transactions**

```go
type Transaction struct {
    client *Client
    txID   uint64
    route  string
}

func (c *KvClient) Begin(ctx context.Context, route string) (*Transaction, error) {
    req := &BeginRequest{
        Route:      route,
        Mode:       ModeReadWrite,
        Durability: DurabilitySync,
    }
    
    resp, err := c.client.sendRequest(ctx, MessageTypeBegin, req)
    if err != nil {
        return nil, err
    }
    
    return &Transaction{
        client: c.client,
        txID:   resp.TxID,
        route:  route,
    }, nil
}

func (tx *Transaction) Put(ctx context.Context, key string, value []byte) error {
    req := &PutRequest{
        TxID:  tx.txID,
        Route: tx.route,
        Key:   []byte(key),
        Value: value,
    }
    
    _, err := tx.client.sendRequest(ctx, MessageTypePut, req)
    return err
}
```

**4. Notice subscriptions with channels**

```go
type Subscription struct {
    subscriptionID uint64
    messages       chan *Notice
    done           chan struct{}
}

func (n *NoticeClient) Subscribe(ctx context.Context, pattern string) (*Subscription, error) {
    resp, err := n.client.sendRequest(ctx, MessageTypeSubscribe, &SubscribeRequest{
        Pattern: pattern,
    })
    if err != nil {
        return nil, err
    }
    
    sub := &Subscription{
        subscriptionID: resp.SubscriptionID,
        messages:       make(chan *Notice, 100), // Buffered channel
        done:           make(chan struct{}),
    }
    
    n.registerSubscription(sub)
    return sub, nil
}

// Usage
sub, _ := client.Notice.Subscribe(ctx, "notice://prod/orders/*")
for {
    select {
    case notice := <-sub.Messages():
        fmt.Printf("Received: %s\n", notice.Route)
    case <-ctx.Done():
        return
    }
}
```

**5. RPC with timeout**

```go
func (r *RpcClient) Call(ctx context.Context, route string, payload []byte, timeout time.Duration) ([]byte, error) {
    ctx, cancel := context.WithTimeout(ctx, timeout)
    defer cancel()
    
    correlationID := uuid.New()
    
    req := &CallRequest{
        Route:         route,
        CorrelationID: correlationID[:],
        Payload:       payload,
    }
    
    respChan := r.registerPendingCall(correlationID)
    defer r.unregisterPendingCall(correlationID)
    
    if err := r.client.sendRequest(ctx, MessageTypeCall, req); err != nil {
        return nil, err
    }
    
    select {
    case resp := <-respChan:
        return resp.Payload, nil
    case <-ctx.Done():
        return nil, ctx.Err() // "deadline exceeded" or "canceled"
    }
}
```

#### Best Practices

- ✅ Use `context.Context` everywhere for cancellation
- ✅ Return `error` interface, not strings
- ✅ Use channels for async notifications (Notice, RPC)
- ✅ Provide synchronous and async APIs where appropriate
- ✅ Use builder pattern for complex configurations

```go
client := fitz.NewClient("ws://localhost:4090").
    WithReconnect(true).
    WithMaxRetries(5).
    WithLogger(logger).
    Build()
```

---

### Python Implementation

#### Project Structure

```
fitz-python/
├── fitz/
│   ├── __init__.py
│   ├── client.py          # Main client
│   ├── kv.py              # KV facade
│   ├── notice.py          # Notice facade
│   ├── connection.py      # WebSocket manager
│   ├── protocol/
│   │   ├── tlv.py         # TLV encoding
│   │   └── messages.py    # Wire types
│   └── errors.py          # Domain errors
├── tests/
└── examples/
```

#### Idiomatic Patterns

**1. Async/await throughout**

```python
import asyncio
from fitz import FitzClient

async def main():
    async with FitzClient("ws://localhost:4090") as client:
        await client.connect(jwt="your-token")
        
        # KV transaction
        async with await client.kv.begin("kv://prod/users", durability="Sync") as tx:
            await tx.put("user:123", b"alice")
            await tx.commit()  # Auto-commit on context exit

asyncio.run(main())
```

**2. Context managers for cleanup**

```python
class Transaction:
    def __init__(self, client, tx_id, route):
        self.client = client
        self.tx_id = tx_id
        self.route = route
        self._committed = False
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if not self._committed and exc_type is None:
            await self.commit()  # Auto-commit on success
        elif exc_type is not None:
            await self.rollback()  # Auto-rollback on exception
    
    async def put(self, key: str, value: bytes):
        req = PutRequest(
            tx_id=self.tx_id,
            route=self.route,
            key=key.encode(),
            value=value
        )
        await self.client._send_request(MessageType.PUT, req)
    
    async def commit(self):
        await self.client._send_request(MessageType.COMMIT, CommitRequest(self.tx_id))
        self._committed = True
```

**3. Type hints and dataclasses**

```python
from dataclasses import dataclass
from typing import Optional, Union
from enum import IntEnum

class MessageType(IntEnum):
    BEGIN = 100
    COMMIT = 101
    ROLLBACK = 102
    GET = 103
    PUT = 104

@dataclass
class GetResult:
    @dataclass
    class Found:
        value: bytes
    
    @dataclass
    class NotFound:
        pass

# Usage with pattern matching (Python 3.10+)
result = await tx.get("user:123")
match result:
    case GetResult.Found(value):
        print(f"Found: {value}")
    case GetResult.NotFound():
        print("Not found")
```

**4. Notice subscriptions with async generators**

```python
class NoticeClient:
    async def subscribe(self, pattern: str):
        resp = await self.client._send_request(
            MessageType.SUBSCRIBE,
            SubscribeRequest(pattern=pattern)
        )
        
        subscription_id = resp.subscription_id
        queue = asyncio.Queue()
        self._subscriptions[subscription_id] = queue
        
        return NoticeSubscription(subscription_id, queue)

class NoticeSubscription:
    def __init__(self, subscription_id: int, queue: asyncio.Queue):
        self.subscription_id = subscription_id
        self._queue = queue
    
    async def __aiter__(self):
        return self
    
    async def __anext__(self):
        notice = await self._queue.get()
        if notice is None:  # Sentinel for close
            raise StopAsyncIteration
        return notice

# Usage
sub = await client.notice.subscribe("notice://prod/orders/*")
async for notice in sub:
    print(f"Route: {notice.route}, Payload: {notice.payload}")
```

**5. RPC with timeout**

```python
async def call(self, route: str, payload: bytes, timeout: float = 5.0) -> bytes:
    correlation_id = uuid.uuid4().bytes
    
    req = CallRequest(
        route=route,
        correlation_id=correlation_id,
        payload=payload
    )
    
    response_future = asyncio.Future()
    self._pending_calls[correlation_id] = response_future
    
    try:
        await self.client._send_request(MessageType.CALL, req)
        response = await asyncio.wait_for(response_future, timeout=timeout)
        return response.payload
    except asyncio.TimeoutError:
        raise RpcTimeoutError(f"RPC call to {route} timed out after {timeout}s")
    finally:
        self._pending_calls.pop(correlation_id, None)
```

#### Best Practices

- ✅ Use `async`/`await` for all I/O operations
- ✅ Use context managers (`async with`) for resource cleanup
- ✅ Provide type hints for all public APIs
- ✅ Use `asyncio.Queue` for internal message routing
- ✅ Use async generators for subscriptions
- ✅ Follow PEP 8 naming conventions

---

### Rust Implementation

#### Project Structure

```
fitz-rust/
├── Cargo.toml
├── src/
│   ├── lib.rs
│   ├── client.rs          # Main client
│   ├── kv.rs              # KV facade
│   ├── notice.rs          # Notice facade
│   ├── connection.rs      # WebSocket manager
│   ├── protocol/
│   │   ├── mod.rs
│   │   ├── tlv.rs         # TLV encoding
│   │   └── messages.rs    # Wire types
│   └── error.rs           # Error types
├── examples/
└── tests/
```

#### Idiomatic Patterns

**1. Strong typing with enums**

```rust
use thiserror::Error;

#[derive(Debug, Error)]
pub enum KvError {
    #[error("transaction not found (id={0})")]
    TransactionNotFound(u64),
    
    #[error("key already exists: {0}")]
    KeyExists(String),
    
    #[error("unauthorized: {0}")]
    Unauthorized(String),
    
    #[error("connection error: {0}")]
    Connection(#[from] ConnectionError),
}

pub enum GetResult {
    Found(Vec<u8>),
    NotFound,
}

impl Transaction {
    pub async fn get(&self, key: &str) -> Result<GetResult, KvError> {
        let req = GetRequest {
            tx_id: self.tx_id,
            route: self.route.clone(),
            key: key.as_bytes().to_vec(),
        };
        
        let resp = self.client.send_request(MessageType::Get, req).await?;
        
        Ok(match resp.status {
            0 => GetResult::Found(resp.value),
            1 => GetResult::NotFound,
            _ => return Err(KvError::Protocol("invalid status".into())),
        })
    }
}
```

**2. Ownership and lifetimes**

```rust
pub struct Transaction<'a> {
    client: &'a Client,
    tx_id: u64,
    route: String,
}

impl<'a> Transaction<'a> {
    pub async fn put(&self, key: &str, value: &[u8]) -> Result<(), KvError> {
        let req = PutRequest {
            tx_id: self.tx_id,
            route: self.route.clone(),
            key: key.as_bytes().to_vec(),
            value: value.to_vec(),
        };
        
        self.client.send_request(MessageType::Put, req).await?;
        Ok(())
    }
}

// Alternative: Arc for shared ownership
pub struct Transaction {
    client: Arc<Client>,
    tx_id: u64,
    route: String,
}
```

**3. Notice subscriptions with channels**

```rust
use tokio::sync::mpsc;

pub struct Subscription {
    subscription_id: u64,
    receiver: mpsc::UnboundedReceiver<Notice>,
}

impl Subscription {
    pub async fn recv(&mut self) -> Option<Notice> {
        self.receiver.recv().await
    }
}

impl NoticeClient {
    pub async fn subscribe(&self, pattern: &str) -> Result<Subscription, NoticeError> {
        let req = SubscribeRequest {
            pattern: pattern.to_string(),
        };
        
        let resp = self.client.send_request(MessageType::Subscribe, req).await?;
        
        let (tx, rx) = mpsc::unbounded_channel();
        self.subscriptions.lock().await.insert(resp.subscription_id, tx);
        
        Ok(Subscription {
            subscription_id: resp.subscription_id,
            receiver: rx,
        })
    }
}

// Usage
let mut sub = client.notice.subscribe("notice://prod/orders/*").await?;
while let Some(notice) = sub.recv().await {
    println!("Route: {}, Payload: {:?}", notice.route, notice.payload);
}
```

**4. Builder pattern for config**

```rust
pub struct ClientBuilder {
    url: String,
    reconnect: bool,
    max_retries: usize,
    timeout: Duration,
}

impl ClientBuilder {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            reconnect: true,
            max_retries: 5,
            timeout: Duration::from_secs(30),
        }
    }
    
    pub fn reconnect(mut self, enabled: bool) -> Self {
        self.reconnect = enabled;
        self
    }
    
    pub fn max_retries(mut self, retries: usize) -> Self {
        self.max_retries = retries;
        self
    }
    
    pub async fn build(self) -> Result<Client, ClientError> {
        Client::connect(self).await
    }
}

// Usage
let client = ClientBuilder::new("ws://localhost:4090")
    .reconnect(true)
    .max_retries(10)
    .build()
    .await?;
```

**5. Zero-copy where possible**

```rust
pub struct PutRequest<'a> {
    tx_id: u64,
    route: &'a str,
    key: &'a [u8],
    value: &'a [u8],
}

impl<'a> PutRequest<'a> {
    pub fn encode(&self, buf: &mut Vec<u8>) {
        // Encode directly into buffer without intermediate allocations
        buf.extend_from_slice(&self.tx_id.to_be_bytes());
        encode_string(buf, self.route);
        encode_bytes(buf, self.key);
        encode_bytes(buf, self.value);
    }
}
```

#### Best Practices

- ✅ Use `thiserror` for error types
- ✅ Use `tokio` for async runtime
- ✅ Use `Arc<Mutex<T>>` for shared state
- ✅ Prefer zero-copy with lifetimes where possible
- ✅ Use builder pattern for complex construction
- ✅ Implement `Drop` for cleanup (subscriptions, transactions)

---

### TypeScript Implementation

#### Project Structure

```
fitz-ts/
├── package.json
├── tsconfig.json
├── src/
│   ├── index.ts
│   ├── client.ts          # Main client
│   ├── kv.ts              # KV facade
│   ├── notice.ts          # Notice facade
│   ├── connection.ts      # WebSocket manager
│   ├── protocol/
│   │   ├── tlv.ts         # TLV encoding
│   │   └── messages.ts    # Wire types
│   └── errors.ts          # Error types
├── examples/
└── tests/
```

#### Idiomatic Patterns

**1. Promise-based API**

```typescript
export class FitzClient {
    private connection: Connection;
    public readonly kv: KvClient;
    public readonly notice: NoticeClient;
    
    constructor(url: string) {
        this.connection = new Connection(url);
        this.kv = new KvClient(this.connection);
        this.notice = new NoticeClient(this.connection);
    }
    
    async connect(jwt: string): Promise<void> {
        await this.connection.connect(jwt);
    }
    
    async close(): Promise<void> {
        await this.connection.close();
    }
}
```

**2. Union types for results**

```typescript
export type GetResult = 
    | { type: 'found'; value: Uint8Array }
    | { type: 'not-found' };

export class Transaction {
    async get(key: string): Promise<GetResult> {
        const resp = await this.client.sendRequest(MessageType.GET, {
            txId: this.txId,
            route: this.route,
            key: new TextEncoder().encode(key),
        });
        
        if (resp.status === 0) {
            return { type: 'found', value: resp.value };
        } else {
            return { type: 'not-found' };
        }
    }
}

// Usage with type narrowing
const result = await tx.get('user:123');
if (result.type === 'found') {
    console.log('Value:', result.value);
} else {
    console.log('Not found');
}
```

**3. EventEmitter for subscriptions**

```typescript
import { EventEmitter } from 'events';

export interface Notice {
    route: string;
    payload: Uint8Array;
}

export class NoticeSubscription extends EventEmitter {
    constructor(
        private subscriptionId: bigint,
        private client: Connection
    ) {
        super();
    }
    
    onNotice(handler: (notice: Notice) => void): void {
        this.on('notice', handler);
    }
    
    async unsubscribe(): Promise<void> {
        await this.client.sendRequest(MessageType.UNSUBSCRIBE, {
            subscriptionId: this.subscriptionId,
        });
        this.removeAllListeners();
    }
}

// Usage
const sub = await client.notice.subscribe('notice://prod/orders/*');
sub.onNotice((notice) => {
    console.log(`Received: ${notice.route}`);
});
```

**4. Async iterables (alternative to EventEmitter)**

```typescript
export class NoticeSubscription {
    private queue: Notice[] = [];
    private resolvers: Array<(value: Notice) => void> = [];
    private closed = false;
    
    async *[Symbol.asyncIterator](): AsyncIterableIterator<Notice> {
        while (!this.closed) {
            yield await this.next();
        }
    }
    
    private next(): Promise<Notice> {
        if (this.queue.length > 0) {
            return Promise.resolve(this.queue.shift()!);
        }
        
        return new Promise((resolve) => {
            this.resolvers.push(resolve);
        });
    }
    
    // Called internally when notice arrives
    _pushNotice(notice: Notice): void {
        if (this.resolvers.length > 0) {
            const resolve = this.resolvers.shift()!;
            resolve(notice);
        } else {
            this.queue.push(notice);
        }
    }
}

// Usage
const sub = await client.notice.subscribe('notice://prod/orders/*');
for await (const notice of sub) {
    console.log(`Received: ${notice.route}`);
}
```

**5. Custom error classes**

```typescript
export class KvError extends Error {
    constructor(
        public code: number,
        message: string,
        public operation?: string
    ) {
        super(message);
        this.name = 'KvError';
    }
    
    static keyExists(key: string): KvError {
        return new KvError(1006, `Key already exists: ${key}`, 'insert'); // ERR_KEY_EXISTS
    }
    
    static unauthorized(): KvError {
        return new KvError(1011, 'Unauthorized', 'operation'); // ERR_UNAUTHORIZED
    }
}

// Usage
try {
    await tx.insert('key', value);
} catch (err) {
    if (err instanceof KvError && err.code === 1006) {
        // Handle key exists (ERR_KEY_EXISTS)
    }
}
```

#### Best Practices

- ✅ Use `async`/`await` throughout
- ✅ Use union types for result variants
- ✅ Provide both callback and async iterable APIs for subscriptions
- ✅ Use `Uint8Array` for binary data (not `Buffer` in browser)
- ✅ Export TypeScript types for all public APIs
- ✅ Use ESM modules for tree-shaking

---

### C# Implementation

#### Project Structure

```
Fitz.Client/
├── Fitz.Client.csproj
├── Client.cs              # Main client
├── Kv/
│   ├── KvClient.cs        # KV facade
│   └── Transaction.cs     # Transaction wrapper
├── Notice/
│   ├── NoticeClient.cs    # Notice facade
│   └── Subscription.cs    # Subscription wrapper
├── Protocol/
│   ├── Tlv.cs             # TLV encoding
│   └── Messages.cs        # Wire types
├── Connection.cs          # WebSocket manager
└── Errors.cs              # Exception types
```

#### Idiomatic Patterns

**1. Async/await with Task**

```csharp
public class FitzClient : IAsyncDisposable
{
    private readonly Connection _connection;
    public KvClient Kv { get; }
    public NoticeClient Notice { get; }
    
    public FitzClient(string url)
    {
        _connection = new Connection(url);
        Kv = new KvClient(_connection);
        Notice = new NoticeClient(_connection);
    }
    
    public async Task ConnectAsync(string jwt, CancellationToken ct = default)
    {
        await _connection.ConnectAsync(jwt, ct);
    }
    
    public async ValueTask DisposeAsync()
    {
        await _connection.CloseAsync();
    }
}
```

**2. Result types with discriminated unions (C# 9+)**

```csharp
public abstract record GetResult
{
    public record Found(byte[] Value) : GetResult;
    public record NotFound : GetResult;
}

public class Transaction : IAsyncDisposable
{
    public async Task<GetResult> GetAsync(string key, CancellationToken ct = default)
    {
        var req = new GetRequest
        {
            TxId = _txId,
            Route = _route,
            Key = Encoding.UTF8.GetBytes(key)
        };
        
        var resp = await _client.SendRequestAsync<GetResponse>(MessageType.Get, req, ct);
        
        return resp.Status switch
        {
            0 => new GetResult.Found(resp.Value),
            1 => new GetResult.NotFound(),
            _ => throw new KvException("Invalid response status")
        };
    }
}

// Usage with pattern matching
var result = await tx.GetAsync("user:123");
var message = result switch
{
    GetResult.Found(var value) => $"Found: {Encoding.UTF8.GetString(value)}",
    GetResult.NotFound => "Not found",
    _ => throw new ArgumentOutOfRangeException()
};
```

**3. IAsyncEnumerable for subscriptions**

```csharp
public class NoticeSubscription : IAsyncDisposable
{
    private readonly Channel<Notice> _channel;
    
    public async IAsyncEnumerable<Notice> ReadAllAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var notice in _channel.Reader.ReadAllAsync(ct))
        {
            yield return notice;
        }
    }
    
    public async ValueTask DisposeAsync()
    {
        await UnsubscribeAsync();
        _channel.Writer.Complete();
    }
}

// Usage
await using var sub = await client.Notice.SubscribeAsync("notice://prod/orders/*");
await foreach (var notice in sub.ReadAllAsync(ct))
{
    Console.WriteLine($"Route: {notice.Route}");
}
```

**4. ConfigureAwait best practices**

```csharp
public class Connection
{
    public async Task SendRequestAsync<TReq>(MessageType type, TReq request, CancellationToken ct)
    {
        // Library code should use ConfigureAwait(false) and a bounded semaphore to cap concurrent sends.
        await _semaphore.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            var bytes = EncodeRequest(type, request);
            await _websocket.SendAsync(bytes, WebSocketMessageType.Binary, true, ct)
                .ConfigureAwait(false);
        }
        finally
        {
            _semaphore.Release();
        }
    }
}
```

**5. Nullable reference types**

```csharp
#nullable enable

public class Transaction : IAsyncDisposable
{
    private readonly Client _client;
    private readonly ulong _txId;
    private readonly string _route;
    private bool _disposed;
    
    public async Task<byte[]?> GetAsync(string key, CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        
        var result = await GetResultAsync(key, ct).ConfigureAwait(false);
        return result switch
        {
            GetResult.Found(var value) => value,
            GetResult.NotFound => null,
            _ => throw new InvalidOperationException()
        };
    }
}
```

#### Best Practices

- ✅ Implement `IAsyncDisposable` for cleanup
- ✅ Use `CancellationToken` for all async operations
- ✅ Use `ConfigureAwait(false)` in library code
- ✅ Use `IAsyncEnumerable<T>` for subscriptions
- ✅ Enable nullable reference types
- ✅ Use record types for immutable data
- ✅ Follow .NET naming conventions (PascalCase for public APIs)

---

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

Buffer notices to avoid blocking:

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

### ❌ Pitfall 2: Forgetting to Re-subscribe

**Problem:**
```python
# Subscriptions lost on reconnect
sub = await client.notice.subscribe("notice://prod/orders/*")
# ... connection drops ...
# No more notifications!
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
