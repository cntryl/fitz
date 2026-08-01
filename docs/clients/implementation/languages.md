
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

**3. Notice subscriptions as bounded streams**

```rust
use futures_core::Stream;
use std::{pin::Pin, task::{Context, Poll}};
use tokio_stream::wrappers::BroadcastStream;

pub struct Subscription {
    // Wire IDs stay private and may change during reconnect restoration.
    registration: RestorableRegistration,
    receiver: BroadcastStream<Vec<u8>>,
}

impl Stream for Subscription {
    type Item = Result<Notice, NoticeError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>)
        -> Poll<Option<Self::Item>>
    {
        // Decode only notifications for registration.wire_id(). A lagged
        // bounded receiver terminates with typed backpressure.
        todo!()
    }
}

impl NoticeClient {
    pub async fn subscribe(&self, pattern: &str) -> Result<Subscription, NoticeError> {
        let req = SubscribeRequest {
            pattern: pattern.to_string(),
        };
        
        let resp = self.client.send_request(MessageType::Subscribe, req).await?;
        
        self.register_bounded_subscription(pattern, resp.subscription_id).await
    }
}

// Usage
let mut sub = client.notice()?.subscribe("notice://prod/orders/*").await?;
while let Some(notice) = sub.next().await {
    let notice = notice?;
    println!("Route: {}, Payload: {:?}", notice.route, notice.payload);
}
```

**4. Builder pattern for config**

```rust
// Usage
let client = Client::builder("ws://localhost:4090/ws", token_provider)
    .request_timeout(Duration::from_secs(30))
    .max_in_flight_requests(256)
    .reconnect_policy(ReconnectPolicy::default())
    .build()?;
client.connect().await?;
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
- ✅ Keep write admission bounded and response/notification dispatch independent
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
export interface Client {
    readonly kv: KvClient;
    readonly queue: QueueClient;
    readonly rpc: RpcClient;
    readonly lease: LeaseClient;
    readonly notice: NoticeClient;
    readonly stream: StreamClient;
    readonly schedule: ScheduleClient;
    connect(): Promise<void>;
    close(): Promise<void>;
}

// Closure-backed factories may return plain objects. Domain properties are
// readonly and created lazily from the active connection.
export function createClient(config: ClientConfig): Client;
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
