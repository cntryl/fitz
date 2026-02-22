# cntryl-rs: Fitz Client Library

A high-performance, fully type-safe Rust client library for the [Fitz](../README.md) distributed actor broker.

## Features

- ✅ **Zero coupling to server**: Imports only from standard Rust crates and internal modules
- ✅ **Multi-transport**: Supports TCP and WebSocket equally with runtime selection
- ✅ **All 7 domains**: Full implementation for KV, Queue, Notice, RPC, Lease, Stream, and Schedule
- ✅ **Transaction support**: Full ACID transaction semantics for KV domain
- ✅ **Embedded authentication**: Built-in JWT token generation for testing
- ✅ **Synchronous API**: Blocking API over async tokio runtime (no callback hell)
- ✅ **Type-safe**: Compile-time routes and message types via Rust enums

## Quick Start

### Add to Cargo.toml

```toml
[dependencies]
cntryl = { path = "./cntryl-rs" }
```

### Basic Usage: TCP

```rust
use cntryl::{FitzClient, protocol::TransactionMode};

// Connect to Fitz server
let client = FitzClient::connect_tcp(
    "127.0.0.1",
    4091,
    "my-realm",
    "shared-secret"
)?;

// Get KV client
let kv = client.kv();

// Begin transaction
let mut tx = kv.begin("app", "users", TransactionMode::ReadWrite)?;

// Put / Get / Delete
tx.put(b"alice", b"Alice Johnson")?;
let value = tx.get(b"alice")?.expect("not found");
tx.delete(b"alice")?;

// Commit or rollback
tx.commit()?;
// or: tx.rollback()?;

// Close connection
client.close()?;
```

### Using WebSocket Instead

```rust
// Same API, just swap the connect method
let client = FitzClient::connect_ws(
    "ws://127.0.0.1:4092/fitz",
    "my-realm",
    "shared-secret"
)?;

// Everything else is identical - transport is abstracted
let kv = client.kv();
let mut tx = kv.begin("app", "users", TransactionMode::ReadWrite)?;
// ...
```

## Architecture

### Transport Abstraction

The library uses a **trait-based transport abstraction**, allowing both TCP and WebSocket to be used interchangeably:

```
FitzClient
    └─ Arc<Mutex<FitzConnection>>
           └─ AnyTransport (enum)
                  ├─ Tcp (TcpTransport)
                  └─ WebSocket (WebSocketTransport)
```

### Wire Protocol

All communication uses **TLV (Tag-Length-Value) encoding**:

- **Message frames**: `[u32 BE length][u8 msg_type or u16 msg_type][tlv payload]`
- **TCP**: Length-prefixed frames
- **WebSocket**: Binary message per frame
- **Authentication**: HS256 JWT in CONNECT message

### Message Types (By Domain)

| Domain | Range | Constants |
|--------|-------|-----------|
| KV | 100-199 | `BEGIN`, `GET`, `PUT`, `DELETE`, `COMMIT`, `ROLLBACK` |
| Queue | 200-299 | `SEND`, `RECEIVE`, `ACK` |
| Notice | 300-399 | `PUBLISH`, `SUBSCRIBE`, `UNSUBSCRIBE` |
| RPC | 400-499 | `SEND`, `RESPONSE` |
| Lease | 500-599 | `ACQUIRE`, `EXTEND`, `RELEASE` |
| Stream | 600-699 | `OPEN`, `WRITE`, `READ` |
| Schedule | 700-799 | `SCHEDULE`, `CANCEL` |

## Domain Clients

### KV (Key-Value)

Full transaction support with ACID semantics:

```rust
let kv = client.kv();
let mut tx = kv.begin("area", "resource", TransactionMode::ReadWrite)?;

tx.put(key, value)?;           // Write value
let v = tx.get(key)?;           // Read value (Option<Vec<u8>>)
tx.delete(key)?;               // Delete value

tx.commit()?;                  // Commit all changes
// or
tx.rollback()?;                // Discard all changes
```

**Features:**
- Read-Write and Read-Only modes
- Transaction isolation
- Rollback support

### Queue (Coming Soon)

```rust
let queue = client.queue();
queue.send("queue-area", "queue-name", message)?;
let item = queue.receive("queue-area", "queue-name")?;
queue.ack(item.id)?;
```

### Notice (Pub/Sub)

```rust
let notice = client.notice();
notice.publish("area", "topic", message)?;

let subscriber = notice.subscribe("area", "topic/*")?;  // Supports * and ** wildcards
while let Some(msg) = subscriber.next()? {
    println!("Received: {:?}", msg);
}
```

### RPC (Remote Procedure Call)

```rust
let rpc = client.rpc();
let response = rpc.send("area", "service", request)?;
```

### Lease (Distributed Locks)

```rust
let lease = client.lease();
let grant = lease.acquire("area", "resource", duration)?;
grant.extend()?;
grant.release()?;
```

### Stream (Named Channels)

```rust
let stream = client.stream();
let writer = stream.open_write("area", "stream-name")?;
writer.write(data)?;

let reader = stream.open_read("area", "stream-name")?;
while let Some(chunk) = reader.read()? {
    println!("Got chunk: {:?}", chunk);
}
```

### Schedule (Job Scheduling)

```rust
let schedule = client.schedule();
let job_id = schedule.schedule("area", scheduled_time, job_spec)?;
schedule.cancel(job_id)?;
```

## Testing

### Unit Tests

```bash
cargo test --lib
```

Tests TLV codec, authentication, connection management.

### Integration Tests (Requires Running Server)

```bash
# TCP tests
cargo test integration_kv_tcp -- --ignored --nocapture

# WebSocket tests
cargo test integration_kv_websocket -- --ignored --nocapture

# Both transports with same tests
cargo test integration_multiprotocol -- --ignored --nocapture
```

To run integration tests, start the Fitz server first:

```bash
# In the fitz root directory
cargo run -F boot
```

Server will listen on:
- **TCP**: `127.0.0.1:4091`
- **WebSocket**: `ws://127.0.0.1:4092/fitz`

## Error Handling

All operations return `Result<T, FitzError>`:

```rust
use cntryl::error::FitzError;

match client.kv().begin("app", "data", mode) {
    Ok(tx) => { /* use transaction */ },
    Err(FitzError::Connection(msg)) => { /* connection error */ }
    Err(FitzError::Protocol(msg)) => { /* protocol error */ }
    Err(FitzError::Transport(msg)) => { /* transport I/O error */ }
    Err(FitzError::Timeout) => { /* operation timeout */ }
    Err(e) => { /* other error */ }
}
```

## Performance

The library is optimized for:

1. **Minimal allocations** in hot paths
2. **Zero-copy** frame handling where possible
3. **Connection pooling** ready (implement via Arc<Client>)
4. **Synchronous blocking API** to avoid scheduler overhead

For async use, `block_in_place` integrates cleanly with tokio:

```rust
// Inside async context
let result = tokio::task::block_in_place(|| {
    client.kv().begin("app", "data", mode)
})?;
```

## Troubleshooting

### Connection Refused

- Verify Fitz server is running: `cargo run -F boot` in repo root
- Check host/port: defaults are `127.0.0.1:4091` (TCP), `ws://127.0.0.1:4092/fitz` (WS)
- Check realm matches between client and server expectations

### Authentication Failed

- Secret key must match server configuration
- JWT token generation uses HS256, verify algorithm compatibility

### Transaction Errors

- Check `TransactionMode`: `ReadWrite` for modifications, `ReadOnly` for reads
- Verify resource exists (server creates on demand)
- Rollback on error; don't reuse failed transaction

### Frame Too Large

- KV payloads are limited by frame size; split large operations
- Individual put/get is not constrained by individual key size but by maximum frame size

## Architecture Decisions

### Why Sync API Over Async?

The client provides a **synchronous blocking API** despite using tokio internally, because:

1. **Determinism**: No scheduler jitter for latency-sensitive operations
2. **Simplicity**: Synchronous code is easier to reason about and test
3. **Interop**: Works in both sync and async contexts (via `block_in_place`)
4. **Performance**: Reduces task overhead for high-throughput scenarios

### Why Transport Trait?

The `Transport` trait abstraction enables:

1. **Protocol flexibility**: Easy to add gRPC, HTTP/3, or other protocols
2. **Testing**: Mock transport for unit tests without server
3. **Runtime selection**: Choose TCP or WebSocket based on environment/config
4. **Zero coupling**: Transport layer independent of domain logic

### Why Zero Server Coupling?

This design enables:

1. **Standalone client library**: Can be distributed independently
2. **Version compatibility**: Server updates don't break client compilation
3. **Unit testing**: Tests don't depend on server internals
4. **Reusability**: Client works with compatible server implementations

## Contributing

See [CONTRIBUTING.md](../CONTRIBUTING.md) for guidelines.

## License

Same as Fitz: See [LICENSE](../LICENSE)
