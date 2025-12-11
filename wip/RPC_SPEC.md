# RPC Domain Specification

**Version:** 1.0  
**Status:** Implementation In Progress  
**Last Updated:** November 15, 2025  

---

## Overview

Fitz RPC provides low-latency, in-memory request/response semantics with strong correlation guarantees. RPC operations are ephemeral (never persisted) and leverage core Fitz primitives like Publish, Subscribe, Reserve, and Consume for reliable delivery and load balancing.

### Key Features

- **Correlation-based replies**: Strong request/response correlation with IDs
- **Multiple reply-routing modes**: Reply queues, hybrid signal+reserve, direct transport
- **Streaming responses**: Ordered chunks with end-of-stream signaling
- **Load balancing**: Exactly-one responder semantics via leasing
- **Backpressure handling**: Bounded memory queues with graceful degradation
- **Ephemeral storage**: All RPC state is memory-only (no persistence)

### Reply-Routing Modes

#### 1. Reply Queue (Baseline)
- Client creates dedicated reply route
- Worker publishes responses to client's reply queue
- Simple and transport-agnostic

#### 2. Hybrid Signal + Reserve (Recommended)
- Broker signals all workers with lightweight notification
- Workers race to reserve the request using leases
- Exactly-one worker processes each request

#### 3. Direct Transport Reply (Advanced)
- Responses routed directly via originating connection
- Lowest latency but requires complex reconnection handling

---

## Route Format

RPC routes follow the standard Fitz format:

```
rpc://{realm}/{area}/{resource}/{operation}
```

### Examples
- `rpc://acme/auth/user/create` - Create user
- `rpc://acme/inventory/item/update` - Update inventory item
- `rpc://cntryl/analytics/query/run` - Run analytics query

---

## Core Operations

### 1. RPC Call (Client → Worker)

**Route Operation:** `rpc://{realm}/{area}/{resource}/{operation}`  
**TLV Tags:** `TAG_ROUTE`, `TAG_BODY`, `TAG_ID`, `TAG_ROUTE_REPLY` (optional)

**Behavior:**
- Publishes request to RPC route with correlation ID
- Optionally specifies reply route for responses
- Workers receive via subscription or reservation

**Response TLV:** Success acknowledgment (request queued)

### 2. RPC Reply (Worker → Client)

**Route Operation:** Reply route specified in request  
**TLV Tags:** `TAG_ID`, `TAG_BODY`, `TAG_SEQ` (optional), `TAG_STREAM_END` (optional)

**Behavior:**
- Worker publishes response to client's reply route
- Includes correlation ID for matching
- Supports streaming with sequence numbers

**Response TLV:** Success acknowledgment

### 3. Reserve Request (Worker)

**Route Operation:** `rpc://{realm}/{area}/{resource}/{operation}/reserve`  
**TLV Tags:** `TAG_ROUTE`, `TAG_LEASE_SECS`

**Behavior:**
- Worker attempts to claim pending RPC request
- Uses lease-based exactly-once semantics
- Returns request details if lease granted

**Response TLV:** `TAG_ID`, `TAG_BODY`, `TAG_DELIVERY_TOKEN`

### 4. Acknowledge Processing (Worker)

**Route Operation:** `rpc://{realm}/{area}/{resource}/{operation}/ack`  
**TLV Tags:** `TAG_DELIVERY_TOKEN`

**Behavior:**
- Worker confirms successful processing
- Releases lease and cleans up request state

**Response TLV:** Success acknowledgment

---

## Reply-Routing Modes

### Reply Queue Mode

**Process:**
1. Client creates reply route: `rpc/reply/{client-id}`
2. Client subscribes to reply route
3. Client publishes request with `TAG_ROUTE_REPLY`
4. Worker consumes request and publishes to reply route
5. Client receives responses matched by `TAG_ID`

**Pros:** Simple, transport-agnostic, works with any subscription model  
**Cons:** Requires per-client reply route management

### Hybrid Signal + Reserve Mode

**Process:**
1. Client publishes request with `TAG_ID` and `TAG_ROUTE_REPLY`
2. Broker sends lightweight signal to all subscribers (no body)
3. Workers call reserve() to claim the request
4. Broker grants lease to one worker with full request details
5. Worker processes and publishes replies, then acknowledges

**Pros:** Exactly-one responder, minimal network fanout, natural backpressure  
**Cons:** Requires coordination between notice and queue domains

### Direct Transport Reply Mode

**Process:**
1. Client includes `TAG_REPLY_HINT` in request
2. Engine maps correlation ID to originating transport
3. Worker publishes response directly to transport connection

**Pros:** Lowest latency, no intermediate routing  
**Cons:** Complex reconnection semantics, transport coupling

---

## Data Model

### RPC Request

```rust
#[derive(Debug, Clone)]
pub struct RpcRequest {
    pub route: String,
    pub correlation_id: String,
    pub body: Vec<u8>,
    pub reply_route: Option<String>,
    pub protocol_version: Option<String>,
    pub timestamp: u64,
}
```

### RPC Response

```rust
#[derive(Debug, Clone)]
pub struct RpcResponse {
    pub correlation_id: String,
    pub body: Vec<u8>,
    pub sequence: Option<u64>,     // For streaming responses
    pub is_end_of_stream: bool,
    pub timestamp: u64,
}
```

### Lease Token

```rust
#[derive(Debug, Clone)]
pub struct LeaseToken {
    pub request_id: String,
    pub route: String,
    pub expires_at: u64,
    pub token: String,  // Cryptographically secure
}
```

---

## Flow Control and Backpressure

### Bounded Memory Queues

- **Storage:** In-memory only (no persistence)
- **Limits:** Configurable per-route capacity
- **Policy:** Reject new requests when full (`RpcError::Backpressure`)

```rust
#[derive(Debug)]
pub struct RpcQueue {
    requests: VecDeque<RpcRequest>,
    capacity: usize,
    active_leases: HashMap<String, LeaseToken>,
}
```

### Client Throttling

- **Behavior:** Clients must implement retry with jitter
- **Detection:** Monitor `RpcError::Backpressure` responses
- **Recovery:** Exponential backoff until capacity available

### Worker Acknowledgments

- **Timing:** Workers must ack promptly to release capacity
- **Failure:** Lease expiry triggers redelivery to another worker
- **Cleanup:** Expired leases automatically cleaned up

---

## Streaming Responses

### Sequence-Based Ordering

```rust
// Worker sends streaming response
let chunks = response_body.chunks(64 * 1024); // 64KB chunks
for (i, chunk) in chunks.enumerate() {
    let response = RpcResponse {
        correlation_id: request_id.clone(),
        body: chunk.to_vec(),
        sequence: Some(i as u64),
        is_end_of_stream: false,
        timestamp: now(),
    };
    publish_response(reply_route, response).await?;
}

// Mark end of stream
let end_response = RpcResponse {
    correlation_id: request_id,
    body: Vec::new(),
    sequence: Some(chunks.len() as u64),
    is_end_of_stream: true,
    timestamp: now(),
};
publish_response(reply_route, end_response).await?;
```

### Client Consumption

```rust
// Client collects streaming response
let mut chunks = Vec::new();
let mut expected_seq = 0;

while let Some(response) = receive_response().await? {
    if response.correlation_id != request_id {
        continue; // Not our response
    }

    if let Some(seq) = response.sequence {
        if seq != expected_seq {
            return Err(RpcError::OutOfOrderSequence);
        }
        expected_seq += 1;
    }

    if !response.body.is_empty() {
        chunks.push(response.body);
    }

    if response.is_end_of_stream {
        break;
    }
}

let full_response = chunks.concat();
```

---

## TLV Framing Details

### RPC Request
```
DAT Frame:
- TAG_ROUTE (0x20): "rpc://acme/auth/user/create"
- TAG_ID (0x??): "req_12345"
- TAG_BODY (0x22): <request payload>
- TAG_ROUTE_REPLY (0x??): "rpc/reply/client_abc" (optional)
```

### RPC Response
```
DAT Frame:
- TAG_ID (0x??): "req_12345"
- TAG_BODY (0x22): <response payload>
- TAG_SEQ (0x??): 0 (optional, for streaming)
- TAG_STREAM_END (0x??): (empty, marks end of stream)
```

### Reserve Request
```
REG Frame:
- TAG_ROUTE (0x20): "rpc://acme/auth/user/create"
- TAG_LEASE_SECS (0x??): 30
```

### Lease Grant
```
DAT Frame:
- TAG_ID (0x??): "req_12345"
- TAG_BODY (0x22): <request payload>
- TAG_DELIVERY_TOKEN (0x??): "lease_abc123"
```

---

## Error Handling

### Error Codes

| Code | Name | Description | Client Action |
|---|---|---|---|
| 4001 | ERR_RPC_TIMEOUT | No reply received within timeout | Retry or increase timeout |
| 4002 | ERR_RPC_NOT_FOUND | No handler for route | Check route format |
| 4003 | ERR_PERMISSION_DENIED | Unauthorized RPC access | Check authentication |
| 4004 | ERR_BACKPRESSURE | Queue capacity exceeded | Retry with backoff |
| 4005 | ERR_INVALID_TOKEN | Bad or expired lease token | Re-reserve request |
| 4006 | ERR_CORRELATION_MISMATCH | Wrong correlation ID | Check client implementation |
| 4007 | ERR_OUT_OF_ORDER_SEQUENCE | Streaming sequence gap | Restart streaming call |
| 4008 | ERR_INVALID_REPLY_ROUTE | Malformed reply route | Check route format |

### Timeout Handling

```rust
async fn rpc_call_with_timeout(
    &self,
    route: &str,
    request: RpcRequest,
    timeout: Duration,
) -> Result<RpcResponse, RpcError> {
    let start_time = Instant::now();

    // Send request
    self.publish_request(route, request.clone()).await?;

    // Wait for response with timeout
    match timeout_at(start_time + timeout, self.receive_response(request.correlation_id)).await {
        Ok(response) => Ok(response),
        Err(_) => Err(RpcError::Timeout),
    }
}
```

---

## Client and Worker Patterns

### RPC Client Helper

```rust
pub struct RpcClient {
    engine: EngineHandle,
    reply_route: String,
    timeout: Duration,
}

impl RpcClient {
    pub async fn call(&self, route: &str, body: &[u8]) -> Result<Vec<u8>, RpcError> {
        let correlation_id = generate_id();
        let request = RpcRequest {
            route: route.to_string(),
            correlation_id: correlation_id.clone(),
            body: body.to_vec(),
            reply_route: Some(self.reply_route.clone()),
            timestamp: now(),
        };

        self.engine.publish_request(route, request).await?;

        // Wait for response
        let response = self.receive_response(correlation_id, self.timeout).await?;
        Ok(response.body)
    }

    pub async fn call_stream(&self, route: &str, body: &[u8]) -> Result<impl Stream<Item = Vec<u8>>, RpcError> {
        // Similar to call() but returns streaming response
        // Collect chunks until TAG_STREAM_END
    }
}
```

### Worker (Reply Queue Mode)

```rust
async fn handle_rpc_requests() {
    loop {
        // Consume from RPC route
        let (correlation_id, body, token) = engine.reserve("rpc://acme/auth/user/create", 30).await?;

        // Process request
        let response = process_request(body).await?;

        // Send reply
        engine.publish_reply(reply_route, correlation_id, response).await?;

        // Acknowledge
        engine.ack("rpc://acme/auth/user/create", token).await?;
    }
}
```

### Worker (Signal + Reserve Mode)

```rust
async fn handle_rpc_signals() {
    // Subscribe to RPC route for signals
    engine.subscribe("rpc://acme/auth/user/create").await?;

    loop {
        // Receive signal (no body)
        let signal = receive_signal().await?;

        // Race to reserve the actual request
        match engine.reserve("rpc://acme/auth/user/create", 30).await {
            Ok((correlation_id, body, token)) => {
                // We won the lease
                let response = process_request(body).await?;
                engine.publish_reply(signal.reply_route, correlation_id, response).await?;
                engine.ack("rpc://acme/auth/user/create", token).await?;
            }
            Err(_) => {
                // Another worker got it, continue
                continue;
            }
        }
    }
}
```

---

## Configuration

### RPC Settings

```yaml
rpc:
  # Global defaults
  default_timeout_seconds: 30
  max_queue_size: 1000              # Per-route capacity
  lease_duration_seconds: 30        # Default lease time
  max_payload_size: 10485760        # 10MB

  # Route-specific overrides
  "rpc://acme/auth/**":
    timeout_seconds: 10             # Faster auth calls
    max_queue_size: 500

  "rpc://acme/analytics/**":
    timeout_seconds: 300            # Longer analytics
    max_queue_size: 100
    max_payload_size: 104857600     # 100MB for large queries
```

### Inbox Management

```yaml
inbox:
  route_prefix: "rpc/reply/"
  cleanup_interval_seconds: 60      # Periodic cleanup
  max_inboxes_per_session: 10
  inbox_ttl_seconds: 3600           # 1 hour
```

---

## Observability

### Metrics

- `rpc_requests_total{route,method}`
- `rpc_responses_total{route,status}`
- `rpc_request_duration_seconds{route}`
- `rpc_queue_size{route}`
- `rpc_active_leases{route}`
- `rpc_timeouts_total{route}`
- `rpc_backpressure_total{route}`

### Logging

```json
{
  "timestamp": "2025-11-15T10:30:00Z",
  "level": "info",
  "message": "rpc_request_queued",
  "route": "rpc://acme/auth/user/create",
  "correlation_id": "req_12345",
  "queue_size": 5
}
```

```json
{
  "timestamp": "2025-11-15T10:30:05Z",
  "level": "info",
  "message": "rpc_request_processed",
  "route": "rpc://acme/auth/user/create",
  "correlation_id": "req_12345",
  "duration_ms": 150,
  "worker_id": "worker_001"
}
```

---

## Implementation Status

### ✅ Completed
- TLV format definitions and correlation semantics
- Route format standardization
- Reply-routing mode designs (all three modes)
- Flow control and backpressure design
- Error model and codes

### 🚧 In Progress
- Core RPC domain handler implementation
- Bounded in-memory queue management
- Notice integration for hybrid signal+reserve
- Inbox lifecycle and security
- Streaming response support

### 📋 TODO
- Direct transport reply optimization
- Advanced load balancing policies
- Request deduplication and idempotency
- Cross-realm RPC calls
- RPC service discovery

---

## Testing Requirements

### Unit Tests (48 total)
- **Basic RPC:** Request delivery, reply correlation, handler dispatch
- **Inbox Management:** Allocation, security, cleanup, collision prevention
- **Streaming:** Ordered chunks, end-of-stream, large payload handling
- **Concurrency:** Isolation by correlation ID, concurrent calls
- **Error Handling:** Timeouts, invalid routes, crash recovery
- **Load Balancing:** Single handler per request, distribution across workers
- **Idempotency:** Request deduplication, cancellation support

### Integration Tests
- End-to-end RPC call with reply queue mode
- Hybrid signal+reserve with multiple workers
- Streaming response collection and ordering
- Backpressure handling and client retry logic
- Worker crash recovery and request redelivery

### Performance Benchmarks
- RPC call latency (round-trip)
- Queue throughput under load
- Streaming response bandwidth
- Concurrent worker scaling
- Memory usage per queued request

---

## Usage Patterns

### Synchronous RPC Call

```rust
// Client makes synchronous call
let client = RpcClient::new(engine, "rpc/reply/client_123");
let request = b"{\"user_id\": 123, \"action\": \"get_profile\"}";
let response = client.call("rpc://acme/auth/user/get", request, Duration::from_secs(10)).await?;
let profile: UserProfile = serde_json::from_slice(&response)?;
```

### Streaming RPC Response

```rust
// Client handles streaming response
let mut stream = client.call_stream("rpc://acme/analytics/query/run", query).await?;
let mut results = Vec::new();

while let Some(chunk) = stream.next().await {
    let partial: QueryResult = serde_json::from_slice(&chunk)?;
    results.push(partial);

    if results.len() >= 1000 {
        // Process batch
        process_batch(&results).await?;
        results.clear();
    }
}
```

### Worker Implementation

```rust
// Worker processes RPC requests
async fn auth_worker(engine: &EngineHandle) {
    loop {
        // Reserve request (hybrid mode)
        let (correlation_id, body, token) = engine
            .reserve("rpc://acme/auth/user/create", 30)
            .await?;

        // Process request
        let user_req: CreateUserRequest = serde_json::from_slice(&body)?;
        let user = create_user(user_req).await?;
        let response = serde_json::to_vec(&user)?;

        // Send reply
        engine.publish_reply(correlation_id, response).await?;

        // Acknowledge
        engine.ack(token).await?;
    }
}
```

---

*See OVERVIEW.md for system-level context and other domain specifications.*</content>
<parameter name="filePath">d:\repos\cntryl\fitz\docs\RPC_SPEC.md