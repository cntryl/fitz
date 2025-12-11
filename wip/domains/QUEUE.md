# Queue Domain Specification

**Version:** 1.0  
**Status:** Implementation in Progress  
**Last Updated:** November 15, 2025  

---

## Overview

Fitz Queues provide durable, at-least-once message delivery with lease-based processing semantics. Queues are designed for reliable task distribution, job processing, and any scenario requiring guaranteed delivery with worker-based consumption.

### Key Features

- **Durable Storage**: Messages persisted across broker restarts
- **Lease Semantics**: Time-bound message ownership with automatic redelivery
- **At-Least-Once Delivery**: Guaranteed delivery with deduplication support
- **Dead Letter Queue**: Automatic handling of poison messages
- **Visibility Timeout**: Configurable message lease durations
- **Batch Processing**: Support for processing multiple messages per lease

### Differences from Streams

| Feature | Queues | Streams |
|---------|--------|---------|
| Persistence | Durable, consumed | Durable, replayable |
| Ordering | Best-effort | Strict sequence numbers |
| Consumption | Single worker, destructive | Multiple readers, non-destructive |
| Use Cases | Task distribution, job queues | Event sourcing, audit logs |

---

## Route Format

Queue routes follow the standard Fitz format:

```
queue://{realm}/{area}/{resource}[/{operation}]
```

### Examples
- `queue://acme/jobs/thumbnail` - Basic queue
- `queue://acme/orders/processing` - Order processing queue
- `queue://acme/alerts/dlq` - Dead letter queue

---

## Core Operations

### 1. Enqueue (Publish)

**Route Operation:** `queue://{realm}/{area}/{resource}`  
**TLV Tags:** `TAG_ROUTE`, `TAG_BODY`, `TAG_ID` (optional dedupe key)

**Behavior:**
- Stores message durably with generated message ID
- Supports optional client-provided deduplication key
- Returns message ID in response

**Response TLV:** `TAG_ID` (message ID)

### 2. Lease (Reserve)

**Route Operation:** `queue://{realm}/{area}/{resource}/reserve`  
**TLV Tags:** `TAG_ROUTE`, `TAG_LEASE` (visibility seconds), `TAG_BATCH_SIZE` (optional)

**Behavior:**
- Returns next available message(s) with lease token
- Message becomes invisible to other consumers for lease duration
- Supports batch leasing (default: 1, max: configurable)

**Response TLV:** `TAG_ID`, `TAG_BODY`, `TAG_DELIVERY_TOKEN`, `TAG_LEASE`

### 3. Extend Lease

**Route Operation:** `queue://{realm}/{area}/{resource}/extend`  
**TLV Tags:** `TAG_ROUTE`, `TAG_ID`, `TAG_DELIVERY_TOKEN`, `TAG_LEASE` (additional seconds)

**Behavior:**
- Extends visibility timeout for leased message
- Requires valid delivery token
- Maximum extension limits apply

**Response TLV:** Success acknowledgment

### 4. Complete (Acknowledge)

**Route Operation:** `queue://{realm}/{area}/{resource}/complete`  
**TLV Tags:** `TAG_ROUTE`, `TAG_ID`, `TAG_DELIVERY_TOKEN`

**Behavior:**
- Removes message from queue permanently
- Requires valid delivery token
- Invalid tokens result in error

**Response TLV:** Success acknowledgment

### 5. Peek

**Route Operation:** `queue://{realm}/{area}/{resource}/peek`  
**TLV Tags:** `TAG_ROUTE`

**Behavior:**
- Returns next available message without leasing
- Message remains visible to other consumers
- Read-only operation for inspection

**Response TLV:** `TAG_ID`, `TAG_BODY`

---

## Data Model

### QueueMessage Structure

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueMessage {
    // Identity
    pub id: String,                    // Server-generated unique ID
    pub route: String,                 // Original enqueue route

    // Content
    pub body: Vec<u8>,                 // Message payload (opaque bytes)
    pub metadata: Option<Vec<u8>>,     // Optional metadata (CBOR/JSON)

    // Lifecycle
    pub created_at: u64,               // Unix timestamp (seconds)
    pub lease_until: Option<u64>,      // Current lease expiration
    pub delivery_count: u32,           // Redelivery attempts

    // Internal
    pub delivery_token: Option<String>, // Lease token (HMAC)
}
```

### Lease Token Security

Delivery tokens use HMAC-SHA256 with server-side secret:
```
token = HMAC_SHA256(secret_key, route + message_id + lease_until)
```

- **Time-bound**: Tokens include expiration timestamp
- **Route-scoped**: Cannot be used across different queues
- **Single-use**: Tokens invalidated after completion

---

## Storage Interface

```rust
#[async_trait]
pub trait QueueStore {
    /// Enqueue a message, returning its ID
    async fn enqueue(
        &mut self,
        route: &str,
        message: QueueMessage,
        dedupe_key: Option<&str>
    ) -> Result<String, StoreError>;

    /// Lease next available messages
    async fn lease(
        &mut self,
        route: &str,
        visibility_ms: u32,
        max_batch: usize
    ) -> Result<Vec<QueueMessage>, StoreError>;

    /// Extend lease on a message
    async fn extend_lease(
        &mut self,
        route: &str,
        message_id: &str,
        delivery_token: &str,
        additional_ms: u32
    ) -> Result<u32, StoreError>;

    /// Complete (acknowledge) a message
    async fn complete(
        &mut self,
        route: &str,
        message_id: &str,
        delivery_token: &str
    ) -> Result<(), StoreError>;

    /// Peek at next message without leasing
    async fn peek(
        &self,
        route: &str
    ) -> Result<Option<QueueMessage>, StoreError>;

    /// List available queues (admin)
    async fn list_queues(
        &self,
        prefix: &str
    ) -> Result<Vec<String>, StoreError>;
}
```

---

## Processing Patterns

### Basic Worker Loop

```rust
async fn worker_loop(queue_route: &str, lease_duration: u32) {
    loop {
        // Lease messages
        let messages = queue_store.lease(queue_route, lease_duration, 1).await?;

        for message in messages {
            // Process message
            match process_message(&message).await {
                Ok(_) => {
                    // Acknowledge successful processing
                    queue_store.complete(queue_route, &message.id, &message.delivery_token.unwrap()).await?;
                }
                Err(_) => {
                    // Message will be redelivered when lease expires
                    // Consider DLQ after max retries
                }
            }
        }

        // Brief pause if no messages
        if messages.is_empty() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}
```

### Reliable Processing with Extension

```rust
async fn reliable_worker(queue_route: &str) {
    let messages = queue_store.lease(queue_route, 300_000, 1).await?; // 5 min lease

    for message in messages {
        // Start processing in background
        let processing = tokio::spawn(process_heavy_task(message.body.clone()));

        // Extend lease periodically
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        loop {
            tokio::select! {
                result = &mut processing => {
                    match result {
                        Ok(Ok(_)) => {
                            queue_store.complete(queue_route, &message.id, &message.delivery_token.unwrap()).await?;
                        }
                        _ => {
                            // Let lease expire for redelivery
                        }
                    }
                    break;
                }
                _ = interval.tick() => {
                    // Extend lease by another 5 minutes
                    queue_store.extend_lease(queue_route, &message.id, &message.delivery_token.unwrap(), 300_000).await?;
                }
            }
        }
    }
}
```

---

## Dead Letter Queue (DLQ)

### Automatic DLQ Movement

Messages that exceed `max_delivery_attempts` are moved to DLQ:

```rust
const MAX_DELIVERY_ATTEMPTS: u32 = 5;
const DLQ_SUFFIX: &str = ".dlq";

// In lease operation:
if message.delivery_count >= MAX_DELIVERY_ATTEMPTS {
    let dlq_route = format!("{}{}", route.trim_end_matches(DLQ_SUFFIX), DLQ_SUFFIX);
    queue_store.move_to_dlq(&dlq_route, &message.id).await?;
    continue; // Skip this message in lease response
}
```

### DLQ Route Format
- Original: `queue://acme/jobs/process`
- DLQ: `queue://acme/jobs/process.dlq`

### DLQ Processing
- DLQ messages can be inspected with `peek` operations
- Manual reprocessing via `enqueue` to original queue
- Separate monitoring and alerting for DLQ growth

---

## Error Handling

### Error Codes

| Code | Name | Description | Client Action |
|---|---|---|---|
| 1001 | ERR_UNAUTHORIZED | Invalid delivery token | Check token validity |
| 1002 | ERR_NOT_FOUND | Message not found | Message may be completed/expired |
| 1003 | ERR_LEASE_EXPIRED | Lease timeout exceeded | Re-lease message |
| 1004 | ERR_ALREADY_COMPLETED | Message already acknowledged | Idempotent operation |
| 1005 | ERR_INVALID_TOKEN | Malformed delivery token | Check token format |
| 1006 | ERR_BACKPRESSURE | Queue at capacity | Retry with backoff |

### Retry Semantics

- **Lease Expiration**: Automatic redelivery after visibility timeout
- **Token Errors**: Client should re-lease message
- **Backpressure**: Exponential backoff (50ms → 100ms → 200ms → 400ms)
- **Network Errors**: Safe to retry (operations are idempotent with tokens)

---

## Configuration

### Queue-Level Settings

```yaml
queues:
  # Realm-level defaults
  "queue://acme/**":
    max_delivery_attempts: 5
    default_lease_seconds: 120
    max_batch_size: 10

  # Area-specific overrides
  "queue://acme/jobs/**":
    max_delivery_attempts: 3
    default_lease_seconds: 300

  # Queue-specific settings
  "queue://acme/jobs/high-priority":
    max_delivery_attempts: 10
    default_lease_seconds: 60
```

### Global Limits

```yaml
limits:
  max_message_size: 1048576        # 1MB
  max_queue_depth: 10000          # Per queue
  max_batch_size: 100             # Lease batch limit
  max_lease_extension: 3600       # 1 hour max extension
```

---

## Observability

### Metrics

- `queue_messages_enqueued_total{route}`
- `queue_messages_leased_total{route}`
- `queue_messages_completed_total{route}`
- `queue_messages_redelivered_total{route}`
- `queue_dlq_messages_total{route}`
- `queue_lease_duration_seconds{route}`
- `queue_depth{route}`

### Logging

```json
{
  "timestamp": "2025-11-15T10:30:00Z",
  "level": "info",
  "message": "message_completed",
  "route": "queue://acme/jobs/process",
  "message_id": "msg_12345",
  "processing_duration_ms": 2500,
  "delivery_count": 1
}
```

---

## Implementation Status

### ✅ Completed
- Queue message data structures
- Lease token generation and validation
- Basic enqueue/lease/complete operations
- Storage interface abstraction
- Error handling framework

### 🚧 In Progress
- DLQ automatic movement
- Batch lease operations
- Lease extension logic
- Deduplication support
- Admin listing APIs

### 📋 TODO
- Cloud storage backend integration
- Queue depth monitoring
- Configurable retry policies
- Message priority support
- FIFO ordering guarantees

---

## Testing Requirements

### Unit Tests
- Happy path: enqueue → lease → complete
- Lease expiration: enqueue → lease → wait → re-lease
- Invalid token: lease → attempt complete with wrong token
- DLQ movement: repeated failures → automatic DLQ
- Batch operations: lease multiple messages
- Concurrent access: multiple workers leasing simultaneously

### Integration Tests
- End-to-end worker processing loop
- Lease extension during long-running tasks
- Network interruption recovery
- Storage backend failover
- Multi-tenant isolation

### Performance Benchmarks
- Enqueue throughput (messages/second)
- Lease latency (p95, p99)
- Concurrent worker scaling
- Storage backend comparison

---

## Migration Notes

### From Legacy Queue Systems

**SQS Migration:**
- Map SQS queues to Fitz queue routes
- Configure appropriate lease durations
- Implement DLQ equivalent with `.dlq` suffix
- Update worker token handling

**Redis Queue Migration:**
- Preserve ordering if required
- Implement deduplication for at-least-once conversion
- Add lease semantics for reliability

**Custom Queue Migration:**
- Map existing APIs to Fitz operations
- Implement lease-based processing pattern
- Add delivery count tracking for poison message handling

---

*See ARCHITECTURE.md for system-level context and other domain specifications.*</content>
<parameter name="filePath">d:\repos\cntryl\fitz\docs\QUEUE_SPEC.md