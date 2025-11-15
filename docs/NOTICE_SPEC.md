# Notice Domain Specification

**Version:** 1.0  
**Status:** Implementation Complete  
**Last Updated:** November 15, 2025  

---

## Overview

Fitz Notices provide fire-and-forget informational events with low-latency delivery to active subscribers. Notices prioritize speed and simplicity over guaranteed durability, making them ideal for real-time notifications, monitoring alerts, and status updates.

### Key Features

- **Fire-and-forget delivery**: No acknowledgments required by default
- **Session-scoped subscriptions**: Subscriptions tied to transport connections
- **Route-based filtering**: Hierarchical matching with wildcards
- **Backpressure handling**: Configurable policies for slow subscribers
- **Optional reliable mode**: Delivery tokens and acknowledgments (future)
- **In-memory registry**: Fast subscription matching and dispatch

### Use Cases

- Real-time monitoring alerts
- Status updates and heartbeats
- Event notifications (non-critical)
- Chat messages and broadcasts
- Configuration change notifications

---

## Route Format

Notice routes follow the standard Fitz format:

```
notice://{realm}/{area}/{resource}[/{operation}]
```

### Examples
- `notice://acme/monitoring/alerts` - System alerts
- `notice://acme/chat/general` - General chat messages
- `notice://acme/orders/status` - Order status updates

---

## Core Operations

### 1. Subscribe

**Route Operation:** `notice://{realm}/{area}/{resource}`  
**TLV Tags:** `TAG_ROUTE`, `TAG_SUBSCRIBE`

**Behavior:**
- Registers interest in notifications for the specified route
- Subscriptions are session-scoped (tied to transport connection)
- Supports wildcard matching for hierarchical routing

**Response TLV:** `TAG_ROUTE` (echoed in ACK)

### 2. Unsubscribe

**Route Operation:** `notice://{realm}/{area}/{resource}`  
**TLV Tags:** `TAG_ROUTE`, `TAG_UNSUBSCRIBE`

**Behavior:**
- Removes subscription for the specified route
- Cleans up any buffered notifications for this subscriber

**Response TLV:** `TAG_ROUTE` (echoed in ACK)

### 3. Publish (Implicit)

**Route Operation:** Any notice route  
**TLV Tags:** `TAG_ROUTE`, `TAG_BODY`

**Behavior:**
- Sends notification to all active subscribers matching the route
- Uses DAT frame with TAG_NOTIFICATION marker
- Best-effort delivery (no persistence required)

**Response TLV:** Success acknowledgment

---

## Subscription Models

### Session-Scoped Subscriptions (Implemented)

- **Lifetime**: Tied to transport connection
- **Cleanup**: Automatic removal on disconnect
- **Storage**: In-memory registry only
- **Persistence**: None (subscriptions lost on server restart)

### Persistent Subscriptions (Future)

- **Lifetime**: Survive disconnects and server restarts
- **Storage**: Requires durable subscription registry
- **Buffering**: Server buffers notifications for offline subscribers
- **Reconnection**: Deliver buffered notifications on reconnect

---

## Delivery Semantics

### Best-Effort Delivery (Implemented)

- **Guarantees**: No delivery guarantees for disconnected subscribers
- **Timing**: Immediate dispatch to active subscribers
- **Storage**: Optional in-memory buffering (no persistence)
- **Acks**: Not required (fire-and-forget)

### Reliable Delivery (Future)

- **Guarantees**: At-least-once delivery with acknowledgments
- **Tokens**: Delivery tokens for tracking pending notifications
- **Retries**: Automatic retry on failed delivery
- **DLQ**: Dead letter queue for persistently failing deliveries

---

## Route Matching and Filtering

### Matching Rules

Fitz supports hierarchical route matching with wildcards:

#### Exact Match
```
Pattern: "a/b/c"
Matches: "a/b/c"
No Match: "a/b/d", "a/b/c/d"
```

#### Prefix Wildcard
```
Pattern: "a/b/*"
Matches: "a/b/c", "a/b/d", "a/b/c/d"
No Match: "a/c/d", "x/b/c"
```

#### Hierarchical Prefix
```
Pattern: "a/b"
Matches: "a/b", "a/b/c", "a/b/c/d"
No Match: "a/c", "x/b/c"
```

#### Global Wildcard
```
Pattern: "*"
Matches: All routes
```

### Implementation

```rust
#[derive(Debug, Clone)]
pub struct Subscription {
    pub route_pattern: String,
    pub transport_id: String,
    pub channel_id: Option<String>,
    pub created_at: u64,
}

impl Subscription {
    pub fn matches(&self, notification_route: &str) -> bool {
        if self.route_pattern == "*" {
            return true;
        }

        let pattern_parts: Vec<&str> = self.route_pattern.split('/').collect();
        let route_parts: Vec<&str> = notification_route.split('/').collect();

        if pattern_parts.len() > route_parts.len() {
            return false;
        }

        for (i, &pattern_part) in pattern_parts.iter().enumerate() {
            match pattern_part {
                "*" => return true, // Trailing wildcard matches rest
                part if part == route_parts[i] => continue,
                _ => return false,
            }
        }

        // Exact match or prefix match
        pattern_parts.len() == route_parts.len() || pattern_parts.last() == Some(&"*")
    }
}
```

---

## Backpressure Handling

### Per-Subscriber Buffering

Each subscription maintains a bounded async channel:

```rust
#[derive(Debug)]
pub struct SubscriberChannel {
    pub sender: mpsc::Sender<Notification>,
    pub buffer_size: usize,
    pub drop_count: AtomicUsize,
}
```

### Drop Policy (Implemented)

- **Behavior**: When subscriber channel is full, drop the notification
- **Logging**: Increment drop counter for monitoring
- **Continuation**: Server continues without blocking publisher

```rust
async fn send_notification(&self, notification: Notification) -> Result<(), NoticeError> {
    match self.sender.try_send(notification) {
        Ok(()) => Ok(()),
        Err(TrySendError::Full(_)) => {
            self.drop_count.fetch_add(1, Ordering::Relaxed);
            Ok(()) // Drop silently, don't block
        }
        Err(TrySendError::Closed(_)) => {
            Err(NoticeError::SubscriberDisconnected)
        }
    }
}
```

### Alternative Policies (Future)

- **Close Policy**: Close subscription on backpressure
- **Block Policy**: Buffer until subscriber drains (risky)
- **DLQ Policy**: Send failed notifications to dead letter queue

---

## Data Model

### Notification Structure

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Notification {
    pub route: String,
    pub body: Vec<u8>,
    pub metadata: Option<Vec<u8>>, // Optional CBOR/JSON metadata
    pub timestamp: u64,           // Unix timestamp
}
```

### Subscription Registry

```rust
#[derive(Debug)]
pub struct NoticeRegistry {
    subscriptions: HashMap<String, Vec<Subscription>>, // route_pattern -> subscriptions
    transport_subscriptions: HashMap<String, HashSet<String>>, // transport_id -> route_patterns
}
```

---

## TLV Framing Details

### Subscribe Request
```
REG Frame:
- TAG_ROUTE (0x20): "notice://acme/alerts/system"
- TAG_SUBSCRIBE (0x90): (empty)

ACK Response:
- TAG_ROUTE (0x20): "notice://acme/alerts/system"
```

### Unsubscribe Request
```
REG Frame:
- TAG_ROUTE (0x20): "notice://acme/alerts/system"
- TAG_UNSUBSCRIBE (0x91): (empty)

ACK Response:
- TAG_ROUTE (0x20): "notice://acme/alerts/system"
```

### Notification Delivery
```
DAT Frame:
- TAG_NOTIFICATION (0x92): (empty marker)
- TAG_ROUTE (0x20): "notice://acme/alerts/system"
- TAG_BODY (0x22): <notification payload>
```

---

## Error Handling

### Error Codes

| Code | Name | Description | Client Action |
|---|---|---|---|
| 3001 | ERR_INVALID_SUBSCRIPTION_ROUTE | Malformed route pattern | Check route format |
| 3002 | ERR_SUBSCRIPTION_EXISTS | Already subscribed to route | Unsubscribe first or ignore |
| 3003 | ERR_SUBSCRIPTION_NOT_FOUND | Not subscribed to route | Subscribe first |
| 3004 | ERR_TRANSPORT_DISCONNECTED | Transport connection lost | Reconnect and resubscribe |
| 3005 | ERR_BACKPRESSURE_LIMIT | Subscriber channel full | Slow down or increase buffer |

### Protocol Validation

```rust
fn validate_subscription_request(frame: &Frame) -> Result<(), NoticeError> {
    // Must have TAG_ROUTE
    let route = frame.get_string(TAG_ROUTE)
        .ok_or(NoticeError::MissingRoute)?;

    // Must have either TAG_SUBSCRIBE or TAG_UNSUBSCRIBE (not both)
    let has_subscribe = frame.has_tag(TAG_SUBSCRIBE);
    let has_unsubscribe = frame.has_tag(TAG_UNSUBSCRIBE);

    match (has_subscribe, has_unsubscribe) {
        (true, false) => Ok(()),
        (false, true) => Ok(()),
        _ => Err(NoticeError::InvalidSubscriptionRequest),
    }
}
```

---

## Configuration

### Notice Settings

```yaml
notices:
  # Global defaults
  default_buffer_size: 100      # Per-subscriber notification buffer
  max_subscriptions_per_transport: 50
  cleanup_interval_seconds: 30  # Periodic cleanup of dead transports

  # Route-specific overrides
  "notice://acme/monitoring/**":
    buffer_size: 1000           # Larger buffer for monitoring
    reliable_mode: false

  "notice://acme/chat/**":
    buffer_size: 50             # Smaller buffer for chat
    reliable_mode: false
```

### Backpressure Policies

```yaml
backpressure:
  policy: drop                  # drop | close | block
  max_buffer_size: 1000
  drop_threshold: 0.8          # Drop when 80% full
```

---

## Observability

### Metrics

- `notice_subscriptions_active_total{route_pattern}`
- `notice_notifications_published_total{route}`
- `notice_notifications_delivered_total{route}`
- `notice_notifications_dropped_total{route,reason}`
- `notice_subscriptions_per_transport`
- `notice_publish_latency_seconds`

### Logging

```json
{
  "timestamp": "2025-11-15T10:30:00Z",
  "level": "info",
  "message": "notification_published",
  "route": "notice://acme/alerts/system",
  "subscriber_count": 5,
  "body_size_bytes": 256
}
```

```json
{
  "timestamp": "2025-11-15T10:30:05Z",
  "level": "warn",
  "message": "notification_dropped",
  "route": "notice://acme/alerts/system",
  "transport_id": "ws_12345",
  "reason": "backpressure",
  "drop_count": 1
}
```

---

## Implementation Status

### ✅ Completed
- Session-scoped subscriptions with automatic cleanup
- Hierarchical route matching with wildcards
- Best-effort delivery with backpressure handling
- In-memory subscription registry
- TLV framing for subscribe/unsubscribe/publish
- Transport disconnection handling

### 🚧 In Progress
- Persistent subscriptions (storage integration)
- Reliable delivery mode with acknowledgments
- Advanced backpressure policies (close/block)
- Subscription metadata and filtering

### 📋 TODO
- Cross-transport subscription sharing
- Subscription groups and permissions
- Notification batching and compression
- Subscription persistence across server restarts

---

## Testing Requirements

### Unit Tests
- Route matching: exact, prefix, wildcard patterns
- Subscription lifecycle: subscribe/unsubscribe/cleanup
- Backpressure handling: drop policy implementation
- Registry management: transport disconnection cleanup
- TLV framing: valid/invalid subscription requests

### Integration Tests
- End-to-end pub/sub: subscribe → publish → receive
- Multiple subscribers: fan-out to multiple transports
- Transport disconnect: automatic subscription cleanup
- Wildcard routing: hierarchical pattern matching
- Backpressure scenarios: slow subscriber drop behavior

### Performance Benchmarks
- Subscription registry lookup speed
- Notification fan-out latency
- Memory usage per subscription
- Concurrent subscriber scaling

---

## Usage Patterns

### Real-Time Monitoring

```rust
// Monitoring service publishes alerts
async fn publish_alert(alert: SystemAlert) {
    let route = "notice://acme/monitoring/alerts".to_string();
    let notification = Notification {
        route,
        body: serde_json::to_vec(&alert)?,
        timestamp: now(),
    };

    notice_service.publish(notification).await?;
}
```

### Chat System

```rust
// Client subscribes to chat room
async fn join_chat_room(room: &str) {
    let route = format!("notice://acme/chat/{}", room);
    transport.send_subscribe_frame(route).await?;
}

// Server broadcasts messages
async fn broadcast_message(room: &str, message: ChatMessage) {
    let route = format!("notice://acme/chat/{}", room);
    let notification = Notification {
        route,
        body: serde_json::to_vec(&message)?,
        timestamp: now(),
    };

    notice_service.publish(notification).await?;
}
```

### Status Updates

```rust
// Order service publishes status changes
async fn publish_order_status(order_id: &str, status: OrderStatus) {
    let route = format!("notice://acme/orders/{}/status", order_id);
    let notification = Notification {
        route,
        body: serde_json::to_vec(&status)?,
        timestamp: now(),
    };

    notice_service.publish(notification).await?;
}

// Client subscribes to specific order
async fn watch_order(order_id: &str) {
    let route = format!("notice://acme/orders/{}/status", order_id);
    transport.send_subscribe_frame(route).await?;
}
```

---

*See OVERVIEW.md for system-level context and other domain specifications.*</content>
<parameter name="filePath">d:\repos\cntryl\fitz\docs\NOTICE_SPEC.md