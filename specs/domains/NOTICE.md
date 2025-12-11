# Fitz Notice Domain Specification — v2 (Actor Model)

**Version:** 2.0
**Status:** Design Complete / Ready for Implementation
**Durability:** Ephemeral (in-memory only)
**Last Updated:** December 11, 2025

---

## 1. Overview

Notice v2 is the **simplest, fastest** domain in Fitz:

* **Fire-and-forget**: No durability, no acks, no replay
* **Session-scoped subscriptions**: Tied to live connections
* **Route-based fanout**: One write, many subscribers
* **Actor-only state**: No shared locks, no global maps
* **Best-effort delivery**: Drops under backpressure are acceptable

Use Notice for:

* UI notifications & toasts
* Monitoring and alerts
* Status updates / heartbeats
* Broadcasts to active sessions
* Internal signals (e.g., waking workers for RPC / Queue / Stream)

Notice v2 is the "**UDP of Fitz**": fast, simple, and ephemeral.

---

## 2. Route Format

Notice keeps the standard Fitz route shape:

```
notice://{realm}/{area}/{resource}[/{operation}]
```

Examples:

* `notice://acme/monitoring/alerts`
* `notice://acme/chat/general`
* `notice://acme/orders/status`
* `notice://cntryl/system/heartbeat`

---

## 3. Actor Architecture

### 3.1 Main Actors

| Actor                | Responsibility                                            |
| -------------------- | --------------------------------------------------------- |
| **NoticeRouteActor** | Owns a notice route; fans out payloads to subscribers     |
| **SessionActor**     | Represents a single connection; manages its subscriptions |
| **TransportActor**   | Sends TLV frames to/from the wire                         |

No shared global subscription registry; everything lives inside actors and their mailboxes.

### 3.2 High-Level Flow

1. Client (via SessionActor) sends **SUBSCRIBE** for a notice route
2. SessionActor registers with the **NoticeRouteActor**
3. Publisher sends **PUBLISH** to the notice route
4. NoticeRouteActor sends the payload to all subscribed SessionActors
5. SessionActors forward to their TransportActor (connection)
6. On disconnect, SessionActor tears down all subscriptions

---

## 4. Data Model

### 4.1 Subscription

```rust
pub struct NoticeSubscription {
    pub session_id: SessionId,
    pub route_pattern: RoutePattern, // exact or simple prefix/wildcard
    pub created_at: u64,
}
```

### 4.2 Notification

```rust
pub struct Notification {
    pub route: String,        // full notice:// route
    pub body: Vec<u8>,        // opaque payload
    pub timestamp: u64,       // server timestamp
}
```

### 4.3 Route Patterns (MVP)

MVP pattern matching is **simple and fast**:

* Exact route: `notice://acme/chat/general`
* Area prefix: `notice://acme/chat/*`
* Realm prefix: `notice://acme/*/*`
* Global: `notice://*/*/*`

We treat segments **after** the scheme as slash-separated components; `*` only appears on segment boundaries.

---

## 5. Core Operations

### 5.1 Subscribe

**Client → SessionActor → NoticeRouteActor**

**Route:**

```
notice://{realm}/{area}/{resource}   (pattern allowed, see matching)
```

**Request TLV:**

* `TAG_ROUTE`       – the pattern to subscribe to
* `TAG_SUBSCRIBE`   – presence = subscribe

**Behavior:**

* SessionActor validates route & pattern
* SessionActor sends `Subscribe{session_id, pattern}` to NoticeRouteActor
* NoticeRouteActor records subscription
* SessionActor gets ACK

Subscriptions are **session-scoped**: they vanish on disconnect.

---

### 5.2 Unsubscribe

**Request TLV:**

* `TAG_ROUTE`
* `TAG_UNSUBSCRIBE`

**Behavior:**

* Remove subscription for that session & route pattern
* Idempotent: unsubscribing twice is OK

---

### 5.3 Publish

**Route:**

```
notice://{realm}/{area}/{resource}[/{operation}]
```

**Request TLV:**

* `TAG_ROUTE` – full route
* `TAG_BODY`  – payload

**Behavior:**

1. Engine routes frame to appropriate NoticeRouteActor (by route family/realm/area)
2. NoticeRouteActor finds all matching subscriptions (fast in-memory index)
3. For each subscriber, sends `Notification` message to its SessionActor
4. SessionActor enqueues frame to TransportActor (may drop on local backpressure)

**Delivery guarantees:** best-effort, in-memory only.

---

## 6. Route Matching

Fast, segment-based prefix matching:

```rust
enum RoutePattern {
    Exact(Vec<String>),    // "acme/chat/general"
    Prefix(Vec<String>),   // "acme/chat/*"
    RealmWide(String),     // "acme/*/*"
    Global,                // "*"
}
```

MVP rules:

* `notice://acme/chat/general` → exact
* `notice://acme/chat/*` → prefix
* `notice://acme/*/*` → realm-wide
* `notice://*/*/*` or just `notice://*` → global

Matching happens *inside* NoticeRouteActor, over a small set of subscribers for that realm/area.

---

## 7. Backpressure & Drop Policy

### 7.1 Per-Session Output Buffer

Each SessionActor has a bounded buffer:

```rust
pub struct SessionOutbox {
    pub sender: mpsc::Sender<TlvFrame>,
    pub capacity: usize,
    pub dropped_notices: u64,
}
```

When sending a notification:

```rust
match sender.try_send(frame) {
    Ok(()) => {}
    Err(TrySendError::Full(_)) => {
        // Drop policy
        dropped_notices += 1;
        // Optionally log or emit metric
    }
    Err(TrySendError::Closed(_)) => {
        // Session is gone; cleanup will happen elsewhere
    }
}
```

### 7.2 Policies (MVP)

* **Drop-on-full** (default): never block publisher, just drop.
* Per-route / per-session `capacity` configurable.

Future candidates (not in v2 MVP):

* Close-on-full
* Block-until-drain
* DLQ for "important" notices (probably stream/queue domain instead)

---

## 8. TLV Framing

### 8.1 Subscribe

**Client → Server**

```
REG Frame:
- TAG_ROUTE      (0x20): "notice://acme/chat/general"
- TAG_SUBSCRIBE  (0x90): (empty)
```

**Server → Client (ACK)**

```
DAT Frame:
- TAG_ROUTE      (0x20): echoed route/pattern
- TAG_STATUS     (0x30): "ok" or error code
```

---

### 8.2 Unsubscribe

```
REG Frame:
- TAG_ROUTE        (0x20): "notice://acme/chat/general"
- TAG_UNSUBSCRIBE  (0x91): (empty)
```

ACK as above.

---

### 8.3 Publish

```
DAT Frame:
- TAG_ROUTE        (0x20): "notice://acme/chat/general"
- TAG_BODY         (0x22): <payload bytes>
- TAG_NOTICE_MARK  (0x92): (optional marker tag)
```

Server ACK is optional; typical clients don't wait for it.

---

## 9. Error Model

| Code | Name                       | Description                   | Typical Cause                     |
| ---- | -------------------------- | ----------------------------- | --------------------------------- |
| 3001 | ERR_INVALID_NOTICE_ROUTE   | Route malformed               | Bad scheme/segments               |
| 3002 | ERR_INVALID_NOTICE_PATTERN | Pattern too broad / malformed | e.g. multiple `*` in weird places |
| 3003 | ERR_SUBSCRIPTION_LIMIT     | Too many subs for session     | Exceeded config                   |
| 3004 | ERR_TRANSPORT_CLOSED       | Session is gone               | Client disconnected               |

Errors are returned in an ACK frame:

```
DAT Frame:
- TAG_STATUS   (0x30): "error"
- TAG_ERROR    (0x31): "ERR_SUBSCRIPTION_LIMIT"
- TAG_ROUTE    (0x20): original route
```

---

## 10. Configuration

```yaml
notices:
  default_session_buffer: 256      # messages per session
  max_subscriptions_per_session: 64
  max_patterns_per_route: 10_000   # safety cap per NoticeRouteActor

  routes:
    "notice://acme/monitoring/**":
      session_buffer: 1024         # more tolerant
    "notice://acme/chat/**":
      session_buffer: 128
```

---

## 11. Observability

### Metrics

* `notice_subscriptions_active{realm,area}`
* `notice_published_total{route}`
* `notice_delivered_total{route}`
* `notice_dropped_total{route,reason}`
* `notice_session_buffer_usage{session}` (sampled)

### Logs

* subscription added / removed
* subscription limit exceeded
* repeated drops for a given session / route

---

## 12. Implementation Status (Intended)

### MVP v2 Scope

✅ Session-scoped subscriptions
✅ Actor-backed fanout (NoticeRouteActor + SessionActor)
✅ Segment-based wildcard patterns
✅ Drop-on-full backpressure
✅ Simple TLV subscribe/unsubscribe/publish frames

### Out-of-Scope for v2

* Persistent subscriptions
* Durable/reliable notices
* Per-subscriber filtering expressions
* DLQs for notice traffic

Those should be solved via **Streams/Queues**, not Notice.

---

## 13. Usage Patterns

### 13.1 UI Notifications

* Frontend subscribes to `notice://acme/ui/{tenant}/events`
* Backend publishes notifications on status changes
* Lost messages → user refresh or poll backing KV/Stream

### 13.2 Monitoring / Alerts

* Agents publish to `notice://acme/monitoring/alerts`
* Dashboards and alerting services subscribe
* Drops under extreme load are acceptable — durable alerts go via Stream/Queue.

### 13.3 Internal Signals (RPC/Queue)

* RPC/Queue domains use Notice internally as a **signal bus**:

  * "there is work to reserve"
  * "there is a new job batch"
* Workers subscribe to the signal routes; actual work is claimed via RPC/Queue semantics.

---

## 14. Design Summary

Notice v2 is:

* **Fire-and-forget only** (by design)
* **Actor-owned**, zero locks, zero global maps
* **Session-scoped** for automatic cleanup
* **Backpressure-safe** via drop-on-full semantics
* **Wildcards, but fast** (no regex, just segments)
* **The absolute fastest broadcast mechanism in Fitz**
