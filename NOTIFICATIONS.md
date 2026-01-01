# Notifications Domain - Implementation Summary

## Overview

The Notifications domain provides **fire-and-forget pub/sub messaging** with **NATS-like wildcard routing** scoped to isolation boundaries (RouteFamily).

## Architecture

### Components

- **`matcher.rs`**: Wildcard pattern matching engine
  - Parses patterns with `*` and `**` wildcards
  - Matches published routes against subscription patterns
  - Purely structural matching (no domain semantics)

- **`protocol/mod.rs`**: Message types
  - `PublishMessage`: Send payload to all matching subscribers
  - `SubscribeMessage`: Register interest in a pattern
  - `UnsubscribeMessage`: Unregister from a pattern
  - `NotifyMessage`: Delivered to matching subscribers

- **`actor/mod.rs`**: NotificationsActor implementation
  - In-memory subscriber registry per (RouteFamily, pattern)
  - Fan-out delivery using matched subscribers
  - Fire-and-forget semantics (best-effort only)

## Semantics

### Guarantees

- ✅ **Fire-and-forget**: No acknowledgements, retries, or delivery guarantees
- ✅ **Best-effort**: Delivered only to subscribers alive at publish time
- ✅ **Isolated**: All messaging scoped to (RouteFamilyId, route) pairs
- ✅ **Stateless**: No ordering, durability, or persistence

### Wildcard Syntax

#### Single-Level Wildcard: `*`

Matches any sequence of characters within a single path segment.

```
Pattern: notify://acme/orders/*
Matches:
  - notify://acme/orders/create
  - notify://acme/orders/update
  - notify://acme/orders/delete
Does NOT match:
  - notify://acme/orders/items/create (nested path)
```

#### Multi-Level Wildcard: `**`

Matches zero or more complete path segments.

```
Pattern: notify://acme/**
Matches:
  - notify://acme/orders
  - notify://acme/orders/create
  - notify://acme/orders/items/create
  
Pattern: notify://acme/**/created
Matches:
  - notify://acme/created
  - notify://acme/orders/created
  - notify://acme/orders/items/created
```

### RouteFamily Isolation

Wildcards **never cross RouteFamily boundaries**. Same route in different families is fully isolated:

```rust
// These are completely separate subscriptions
subscribe(family1, "notify://realm/orders/*", sub1);
subscribe(family2, "notify://realm/orders/*", sub1);

// Publish to family1 only reaches family1 subscribers
publish(family1, "notify://realm/orders/create", payload);
```

## Implementation Details

### Subscription Registry

```rust
HashMap<(RouteFamily, pattern_string), Vec<Subscription>>
  └─ Each Subscription tracks:
     - Pattern (parsed for matching)
     - Subscriber RouteAddress
```

### Publishing Flow

1. **Receive**: `PublishMessage { family_id, route, payload }`
2. **Filter**: Find all (family_id, pattern) entries where pattern matches route
3. **Fan-out**: Send `NotifyMessage` to each matching subscriber via Router
4. **Complete**: Return immediately (fire-and-forget)

### Subscription Operations

- **Subscribe**: Add (family, pattern, subscriber) entry
  - Idempotent: adding duplicate is safe (creates separate entry)
  - Multiple subscribers per pattern supported
  
- **Unsubscribe**: Remove matching (family, pattern, subscriber)
  - Idempotent: removing non-existent is safe
  - Leaves pattern alive if other subscribers exist

## Testing

Comprehensive test coverage (19 tests total):

### Matcher Tests (11 tests)
- Exact route matching
- Single-level wildcard matching
- Multi-level wildcard matching
- Wildcard boundaries (cross-segment failures)
- Pattern without scheme
- Overlapping wildcards

### Actor Tests (8 tests)
- Subscription management
- Unsubscription correctness
- RouteFamily isolation
- Multiple subscribers per pattern
- Selective unsubscribe
- Idempotent operations

## Usage Example

```rust
// Subscribe to all order operations
let notify_actor = /* NotificationActor spawned as actor */;
let subscriber = /* Some actor ref */;

notify_actor.send(
    NotificationMessage::Subscribe(
        SubscribeMessage::new(
            family_id,
            Route::new("notify://acme/orders/*"),
            subscriber_address,
        )
    )
);

// Publish to matching subscribers
notify_actor.send(
    NotificationMessage::Publish(
        PublishMessage::new(
            family_id,
            Route::new("notify://acme/orders/create"),
            Bytes::from("order created"),
        )
    )
);

// Unsubscribe when done
notify_actor.send(
    NotificationMessage::Unsubscribe(
        UnsubscribeMessage::new(
            family_id,
            Route::new("notify://acme/orders/*"),
            subscriber_address,
        )
    )
);
```

## Design Decisions

### Fire-and-Forget Only

- No acknowledgements: Subscribers don't respond to notifications
- No retries: Lost messages stay lost
- Best-effort: Delivery depends on subscriber being alive at publish time
- Rationale: Simplicity, predictable latency, no backpressure

### In-Memory Only

- No durability: Subscriptions lost on restart
- No persistence layer
- No WAL or logging
- Rationale: Notifications are transient; ephemeral subscriptions are sufficient

### Structural Matching Only

- No domain-specific interpretation
- Routes are treated as hierarchical paths
- Wildcards apply to segments
- Rationale: Keep domain logic in domain actors, not in infrastructure

### RouteFamily Isolation

- Enforced at subscription registry level
- No cross-family delivery ever
- Same pattern in different families is independent
- Rationale: RouteFamily is Fitz's isolation boundary

## Future Enhancements

Not implemented (out of scope):

- Delivery guarantees (at-least-once, exactly-once)
- Message filtering beyond route patterns
- Subscription persistence
- Topic subscriptions (subscribe to routes before they exist)
- Backpressure / flow control
- Metrics and observability
- Message ordering
- Lease-based subscription lifecycle

## Files

- `src/domains/notification/matcher.rs` (180 lines)
  - Pattern parsing and wildcard matching
  
- `src/domains/notification/protocol/mod.rs` (100 lines)
  - Message type definitions
  
- `src/domains/notification/actor/mod.rs` (290 lines)
  - NotificationsActor and subscription management
  
- `src/domains/notification/mod.rs` (25 lines)
  - Module exports
