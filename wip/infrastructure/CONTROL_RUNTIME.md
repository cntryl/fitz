# Control Plane Runtime Domain Specification

**Version:** 1.0  
**Status:** Specification  
**Durability:** Ephemeral (runtime state)  
**Last Updated:** December 11, 2025  

---

## Overview

The Control Plane Runtime domain manages live system state and coordination that doesn't need persistence. This includes current connection counts, active route families, runtime health, and operational state.

### Key Features

- **Connection tracking**: Active WebSocket count per family
- **Route family registry**: Which families are hosted
- **Health monitoring**: System status and liveness
- **Runtime coordination**: Inter-actor signaling
- **Operational metrics**: Real-time counters and gauges

### Ephemeral Characteristics

- **Not persisted**: State rebuilt on startup
- **Derived state**: Computed from actor messages
- **Resettable**: Clean slate on restart

---

## Core Operations

### 1. Register Route Family

Track that system is hosting a family.

**Internal Message:**
```rust
ControlRuntimeMsg::RegisterFamily {
    route_family: String,
    max_connections: usize,
}
```

---

### 2. Track Connection

Increment/decrement connection count.

**Internal Message:**
```rust
ControlRuntimeMsg::ConnectionOpened {
    route_family: String,
    session_id: String,
}

ControlRuntimeMsg::ConnectionClosed {
    route_family: String,
    session_id: String,
}
```

---

### 3. Query System Health

Get current runtime state.

**Internal Message:**
```rust
ControlRuntimeMsg::GetHealth {
    reply_to: ActorRef<ControlRuntimeReply>,
}
```

**Response:**
```rust
ControlRuntimeReply::Health {
    uptime: Duration,
    total_connections: usize,
    families: Vec<FamilyStatus>,
}

struct FamilyStatus {
    route_family: String,
    active_connections: usize,
    max_connections: usize,
    status: String,
}
```

---

## Actor Implementation

```rust
pub struct ControlRuntimeActor {
    /// Route families hosted by this instance
    families: DashMap<String, FamilyState>,
    
    /// System start time
    started_at: Instant,
    
    /// Configuration reference
    control_config: ActorRef<ControlConfigMsg>,
}

struct FamilyState {
    route_family: String,
    active_connections: AtomicUsize,
    max_connections: usize,
    registered_at: Instant,
}
```

---

## References

- [Control Configuration](../durable/CONTROL_CONFIG.md)
- [Sessions Domain](SESSIONS.md)
