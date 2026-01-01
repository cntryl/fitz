# Discovery Domain Specification

**Version:** 1.0  
**Status:** Specification  
**Durability:** Ephemeral (computed on demand)  
**Last Updated:** December 11, 2025  

---

## Overview

The Discovery domain provides system introspection and capability metadata. Clients can query what route families, schemes, and operations are available without prior knowledge.

### Key Features

- **Route family listing**: What families does this instance host?
- **Scheme discovery**: What domain types are available? (stream, queue, kv, etc.)
- **Operation enumeration**: What operations does each domain support?
- **Version information**: Fitz version, protocol version
- **Capability negotiation**: Feature flags and supported extensions

### Ephemeral Characteristics

- **Computed**: Metadata derived from system configuration
- **Static metadata**: Doesn't change at runtime (per instance)
- **No persistence**: Not stored, generated on request

---

## Core Operations

### 1. List Route Families

Query which families this instance hosts.

**Route:** `discovery://system/families/list`

**Response:**
```json
{
  "families": [
    {
      "name": "acme-prod",
      "status": "active",
      "max_connections": 1000
    },
    {
      "name": "acme-dev",
      "status": "active",
      "max_connections": 100
    }
  ]
}
```

---

### 2. List Supported Schemes

Query available domain types.

**Route:** `discovery://system/schemes/list`

**Response:**
```json
{
  "schemes": [
    "stream",
    "queue",
    "kv",
    "rpc",
    "lease",
    "notice",
    "metrics"
  ]
}
```

---

### 3. Describe Scheme

Get operations for a domain type.

**Route:** `discovery://system/schemes/{scheme}/describe`

**Response:**
```json
{
  "scheme": "stream",
  "operations": ["append", "read", "subscribe"],
  "capabilities": ["batch_append", "replay"]
}
```

---

### 4. Get System Info

Query Fitz version and capabilities.

**Route:** `discovery://system/info`

**Response:**
```json
{
  "version": "2.0.0",
  "protocol_version": "1.0",
  "capabilities": [
    "tls",
    "jwt_auth",
    "compression"
  ]
}
```

---

## Actor Implementation

```rust
pub struct DiscoveryActor {
    /// System metadata
    version: String,
    protocol_version: String,
    capabilities: Vec<String>,
    
    /// References to query runtime state
    control_runtime: ActorRef<ControlRuntimeMsg>,
}
```

---

## Use Cases

- Client bootstrapping
- API exploration
- Multi-family SaaS dashboards
- Monitoring and observability

---

## References

- [Control Runtime](CONTROL_RUNTIME.md)
- [Routing Domain](ROUTING.md)
