# Routing Domain Specification

**Version:** 1.0  
**Status:** Specification  
**Durability:** Ephemeral (rebuilt on restart)  
**Last Updated:** December 11, 2025  

---

## Overview

The Routing domain is the central dispatch mechanism in Fitz. It receives a resolved **Route Family** (top-level isolation boundary) from the session/connection context, parses route strings, interns components for efficient matching, and routes messages to the appropriate domain actors based on the route scheme (stream, queue, kv, etc.).

### Route Family vs Route Scheme

**Route Family** (top-level):
- Isolation boundary
- Examples: `acme-prod`, `acme-dev`, `customer-42`
- Maps to Midge column families
    - Determines which storage partition and actor set to use

**Route Scheme** (within route):
- Domain type: `stream://`, `queue://`, `kv://`, etc.
- Determines which actor to dispatch to
- Not related to tenancy

### Key Features

- **Route parsing**: Parse `ftz://realm/area/resource/operation` format
- **String interning**: Deduplicate route components for memory efficiency
- **Family-based dispatch**: Route to domain actors by family (kv, stream, queue, etc.)
- **Pattern matching**: Support wildcards for notifications
- **Fast lookup**: O(1) dispatch to domain actors
- **Zero-copy**: Use borrowed slices where possible

### Ephemeral Characteristics

- **Intern table**: Rebuilt on restart
- **Route cache**: Not persisted
- **Domain registry**: Static mapping of route families to actors

### Use Cases

- Message dispatch from sessions to domains
- Route normalization and validation
- String interning for memory efficiency
- Domain actor registration

---

## Route Format

All Fitz routes exist within a **Route Family** context and follow this structure:

```
Route Family: acme-prod
    ↓
Route: {scheme}://{realm}/{area}/{resource}[/{operation}]
```

### Route Schemes (Domain Types)

- `stream://` → StreamActor
- `queue://` → QueueActor
- `kv://` → KvActor (via MidgeActor)
- `rpc://` → RpcActor
- `lease://` → LeaseActor
- `notice://` → NoticeActor (notifications)
- `metrics://` → MetricsActor
- `authcfg://` → AuthConfigActor
- `ctrlcfg://` → ControlConfigActor

### Examples

```
Family: acme-prod
  - stream://orders/logs/events/append → StreamActor
  - kv://auth/config/feature-flags/get → MidgeActor (KV)
  - rpc://billing/payments/refund/invoke → RpcActor
  - lease://orders/locks/reconciliation/acquire → LeaseActor

Family: acme-dev
  - stream://orders/logs/events/append → StreamActor (different storage)
  - kv://auth/config/feature-flags/get → MidgeActor (different storage)
```

Note: Same route string in different families → different storage partitions.

---

## Core Operations

### 1. Parse Route

Parse route string into components.

**Internal Message:**
```rust
RoutingMsg::ParseRoute {
    route: String,
    reply_to: ActorRef<RoutingReply>,
}
```

**Parsing Logic:**
```rust
#[derive(Debug, Clone)]
pub struct ParsedRoute {
    route_family: InternedString,  // From session/connection context: acme-prod, acme-dev, etc.
    scheme: RouteScheme,            // Domain type: stream, queue, kv, etc.
    realm: InternedString,          // Logical grouping (not an isolation boundary!)
    area: InternedString,
    resource: InternedString,
    operation: Option<InternedString>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteScheme {
    Stream,
    Queue,
    Kv,
    Rpc,
    Lease,
    Notice,
    Metrics,
    AuthConfig,
    ControlConfig,
}

fn parse_route(route_family: &str, route: &str, interner: &GlobalInternTable) -> Result<ParsedRoute, String> {
    // Parse "scheme://realm/area/resource/operation" within a resolved route_family context
    let parts: Vec<&str> = route.split("://").collect();
    if parts.len() != 2 {
        return Err("invalid route format".to_string());
    }

    let scheme = parse_scheme(parts[0])?;
    let path_parts: Vec<&str> = parts[1].split('/').collect();
    
    if path_parts.len() < 3 {
        return Err("route must have realm/area/resource".to_string());
    }
    
    Ok(ParsedRoute {
        route_family: interner.intern(route_family),
        scheme,
        realm: interner.intern(path_parts[0]),
        area: interner.intern(path_parts[1]),
        resource: interner.intern(path_parts[2]),
        operation: path_parts.get(3).map(|s| interner.intern(s)),
    })
}
```

---

### 2. Dispatch Message

Route message to appropriate domain actor.

**Internal Message:**
```rust
RoutingMsg::Dispatch {
    route: ParsedRoute,
    frame: TlvFrame,
    session_id: String,
    reply_to: ActorRef<RoutingReply>,
}
```

**Dispatch Logic:**
```rust
fn dispatch(&self, route: ParsedRoute, frame: TlvFrame, session_id: String) {
    match route.scheme {
        RouteScheme::Stream => {
            self.stream_actor.send(StreamMsg::Handle {
                route,
                frame,
                session_id,
                reply_to: ...,
            });
        }
        RouteScheme::Queue => {
            self.queue_actor.send(QueueMsg::Handle {
                route,
                frame,
                session_id,
                reply_to: ...,
            });
        }
        RouteScheme::Kv => {
            // KV handled by MidgeActor
            self.midge_actor.send(MidgeMsg::KvOperation {
                route,
                frame,
                reply_to: ...,
            });
        }
        RouteScheme::Rpc => {
            self.rpc_actor.send(RpcMsg::Handle {
                route,
                frame,
                session_id,
                reply_to: ...,
            });
        }
        RouteScheme::Lease => {
            self.lease_actor.send(LeaseMsg::Handle {
                route,
                frame,
                reply_to: ...,
            });
        }
        RouteScheme::Notice => {
            self.notice_actor.send(NoticeMsg::Handle {
                route,
                frame,
                session_id,
                reply_to: ...,
            });
        }
        RouteScheme::Metrics => {
            self.metrics_actor.send(MetricsMsg::Handle {
                route,
                frame,
                reply_to: ...,
            });
        }
        // ... other families
    }
}
```

---

### 3. Register Domain

Domain actors register themselves on startup.

**Internal Message:**
```rust
RoutingMsg::RegisterDomain {
    family: RouteFamily,
    actor_ref: Box<dyn Any + Send>,
}
```

**Registration:**
```rust
impl RouterActor {
    fn register_domain(&mut self, family: RouteFamily, actor_ref: ActorRef<DomainMsg>) {
        self.domain_registry.insert(family, actor_ref);
    }
}
```

---

## String Interning

### GlobalInternTable

Deduplicate commonly used strings:

```rust
pub struct GlobalInternTable {
    strings: DashMap<String, Arc<str>>,
    reverse: DashMap<u64, Arc<str>>,
}

pub type InternedString = Arc<str>;

impl GlobalInternTable {
    pub fn intern(&self, s: &str) -> InternedString {
        if let Some(interned) = self.strings.get(s) {
            return interned.value().clone();
        }
        
        let arc = Arc::from(s);
        self.strings.insert(s.to_string(), arc.clone());
        arc
    }
    
    pub fn get(&self, s: &str) -> Option<InternedString> {
        self.strings.get(s).map(|r| r.value().clone())
    }
}
```

### Benefits

- **Memory**: Single allocation per unique string
- **Comparison**: Pointer equality instead of string comparison
- **Hashing**: Hash once, reuse
- **Threading**: Arc allows shared ownership

---

## Actor Implementation

### RouterActor State

```rust
pub struct RouterActor {
    /// Global string interner
    interner: Arc<GlobalInternTable>,
    
    /// Domain actor registry
    stream_actor: ActorRef<StreamMsg>,
    queue_actor: ActorRef<QueueMsg>,
    midge_actor: ActorRef<MidgeMsg>,
    rpc_actor: ActorRef<RpcMsg>,
    lease_actor: ActorRef<LeaseMsg>,
    notice_actor: ActorRef<NoticeMsg>,
    metrics_actor: ActorRef<MetricsMsg>,
    auth_config_actor: ActorRef<AuthConfigMsg>,
    control_config_actor: ActorRef<ControlConfigMsg>,
    
    /// Route cache (optional optimization)
    route_cache: DashMap<String, ParsedRoute>,
}
```

---

### Message Handler

```rust
impl Actor for RouterActor {
    type Message = RoutingMsg;
    
    fn on_message(&mut self, msg: Self::Message, ctx: &ActorContext<Self>) {
        match msg {
            RoutingMsg::ParseRoute { route, reply_to } => {
                // Check cache first
                if let Some(parsed) = self.route_cache.get(&route) {
                    reply_to.send(RoutingReply::ParsedRoute(parsed.clone()));
                    return;
                }
                
                // Parse and intern
                match parse_route(&route, &self.interner) {
                    Ok(parsed) => {
                        self.route_cache.insert(route, parsed.clone());
                        reply_to.send(RoutingReply::ParsedRoute(parsed));
                    }
                    Err(e) => {
                        reply_to.send(RoutingReply::Error(e));
                    }
                }
            }
            
            RoutingMsg::Dispatch { route, frame, session_id, reply_to } => {
                self.dispatch(route, frame, session_id);
            }
        }
    }
}
```

---

## Pattern Matching (for Notifications)

Support wildcard patterns:

```rust
fn matches_pattern(route: &ParsedRoute, pattern: &str) -> bool {
    // pattern: "notice://acme/events/*/published"
    // route: "notice://acme/events/order/published"
    
    let pattern_parts: Vec<&str> = pattern.split('/').collect();
    let route_str = format!("{}/{}/{}/{}", 
        route.realm.as_ref(),
        route.area.as_ref(),
        route.resource.as_ref(),
        route.operation.as_ref().map(|s| s.as_ref()).unwrap_or("")
    );
    let route_parts: Vec<&str> = route_str.split('/').collect();
    
    if pattern_parts.len() != route_parts.len() {
        return false;
    }
    
    for (p, r) in pattern_parts.iter().zip(route_parts.iter()) {
        if *p == "*" {
            continue; // Wildcard matches anything
        }
        if p != r {
            return false;
        }
    }
    
    true
}
```

---

## Error Handling

### Error Codes

- `INVALID_ROUTE_FORMAT` - Malformed route string
- `UNKNOWN_FAMILY` - Unrecognized route family
- `DOMAIN_NOT_REGISTERED` - No actor for family
- `ROUTE_TOO_LONG` - Exceeds length limit

### Validation

- Route must have `family://realm/area/resource`
- Family must be recognized
- Realm/area/resource must be valid identifiers
- Operation is optional

---

## Performance Characteristics

### Latency

- **Parse (cached)**: <100ns (hash lookup)
- **Parse (uncached)**: <1µs (string parsing + interning)
- **Dispatch**: <50ns (actor ref send)
- **Pattern match**: <500ns per pattern

### Memory

- **Interned string**: 24 bytes (Arc overhead)
- **ParsedRoute**: 80 bytes
- **Route cache entry**: 160 bytes (key + value)

### Scalability

- DashMap for concurrent access
- Arc for zero-copy sharing
- Cache hit rate >95% in typical workloads

---

## Testing Strategy

### Unit Tests

- Route parsing correctness
- String interning deduplication
- Pattern matching logic
- Family dispatch

### Integration Tests

- End-to-end routing flow
- Cache effectiveness
- Concurrent routing
- Invalid route handling

### Benchmarks

- Parse throughput
- Dispatch latency
- Memory usage with N routes
- Cache hit rate

---

## References

- [Sessions Domain](SESSIONS.md)
- [All Domain Specs](../)
- [String Interning in Rust](https://matklad.github.io/2020/03/22/fast-simple-rust-interner.html)
