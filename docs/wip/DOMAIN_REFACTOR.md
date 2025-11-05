# Domain-Based Architecture Refactor

## Overview

We've refactored the engine from a monolithic handler to a clean domain-dispatch pattern.

### Before
- Engine knew about every operation in every domain
- Massive `EngineCommand` enum with 30+ variants
- Engine directly called store methods for kv, queue, stream, lease, etc.
- Difficult to modify one domain without touching engine

### After
- Engine: parse route → check authz → dispatch to domain
- Each domain handles its own TLV parsing and routing
- Domains are self-contained and own their logic
- Clean separation of concerns

## Architecture

```
Transport Layer (WebSocket/HTTP/TCP)
         ↓
    [Frame with TLV payload]
         ↓
       Engine
    - Parse route
    - Check authz
    - Dispatch to domain
         ↓
   Domain Handler (kv, queue, stream, etc.)
    - Parse TLV tags from payload
    - Detect operation
    - Call store methods
    - Build TLV response
         ↓
     [Response Frame]
```

## Domain Trait

```rust
pub trait Domain: Send + Sync {
    async fn handle(&self, request: DomainRequest, store: Arc<Mutex<MemStore>>) -> DomainResponse;
    fn schemes(&self) -> &[&str];
}
```

### DomainRequest
- `route: Route` - Parsed route (scheme, realm, area, resource)
- `route_str: String` - Raw route string
- `payload: Vec<u8>` - TLV-encoded frame payload
- `channel_id: u32` - Channel for subscriptions/sessions

### DomainResponse
- `Ok` - Success with no data
- `Frame(Vec<u8>)` - Success with TLV response
- `Error(String)` - Error message

## Domains Created

All domains are stubbed with `panic!("not yet implemented")` for now:

1. **QueueDomain** - `queue://` routes
2. **KvDomain** - `kv://` routes
3. **StreamDomain** - `stream://` routes
4. **LeaseDomain** - `lease://` routes
5. **NoticeDomain** - `notice://` routes
6. **ControlDomain** - `control://` routes
7. **RpcDomain** - `rpc://` routes

## Next Steps

1. ✅ Create domain trait and request/response types
2. ✅ Stub all domain handlers
3. ⏳ Refactor engine to dispatch pattern
4. ⏳ Implement one domain at a time (queue first)
5. ⏳ Update tests to work with new architecture

## Benefits

- **Isolation**: Each domain is self-contained
- **Testability**: Can test domains independently
- **Maintainability**: Changes to one domain don't affect others
- **Clarity**: Engine is now a simple dispatcher
- **Extensibility**: Easy to add new domains

## Implementation Strategy

We're implementing domains one at a time:
1. Get architecture in place
2. Implement QueueDomain first (most complete)
3. Then KV, Stream, Lease, etc.
4. Each domain owns its TLV parsing and operation routing
