# Route Family Integration - Implementation Plan

## Current State (Before Route Families)

- **Addressing**: ActorId (u64) assigned by scheduler
- **Router**: HashMap<ActorId, Arc<dyn MailboxSink>>
- **Envelope**: source/destination are ActorId
- **Leases**: Scoped by lease_id (string), no family concept

## Target State (After Route Families)

- **Addressing**: RouteAddress = (RouteFamily, Route)
- **Router**: HashMap<RouteAddress, Arc<dyn MailboxSink>>
- **Envelope**: source/destination are RouteAddress
- **Leases**: Scoped by (RouteFamily, lease_id)

## Implementation Steps

### Phase 1: Core Types ✅
- [x] Create routing.rs with RouteFamily, Route, RouteAddress
- [x] Add basic tests for isolation guarantees
- [x] Integrate into lib.rs

### Phase 2: Router Migration (IN PROGRESS)
- [ ] Update Router to use RouteAddress as keys
- [ ] Update RouteError to use RouteAddress
- [ ] Update ActorRegistry → RouteRegistry
- [ ] Maintain backward compat tests initially

### Phase 3: Envelope Migration
- [ ] Change Envelope to use RouteAddress for source/dest
- [ ] Update envelope creation to require RouteAddress
- [ ] Update reply_to logic to preserve family

### Phase 4: Actor/Context Migration  
- [ ] ActorRef now wraps RouteAddress (not ActorId)
- [ ] Context::send() requires RouteAddress
- [ ] Context::reply() preserves source family
- [ ] Remove ActorId from public API

### Phase 5: Scheduler Migration
- [ ] spawn() now registers actors at RouteAddress
- [ ] Remove ActorId counter (routes are externally defined)
- [ ] Update dispatch to use RouteAddress

### Phase 6: Lease Migration
- [ ] Add RouteFamily to all lease operations
- [ ] Leases keyed by (RouteFamily, lease_id)
- [ ] Update LeaseHandle to include family
- [ ] Add isolation tests

### Phase 7: Integration Tests
- [ ] Same route in different families → isolated
- [ ] Leases don't conflict across families
- [ ] Messages never cross family boundaries
- [ ] No fallback or inheritance between families

## Key Design Decisions

1. **No ActorId in Public API**: Routes are defined externally, not assigned
2. **No Default Family**: All sends must specify family explicitly
3. **Pure Isolation**: No prefix matching, no wildcards (yet)
4. **Opaque Types**: RouteFamily and Route are opaque strings

## Breaking Changes

This is a breaking change to the actor runtime API:
- `scheduler.spawn()` signature changes
- `ActorRef` type parameter changes
- All `send()` calls need RouteAddress

## Non-Goals (For Now)

- Wildcard routes (e.g., "/*")
- Hierarchical families
- Network routing
- Persistence

## Status

**Current**: Completed Phase 1 (routing types)
**Next**: Phase 2 (Router migration)
**Blocked**: None
