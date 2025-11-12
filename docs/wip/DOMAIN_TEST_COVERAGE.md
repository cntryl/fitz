# Domain Test Coverage Summary

## Overview
Complete inventory of test coverage across all domains extracted from `tests/` directory.

## Test Count by Domain

| Domain | Tests | Status | Complexity |
|--------|-------|--------|------------|
| **Stream** | 106 | ❌ All blocked | ⚠️ Highest |
| **RPC** | 48 | ❌ All blocked | ⚠️ High |
| **Queue** | 47 | ❌ All blocked | Medium |
| **KV** | 45 | ❌ All blocked | Low |
| **Lease** | 21 | ❌ All blocked | Medium |
| **Control** | 21 | ❌ All blocked | Medium |
| **Notice** | 16 | ❌ All blocked | Low |
| **TOTAL** | **304** | **0 passing** | - |

## Implementation Priority

### Phase 1: Foundation Domains (Low Complexity)
1. **KV** (45 tests) - Simplest, good warmup
2. **Notice** (16 tests) - Depends on Router (already implemented)

### Phase 2: Core Messaging (Medium Complexity)
3. **Queue** (47 tests) - Critical for messaging
4. **Lease** (21 tests) - Similar patterns to queue

### Phase 3: Advanced Features (High Complexity)
5. **Control** (21 tests) - Coordinates system config
6. **RPC** (48 tests) - Builds on notice/queue patterns
7. **Stream** (106 tests) - Most complex, gap detection/watermarks

## Detailed Coverage Documents
- [`queue_test_coverage.md`](./queue_test_coverage.md)
- [`kv_test_coverage.md`](./kv_test_coverage.md)
- [`stream_test_coverage.md`](./stream_test_coverage.md)
- [`rpc_test_coverage.md`](./rpc_test_coverage.md)
- [`notice_test_coverage.md`](./notice_test_coverage.md)
- [`lease_test_coverage.md`](./lease_test_coverage.md)
- [`control_test_coverage.md`](./control_test_coverage.md)

## Architecture Status

### ✅ Completed
- Domain trait interface (`src/core/domain.rs`)
- All 7 domain handlers stubbed:
  - `src/core/queue/handler.rs`
  - `src/core/kv/handler.rs`
  - `src/core/stream/handler.rs`
  - `src/core/lease/handler.rs`
  - `src/core/notice/handler.rs`
  - `src/core/control/handler.rs`
  - `src/core/rpc/handler.rs`
- Engine refactored to dispatcher pattern (`src/core/engine.rs`)
- Backward compatibility methods added

### ⏳ In Progress
- All domain implementations (currently panic!)

### 📋 TODO
- Implement domain handlers one at a time
- Update tests to work with new architecture
- Remove engine_old.rs backup after validation

## Test Guidelines Reference
All tests follow strict guidelines from `.github/copilot-instructions.md`:
- ✅ `should_*` naming convention
- ✅ AAA structure (Arrange/Act/Assert)
- ✅ Single behavior per test
- ✅ No multiple Act sections

## Next Actions
1. Pick a domain (recommend starting with KV)
2. Implement `Domain::handle()` method
3. Parse TLV tags from `DomainContext.payload`
4. Route to appropriate operation
5. Build TLV response frame
6. Run tests and iterate

## Migration Notes
- Old engine logic preserved in `src/core/engine_old.rs` (1128 lines)
- Each domain can reference old engine for implementation patterns
- Tests are complete but currently fail (all domains panic!)
