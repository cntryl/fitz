# Fitz-Go Client Status

## ✅ Compilation Fixed

The Go client now compiles successfully after fixing:
1. Removed circular dependency in go.mod
2. Fixed import path in test/fixture/fixture.go
3. Added missing testify dependency

## 🔄 Spec Compliance Overhaul - IN PROGRESS

### Completed (Phase 1)

✅ **Core Protocol Layer** - `internal/protocol/`
- **message_types.go**: All MessageType constants per CLIENT_SPEC.md
  - Control (1 = CONNECT)
  - KV Domain (100-108)
  - Queue, RPC, Lease, Notice, Stream, Schedule domains
- **frame.go**: Proper wire format encoding/decoding
  - Variable-length MessageType encoding (1 byte for ≤254, 3 bytes for >254)
  - Frame format: `[MessageType][Length (u16 BE)][Payload]`
  - TCP framing support: `[u32 BE frame_length][frame]`
  - Domain routing by MessageType ranges
- **frame_test.go**: Comprehensive tests
  - All tests passing ✅
  - Validates encoding matches CLIENT_SPEC.md examples

### Remaining Work

See [OVERHAUL_PLAN.md](OVERHAUL_PLAN.md) for the complete implementation plan.

**Next Steps (Priority Order):**

1. **Domain Payload Encoders** (Phase 2)
   - KV: BEGIN, PUT, GET, COMMIT, ROLLBACK, etc.
   - Queue: ENQUEUE, RESERVE, COMPLETE, etc.
   - Notice: SUBSCRIBE, PUBLISH, etc.
   - Each must match CLIENT_SPEC.md field order exactly

2. **Connection Layer** (Phase 3)
   - CONNECT handshake (MessageType=1) with JWT
   - Multiplexer routing based on MessageType
   - Response correlation

3. **Transport Layers** (Phase 3)
   - WebSocket: binary frames = complete messages
   - TCP: length-prefixed framing

4. **Domain Client APIs** (Phase 4)
   - Update KV, Queue, RPC, Notice, Stream, Lease, Schedule clients
   - Use new protocol layer
   - Maintain ergonomic APIs (transaction objects, etc.)

5. **Testing** (Phase 5)
   - Integration tests against real broker
   - Acceptance criteria from CLIENT_ACCEPTANCE_CRITERIA.md

## Architecture Overview

```
fitz-go/
├── internal/
│   ├── protocol/              ✅ NEW - Spec-compliant wire protocol
│   │   ├── message_types.go   ✅ MessageType constants
│   │   ├── frame.go           ✅ Frame encoding/decoding
│   │   └── frame_test.go      ✅ Tests (all passing)
│   │
│   ├── connection/            🔄 NEEDS UPDATE
│   │   └── ...                   (CONNECT handshake, mux)
│   │
│   ├── transport/             🔄 NEEDS UPDATE
│   │   └── ...                   (WebSocket, TCP)
│   │
│   └── domains/               🔄 NEEDS UPDATE
│       ├── kv/                   (payload encoders)
│       ├── queue/
│       ├── notice/
│       └── ...
│
├── fitz/                      ✅ Public API (interface only)
│   └── client.go
│
└── test/                      🔄 NEEDS UPDATE
    └── ...

```

## Key Differences from Spec

### ❌ Current (OLD) Implementation
- Uses custom Frame with Channel IDs
- Uses nested TLV (tags within payload)
- Routing by explicit Channel field

### ✅ Required (NEW) Implementation  
- Simple frame: `[MessageType][Length][Payload]`
- Payload is concatenated fields (no nested TLV)
- Routing by MessageType ranges

## Testing

```bash
# Test new protocol layer
go test ./internal/protocol/... -v

# All tests (old implementation, will need updates)
go test ./... -v

# Build
go build ./...
```

## Timeline Estimate

- **Phase 1** (Core Protocol): ✅ Complete
- **Phase 2** (Domain Encoders): ~2-3 days
- **Phase 3** (Connection/Transport): ~2-3 days
- **Phase 4** (Domain APIs): ~3-4 days
- **Phase 5** (Testing): ~2-3 days

**Total**: ~2 weeks for full spec compliance

## Breaking Changes

⚠️ This overhaul **will break** existing code using fitz-go. This is intentional because:
1. Current implementation doesn't match CLIENT_SPEC.md
2. Better to be correct than compatible with incorrect wire protocol
3. Client isn't widely adopted yet

## References

- [CLIENT_SPEC.md](../../docs/clients/CLIENT_SPEC.md) - Wire protocol specification
- [CLIENT_ACCEPTANCE_CRITERIA.md](../../docs/clients/CLIENT_ACCEPTANCE_CRITERIA.md) - Test requirements
- [CLIENT_IMPLEMENTATION_GUIDE.md](../../docs/clients/CLIENT_IMPLEMENTATION_GUIDE.md) - Implementation patterns
- [OVERHAUL_PLAN.md](OVERHAUL_PLAN.md) - Detailed implementation plan
