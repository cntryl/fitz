# Fitz-Go Client Status

## ✅ Compilation Fixed

The Go client now compiles successfully after fixing:
1. Removed circular dependency in go.mod
2. Fixed import path in test/fixture/fixture.go
3. Added missing testify dependency

## 🔧 Recent Improvements (Phase 1 - February 2026)

### Environment-Based Test Configuration
✅ **Test broker addresses are now configurable via environment variables:**
- Set `FITZ_BROKER_TCP_ADDR` to override default TCP address (default: `localhost:4091`)
- Set `FITZ_BROKER_WS_ADDR` to override default WebSocket address (default: `ws://localhost:4090/ws`)
- Tests gracefully skip when broker is unavailable (CI/CD friendly)

### Route Validation Centralization
✅ **All domain entry points now validate routes:**
- KV: `Begin()`, `BeginRead()`
- Lease: `Acquire()`
- Notice: `Publish()`
- Queue: `Send()`, `Receive()`
- RPC: `Send()`, `Subscribe()`
- Stream: `Begin()`
- Schedule: `Create()`, `Cancel()`
- Consistent error messages for malformed routes across all domains

### Buffer Pool Optimization
✅ **Large buffer shedding already implemented:**
- Buffers exceeding 64KB are not returned to pool (prevents memory bloat)
- Both `bytes.Buffer` pool and `ByteSlicePool` have size limits

### Schedule LIST Pagination
✅ **Efficient pagination support added (February 2026):**
- `List(ctx, offset, limit uint64)` now supports pagination for large schedule lists
- Returns: `([]ScheduleEntry, total_count uint64, error)`
- `offset`: starting position (0-based), use 0 for first page
- `limit`: max entries per page, use 0 for server default (100)
- `total_count`: total number of schedules across all pages
- **Server changes:** Added offset/limit parsing, total_count in response
- **Client changes:** Added `WriteOptionalU64()` encoder, updated LIST request/response codec
- **Why:** Prevents frame size violations with 1000+ schedules (16MB client limit, 1MB server production default)

## 🔄 Spec Compliance Overhaul

### Completed

✅ **Core Protocol Layer** - `internal/protocol/`
- MessageType constants (including Queue 201 reserved), frame encoding/decoding, TCP framing

✅ **Connection & Transport**
- CONNECT (MessageType=1) with JWT, multiplexer by MessageType, WebSocket and TCP transports

✅ **Lease domain (aligned with CLIENT_SPEC)**
- ACQUIRE response: `response_type` (0=Acquired, 1=AlreadyHeld, 2=Queued, 3=AlreadyQueued) + `fencing_token`
- QUERY response: `has_holder`, `owner_id`, `ttl_remaining_secs`, `pending_waiters`
- RENEW success: parse and store `new_fencing_token`
- ErrLeaseQueued for response_type 2/3

✅ **Schedule domain (aligned with CLIENT_SPEC)**
- CREATE request: flat `[route][cron][payload]` (no nested TLV)
- CANCEL request: `[route]` (route-based identity)
- LIST: empty request; response `has_entry` + per-entry route, cron, payload; `ScheduleEntry` has Route, Cron, Payload
- SUBSCRIBE response: status=0 only (optional subscription_id when server sends it for NOTIFY matching)
- List(ctx) no longer takes route; Cancel(ctx, route)

✅ **Encoder audit**
- KV, Queue, RPC, Notice, Stream encoders match CLIENT_SPEC field order; Queue 201 documented as reserved; RPC 6004 comment added

### Optional / Follow-up

- **Schedule LIST streaming:** Spec allows multiple response frames; current code reads one frame. Add streaming API if server sends multi-frame LIST.
- **Acceptance criteria:** Full pass against CLIENT_ACCEPTANCE_CRITERIA.md with real broker.

## Architecture Overview

```
fitz-go/
├── internal/
│   ├── protocol/              ✅ NEW - Spec-compliant wire protocol
│   │   ├── message_types.go   ✅ MessageType constants
│   │   ├── frame.go           ✅ Frame encoding/decoding
│   │   └── frame_test.go      ✅ Tests (all passing)
│   │
│   ├── connection/            ✅ CONNECT, mux, response correlation
│   ├── transport/             ✅ WebSocket, TCP
│   └── domains/               ✅ Lease & Schedule aligned; KV/Queue/RPC/Notice/Stream audited
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

Integration tests require a running Fitz broker. By default, tests connect to:
- TCP: `localhost:4091`
- WebSocket: `ws://localhost:4090/ws`

To override broker addresses (e.g., for CI/CD or remote brokers), set environment variables:

```bash
# Set custom broker addresses
export FITZ_BROKER_TCP_ADDR="localhost:4091"
export FITZ_BROKER_WS_ADDR="ws://localhost:4090/ws"

# Run tests
go test ./... -v

# Test protocol layer only (no broker required)
go test ./internal/protocol/... -v

# Test specific domain
go test ./test/kv_test.go -v

# Build
go build ./...
```

Tests will skip gracefully if the broker is unavailable.

## Breaking Changes and Migration

The client has been aligned with CLIENT_SPEC.md. If you have existing code, update as follows:

| Area | Old | New |
|------|-----|-----|
| **Schedule.List** | `List(ctx) ([]ScheduleEntry, error)` | `List(ctx, offset uint64, limit uint64) ([]ScheduleEntry, uint64, error)` — pagination support; returns entries + total_count |
| **Schedule.Cancel** | `Cancel(ctx, scheduleID string)` | `Cancel(ctx, route string)` — route-based identity; pass the schedule route (same as Create route or `ScheduleEntry.Route`) |
| **Schedule.Create** | Returned server schedule id | Returns route (identity) when server sends no id; otherwise returns optional id |
| **ScheduleEntry** | `ID` only | `ID`, `Route`, `Cron`, `Payload` (ID is route when server does not send id) |
| **Lease.Acquire** | `has_token` + token | `response_type` (0=Acquired, 1=AlreadyHeld) + `fencing_token`; returns `ErrLeaseQueued` for 2/3 |
| **Lease.QUERY / LeaseInfo** | `Held`, `Token` | Adds `OwnerID`, `TTLRemainingSecs`, `PendingWaiters`; token not set from QUERY |

⚠️ Breaking changes are intentional so the wire protocol matches CLIENT_SPEC.md.

## References

- [CLIENT_SPEC.md](../../docs/clients/CLIENT_SPEC.md) - Wire protocol specification
- [CLIENT_ACCEPTANCE_CRITERIA.md](../../docs/clients/CLIENT_ACCEPTANCE_CRITERIA.md) - Test requirements
- [CLIENT_IMPLEMENTATION_GUIDE.md](../../docs/clients/CLIENT_IMPLEMENTATION_GUIDE.md) - Implementation patterns
- [OVERHAUL_PLAN.md](OVERHAUL_PLAN.md) - Detailed implementation plan
