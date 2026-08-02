# Four-client operational parity ledger

This ledger is separate from the shared conformance suite and intentionally
scopes only TypeScript, Go, Rust, and .NET. A green CS-001–CS-017 result does
not establish the capabilities below.

| Capability | TypeScript | Go | Rust | .NET | Required evidence |
| --- | --- | --- | --- | --- | --- |
| Startup readiness | pass | pass | pass | pass | One-shot connect plus bounded, cancellable readiness retry; auth rejection is terminal |
| Explicit KV durability | pass | pass | pass | pass | Public caller chooses Buffered or Sync and both wire bytes are covered |
| Managed leases | pass | pass | pass | pass | Renewal, callback cancellation, ownership loss, release, and combined failures |
| Safe retry | pass | pass | pass | pass | Replayable reads only; Queue enqueue only after confirmed negative; ambiguous mutations never retry |
| Reconnect defaults | pass | pass | pass | pass | Unlimited default where supported and documented finite configuration |
| Heartbeat | pass | pass | pass | pass | WebSocket ping/pong idle watchdog and TCP socket keepalive without a Fitz heartbeat frame |
| Observability | pass | pass | pass | pass | Shared lifecycle names and request/error/state/subscription telemetry hooks |
| Error ergonomics | pass | pass | pass | pass | Language-native typed errors or sentinels with wrapping support |
| Documentation truth | pass | pass | pass | pass | Public examples compile against default APIs and migration notes describe breaks |

Status is accepted only with a green native repository gate and exact-head CI.
Package publication, tags, and version changes are outside this ledger.

## Reviewed SDK heads

| Client | Commit |
| --- | --- |
| TypeScript | `c432688f988ed3938bf61808f607893cb6f2bd7e` |
| Go | `69d4786a484e0adb0b2c0c7146849578e8af5829` |
| Rust | `4a5345f2e4f6ef04b81e4d7fa35a67a1dfcedda2` |
| .NET | `b41d70ab242638a472a5d6dba83eb9a46a3e9efe` |

These hashes identify the immutable inputs to the ledger. Hosted CI status is
verified against each exact hash rather than inferred from the branch name.
