# Five-client operational parity ledger

This ledger is separate from the shared conformance suite and intentionally
scopes TypeScript, Go, Python, Rust, and .NET. A green CS-001–CS-017 result does
not establish the capabilities below.

| Capability | TypeScript | Go | Python | Rust | .NET | Required evidence |
| --- | --- | --- | --- | --- | --- | --- |
| Startup readiness | pass | pass | pass | pass | pass | One-shot connect plus bounded, cancellable readiness retry; auth rejection is terminal |
| Explicit KV durability | pass | pass | pass | pass | pass | Public caller chooses Buffered or Sync and both wire bytes are covered |
| Managed leases | pass | pass | pass | pass | pass | Renewal, callback cancellation, ownership loss, release, and combined failures |
| Safe retry | pass | pass | pass | pass | pass | Replayable reads only; Queue enqueue only after confirmed negative; ambiguous mutations never retry |
| Reconnect defaults | pass | pass | pass | pass | pass | Unlimited default where supported and documented finite configuration |
| Heartbeat | pass | pass | pass | pass | pass | WebSocket ping/pong idle watchdog and TCP socket keepalive without a Fitz heartbeat frame |
| Observability | pass | pass | pass | pass | pass | Shared lifecycle names and request/error/state/subscription telemetry hooks |
| Error ergonomics | pass | pass | pass | pass | pass | Language-native typed errors or sentinels with wrapping support |
| Documentation truth | pass | pass | pass | pass | pass | Public examples compile against default APIs and migration notes describe breaks |

Status is accepted only with a green native repository gate and exact-head CI.
Package publication, tags, and version changes are outside this ledger.

## Reviewed SDK heads

| Client | Commit |
| --- | --- |
| TypeScript | `8c88483e948e466c289c658479087a83e845d214` |
| Go | `4453e808c3e4b0e2ec2213fd4b515db0fd60dc60` |
| Python | `fe877bbeac4843591b5a0115c8e4a04743febcce` |
| Rust | `bfba39ead69795448ee20985b1281543467ffad8` |
| .NET | `82de7f605d836cfcd55ace1f7a36398ad2ad3cc8` |

These hashes identify the immutable inputs to the ledger. Hosted CI status is
verified against each exact hash rather than inferred from the branch name.

The Python head is the clean-break 0.2 client. Its repository-owned CI passed
Python 3.11–3.13, wheel smoke installation, and CS-001–CS-017 over TCP and
WebSocket with anonymous and valid-JWT authentication.
