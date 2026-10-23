# Fitz Wire Format 1.0 Design

Status: **decided for the 0.3.0 milestone** (decisions in section 6). This document defines the wire
format Fitz intends to ship in 0.3.0 and then freeze as the 1.0.0 wire format.
Everything after 0.3.0 must be an additive, capability-gated extension of what
is written here. The companion ledger of every breaking change gathered for
the release is [release-0.3.0-breaking-changes.md](release-0.3.0-breaking-changes.md).

The domain meanings in [domain-boundaries-spec.md](domain-boundaries-spec.md)
and the constraints in [architectural-laws.md](architectural-laws.md) do not
change. This is a transport and envelope redesign, not a semantics change.

## 1. Goals

1. **One format to live with.** 1.0.0 is 0.3.0 plus additive capabilities.
   Nothing in this document may need a second breaking release to fix.
2. **Every response is machine-decidable.** A client decides success, which
   condition, and whether to retry from fixed fields, never from message text.
3. **Negotiated, not assumed.** The client states what it speaks; the broker
   answers with the version, capabilities, and limits in force. No silent
   success, no "close the socket within one second" as an error signal.
4. **Every request is correlated.** No "one in-flight request per message
   type" rule, no optional label record, no per-verb exceptions.
5. **Limits fit real workloads.** No 64 KiB ceiling on a single value.
6. **Keep what is not broken.** Verb identifiers stay append-only ABI. Domain
   payload field layouts stay unless a row in section 3 names a defect.

Non-goals: the admin HTTP API, the Midge storage format (tracked in the
ledger), new domains, compression, and transport-level encryption.

## 2. What is wrong with wire format 1

Every row is a defect observed in the current broker or specification, with
the evidence that a reviewer can check.

| Area | Today | Defect |
| --- | --- | --- |
| Framing | TLV records: 1-or-3-byte type, `u16` length, value. One domain record per frame plus optional `CORRELATE`. | A value is capped at 65,535 bytes. `ERR_READ_RESPONSE_TOO_LARGE` (2013) exists only because of that cap. Frame size (1 MiB) is undiscoverable. Multi-record frames with no skip-unknown rule make every client parser special. |
| CONNECT | Raw JWT bytes, no length prefix, no version, no capabilities. | The spec says success is silent while the broker actually sends `SERVER_HELLO`. Failure is a socket close within one second, which clients must infer. The client cannot say what it speaks. |
| Correlation | Optional `CORRELATE` record gated on `CAP_CORRELATION`. | Without it, at most one request of a type may be in flight. With it, the record must precede the request in the same frame and the broker closes the connection on any misplacement. |
| Response envelope | Three layouts: uncoded `[1][string]`, coded `[1][u32 code][string]`, and Stream generation 2 `[2][u32 code][string]`. | Schedule CREATE, CANCEL, SUBSCRIBE, UNSUBSCRIBE, CREATE_BATCH, and LIST_V2 are uncoded; a client tells layouts apart by total length. The overview spec documents the uncoded form as the convention while every codec emits the coded form. |
| Error codes | Per-domain 1000-blocks with inconsistent slots. | Unauthorized is 1011 in KV and x009 everywhere else. Backpressure is `BUSY` (1014, 2014, 3006), `QUEUE_FULL` (4005, and 5007 with a different meaning), and `RPC_BACKPRESSURE` (6003). Schedule 7010 `BACKEND_ERROR` is retryable; every other `BACKEND_ERROR` is fatal. No field says whether a request may have taken effect. |
| Message text | Clients must parse messages for Schedule, and the broker classifies some of its own errors by wording. | Any reword is a silent contract change. Notice keeps 3005 for malformed wildcards only because the spec froze it. |
| Backpressure | The spec says KV, Stream, Notice, and Lease close the connection when buffers overflow. | The broker actually answers `ERR_BUSY`. The spec and the code disagree, and closing a connection destroys unrelated in-flight work. |
| Optional fields | `[u8 present]` prefix, plus capability-gated trailing bytes (KV SCAN `start_exclusive`). | A trailing byte whose presence depends on a handshake bit cannot be validated by length and is easy to get wrong in five SDKs. |
| Verbs | `LIST` and `LIST_V2`; `CREATE` and `CREATE_BATCH`; `ENQUEUE_BATCH` reserved since 0.1; Lease `RENEW` in the spec, `EXTEND` in the laws. | Version suffixes on verbs mean the envelope could not evolve. Naming drift between documents. |
| Session metadata | A separate `SESSION_METADATA` record after the hello. | A second handshake step for one string. |

## 3. The format

### 3.1 Message header

A transport frame (one WebSocket binary message, or one `u32 BE` length-prefixed
TCP frame) carries exactly one message:

```
[u16 BE] type            verb or control identifier
[u8]     flags           bit 0 ACK_REQUESTED; bits 1..7 reserved, must be 0
[u64 BE] correlation     non-zero on requests and their responses; 0 on notifications and control
[u32 BE] length          byte count of value
[bytes]  value           exactly `length` bytes
```

Decisions behind the header:

- A fixed 15-byte header replaces variable-width type encoding and
  multi-record frames. Every language parses it with one read.
- Correlation is a header field, not a label record. The broker echoes it on
  every frame that answers the request, including broker-synthesized errors
  and deferred answers (a parked Queue `RESERVE`, a contended Lease `ACQUIRE`).
  A correlation may receive more than one frame; a domain defines when the
  sequence ends. Zero is reserved for frames that answer nothing.
- `length` is `u32`. The negotiated `max_message_bytes` bounds it, not the
  field width.
- Reserved flag bits must be zero. A set reserved bit is a protocol error
  answered with `UNSUPPORTED`, not a connection close. A later version may
  assign a reserved bit (for example compression or body continuation) only
  behind a capability bit, so a 1.0 client never sees one it did not negotiate.

### 3.2 Handshake

The first message on a connection must be `CONNECT` (type 1). The broker
answers every `CONNECT` with exactly one of `HELLO` (type 2) or
`CONNECT_REJECTED` (type 3), then either serves the session or closes.

```
CONNECT value:
[u16 BE] min_version      lowest wire version the client speaks (2)
[u16 BE] max_version      highest wire version the client speaks
[u32 BE] client_caps      capability bits the client implements
[string] jwt              compact JWT; empty in anonymous mode
[string] service_name     client-reported service name, may be empty
[string] client_build     SDK name and version for diagnostics, may be empty

HELLO value (readers ignore trailing bytes):
[u16 BE] version          the version both sides will use
[u32 BE] broker_caps      capability bits the broker implements
[u32 BE] max_message_bytes
[u32 BE] max_in_flight    requests the broker will hold per connection before BUSY
[u64 BE] session_id       diagnostic identity shown in admin surfaces

CONNECT_REJECTED value:
[u16 BE] code             section 3.4, protocol block
[string] message
```

The version in force is the highest value in both ranges; no overlap is a
`CONNECT_REJECTED` with `UNSUPPORTED_VERSION`. Capabilities are the bitwise
AND of the two masks. A client must not use a capability the broker did not
advertise. `HELLO` is the only message whose readers tolerate trailing bytes,
so later versions can append limits without a new type. `HELLO` does not carry
the route family: `route_family` is broker-internal and stays orthogonal to
the application-visible `realm`.

The broker may send one server control message at any time after `HELLO`:

```
GOAWAY (type 5) value, correlation 0:
[u16 BE] code             section 3.4, protocol block (DRAINING, FORCED_CLOSE, LIMIT_CHANGED)
[u32 BE] grace_ms         time the broker will keep serving before it closes; 0 = closing now
[string] reason
```

`GOAWAY` makes planned drain observable. Today a client learns of a drain only
when its socket closes. A client that receives `DRAINING` should finish
in-flight work and reconnect elsewhere within `grace_ms`.

### 3.3 Response envelope

Every response to every verb in every domain:

```
[u8] status     0 = success, 1 = error
success:  domain payload as specified per verb
error:
  [u16 BE] code       section 3.4; the specific condition
  [u8]     class      section 3.4; 0 = domain-specific, 1..10 = the shared condition
  [u8]     effect     0 NOT_APPLIED, 1 INDETERMINATE, 2 APPLIED
  [string] message    diagnostic only; clients must not branch on it
```

`class` is explicit so an SDK never derives it by arithmetic from `code`, and
so a domain-specific code can still declare that it is, say, a `NOT_FOUND`.
`effect` is the field the current format lacks. It answers the only question a
retry policy needs: `NOT_APPLIED` means re-sending is safe; `INDETERMINATE`
means the broker accepted the request and cannot say whether it took effect;
`APPLIED` means the operation happened and the error describes a follow-on
failure (for example a KV commit that persisted but whose inventory estimate
did not). Retryability is derived by the client from `effect` plus the code
class, never from the code number alone.

### 3.4 Error codes

Codes stay decimal and domain-blocked for familiarity, because every SDK,
spec page, dashboard, and support ticket already uses them. The envelope's
`class` byte carries the shared meaning explicitly; the low two digits of each
block mirror it for readability:

| Class | Name | Meaning | Typical `effect` |
| --- | --- | --- | --- |
| x01 | UNAUTHORIZED | Permission denied for this route and verb | NOT_APPLIED |
| x02 | INVALID_ROUTE | Route shape, scheme, or family mismatch | NOT_APPLIED |
| x03 | INVALID_PATTERN | Registration pattern grammar | NOT_APPLIED |
| x04 | REGISTRATION_LIMIT | Session registration cap reached | NOT_APPLIED |
| x05 | BUSY | Broker declined to accept the request | NOT_APPLIED |
| x06 | BACKEND | Storage or runtime failure after acceptance | INDETERMINATE |
| x07 | TIMEOUT | Accepted, not finished before its deadline | INDETERMINATE |
| x08 | BAD_REQUEST | Payload decodes but violates the verb contract | NOT_APPLIED |
| x09 | NOT_FOUND | The addressed object does not exist | NOT_APPLIED |
| x10 | UNSUPPORTED | Capability or option not negotiated | NOT_APPLIED |

Blocks: `0xx` protocol and session, `1xx` KV, `2xx` Stream, `3xx` Notice,
`4xx` Queue, `5xx` Lease, `6xx` RPC, `7xx` Schedule, each as thousands
(1001 is KV UNAUTHORIZED, 7005 is Schedule BUSY). Domain-specific conditions
start at x50 so they can never collide with a future shared slot. The complete
allocation, with the old-to-new mapping for every existing code, is the first
deliverable of the ledger and lives in `docs/clients/spec/error-codes.md`.

Codes are append-only. A code is never renamed, reused, or given a new
meaning after 0.3.0. There is no `retry_after` hint: `HELLO` already states
`max_in_flight`, and a `BUSY` with `effect = NOT_APPLIED` plus client backoff
is the whole contract.

### 3.5 Notifications and multi-frame answers

Server-initiated frames (`NOTIFY` in every domain) carry correlation 0 and
begin their value with `[u64 BE] subscription_id`, which is already true today
and stays the routing key. RPC keeps its 16-byte end-to-end request identifier
inside the payload; it survives broker, worker, and broker again, which the
connection-level correlation cannot.

### 3.6 Backpressure

Every domain answers `BUSY` with `effect = NOT_APPLIED` when it declines a
request. The broker never closes a connection to signal overflow. Notice
`PUBLISH` stays fire-and-forget by default; a client that sets
`ACK_REQUESTED` in the header receives a status frame, so it can observe drops
when it needs to. `max_in_flight` from `HELLO` tells the client the admission
limit instead of leaving it to discover 2 ms retry budgets empirically.

### 3.7 Optional fields and extension

- Optional payload fields use `[u8 present]` followed by the field. No field
  is ever gated on a capability bit by trailing presence; a capability changes
  what a flag or option value may be, never the byte layout.
- Payloads are exact-length. Trailing bytes are a `BAD_REQUEST`.
- New behavior arrives as a new verb identifier or a new capability bit, never
  as a reinterpretation of an existing layout. Verb identifiers are never
  reused.

### 3.8 Per-domain changes

| Domain | Change | Why |
| --- | --- | --- |
| All | Single envelope from section 3.3; all errors coded | Removes the uncoded layouts and the length-sniffing rule |
| All | `UNAUTHORIZED`, `BUSY`, `BACKEND`, `TIMEOUT` renumbered to shared slots | One retry table for every SDK |
| Schedule | `LIST` becomes the cursor form that `LIST_V2` is today; `LIST_V2` id retired | One list verb |
| Stream | READ uses the common envelope; generation-2 `status=2` retired; `READ_RESPONSE_TOO_LARGE` means larger than `max_message_bytes` only | One envelope; the 64 KiB reason is gone |
| KV | SCAN carries an explicit options byte with `start_exclusive` as a bit; `CAP_KV_SCAN_EXCLUSIVE` retired | No capability-gated trailing byte |
| RPC | Remaining-budget and cancellation fields are part of the base layout; `CAP_RPC_CANCELLATION` retired | Base protocol, not an option |
| Queue | `ENQUEUE_BATCH` (201) reservation retired; the id is never reused | Nothing asked for it in two releases; batching arrives as a new id if demand appears |
| Lease | `RENEW` is the canonical name in every document; `QUEUE_FULL` (5007) becomes `WAITERS_LIMIT` in the domain block | Naming drift |
| Notice | Malformed-wildcard patterns are `INVALID_PATTERN`; the frozen-3005 exception ends | The wart only existed to avoid a wire change |
| Notice | `ACK_REQUESTED` on PUBLISH is base protocol: the broker answers with the common envelope, success meaning accepted for fan-out and never meaning delivered | The only way a publisher can observe a drop; the spec must say "accepted, not delivered" wherever the flag appears |
| KV, Stream | COMMIT keeps the explicit `[u8 persistence]` selector, 0 Buffered, 1 Sync; it is required | Unchanged; restated because it is the durability contract |

## 4. Compatibility and rollout

Wire format 1 has no client-side version field, so a broker cannot tell a v1
client from a v2 client before the first message is parsed. Two options:

- **Flag day (recommended).** 0.3.0 speaks only wire 2. The migration guide
  states a mixed-version prohibition, as the release policy allows when no
  negotiation exists. All five SDKs ship their 2.0 lines before the broker
  tag. This is simpler to test, and the cost is already paid by the Midge
  storage break landing in the same release.
- **Dual-stack for one release.** The broker sniffs the first frame: a v1
  `CONNECT` begins `0x01 [u16 len]` and a v2 `CONNECT` begins `0x00 0x01
  [flags]`. Bytes 0 and 1 differ, so detection is exact. The cost is two
  codecs, two conformance suites, and a second removal release.

The decision is recorded in section 5. Either way, the sequence is: spec and
golden vectors, broker codecs behind this feature branch, SDK ports validated
by the cross-language conformance runner, migration guide, release gates.

## 5. Resolved decisions

Each item was judged with TypeSafe Jev against explicit alternatives, then
settled by the maintainer. Probabilities are Jev's; the call is ours.

1. **Fixed 15-byte header.** Jev 1.00 for fixed over TLV or varint. Decided.
2. **Hand-written exact-length bodies; no IDL; no body-version prefix.** Jev
   1.00. Jev rated the three-year regret risk "possible" (0.53), so section 8
   states the evolution conventions that keep successors clean.
3. **Error codes: decimal blocks plus an explicit `class` byte.** Jev 0.92
   for the hybrid over derived-by-arithmetic (0.06) or renumbering (0.02).
4. **`effect` with three values, no `retry_after`.** Jev 0.42 for the draft
   versus 0.34 for adding the hint; the narrowed question put the hint at
   0.44. `HELLO` already carries `max_in_flight`. Decided: no hint.
5. **Client version range, broker picks.** Jev 1.00.
6. **Flag day.** Jev 0.97. The Midge break already forces a coordinated
   upgrade; dual-stack would add a second codec and a removal release.
7. **`max_message_bytes` default 1 MiB, configurable, advertised.** Jev 0.88
   once the per-connection bound (`max_in_flight` times the limit) was stated.
8. **Notice `ACK_REQUESTED`, base protocol.** Include at 0.65; base over
   capability at 0.58 with low confidence. Jev rated the Law 1 risk
   "moderate: the docs must fight it" (0.69), so every mention of the flag
   says accepted for fan-out, never delivered.
9. **`HELLO` does not carry the route family.** Jev 0.19 for carrying it.
   Orthogonality wins.
10. **`ENQUEUE_BATCH` reservation retired.** Jev 0.77 once "no demand in two
    releases" was on the table.
11. **Correlation is `u64`.** Jev 0.89.
12. **`GOAWAY` ships in 0.3.0.** Jev 0.66; drain exists today and is
    invisible to clients. `REAUTH` was a coin flip (0.54) and is additive
    later, so it is not in 0.3.0. Compression (0.42) and continuation (0.39)
    bits stay reserved and undefined; both can be assigned later behind a
    capability.

Jev's overall longevity judgment for the amended design: "plausible with the
amendments a careful review would add" (0.83), and the amendments it pointed
at (an explicit class, a server-initiated control message, evolution rules)
are now in sections 3 and 8.

## 6. Decision log

| Date | Decision | By | Note |
| --- | --- | --- | --- |
| 2026-10-23 | Open the 0.3.0 milestone as the wire-format break that becomes 1.0.0 | maintainer | Breaking changes gathered over the following days, including Midge |
| 2026-10-23 | Section 5 items 1 through 12 | maintainer with Jev | Two Jev rounds; second round narrowed the five close calls |

## 7. Work breakdown

1. Section 5 is resolved; any later reversal is a new row in section 6.
2. Write `docs/clients/spec/error-codes.md` with the full allocation and the
   old-to-new mapping; regenerate the per-domain spec pages.
3. Produce golden vectors for every verb under the new framing and envelope;
   the conformance runner consumes them.
4. Broker: new frame codec and handshake, envelope encoder, code table,
   per-domain deltas, each red-to-green against the vectors.
5. SDKs: .NET, TypeScript, Go, Python, Rust, in that order of production
   exposure, each passing the conformance runner.
6. Migration guide, release checklist, support matrix, and the mixed-version
   prohibition.
7. Release gates per `release-policy.md`, then tag 0.3.0.

## 8. Evolution conventions after 0.3.0

These rules are what make "no further breaking change" credible without an
IDL.

1. A verb's payload layout is frozen the day its id ships. A changed layout is
   a new verb id whose name says what changed (`READ_FILTERED`, not
   `READ_V2`). The old id keeps working until a major release retires it.
2. Optional behavior is a capability bit advertised in `HELLO` and echoed in
   `CONNECT`; it changes what a value may contain, never the byte layout.
3. Reserved header flag bits are assigned only behind a capability bit.
4. Error codes and classes are appended, never renamed or reused.
5. No verb id, code, or flag bit is reserved for future use. An unused
   reservation is a promise nobody tests; `ENQUEUE_BATCH` sat unimplemented
   for two releases.
6. Every landed change ships golden vectors to the conformance runner before
   any SDK ports it.
