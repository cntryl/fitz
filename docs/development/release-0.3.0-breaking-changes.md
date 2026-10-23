# Fitz 0.3.0 Breaking Changes Ledger

Status: **gathering**. Tracking issue: [#497](https://github.com/cntryl/fitz/issues/497), milestone `v0.3.0`. Every change that breaks a client, a stored format, an
operator surface, or the Rust embedding API lands here before it lands in code.
A row moves to `landed` only when its PR merges to `feature/0.3.0-wire-format`
with tests and a migration-guide entry. Rows are never deleted; a rejected
change is marked `rejected` with the reason.

Wire rows follow [wire-format-1.0-design.md](wire-format-1.0-design.md).
Storage rows follow the Midge work tracked in that repository and the
[format-compatibility](format-compatibility.md) contract.

## Wire

| ID | Change | Why | Status | Issue |
| --- | --- | --- | --- | --- |
| W-01 | Fixed 15-byte message header replaces TLV records and `CORRELATE`/`CORRELATED` | One parser, mandatory correlation, `u32` length | landing: broker decodes and encodes the header on both transports; a zero correlation on a request is still tolerated until every SDK sends one; `max_message_bytes` is a 1 MiB constant until C-01 | |
| W-02 | Structured `CONNECT` with version range, capabilities, JWT, service name, client build; `HELLO` or `CONNECT_REJECTED` always answers it | Negotiation and an explicit failure signal | landing: broker parses CONNECT, negotiates the version, answers HELLO with version, capabilities, limits, and session id, and answers CONNECT_REJECTED with a protocol-block code before closing; in anonymous mode a non-CONNECT first frame still opens the session implicitly | |
| W-03 | One response envelope: `[status][u16 code][u8 effect][string message]` | Removes three layouts and length sniffing | landed: every domain encodes `[1][u16 code][u8 class][u8 effect][string]`; Stream status 2 and the Schedule and Queue uncoded layouts are gone | |
| W-04 | Shared error slots x01..x10 across every domain block; domain codes from x50; full old-to-new mapping | One retry table for every SDK | landed in code: `src/protocol/error_codes.rs` carries the catalog numbering; `WIRE1_TO_WIRE2` in the catalog maps every old code | |
| W-05 | Schedule CREATE, CANCEL, SUBSCRIBE, UNSUBSCRIBE, CREATE_BATCH, and LIST errors become coded | Clients stop parsing messages | landed: every Schedule verb returns the coded envelope | |
| W-06 | Schedule `LIST` takes the cursor form; `LIST_V2` (707) retired | One list verb | landed | 702 accepts an empty payload for the first page; 707 is unassigned and never reused |
| W-07 | Stream READ uses the common envelope; generation-2 `status=2` retired | One envelope | landed: READ uses the common envelope | |
| W-08 | KV SCAN options byte with `start_exclusive`; `CAP_KV_SCAN_EXCLUSIVE` retired | No capability-gated trailing byte | landed | `[u8 options]` replaces `[u8 reverse][u8 start_exclusive?]`; bit 0 reverse, bit 1 start_exclusive, reserved bits rejected |
| W-09 | RPC budget and cancellation fields in the base layout; `CAP_RPC_CANCELLATION` retired | Base protocol | landed | SUBSCRIBE_WORKER ends with `[u8 flags]`; REQUEST ends with `[u8 has_budget][u32?]`; every delivery carries the broker's remaining budget |
| W-10 | `BUSY` with `effect=NOT_APPLIED` in every domain; no connection close on overflow | Spec and code agree | landed for codes: `BUSY` is `x005` in every block with `effect = NOT_APPLIED`; connection-close-on-overflow was already gone | |
| W-11 | Notice `ACK_REQUESTED` header flag for PUBLISH | Observable drops on request | landed | Success means accepted for fan-out, never delivered; the flag forces the replying actor path |
| W-12 | Notice malformed-wildcard patterns become `INVALID_PATTERN` | Ends the frozen-3005 exception | landed: every pattern the compiler rejects is `INVALID_PATTERN` (3003) | |
| W-13 | Stream "route family mismatch" becomes `INVALID_ROUTE` instead of `BACKEND` | Client error, not backend failure | landed: `INVALID_ROUTE` (2002) | |
| W-14 | `SESSION_METADATA` record folded into `CONNECT` | One handshake step | landed: the service name travels in CONNECT; the record, its capability bit, and `CORRELATE`/`CORRELATED`/`SERVER_HELLO` are gone | |
| W-15 | Lease `RENEW` canonical; `QUEUE_FULL` (5007) renamed `WAITERS_LIMIT` | Naming drift | landed: `WAITERS_LIMIT` is 5056; `RENEW` wording follows in the spec pass | |
| W-16 | Queue `ENQUEUE_BATCH` (201) reservation retired; id never reused | Design 5.10 | landed | Docs only: the broker never implemented 201 |
| W-17 | Error envelope carries an explicit `u8 class` alongside the decimal code | Design 5.3 | landed: the envelope carries the explicit `class` byte | |
| W-18 | Server control message `GOAWAY {code, grace_ms, reason}` | Design 5.12 | landed | `Runtime::begin_drain` sends GOAWAY DRAINING with the grace and close reason to every live session; FORCED_CLOSE and LIMIT_CHANGED are defined but not yet emitted |
| W-19 | No `retry_after` hint; no route family in `HELLO`; `REAUTH` deferred as an additive verb | Design 5.4, 5.9, 5.12 | decided | |
| W-20 | Stream `MAX_EVENT_SIZE` (61,247 bytes) still derives from the wire-1 64 KiB value cap; raise it to track `max_message_bytes` | Follows W-01, C-01 | proposed | Additive for clients; `READ_RESPONSE_TOO_LARGE` already means larger than `max_message_bytes` |

## Storage (Midge)

| ID | Change | Why | Status | Issue |
| --- | --- | --- | --- | --- |
| S-01 | Midge format changes in progress | gathered over the next days | gathering | |

## Admin HTTP API

| ID | Change | Why | Status | Issue |
| --- | --- | --- | --- | --- |
| A-01 | None proposed yet | | | |

## Configuration and operations

| ID | Change | Why | Status | Issue |
| --- | --- | --- | --- | --- |
| C-01 | `max_message_bytes` becomes a configured, advertised limit; default stays 1 MiB | Design 5.7 | landed | `FITZ_MAX_MESSAGE_BYTES` (64 KiB to 4 GiB, default 1 MiB) bounds the transport, the outbound sink, and the HELLO advertisement |

## Rust embedding API

| ID | Change | Why | Status | Issue |
| --- | --- | --- | --- | --- |
| R-01 | `StreamClientError` and `NoticeClientError` kinds map to the new code table | Follows W-04 | proposed | |

## Release gates for 0.3.0

- Every `landed` wire row has golden vectors in the conformance runner.
- All five SDKs pass the runner against the 0.3.0 broker.
- Migration guide names the mixed-version rule decided in design 5.2.
- Support matrix and release checklist updated.
