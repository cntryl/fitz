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
| W-02 | Structured `CONNECT` with version range, capabilities, JWT, service name, client build; `HELLO` or `CONNECT_REJECTED` always answers it | Negotiation and an explicit failure signal | proposed | |
| W-03 | One response envelope: `[status][u16 code][u8 effect][string message]` | Removes three layouts and length sniffing | proposed | |
| W-04 | Shared error slots x01..x10 across every domain block; domain codes from x50; full old-to-new mapping | One retry table for every SDK | spec written: `docs/clients/spec/error-codes.md` | |
| W-05 | Schedule CREATE, CANCEL, SUBSCRIBE, UNSUBSCRIBE, CREATE_BATCH, and LIST errors become coded | Clients stop parsing messages | proposed | |
| W-06 | Schedule `LIST` takes the cursor form; `LIST_V2` (707) retired | One list verb | proposed | |
| W-07 | Stream READ uses the common envelope; generation-2 `status=2` retired | One envelope | proposed | |
| W-08 | KV SCAN options byte with `start_exclusive`; `CAP_KV_SCAN_EXCLUSIVE` retired | No capability-gated trailing byte | proposed | |
| W-09 | RPC budget and cancellation fields in the base layout; `CAP_RPC_CANCELLATION` retired | Base protocol | proposed | |
| W-10 | `BUSY` with `effect=NOT_APPLIED` in every domain; no connection close on overflow | Spec and code agree | proposed | |
| W-11 | Notice `ACK_REQUESTED` header flag for PUBLISH | Observable drops on request | proposed | |
| W-12 | Notice malformed-wildcard patterns become `INVALID_PATTERN` | Ends the frozen-3005 exception | proposed | |
| W-13 | Stream "route family mismatch" becomes `INVALID_ROUTE` instead of `BACKEND` | Client error, not backend failure | proposed | |
| W-14 | `SESSION_METADATA` record folded into `CONNECT` | One handshake step | proposed | |
| W-15 | Lease `RENEW` canonical; `QUEUE_FULL` (5007) renamed `WAITERS_LIMIT` | Naming drift | proposed | |
| W-16 | Queue `ENQUEUE_BATCH` (201) reservation retired; id never reused | Design 5.10 | decided | |
| W-17 | Error envelope carries an explicit `u8 class` alongside the decimal code | Design 5.3 | decided | |
| W-18 | Server control message `GOAWAY {code, grace_ms, reason}` | Design 5.12 | decided | |
| W-19 | No `retry_after` hint; no route family in `HELLO`; `REAUTH` deferred as an additive verb | Design 5.4, 5.9, 5.12 | decided | |

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
| C-01 | `max_message_bytes` becomes a configured, advertised limit; default stays 1 MiB | Design 5.7 | decided | |

## Rust embedding API

| ID | Change | Why | Status | Issue |
| --- | --- | --- | --- | --- |
| R-01 | `StreamClientError` and `NoticeClientError` kinds map to the new code table | Follows W-04 | proposed | |

## Release gates for 0.3.0

- Every `landed` wire row has golden vectors in the conformance runner.
- All five SDKs pass the runner against the 0.3.0 broker.
- Migration guide names the mixed-version rule decided in design 5.2.
- Support matrix and release checklist updated.
